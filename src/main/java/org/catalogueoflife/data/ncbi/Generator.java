package org.catalogueoflife.data.ncbi;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * NCBI Taxonomy generator.
 *
 * Converts the NCBI new_taxdump archive to ColDP format.
 * Source: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.zip
 * Format docs: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/taxdump_readme.txt
 *
 * Files used:
 *   nodes.dmp         – taxonomy nodes (tax_id, parent, rank, division, comments …)
 *   names.dmp         – name records with type classification
 *   citations.dmp     – literature references linked to taxa
 *   typematerial.dmp  – type specimen assignments
 *   division.dmp      – division codes (BCT, PLN, VRT …) → nomenclatural code
 *   images.dmp        – organism images (url, license, attribution, source, taxid_list)
 *
 * Processing order:
 *   1. division.dmp   → build divCodes lookup
 *   2. names.dmp      → build sciNames / authorities / synonyms maps; write VernacularName
 *   3. nodes.dmp      → write NameUsage (accepted taxa)
 *   4. synonyms map   → write synonym NameUsage rows
 *   5. citations.dmp  → write Reference
 *   6. typematerial.dmp → write TypeMaterial
 *   7. images.dmp     → write Media
 */
public class Generator extends AbstractColdpGenerator {

  private static final URI    DOWNLOAD_URI  = URI.create(
      "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.zip");
  private static final String ZIP_FN        = "new_taxdump.zip";
  private static final String TAXON_URL     = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=";
  private static final String PUBMED_URL    = "https://pubmed.ncbi.nlm.nih.gov/";

  // NCBI field delimiter is TAB-PIPE-TAB; lines terminate with TAB-PIPE
  static final Pattern SPLITTER = Pattern.compile("\t\\|\t?");

  // Extracts a URL from a license string like "CC BY-SA 3.0 (https://...)"
  private static final Pattern LICENSE_URL_PAT = Pattern.compile("\\(([^)]+)\\)\\s*$");

  /** Names.dmp name classes that we emit as synonym NameUsage rows. */
  private static final Set<String> SYNONYM_CLASSES = Set.of(
      "synonym",
      "equivalent name",
      "genbank synonym"
  );

  /** Names.dmp name classes that we emit as VernacularName rows. */
  private static final Set<String> VERNACULAR_CLASSES = Set.of(
      "common name",
      "genbank common name",
      "blast name"
  );

  // ── In-memory lookups ──────────────────────────────────────────────────────
  /** division_id → GenBank division code (e.g. "BCT", "PLN") */
  private final Map<Integer, String> divCodes  = new HashMap<>();
  /** tax_id → scientific name text */
  private final Map<Integer, String> sciNames  = new HashMap<>(3_000_000);
  /** tax_id → all authority strings for that taxon (there may be several for different synonymous names) */
  private final Map<Integer, List<String>> authorities = new HashMap<>(2_000_000);
  /** tax_id (of accepted taxon) → list of synonym name strings */
  private final Map<Integer, List<String>> synonyms = new HashMap<>(500_000);

  private String version;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  @Override
  protected void prepare() throws Exception {
    // Detect version from Last-Modified header on the download URL
    try {
      var resp = http.head(DOWNLOAD_URI.toString());
      String lastMod = resp.headers().firstValue("Last-Modified").orElse(null);
      if (lastMod != null) {
        version = ZonedDateTime.parse(lastMod, DateTimeFormatter.RFC_1123_DATE_TIME)
            .toLocalDate().toString();
        LOG.info("NCBI version from Last-Modified: {}", version);
      }
    } catch (Exception e) {
      LOG.warn("Could not read Last-Modified from {}: {}", DOWNLOAD_URI, e.getMessage());
    }
    if (version == null) {
      version = LocalDate.now().toString();
      LOG.info("NCBI version defaulting to today: {}", version);
    }
  }

  @Override
  protected void addData() throws Exception {
    File zipFile = download(ZIP_FN, DOWNLOAD_URI);
    extractDmpFiles(zipFile);

    // ── Writers ────────────────────────────────────────────────────────────
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.code,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    TermWriter vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.name,
        ColdpTerm.remarks
    ));
    TermWriter typeMatWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.nameID,
        ColdpTerm.status,
        ColdpTerm.citation
    ));
    TermWriter mediaWriter = additionalWriter(ColdpTerm.Media, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.url,
        ColdpTerm.type,
        ColdpTerm.title,
        ColdpTerm.creator,
        ColdpTerm.license,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    initRefWriter(List.of(
        ColdpTerm.ID,
        ColdpTerm.citation,
        ColdpTerm.link
    ));

    // ── Step 1: division.dmp → divCodes ───────────────────────────────────
    loadDivisions();

    // ── Step 2: names.dmp pass 1 → sciNames / authorities / synonyms + VernacularName ──
    LOG.info("Processing names.dmp (pass 1: collecting names, synonyms, vernaculars)…");
    int nVern = 0;
    try (BufferedReader br = reader(sourceFile("names.dmp"))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] row = split(line);
        if (row == null) continue;
        int    taxId    = parseInt(row, 0);
        String name     = col(row, 1);
        String nameClass = col(row, 3);
        if (name == null || nameClass == null) continue;

        switch (nameClass) {
          case "scientific name" -> sciNames.put(taxId, name);
          case "authority"       -> authorities.computeIfAbsent(taxId, k -> new ArrayList<>()).add(name);
          case "common name", "genbank common name", "blast name" -> {
            vernWriter.set(ColdpTerm.taxonID, taxId);
            vernWriter.set(ColdpTerm.name,    name);
            // annotate non-standard vernacular types
            if (!"common name".equals(nameClass)) {
              vernWriter.set(ColdpTerm.remarks, nameClass);
            }
            vernWriter.next();
            nVern++;
          }
          default -> {
            if (SYNONYM_CLASSES.contains(nameClass)) {
              synonyms.computeIfAbsent(taxId, k -> new ArrayList<>()).add(name);
            }
          }
        }
      }
    }
    LOG.info("Loaded {} scientific names, {} taxa with authority strings, {} synonym groups, {} vernacular names",
        sciNames.size(), authorities.size(), synonyms.size(), nVern);

    // ── Step 3: nodes.dmp → NameUsage (accepted) ──────────────────────────
    LOG.info("Processing nodes.dmp…");
    int nNodes = 0;
    try (BufferedReader br = reader(sourceFile("nodes.dmp"))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] row = split(line);
        if (row == null) continue;
        int    taxId   = parseInt(row, 0);
        int    parentId = parseInt(row, 1);
        String rank    = col(row, 2);
        int    divId   = parseInt(row, 4);
        String comments = col(row, 12);

        String sciName = sciNames.get(taxId);
        if (sciName == null) {
          LOG.warn("No scientific name for taxId {}, skipping", taxId);
          continue;
        }
        List<String> authList = authorities.get(taxId);
        String authorship = extractAuthorship(sciName, authList);

        writer.set(ColdpTerm.ID,     taxId);
        // root node has parent == self; omit self-referential parentID
        if (parentId != taxId && parentId != 0) {
          writer.set(ColdpTerm.parentID, parentId);
        }
        writer.set(ColdpTerm.status, "accepted");
        if (rank != null && !"no rank".equals(rank)) {
          writer.set(ColdpTerm.rank, rank);
        }
        writer.set(ColdpTerm.scientificName, sciName);
        if (authorship != null) {
          writer.set(ColdpTerm.authorship, authorship);
        }
        String nomCode = divToNomCode(divId);
        if (nomCode != null) {
          writer.set(ColdpTerm.code, nomCode);
        }
        writer.set(ColdpTerm.link, TAXON_URL + taxId);
        if (!StringUtils.isBlank(comments)) {
          writer.set(ColdpTerm.remarks, comments);
        }
        writer.next();

        if (++nNodes % 100_000 == 0) {
          LOG.info("  … {} taxa written", nNodes);
        }
      }
    }
    LOG.info("Written {} accepted NameUsage records", nNodes);

    // ── Step 4: synonyms map → NameUsage (synonyms) ───────────────────────
    LOG.info("Writing synonym NameUsage records…");
    int nSyn = 0;
    for (Map.Entry<Integer, List<String>> e : synonyms.entrySet()) {
      int acceptedId = e.getKey();
      int n = 1;
      for (String synName : e.getValue()) {
        writer.set(ColdpTerm.ID,             acceptedId + "-s" + n++);
        writer.set(ColdpTerm.parentID,       acceptedId);
        writer.set(ColdpTerm.status,         "synonym");
        writer.set(ColdpTerm.scientificName, synName);
        writer.next();
        nSyn++;
      }
    }
    LOG.info("Written {} synonym NameUsage records", nSyn);
    synonyms.clear();  // release memory

    // ── Step 5: citations.dmp → Reference ─────────────────────────────────
    LOG.info("Processing citations.dmp…");
    int nRefs = 0;
    try (BufferedReader br = reader(sourceFile("citations.dmp"))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] row = split(line);
        if (row == null) continue;
        String citId   = col(row, 0);
        String pubmedId = col(row, 3);
        String url     = col(row, 4);
        String text    = col(row, 5);

        // unescape \" and \\
        if (text != null) {
          text = text.replace("\\\\", "\\").replace("\\\"", "\"");
        }

        boolean hasPubmed = pubmedId != null && !"0".equals(pubmedId);
        boolean hasUrl    = !StringUtils.isBlank(url);
        boolean hasText   = !StringUtils.isBlank(text);

        if (!hasText && !hasUrl && !hasPubmed) continue;

        refWriter.set(ColdpTerm.ID, "ref:" + citId);
        if (hasText) {
          refWriter.set(ColdpTerm.citation, text);
        }
        if (hasPubmed) {
          refWriter.set(ColdpTerm.link, PUBMED_URL + pubmedId);
        } else if (hasUrl) {
          refWriter.set(ColdpTerm.link, url);
        }
        nextRef();
        nRefs++;
      }
    }
    LOG.info("Written {} Reference records", nRefs);

    // ── Step 6: typematerial.dmp → TypeMaterial ───────────────────────────
    LOG.info("Processing typematerial.dmp…");
    int nTypes = 0;
    try (BufferedReader br = reader(sourceFile("typematerial.dmp"))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] row = split(line);
        if (row == null) continue;
        String taxId      = col(row, 0);
        String typeStatus = col(row, 2);   // holotype, paratype, …
        String identifier = col(row, 3);   // specimen catalogue number / voucher

        if (StringUtils.isBlank(identifier)) continue;

        typeMatWriter.set(ColdpTerm.nameID,  taxId);
        typeMatWriter.set(ColdpTerm.status,  typeStatus);
        typeMatWriter.set(ColdpTerm.citation, identifier);
        typeMatWriter.next();
        nTypes++;
      }
    }
    LOG.info("Written {} TypeMaterial records", nTypes);

    // ── Step 7: images.dmp → Media ────────────────────────────────────────────
    LOG.info("Processing images.dmp…");
    int nMedia = 0;
    File imagesFile = sourceFile("images.dmp");
    if (imagesFile.exists()) {
      try (BufferedReader br = reader(imagesFile)) {
        for (String line = br.readLine(); line != null; line = br.readLine()) {
          String[] row = split(line);
          if (row == null) continue;
          // col 1: image_key like "image:Homo sapiens"  → strip "image:" prefix for title
          // col 2: url
          // col 3: license string (may contain URL in parentheses)
          // col 4: attribution / creator
          // col 5: source (Wikimedia Commons, iNaturalist, …)
          // col 7: taxid_list – space-separated tax_ids
          String imageKey  = col(row, 1);
          String url       = col(row, 2);
          String licenseRaw = col(row, 3);
          String creator   = col(row, 4);
          String source    = col(row, 5);
          String taxIdList = col(row, 7);

          if (url == null || taxIdList == null) continue;

          String title = imageKey != null && imageKey.startsWith("image:")
              ? imageKey.substring(6).trim() : imageKey;
          String license = extractLicenseUrl(licenseRaw);

          for (String taxIdStr : taxIdList.trim().split("\\s+")) {
            if (taxIdStr.isEmpty()) continue;
            mediaWriter.set(ColdpTerm.taxonID, taxIdStr);
            mediaWriter.set(ColdpTerm.url,     url);
            mediaWriter.set(ColdpTerm.type,    "image");
            if (!StringUtils.isBlank(title))   mediaWriter.set(ColdpTerm.title,   title);
            if (!StringUtils.isBlank(creator)) mediaWriter.set(ColdpTerm.creator, creator);
            if (!StringUtils.isBlank(license)) mediaWriter.set(ColdpTerm.license, license);
            if (!StringUtils.isBlank(source))  mediaWriter.set(ColdpTerm.remarks, source);
            mediaWriter.next();
            nMedia++;
          }
        }
      }
    } else {
      LOG.warn("images.dmp not found — no Media records written");
    }
    LOG.info("Written {} Media records", nMedia);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued",  version);
    super.addMetadata();
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  /**
   * Loads division.dmp into {@link #divCodes}: division_id → GenBank code (BCT, PLN, …).
   */
  private void loadDivisions() throws IOException {
    try (BufferedReader br = reader(sourceFile("division.dmp"))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] row = split(line);
        if (row == null || row.length < 2) continue;
        int    id   = parseInt(row, 0);
        String code = col(row, 1);
        if (code != null) divCodes.put(id, code);
      }
    }
    LOG.info("Loaded {} division codes: {}", divCodes.size(), divCodes);
  }

  /**
   * Extracts the .dmp files needed for processing from the downloaded ZIP archive.
   * Already-extracted files are reused without re-reading the ZIP.
   */
  private void extractDmpFiles(File zipFile) throws IOException {
    String[] needed = {
        "nodes.dmp", "names.dmp", "citations.dmp", "typematerial.dmp", "division.dmp", "images.dmp"
    };
    Set<String> missing = new HashSet<>();
    for (String fn : needed) {
      if (!sourceFile(fn).exists()) missing.add(fn);
    }
    if (missing.isEmpty()) {
      LOG.info("All .dmp files already extracted under {}", sources);
      return;
    }
    LOG.info("Extracting {} from {} …", missing, zipFile.getName());
    try (ZipFile zip = new ZipFile(zipFile)) {
      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements() && !missing.isEmpty()) {
        ZipEntry entry = entries.nextElement();
        // Entry names may include a path prefix; match on the bare filename
        String entryName = new File(entry.getName()).getName();
        if (!missing.contains(entryName)) continue;
        File dest = sourceFile(entryName);
        LOG.info("  Extracting {} ({} bytes) …", entryName,
            entry.getSize() < 0 ? "?" : entry.getSize());
        try (InputStream in  = zip.getInputStream(entry);
             OutputStream out = new FileOutputStream(dest)) {
          in.transferTo(out);
        }
        missing.remove(entryName);
      }
    }
    if (!missing.isEmpty()) {
      LOG.warn("Could not find in ZIP: {}", missing);
    }
  }

  /**
   * Splits an NCBI .dmp line on the {@code \t|\t} delimiter, stripping the
   * trailing {@code \t|} record terminator. Returns {@code null} for blank lines.
   */
  static String[] split(String line) {
    if (line == null || line.isBlank()) return null;
    // Strip trailing record terminator \t| (may or may not have a trailing newline already stripped)
    String s = line;
    if (s.endsWith("\t|")) s = s.substring(0, s.length() - 2);
    else if (s.endsWith("|"))  s = s.substring(0, s.length() - 1);
    return SPLITTER.split(s, -1);
  }

  /**
   * Returns the trimmed value at column {@code i}, or {@code null} if absent or blank.
   */
  static String col(String[] row, int i) {
    if (row == null || i >= row.length) return null;
    String v = row[i].trim();
    return v.isEmpty() ? null : v;
  }

  /**
   * Parses the integer at column {@code i}, returning 0 for absent/unparseable values.
   */
  static int parseInt(String[] row, int i) {
    String v = col(row, i);
    if (v == null) return 0;
    try {
      return Integer.parseInt(v);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  static BufferedReader reader(File f) throws IOException {
    return new BufferedReader(
        new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8));
  }

  /**
   * Extracts the authorship from an NCBI "authority" name string by stripping
   * the leading scientific name.
   *
   * <p>Example: sciName = "Quercus robur", authority = "Quercus robur L., 1753"
   * → returns "L., 1753"
   *
   * <p>If the authority string does not start with the scientific name (unusual),
   * the full authority string is returned as-is.
   */
  /**
   * Picks the authorship from a list of NCBI "authority" strings that applies to {@code sciName}.
   * Each string may start with a quoted name indicating which taxon name the authorship belongs to;
   * entries for other names are skipped so a synonym's authorship is not misapplied to the accepted name.
   */
  static String extractAuthorship(String sciName, List<String> authorities) {
    if (authorities == null || sciName == null) return null;
    for (String authority : authorities) {
      String trimmed = authority.trim();
      if (trimmed.startsWith("\"")) {
        int close = trimmed.indexOf('"', 1);
        if (close > 1) {
          String quotedName = trimmed.substring(1, close);
          if (!quotedName.equals(sciName)) continue;
          String rest = trimmed.substring(close + 1).trim();
          return rest.isEmpty() ? null : rest;
        }
      } else if (trimmed.startsWith(sciName)) {
        String rest = trimmed.substring(sciName.length()).trim();
        return rest.isEmpty() ? null : rest;
      }
    }
    return null;
  }

  /**
   * Extracts the canonical license URI from the NCBI license string.
   * E.g. "CC BY-SA 3.0 (https://creativecommons.org/licenses/by-sa/3.0/)" → "https://..."
   * Falls back to the full string if no parenthesised URL is found.
   */
  static String extractLicenseUrl(String license) {
    if (license == null) return null;
    Matcher m = LICENSE_URL_PAT.matcher(license.trim());
    if (m.find()) {
      String url = m.group(1).trim();
      if (url.startsWith("http")) return url;
    }
    return license.trim();
  }

  /**
   * Maps an NCBI division ID to a ColDP nomenclatural code acronym, or {@code null}
   * if the division does not map to a recognised code.
   */
  String divToNomCode(int divisionId) {
    String code = divCodes.get(divisionId);
    if (code == null) return null;
    return switch (code) {
      case "BCT"                               -> NomCode.BACTERIAL.getAcronym();
      case "VRL", "PHG"                        -> NomCode.VIRUS.getAcronym();
      case "PLN"                               -> NomCode.BOTANICAL.getAcronym();
      case "INV", "MAM", "PRI", "ROD", "VRT"  -> NomCode.ZOOLOGICAL.getAcronym();
      default                                  -> null;
    };
  }
}
