package org.catalogueoflife.data.itis;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;

import java.io.*;
import java.net.URI;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * ITIS (Integrated Taxonomic Information System) generator.
 * https://www.itis.gov/
 *
 * Downloads the ITIS SQLite database dump and converts it to ColDP format.
 * The version is detected from the Last-Modified HTTP header of the download;
 * the TSN (Taxonomic Serial Number) is used directly as NameUsage.ID.
 *
 * Key ITIS tables used:
 *   taxonomic_units  — core taxa (tsn, complete_name, name_usage, parent_tsn, rank_id, kingdom_id, taxon_author_id)
 *   synonym_links    — tsn (invalid) → tsn_accepted (valid)
 *   taxon_unit_types — rank_id → rank_name
 *   kingdoms         — kingdom_id → kingdom_name
 *   taxon_authors_lkp — taxon_author_id → taxon_author string
 *   vernaculars      — tsn, vernacular_name, language
 *   geographic_div   — tsn, geographic_value  (global coarse ranges)
 *   jurisdiction     — tsn, jurisdiction_value, origin  (North American jurisdictions)
 */
public class Generator extends AbstractColdpGenerator {

  private static final URI    DOWNLOAD_URI = URI.create("https://www.itis.gov/downloads/itisSqlite.zip");
  private static final String ZIP_FN       = "itisSqlite.zip";
  private static final String DB_FN        = "itis.sqlite";

  // name_usage values for accepted names
  private static final Set<String> ACCEPTED_USAGES = Set.of("accepted", "valid");

  private String version;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void prepare() throws Exception {
    // Detect version from the Last-Modified header on the download URL.
    // Fall back to today's date if the header is absent or cannot be parsed.
    try {
      var resp = http.head(DOWNLOAD_URI.toString());
      String lastMod = resp.headers().firstValue("Last-Modified").orElse(null);
      if (lastMod != null) {
        version = ZonedDateTime.parse(lastMod, DateTimeFormatter.RFC_1123_DATE_TIME)
            .toLocalDate().toString();
        LOG.info("ITIS version from Last-Modified: {}", version);
      }
    } catch (Exception e) {
      LOG.warn("Could not read Last-Modified from {}: {}", DOWNLOAD_URI, e.getMessage());
    }
    if (version == null) {
      version = LocalDate.now().toString();
      LOG.info("ITIS version defaulting to today: {}", version);
    }
  }

  @Override
  protected void addData() throws Exception {
    File zipFile = download(ZIP_FN, DOWNLOAD_URI);
    File dbFile  = extractDb(zipFile);

    try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath())) {

      // ── Lookup tables ──────────────────────────────────────────────────────
      Map<Integer, String> kingdoms = queryIntStringMap(conn,
          "SELECT kingdom_id, kingdom_name FROM kingdoms");
      // taxon_unit_types has one row per (rank_id, kingdom_id) pair; ranks share the same
      // name across kingdoms so grouping by rank_id is safe.
      Map<Integer, String> ranks = queryIntStringMap(conn,
          "SELECT rank_id, rank_name FROM taxon_unit_types GROUP BY rank_id");
      Map<Integer, String> authors = queryIntStringMap(conn,
          "SELECT taxon_author_id, taxon_author FROM taxon_authors_lkp");

      // synonym_links: not-accepted TSN → accepted TSN
      Map<Integer, Integer> synLinks = new HashMap<>();
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT tsn, tsn_accepted FROM synonym_links")) {
        while (rs.next()) synLinks.put(rs.getInt(1), rs.getInt(2));
      }
      LOG.info("Loaded {} synonym links", synLinks.size());

      // ── Writers ────────────────────────────────────────────────────────────
      newWriter(ColdpTerm.NameUsage, List.of(
          ColdpTerm.ID,
          ColdpTerm.parentID,
          ColdpTerm.status,
          ColdpTerm.rank,
          ColdpTerm.scientificName,
          ColdpTerm.authorship,
          ColdpTerm.code
      ));
      TermWriter vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
          ColdpTerm.taxonID,
          ColdpTerm.name,
          ColdpTerm.language
      ));
      TermWriter distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
          ColdpTerm.taxonID,
          ColdpTerm.area,
          ColdpTerm.remarks
      ));

      // ── NameUsage ──────────────────────────────────────────────────────────
      int nAccepted = 0, nSynonyms = 0;
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery(
               "SELECT tsn, parent_tsn, rank_id, kingdom_id, complete_name, " +
               "       name_usage, taxon_author_id " +
               "FROM taxonomic_units ORDER BY tsn")) {
        while (rs.next()) {
          int    tsn       = rs.getInt("tsn");
          int    parentTsn = rs.getInt("parent_tsn");  // 0 when NULL
          int    rankId    = rs.getInt("rank_id");
          int    kingdomId = rs.getInt("kingdom_id");
          String sciName   = rs.getString("complete_name");
          String usage     = rs.getString("name_usage");
          int    authorId  = rs.getInt("taxon_author_id");  // 0 when NULL

          boolean isAccepted = usage != null && ACCEPTED_USAGES.contains(usage.toLowerCase(Locale.ENGLISH));
          String status = isAccepted ? "accepted" : "synonym";

          // parentID: for accepted taxa use parent_tsn; for synonyms use tsn_accepted
          String parentId;
          if (isAccepted) {
            parentId = parentTsn > 0 ? String.valueOf(parentTsn) : null;
          } else {
            Integer accTsn = synLinks.get(tsn);
            parentId = accTsn != null ? String.valueOf(accTsn) : null;
          }

          writer.set(ColdpTerm.ID, String.valueOf(tsn));
          if (parentId != null) writer.set(ColdpTerm.parentID, parentId);
          writer.set(ColdpTerm.status, status);
          String rankName = ranks.get(rankId);
          if (rankName != null) writer.set(ColdpTerm.rank, rankName.toLowerCase(Locale.ENGLISH));
          if (sciName != null) writer.set(ColdpTerm.scientificName, sciName);
          if (authorId > 0) {
            String author = authors.get(authorId);
            if (author != null) writer.set(ColdpTerm.authorship, author);
          }
          String code = nomCode(kingdoms.get(kingdomId));
          if (code != null) writer.set(ColdpTerm.code, code);
          writer.next();

          if (isAccepted) nAccepted++; else nSynonyms++;
        }
      }
      LOG.info("ITIS {}: {} accepted, {} synonyms written", version, nAccepted, nSynonyms);

      // ── VernacularName ─────────────────────────────────────────────────────
      int nVern = 0;
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery(
               "SELECT tsn, vernacular_name, language FROM vernaculars ORDER BY tsn")) {
        while (rs.next()) {
          String vName = rs.getString("vernacular_name");
          if (vName == null || vName.isBlank()) continue;
          vernWriter.set(ColdpTerm.taxonID,  String.valueOf(rs.getInt("tsn")));
          vernWriter.set(ColdpTerm.name,      vName.trim());
          String lang = rs.getString("language");
          if (lang != null && !lang.isBlank()) vernWriter.set(ColdpTerm.language, lang.trim());
          vernWriter.next();
          nVern++;
        }
      }
      LOG.info("ITIS: {} vernacular names written", nVern);

      // ── Distribution: geographic_div (global coarse regions) ───────────────
      int nDist = 0;
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery(
               "SELECT tsn, geographic_value FROM geographic_div ORDER BY tsn")) {
        while (rs.next()) {
          String area = rs.getString("geographic_value");
          if (area == null || area.isBlank()) continue;
          distWriter.set(ColdpTerm.taxonID, String.valueOf(rs.getInt("tsn")));
          distWriter.set(ColdpTerm.area,    area.trim());
          distWriter.next();
          nDist++;
        }
      }

      // ── Distribution: jurisdiction (North American, with native/introduced origin) ──
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery(
               "SELECT tsn, jurisdiction_value, origin FROM jurisdiction ORDER BY tsn")) {
        while (rs.next()) {
          String area = rs.getString("jurisdiction_value");
          if (area == null || area.isBlank()) continue;
          distWriter.set(ColdpTerm.taxonID, String.valueOf(rs.getInt("tsn")));
          distWriter.set(ColdpTerm.area,    area.trim());
          String origin = rs.getString("origin");
          if (origin != null && !origin.isBlank()) distWriter.set(ColdpTerm.remarks, origin.trim());
          distWriter.next();
          nDist++;
        }
      }
      LOG.info("ITIS: {} distribution records written", nDist);
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued",  version);
    super.addMetadata();
  }

  /**
   * Extracts the first {@code .sqlite} (or {@code .db}) file found in the zip
   * and saves it as {@value #DB_FN} under the sources directory.
   * Re-uses the extracted file on subsequent runs.
   */
  private File extractDb(File zipFile) throws IOException {
    File dbFile = sourceFile(DB_FN);
    if (dbFile.exists()) {
      LOG.info("Reusing extracted SQLite database {}", dbFile);
      return dbFile;
    }
    LOG.info("Extracting SQLite database from {}", zipFile);
    try (ZipFile zip = new ZipFile(zipFile)) {
      ZipEntry entry = zip.stream()
          .filter(e -> !e.isDirectory())
          .filter(e -> {
            String n = e.getName().toLowerCase(Locale.ENGLISH);
            return n.endsWith(".sqlite") || n.endsWith(".db");
          })
          .findFirst()
          .orElseThrow(() -> new IllegalStateException(
              "No .sqlite or .db file found inside " + zipFile));
      LOG.info("Found {} ({} bytes) in zip", entry.getName(), entry.getSize());
      try (InputStream in  = zip.getInputStream(entry);
           OutputStream out = new FileOutputStream(dbFile)) {
        in.transferTo(out);
      }
    }
    LOG.info("Extracted SQLite database to {}", dbFile);
    return dbFile;
  }

  /**
   * Executes a two-column query (INTEGER key, TEXT value) and returns a map.
   */
  private static Map<Integer, String> queryIntStringMap(Connection conn, String sql) throws SQLException {
    Map<Integer, String> map = new HashMap<>();
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) map.put(rs.getInt(1), rs.getString(2));
    }
    return map;
  }

  /** Returns the ColDP nomenclatural code acronym for the given ITIS kingdom name. */
  private static String nomCode(String kingdomName) {
    if (kingdomName == null) return null;
    return switch (kingdomName) {
      case "Animalia", "Protozoa"          -> NomCode.ZOOLOGICAL.getAcronym();
      case "Plantae", "Fungi", "Chromista" -> NomCode.BOTANICAL.getAcronym();
      case "Bacteria", "Archaea"           -> NomCode.BACTERIAL.getAcronym();
      default                              -> null;
    };
  }
}
