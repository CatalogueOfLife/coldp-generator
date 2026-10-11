package org.catalogueoflife.data.lcvp;

import com.fasterxml.jackson.databind.JsonNode;
import com.univocity.parsers.csv.CsvParser;
import life.catalogue.api.model.DOI;
import life.catalogue.coldp.ColdpTerm;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.CsvUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.catalogueoflife.data.lcvp.LcvpMappings.*;

/**
 * Leipzig Catalogue of Vascular Plants (LCVP).
 * <p>
 * The raw data is a zipped CSV export kept in the raw_data_LCVP folder of the LCVP R data package on GitHub.
 * Each row is a name with its LCVP status and the id of the accepted output taxon; families and orders are
 * only given as columns, genera not at all, so the classification above species is synthesized.
 * <p>
 * Accepted names must not use their own id as parentID, which is what the old R conversion did
 * and which flattened the whole tree in ChecklistBank (backend#1625).
 */
public class Generator extends AbstractColdpGenerator {
  private static final String REPO = "idiv-biodiversity/LCVP";
  private static final URI RAW_DIR_API = URI.create("https://api.github.com/repos/" + REPO + "/contents/raw_data_LCVP");
  private static final URI DESCRIPTION = URI.create("https://raw.githubusercontent.com/" + REPO + "/master/DESCRIPTION");
  // e.g. "LCVP 2.0 export 27.7.2022.zip"
  private static final Pattern EXPORT_DATE = Pattern.compile("(\\d{1,2})\\.(\\d{1,2})\\.(\\d{4})");
  private static final Pattern PKG_VERSION = Pattern.compile("(?m)^Version:\\s*(\\S+)");
  private static final DOI PAPER = new DOI("10.1038/s41597-020-00702-z");

  private static final String ROOT_ID = "Tracheophyta";
  private static final String ORDER_PREFIX = "o:";
  private static final String FAMILY_PREFIX = "f:";
  private static final String GENUS_PREFIX = "g:";

  // CSV columns
  private static final int COL_ID = 0;
  private static final int COL_GENUS = 1;
  private static final int COL_EPITHET = 2;
  private static final int COL_RANK = 3;
  private static final int COL_INFRA = 4;
  private static final int COL_AUTHORS = 5;
  private static final int COL_STATUS = 6;
  private static final int COL_OUTPUT_ID = 7;
  private static final int COL_FAMILY = 9;
  private static final int COL_ORDER = 10;
  private static final int COL_LITERATURE = 11;
  private static final int COL_COMMENTS = 12;

  // built during the first pass
  private final Set<String> accepted = new HashSet<>();
  private final Map<String, String> outputIds = new HashMap<>();
  // global.Id is not unique: the row keeping the id is the accepted one, or else the first
  private final Map<String, Long> idOwner = new HashMap<>();
  private final Set<String> duplicateIds = new HashSet<>();
  private final Map<String, String> genusFamily = new TreeMap<>();
  private final Map<String, String> familyOrder = new TreeMap<>();
  // accepted species by genus|epithet; null value for ambiguous keys
  private final Map<String, String> acceptedSpecies = new HashMap<>();
  private final Map<String, String> refIds = new HashMap<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    File zip = downloadExport();
    readVersion();

    LOG.info("Pass 1: index accepted names and classification");
    parse(zip, this::index);
    LOG.info("Indexed {} accepted names in {} genera, {} families, {} orders. {} duplicate ids",
        accepted.size(), genusFamily.size(), familyOrder.size(), new HashSet<>(familyOrder.values()).size(),
        duplicateIds.size());

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID, ColdpTerm.parentID, ColdpTerm.status, ColdpTerm.rank,
        ColdpTerm.scientificName, ColdpTerm.authorship,
        ColdpTerm.genericName, ColdpTerm.specificEpithet, ColdpTerm.infraspecificEpithet, ColdpTerm.notho,
        ColdpTerm.code, ColdpTerm.referenceID, ColdpTerm.remarks));
    initRefWriter(List.of(ColdpTerm.ID, ColdpTerm.citation));

    writeClassification();
    LOG.info("Pass 2: write name usages");
    Counter counter = new Counter();
    parse(zip, row -> write(row, counter));
    LOG.info("Wrote {} accepted, {} synonyms ({} unresolved in LCVP), {} bare names, {} references",
        counter.accepted, counter.synonyms, counter.unresolved, counter.bare, refIds.size());
  }

  private static class Counter {
    long line;
    int accepted, synonyms, unresolved, bare;
  }

  /** Finds the zipped export in the raw data folder, so a new export is picked up without code changes. */
  private File downloadExport() throws IOException {
    String fn = null;
    URI url = null;
    if (!cfg.noDownload) {
      JsonNode files = mapper.readTree(http.getJSON(RAW_DIR_API));
      for (JsonNode f : files) {
        String name = f.path("name").asText();
        if (name.toLowerCase().endsWith(".zip") && (fn == null || name.compareTo(fn) > 0)) {
          fn = name;
          url = URI.create(f.path("download_url").asText());
        }
      }
      if (fn == null) {
        throw new IllegalStateException("No zipped LCVP export found in " + RAW_DIR_API);
      }
    } else {
      File[] zips = sources.listFiles((d, n) -> n.toLowerCase().endsWith(".zip"));
      if (zips == null || zips.length == 0) {
        throw new IllegalStateException("--no-download set but no LCVP export found in " + sources);
      }
      fn = Arrays.stream(zips).map(File::getName).max(String::compareTo).get();
    }
    Matcher m = EXPORT_DATE.matcher(fn);
    if (m.find()) {
      metadata.put("issued", LocalDate.of(Integer.parseInt(m.group(3)), Integer.parseInt(m.group(2)),
          Integer.parseInt(m.group(1))).toString());
    }
    return url == null ? sourceFile(fn) : download(fn, url);
  }

  /** The data has no version of its own, the R package version is what LCVP releases are known by. */
  private void readVersion() {
    if (cfg.noDownload) return;
    try {
      Matcher m = PKG_VERSION.matcher(http.get(DESCRIPTION));
      if (m.find()) {
        metadata.put("version", m.group(1));
      }
    } catch (IOException e) {
      LOG.warn("Failed to read the LCVP package version from {}", DESCRIPTION, e);
    }
  }

  private void parse(File zip, Consumer<String[]> handler) throws IOException {
    try (ZipFile zf = new ZipFile(zip)) {
      ZipEntry csv = zf.stream()
          .filter(e -> !e.isDirectory() && e.getName().toLowerCase().endsWith(".csv"))
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("No CSV file in " + zip));
      LOG.info("Read {} from {}", csv.getName(), zip.getName());
      try (Reader r = new InputStreamReader(zf.getInputStream(csv), StandardCharsets.UTF_8)) {
        CsvParser parser = CsvUtils.newCsvParser(4096);
        parser.beginParsing(r);
        parser.parseNext(); // header
        String[] row;
        while ((row = parser.parseNext()) != null) {
          handler.accept(row);
        }
        parser.stopParsing();
      }
    }
  }

  private static String col(String[] row, int idx) {
    return idx < row.length ? clean(row[idx]) : null;
  }

  private long line;

  private void index(String[] row) {
    line++;
    String id = col(row, COL_ID);
    String status = col(row, COL_STATUS);
    if (id == null) return;
    boolean isAccepted = ACCEPTED.equals(status);
    Long owner = idOwner.get(id);
    if (owner != null) {
      duplicateIds.add(id);
      LOG.info("Duplicate id {} in line {}", id, line);
    }
    if (owner == null || isAccepted) {
      idOwner.put(id, line);
    }
    if (isAccepted) {
      accepted.add(id);
      outputIds.remove(id);
      String genus = col(row, COL_GENUS);
      String family = col(row, COL_FAMILY);
      String order = col(row, COL_ORDER);
      if (genus != null) {
        genusFamily.putIfAbsent(genus, family);
      }
      if (family != null) {
        familyOrder.putIfAbsent(family, order);
      }
      if ("species".equals(col(row, COL_RANK))) {
        String key = speciesKey(genus, col(row, COL_EPITHET));
        if (acceptedSpecies.containsKey(key)) {
          acceptedSpecies.put(key, null);
        } else {
          acceptedSpecies.put(key, id);
        }
      }
    } else if (!accepted.contains(id)) {
      outputIds.putIfAbsent(id, col(row, COL_OUTPUT_ID));
    }
  }

  private void writeClassification() throws IOException {
    usage(ROOT_ID, null, "phylum", ROOT_ID);
    for (String order : new TreeSet<>(familyOrder.values())) {
      if (order != null) {
        usage(ORDER_PREFIX + order, ROOT_ID, "order", order);
      }
    }
    for (var e : familyOrder.entrySet()) {
      usage(FAMILY_PREFIX + e.getKey(), e.getValue() == null ? ROOT_ID : ORDER_PREFIX + e.getValue(),
          "family", e.getKey());
    }
    for (var e : genusFamily.entrySet()) {
      Part genus = Part.of(e.getKey());
      writer.set(ColdpTerm.genericName, genus.value());
      writer.set(ColdpTerm.notho, genus.hybrid() ? "generic" : null);
      usage(GENUS_PREFIX + e.getKey(), e.getValue() == null ? ROOT_ID : FAMILY_PREFIX + e.getValue(),
          "genus", (genus.hybrid() ? "×" : "") + genus.value());
    }
  }

  private void usage(String id, String parentID, String rank, String name) throws IOException {
    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentID);
    writer.set(ColdpTerm.status, ACCEPTED);
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.code, "botanical");
    writer.next();
  }

  private void write(String[] row, Counter cnt) {
    cnt.line++;
    try {
      String rawId = col(row, COL_ID);
      if (rawId == null) return;
      // a duplicate id is kept only by its owner, all other rows get the line number appended
      String id = duplicateIds.contains(rawId) && idOwner.get(rawId) != cnt.line ? rawId + "-" + cnt.line : rawId;
      String lcvpStatus = col(row, COL_STATUS);
      String rawGenus = col(row, COL_GENUS);
      Part genus = Part.of(rawGenus);
      Part species = Part.of(col(row, COL_EPITHET));
      Part infra = Part.of(col(row, COL_INFRA));
      String rank = col(row, COL_RANK);
      List<String> remarks = new ArrayList<>();

      String status = status(lcvpStatus);
      String parentID;
      if (ACCEPTED.equals(lcvpStatus)) {
        parentID = null;
        if (infra.value() != null) {
          parentID = acceptedSpecies.get(speciesKey(rawGenus, col(row, COL_EPITHET)));
        }
        if (parentID == null && rawGenus != null) {
          parentID = GENUS_PREFIX + rawGenus;
        }
        cnt.accepted++;
      } else {
        parentID = resolveAccepted(col(row, COL_OUTPUT_ID), accepted, outputIds);
        if (UNRESOLVED.equals(lcvpStatus)) {
          remarks.add("Unresolved in LCVP: the name could be accepted or a synonym of the linked name");
          cnt.unresolved++;
        }
        if (parentID == null) {
          // never drop or re-file a synonym without an accepted name, keep it as a bare name
          remarks.add("LCVP " + lcvpStatus + " without an accepted name");
          status = BARE_NAME;
          cnt.bare++;
        } else {
          cnt.synonyms++;
        }
      }

      List<String> refs = new ArrayList<>();
      for (String citation : literature(col(row, COL_LITERATURE))) {
        if (citation.equalsIgnoreCase(SELF)) {
          remarks.add("Status decided by the LCVP authors");
        } else {
          refs.add(refId(citation));
        }
      }
      String comments = col(row, COL_COMMENTS);
      if (comments != null) {
        remarks.add(comments);
      }

      writer.set(ColdpTerm.ID, id);
      writer.set(ColdpTerm.parentID, parentID);
      writer.set(ColdpTerm.status, status);
      writer.set(ColdpTerm.rank, rank);
      writer.set(ColdpTerm.scientificName, scientificName(genus, species, rank, infra));
      writer.set(ColdpTerm.authorship, col(row, COL_AUTHORS));
      writer.set(ColdpTerm.genericName, genus.value());
      writer.set(ColdpTerm.specificEpithet, species.value());
      writer.set(ColdpTerm.infraspecificEpithet, infra.value());
      writer.set(ColdpTerm.notho, notho(genus, species, infra));
      writer.set(ColdpTerm.code, "botanical");
      writer.set(ColdpTerm.referenceID, refs.isEmpty() ? null : String.join(",", new LinkedHashSet<>(refs)));
      writer.set(ColdpTerm.remarks, remarks.isEmpty() ? null : String.join("; ", remarks));
      writer.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String refId(String citation) throws IOException {
    String id = refIds.get(citation);
    if (id == null) {
      refWriter.set(ColdpTerm.citation, citation);
      id = nextRef();
      refIds.put(citation, id);
    }
    return id;
  }

  @Override
  protected void addMetadata() throws Exception {
    addSource(PAPER);
    super.addMetadata();
  }
}
