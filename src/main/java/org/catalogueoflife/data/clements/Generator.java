package org.catalogueoflife.data.clements;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * eBird/Clements Checklist of Birds of the World.
 * https://www.birds.cornell.edu/clementschecklist/
 *
 * Downloads the annual CSV (published in October each year).
 * Example: https://www.birds.cornell.edu/clementschecklist/wp-content/uploads/2025/10/Clements_v2025-October-2025.csv
 */
public class Generator extends AbstractColdpGenerator {
  // https://www.birds.cornell.edu/clementschecklist/wp-content/uploads/2025/10/Clements_v2025-October-2025.csv
  private static final String DOWNLOAD =
      "https://www.birds.cornell.edu/clementschecklist/wp-content/uploads/{YEAR}/{MONTH}/Clements_v{YEAR}-{MONTH_NAME}-{YEAR}.csv";

  // CSV column indices (0-based), as of v2025:
  // sort v2025 | species_code | taxon concept ID | Clements v2025 change | text for website v2025
  // | category | English name | scientific name | authority | name and authority
  // | range | order | family | extinct | extinct year | sort_v2024
  private static final int COL_ID           = 0;  // sort v20xx — used as taxon ID
  private static final int COL_SPECIES_CODE = 1;  // eBird species code
  private static final int COL_AVIBASE_ID   = 2;  // taxon concept ID (Avibase)
  private static final int COL_CATEGORY     = 5;
  private static final int COL_EN_NAME      = 6;
  private static final int COL_NAME         = 7;
  private static final int COL_AUTHORITY    = 8;
  private static final int COL_ORDER        = 11;
  private static final int COL_FAMILY       = 12;
  private static final int COL_EXTINCT      = 13;

  private static final Pattern FAMILY_AUTH = Pattern.compile("^(.+?)\\s*(?:\\((.+)\\))?$");
  private static final String EBIRD_URL    = "https://ebird.org/species/";

  private LocalDate issued;
  private String version;
  private URI csvUri;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  static String buildUrl(LocalDate date) {
    String monthName = StringUtils.capitalize(
        date.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH).toLowerCase());
    return DOWNLOAD
        .replace("{YEAR}", String.valueOf(date.getYear()))
        .replace("{MONTH}", String.format("%02d", date.getMonthValue()))
        .replace("{MONTH_NAME}", monthName);
  }

  @Override
  protected void prepare() throws IOException {
    // Published annually, usually in October. Probe backwards up to 2 years.
    LocalDate today = LocalDate.now();
    for (int i = 0; i <= 24; i++) {
      LocalDate date = today.minus(i, ChronoUnit.MONTHS);
      String url = buildUrl(date);
      if (http.exists(url)) {
        issued = date;
        version = String.valueOf(date.getYear());
        csvUri = URI.create(url);
        break;
      }
    }
    if (issued == null) throw new IllegalStateException("Unable to find any Clements release in the last 2 years");
    LOG.info("Using Clements {} at {}", version, csvUri);

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.alternativeID,
        ColdpTerm.extinct,
        ColdpTerm.link
    ));
  }

  @Override
  protected void addData() throws Exception {
    File csv = download("clements.csv", csvUri);
    TermWriter vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.name,
        ColdpTerm.language
    ));

    // Single pass: collect orders and families in insertion order, then write all records.
    // Rows are in taxonomic sequence; subspecies (issf) immediately follow their parent species.
    Map<String, String> familyToOrder = new LinkedHashMap<>(); // family raw string → order name

    var parser = csvParser();
    parser.beginParsing(csv);
    parser.parseNext(); // skip header

    List<String[]> rows = new ArrayList<>();
    String[] row;
    while ((row = parser.parseNext()) != null) {
      String order  = col(row, COL_ORDER);
      String family = col(row, COL_FAMILY);
      if (order != null && family != null) familyToOrder.putIfAbsent(family, order);
      rows.add(row.clone());
    }
    parser.stopParsing();

    // Write order records
    Set<String> orders = new LinkedHashSet<>(familyToOrder.values());
    for (String order : orders) {
      writer.set(ColdpTerm.ID,            "order:" + order);
      writer.set(ColdpTerm.scientificName, order);
      writer.set(ColdpTerm.rank,           "order");
      writer.set(ColdpTerm.status,         "accepted");
      writer.next();
    }

    // Write family records
    for (var e : familyToOrder.entrySet()) {
      String rawFamily = e.getKey();
      String famName   = parseFamilyName(rawFamily);
      String famAuth   = parseFamilyAuth(rawFamily);
      writer.set(ColdpTerm.ID,            "fam:" + famName);
      writer.set(ColdpTerm.parentID,       "order:" + e.getValue());
      writer.set(ColdpTerm.scientificName, famName);
      writer.set(ColdpTerm.authorship,     famAuth);
      writer.set(ColdpTerm.rank,           "family");
      writer.set(ColdpTerm.status,         "accepted");
      writer.next();
    }

    // Write species and subspecies records. Subspecies (issf) are parented to the last seen species.
    String currentSpeciesId = null;
    int speciesCount = 0, issfCount = 0, skipped = 0;
    for (String[] r : rows) {
      String id       = col(r, COL_ID);
      String category = col(r, COL_CATEGORY);
      String name     = col(r, COL_NAME);
      if (id == null || category == null || name == null) continue;

      String rank = categoryToRank(category);
      if (rank == null) { skipped++; continue; }

      String rawFamily = col(r, COL_FAMILY);
      String famName   = rawFamily != null ? parseFamilyName(rawFamily) : null;
      String parentId  = switch (rank) {
        case "subspecies" -> currentSpeciesId;
        default           -> famName != null ? "fam:" + famName : null;
      };

      if ("species".equals(rank)) { currentSpeciesId = id; speciesCount++; }
      else { issfCount++; }

      writer.set(ColdpTerm.ID,            id);
      writer.set(ColdpTerm.parentID,       parentId);
      writer.set(ColdpTerm.rank,           rank);
      writer.set(ColdpTerm.scientificName, name);
      writer.set(ColdpTerm.authorship,     col(r, COL_AUTHORITY));
      String avibId = col(r, COL_AVIBASE_ID);
      if (avibId != null) writer.set(ColdpTerm.alternativeID, "avibase:" + avibId);
      writer.set(ColdpTerm.status,         "accepted");
      String extinct = col(r, COL_EXTINCT);
      if ("extinct".equalsIgnoreCase(extinct) || "1".equals(extinct)) {
        writer.set(ColdpTerm.extinct, "true");
      }
      String code = col(r, COL_SPECIES_CODE);
      if (code != null) writer.set(ColdpTerm.link, EBIRD_URL + code);
      writer.next();

      // English name as vernacular name
      String enName = col(r, COL_EN_NAME);
      if (enName != null) {
        vernWriter.set(ColdpTerm.taxonID,  id);
        vernWriter.set(ColdpTerm.name,     enName);
        vernWriter.set(ColdpTerm.language, "eng");
        vernWriter.next();
      }
    }
    LOG.info("{} species, {} subspecies written; {} non-species rows skipped",
        speciesCount, issfCount, skipped);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued", issued);
    super.addMetadata();
  }

  private static String categoryToRank(String category) {
    return switch (category.toLowerCase().trim()) {
      case "species" -> "species";
      case "issf"    -> "subspecies";
      default        -> null; // slash, form, intergrade, hybrid, domestic — skip
    };
  }

  private static String parseFamilyName(String raw) {
    Matcher m = FAMILY_AUTH.matcher(raw);
    return m.matches() ? m.group(1).trim() : raw.trim();
  }

  private static String parseFamilyAuth(String raw) {
    Matcher m = FAMILY_AUTH.matcher(raw);
    return (m.matches() && m.group(2) != null) ? m.group(2).trim() : null;
  }

  private static String col(String[] row, int index) {
    if (row == null || index >= row.length) return null;
    String v = row[index];
    return (v == null || v.isBlank()) ? null : v.strip();
  }

  private static CsvParser csvParser() {
    var settings = new CsvParserSettings();
    settings.setNullValue(null);
    settings.setHeaderExtractionEnabled(false);
    settings.setLineSeparatorDetectionEnabled(true);
    settings.setMaxCharsPerColumn(8192);
    return new CsvParser(settings);
  }
}
