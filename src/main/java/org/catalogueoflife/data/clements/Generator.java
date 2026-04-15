package org.catalogueoflife.data.clements;

import com.univocity.parsers.csv.CsvParser;
import life.catalogue.api.util.ObjectUtils;
import org.catalogueoflife.data.utils.CsvUtils;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.RemarksBuilder;

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
  private static final int COL_SORT         = 0;  // sorting
  private static final int COL_SPECIES_CODE = 1;  // eBird species code used as taxon ID
  private static final int COL_AVIBASE_ID   = 2;  // taxon concept ID (Avibase)
  private static final int COL_REMARKS      = 4;
  private static final int COL_CATEGORY     = 5;
  private static final int COL_EN_NAME      = 6;
  private static final int COL_NAME         = 7;
  private static final int COL_AUTHORITY    = 8;
  private static final int COL_RANGE        = 10;
  private static final int COL_ORDER        = 11;
  private static final int COL_EXTINCT      = 13;
  private static final int COL_EXTINCT_YEAR = 14;

  private static final Pattern FAMILY_AUTH = Pattern.compile("^(.+?)\\s*(?:\\((.+)\\))?$");
  private static final String BOW_URL = "https://birdsoftheworld.org/bow/species/";
  private static final String BOW_SSP_PATH = "/cur/systematics#subsp-";

  private LocalDate issued;
  private String version;
  private URI csvUri;
  private TermWriter distWriter;
  private TermWriter vernWriter;

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
        ColdpTerm.ordinal,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.alternativeID,
        ColdpTerm.extinct,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.name,
        ColdpTerm.language
    ));
    distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.areaID,
        ColdpTerm.area,
        ColdpTerm.gazetteer,
        ColdpTerm.status,
        ColdpTerm.referenceID
    ));
  }

  @Override
  protected void addData() throws Exception {
    final Pattern YEAR = Pattern.compile("\\d{4}");
    File csv = download("clements.csv", csvUri);

    // Families, species and subspecies do and have a "species" code.
    // Rows are in taxonomic sequence; species follow their family, subspecies immediately follow their parent species.

    var parser = csvParser();
    parser.beginParsing(csv);
    parser.parseNext(); // skip header

    Set<String> orders = new HashSet<>();
    Map<String, Integer> categoryCounts = new LinkedHashMap<>();
    String currentFamilyId  = null;
    String currentSpeciesId = null;
    String[] row;
    while ((row = parser.parseNext()) != null) {
      String order  = col(row, COL_ORDER);
      if (orders.add(order)) {
        // Write order records
        writer.set(ColdpTerm.ID,             order);
        writer.set(ColdpTerm.scientificName, order);
        writer.set(ColdpTerm.rank,           "order");
        writer.set(ColdpTerm.status,         "accepted");
        writer.next();
      }

      // Write all explicit family, species and subspecies records. Rows are parented to the last seen higher taxon.
      String id       = col(row, COL_SPECIES_CODE);
      String category = col(row, COL_CATEGORY);
      String name     = col(row, COL_NAME);
      if (id == null || category == null || name == null) continue;

      String rank = categoryToRank(category);
      boolean subSpecific = isSubSpecific(rank);
      categoryCounts.merge(category + " → " + rank, 1, Integer::sum);
      String parentId;
      if ("family".equals(rank)) {
        parentId         = order;
        currentFamilyId  = id;
        currentSpeciesId = null;
      } else if ("species".equals(rank)) {
        parentId         = currentFamilyId;
        currentSpeciesId = id;
      } else {
        parentId         = currentSpeciesId;
      }

      writer.set(ColdpTerm.ID,            id);
      writer.set(ColdpTerm.parentID,       parentId);
      writer.set(ColdpTerm.ordinal,        col(row, COL_SORT));
      writer.set(ColdpTerm.rank,           rank);
      writer.set(ColdpTerm.scientificName, name);
      writer.set(ColdpTerm.authorship,     col(row, COL_AUTHORITY));
      String avibId = col(row, COL_AVIBASE_ID);
      if (avibId != null) {
        writer.set(ColdpTerm.alternativeID, avibId.replace("-", ":"));
      }
      writer.set(ColdpTerm.status,         "accepted");
      RemarksBuilder remarks = new RemarksBuilder();
      remarks.append(col(row, COL_REMARKS));

      String extinct = col(row, COL_EXTINCT);
      if ("extinct".equalsIgnoreCase(extinct) || "1".equals(extinct)) {
        writer.set(ColdpTerm.extinct, "true");
        var extinctYear = col(row, COL_EXTINCT_YEAR);
        if (extinctYear != null && YEAR.matcher(extinctYear).find() && !remarks.toString().contains(extinctYear)) {
          remarks.append("Extinct, last reported " + extinctYear);
        }
      }
      writer.set(ColdpTerm.remarks, remarks.toString());
      StringBuilder link = new StringBuilder(BOW_URL);
      if (subSpecific) {
        // get species codes by removing subspecies integer suffix
        link.append(id.replaceAll("\\d+$", ""));
        link.append(BOW_SSP_PATH);
        link.append(id);
      } else {
        link.append(id);
      }
      writer.set(ColdpTerm.link, link.toString());
      writer.next();

      // English name as vernacular name
      String enName = col(row, COL_EN_NAME);
      if (enName != null) {
        vernWriter.set(ColdpTerm.taxonID,  id);
        vernWriter.set(ColdpTerm.name,     enName);
        vernWriter.set(ColdpTerm.language, "eng");
        vernWriter.next();
      }

      // Distribution range
      String range = col(row, COL_RANGE);
      if (range != null) {
        distWriter.set(ColdpTerm.taxonID,  id);
        distWriter.set(ColdpTerm.area,     range);
        distWriter.set(ColdpTerm.gazetteer, "text");
        distWriter.next();
      }
    }
    parser.stopParsing();
    LOG.info("Written by category: {}", categoryCounts);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued", issued);
    super.addMetadata();
  }

  private static String categoryToRank(String category) {
    return switch (category.toLowerCase().trim()) {
      // group (monotypic)
      case "group (monotypic)"            -> "subspecies";
      // family
      // species
      // subspecies
      // group (polytypic)
      default                             -> category;
    };
  }

  /** Returns true for ranks that sit below species and should be parented to the current species. */
  private static boolean isSubSpecific(String rank) {
    return "subspecies".equals(rank) || rank.startsWith("group");
  }

  private static String col(String[] row, int index) {
    if (row == null || index >= row.length) return null;
    String v = row[index];
    return (v == null || v.isBlank()) ? null : v.strip();
  }

  private static CsvParser csvParser() {
    return CsvUtils.newCsvParser(8192);
  }
}
