package org.catalogueoflife.data.ioc;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.catalogueoflife.data.AbstractXlsSrcGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * IOC World Bird List generator.
 * https://www.worldbirdnames.org/
 *
 * Downloads and processes the IOC master list and multilingual vernacular name files.
 * The latest version is auto-detected by probing the download URLs.
 *
 * Master list column layout (header at row index 3, data from row index 4):
 *   0  Infraclass  (e.g. PALAEOGNATHAE; populated for infraclass rows only)
 *   1  Parvclass   (rare; skipped)
 *   2  Order       (e.g. STRUTHIONIFORMES; populated for order rows only)
 *   3  Family (Scientific)  (e.g. Struthionidae; populated for family rows only)
 *   4  Family (English)     (e.g. Ostriches; also English name for infraclass rows)
 *   5  Genus       (populated for genus rows only; carried forward for species/ssp)
 *   6  Species (Scientific) — species epithet only (e.g. "camelus"); blank for ssp rows
 *   7  Subspecies  — subspecies epithet only (e.g. "camelus"); blank for species rows
 *   8  Authority
 *   9  Species (English)    — English name; populated for species rows only
 *  10  Breeding Range
 *  11  Breeding Range-Subregion
 *  12  Nonbreeding Range
 *  13  Code        (change codes: TAX, ENG, AS, EXT, etc.; not taxonomic status)
 *  14  Comment
 *
 * Extinct taxa are marked by † or †† in the epithet cell.
 */
public class Generator extends AbstractXlsSrcGenerator {

  // URL templates — {VERSION} replaced with e.g. "15.1"
  private static final String MASTER_URL    = "https://worldbirdnames.org/master_ioc_list_v{VERSION}.xlsx";
  private static final String MULTILING_URL = "https://worldbirdnames.org/Multiling%20IOC%20{VERSION}_d.xlsx";
  private static final String MASTER_FN     = "master.xlsx";
  private static final String MULTILING_FN  = "multilingual.xlsx";

  // Master list column indices
  private static final int COL_INFRACLASS = 0;
  // col 1 = Parvclass (2 rows total; orders are parented directly to infraclass instead)
  private static final int COL_ORDER      = 2;
  private static final int COL_FAMILY_LAT = 3;
  private static final int COL_FAMILY_EN  = 4;  // also English name for infraclass rows
  private static final int COL_GENUS      = 5;
  private static final int COL_SPECIES_EP = 6;  // species epithet only (NOT full binomial)
  private static final int COL_SSP_EP     = 7;  // subspecies epithet only
  private static final int COL_AUTHORITY  = 8;
  private static final int COL_EN_NAME    = 9;  // English name (species-level rows only)
  private static final int COL_BREED      = 10;
  private static final int COL_BREED_SUB  = 11;
  private static final int COL_NONBREED   = 12;
  // col 13 = Code (change notes: TAX, ENG, AS, EXT, etc.) — informational only, not read
  private static final int COL_COMMENT    = 14;
  private static final int HEADER_ROWS    = 4;  // rows 0-3 are headers; data starts at row 4

  // Multilingual file column indices
  private static final int ML_SCINAME    = 3;   // full binomial (e.g. "Struthio camelus")
  private static final int ML_LANG_START = 4;   // first name column (English at 4, then other langs)

  // Map from multilingual column header to ISO 639-3 language code.
  // Duplicate lang codes (Chinese Trad/Simplified, Portuguese variants, French variants)
  // are intentional — all names are written as separate VernacularName records.
  private static final Map<String, String> LANG_MAP = new LinkedHashMap<>();
  static {
    LANG_MAP.put("English",                  "eng");
    LANG_MAP.put("Afrikaans",                "afr");
    LANG_MAP.put("Arabic",                   "ara");
    LANG_MAP.put("Belarusian",               "bel");
    LANG_MAP.put("Bulgarian",                "bul");
    LANG_MAP.put("Catalan",                  "cat");
    LANG_MAP.put("Chinese",                  "zho");
    LANG_MAP.put("Chinese (Traditional)",    "zho");
    LANG_MAP.put("Croatian",                 "hrv");
    LANG_MAP.put("Czech",                    "ces");
    LANG_MAP.put("Danish",                   "dan");
    LANG_MAP.put("Dutch",                    "nld");
    LANG_MAP.put("Esperanto",                "epo");
    LANG_MAP.put("Estonian",                 "est");
    LANG_MAP.put("Finnish",                  "fin");
    LANG_MAP.put("French",                   "fra");
    LANG_MAP.put("French (Gaudin)",          "fra");
    LANG_MAP.put("German",                   "deu");
    LANG_MAP.put("Greek",                    "ell");
    LANG_MAP.put("Hebrew",                   "heb");
    LANG_MAP.put("Hungarian",                "hun");
    LANG_MAP.put("Icelandic",                "isl");
    LANG_MAP.put("Indonesian",               "ind");
    LANG_MAP.put("Italian",                  "ita");
    LANG_MAP.put("Japanese",                 "jpn");
    LANG_MAP.put("Korean",                   "kor");
    LANG_MAP.put("Latvian",                  "lav");
    LANG_MAP.put("Lithuanian",               "lit");
    LANG_MAP.put("Macedonian",               "mkd");
    LANG_MAP.put("Malayalam",                "mal");
    LANG_MAP.put("Northern Sami",            "sme");
    LANG_MAP.put("Norwegian",                "nor");
    LANG_MAP.put("Persian",                  "fas");
    LANG_MAP.put("Polish",                   "pol");
    LANG_MAP.put("Portuguese (Lusophone)",   "por");
    LANG_MAP.put("Portuguese (Portuguese)",  "por");
    LANG_MAP.put("Romanian",                 "ron");
    LANG_MAP.put("Russian",                  "rus");
    LANG_MAP.put("Serbian",                  "srp");
    LANG_MAP.put("Slovak",                   "slk");
    LANG_MAP.put("Slovenian",                "slv");
    LANG_MAP.put("Spanish",                  "spa");
    LANG_MAP.put("Swedish",                  "swe");
    LANG_MAP.put("Thai",                     "tha");
    LANG_MAP.put("Turkish",                  "tur");
    LANG_MAP.put("Ukrainian",                "ukr");
  }

  private String version;
  private URI multilingUri;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void prepare() throws IOException {
    // Auto-detect the latest IOC version by probing download URLs.
    // IOC releases up to two versions per year (major.1, major.2).
    outer:
    for (int major = 18; major >= 10; major--) {
      for (int minor : new int[]{2, 1}) {
        String ver = major + "." + minor;
        String url = MASTER_URL.replace("{VERSION}", ver);
        if (http.exists(url)) {
          version = ver;
          multilingUri = URI.create(MULTILING_URL.replace("{VERSION}", ver));
          LOG.info("Latest IOC version: {} at {}", ver, url);
          download(MASTER_FN, URI.create(url));
          break outer;
        }
      }
    }
    if (version == null) {
      throw new IllegalStateException("Cannot find latest IOC World Bird List (checked v10.1 – v18.2)");
    }
    // super.prepare() initialises the private DataFormatter/FormulaEvaluator fields; it looks
    // for "data.xls" which won't exist, so it only does the formatter initialisation step.
    super.prepare();
    prepareWB(sourceFile(MASTER_FN));
  }

  @Override
  protected void addData() throws Exception {
    // Load multilingual names keyed by full scientific name.
    // Map: scientificName -> list of [langCode, vernacularName] pairs (English excluded).
    Map<String, List<String[]>> multiNames = loadMultilingual();

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.ordinal,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.code,
        ColdpTerm.extinct,
        ColdpTerm.remarks
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

    Sheet sheet = wb.getSheetAt(0); // "Master" sheet

    // Hierarchy tracking
    String currentInfraclass = null;
    String currentOrder      = null;
    String currentFamily     = null;
    String currentGenus      = null;
    String currentSpeciesId  = null;
    String currentSpeciesEp  = null;

    int ordinal = 0, spCount = 0, sspCount = 0, orderCount = 0, famCount = 0, genCount = 0;

    for (Row row : sheet) {
      if (row.getRowNum() < HEADER_ROWS) continue;

      String infraclass = col(row, COL_INFRACLASS);
      String order      = col(row, COL_ORDER);
      String famLat     = col(row, COL_FAMILY_LAT);
      String genus      = col(row, COL_GENUS);
      String speciesEp  = col(row, COL_SPECIES_EP);
      String sspEp      = col(row, COL_SSP_EP);
      String authority  = col(row, COL_AUTHORITY);
      String enName     = col(row, COL_EN_NAME);
      String breed      = col(row, COL_BREED);
      String breedSub   = col(row, COL_BREED_SUB);
      String nonbreed   = col(row, COL_NONBREED);
      String comment    = col(row, COL_COMMENT);

      if (infraclass != null) {
        // Infraclass row (PALAEOGNATHAE / NEOGNATHAE).
        // col 4 holds the English name (e.g. "RATITES") for the infraclass.
        currentInfraclass = infraclass;
        String id = "infraclass:" + infraclass;
        writer.set(ColdpTerm.ID, id);
        writer.set(ColdpTerm.rank, "infraclass");
        writer.set(ColdpTerm.scientificName, toTitleCase(infraclass));
        writer.set(ColdpTerm.status, "accepted");
        writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
        writer.next();
        String infraEn = col(row, COL_FAMILY_EN);
        if (infraEn != null) {
          vernWriter.set(ColdpTerm.taxonID, id);
          vernWriter.set(ColdpTerm.name, toTitleCase(infraEn));
          vernWriter.set(ColdpTerm.language, "eng");
          vernWriter.next();
        }

      } else if (order != null) {
        currentOrder = order;
        orderCount++;
        String id = "order:" + order;
        writer.set(ColdpTerm.ID, id);
        if (currentInfraclass != null) writer.set(ColdpTerm.parentID, "infraclass:" + currentInfraclass);
        writer.set(ColdpTerm.rank, "order");
        writer.set(ColdpTerm.scientificName, toTitleCase(order));
        writer.set(ColdpTerm.status, "accepted");
        writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
        if (comment != null) writer.set(ColdpTerm.remarks, comment);
        writer.next();

      } else if (famLat != null) {
        currentFamily = famLat;
        famCount++;
        String famEn = col(row, COL_FAMILY_EN);
        String id = "fam:" + famLat;
        writer.set(ColdpTerm.ID, id);
        writer.set(ColdpTerm.parentID, "order:" + currentOrder);
        writer.set(ColdpTerm.rank, "family");
        writer.set(ColdpTerm.scientificName, famLat);
        writer.set(ColdpTerm.status, "accepted");
        writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
        if (comment != null) writer.set(ColdpTerm.remarks, comment);
        writer.next();
        if (famEn != null) {
          vernWriter.set(ColdpTerm.taxonID, id);
          vernWriter.set(ColdpTerm.name, famEn);
          vernWriter.set(ColdpTerm.language, "eng");
          vernWriter.next();
        }

      } else if (genus != null) {
        currentGenus = genus;
        genCount++;
        String id = "gen:" + genus;
        writer.set(ColdpTerm.ID, id);
        writer.set(ColdpTerm.parentID, "fam:" + currentFamily);
        writer.set(ColdpTerm.rank, "genus");
        writer.set(ColdpTerm.scientificName, genus);
        writer.set(ColdpTerm.authorship, authority);
        writer.set(ColdpTerm.status, "accepted");
        writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
        writer.next();

      } else if (speciesEp != null) {
        // Species row: col 6 has the epithet only; build full binomial from current genus.
        ordinal++;
        spCount++;
        boolean extinct = speciesEp.contains("†");
        currentSpeciesEp = stripDaggers(speciesEp);
        String sciName = currentGenus + " " + currentSpeciesEp;
        String id = currentGenus + "_" + currentSpeciesEp;
        currentSpeciesId = id;

        writer.set(ColdpTerm.ID, id);
        writer.set(ColdpTerm.parentID, "gen:" + currentGenus);
        writer.set(ColdpTerm.ordinal, String.valueOf(ordinal));
        writer.set(ColdpTerm.rank, "species");
        writer.set(ColdpTerm.scientificName, sciName);
        writer.set(ColdpTerm.authorship, authority);
        writer.set(ColdpTerm.status, "accepted");
        writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
        if (extinct) writer.set(ColdpTerm.extinct, "true");
        if (comment != null) writer.set(ColdpTerm.remarks, comment);
        writer.next();

        // English name from master list
        if (enName != null) {
          vernWriter.set(ColdpTerm.taxonID, id);
          vernWriter.set(ColdpTerm.name, enName);
          vernWriter.set(ColdpTerm.language, "eng");
          vernWriter.next();
        }
        // Multilingual names from the multilingual file
        List<String[]> langs = multiNames.get(sciName);
        if (langs != null) {
          for (String[] pair : langs) {
            vernWriter.set(ColdpTerm.taxonID, id);
            vernWriter.set(ColdpTerm.language, pair[0]);
            vernWriter.set(ColdpTerm.name, pair[1]);
            vernWriter.next();
          }
        }
        // Distribution (species-level ranges)
        writeDistribution(distWriter, id, breed, breedSub, nonbreed);

      } else if (sspEp != null) {
        // Subspecies row: col 7 has the epithet only.
        sspCount++;
        boolean extinct = sspEp.contains("†");
        String cleanSspEp = stripDaggers(sspEp);
        String sciName = currentGenus + " " + currentSpeciesEp + " " + cleanSspEp;
        String id = currentGenus + "_" + currentSpeciesEp + "_" + cleanSspEp;

        writer.set(ColdpTerm.ID, id);
        writer.set(ColdpTerm.parentID, currentSpeciesId);
        writer.set(ColdpTerm.rank, "subspecies");
        writer.set(ColdpTerm.scientificName, sciName);
        writer.set(ColdpTerm.authorship, authority);
        writer.set(ColdpTerm.status, "accepted");
        writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
        if (extinct) writer.set(ColdpTerm.extinct, "true");
        writer.next();
        writeDistribution(distWriter, id, breed, breedSub, nonbreed);
      }
      // Parvclass rows (col 1 only, 2 rows total) are intentionally skipped;
      // orders are parented directly to their infraclass.
    }

    LOG.info("IOC {}: {} orders, {} families, {} genera, {} species, {} subspecies written",
        version, orderCount, famCount, genCount, spCount, sspCount);
  }

  /** Writes distribution record(s) for a taxon given breeding and nonbreeding ranges. */
  private static void writeDistribution(TermWriter distWriter, String taxonId,
      String breed, String breedSub, String nonbreed) throws IOException {
    if (breed != null || breedSub != null) {
      String area = breed != null ? breed : "";
      if (breedSub != null) area = area.isEmpty() ? breedSub : area + " (" + breedSub + ")";
      distWriter.set(ColdpTerm.taxonID, taxonId);
      distWriter.set(ColdpTerm.area, area);
      distWriter.next();
    }
    if (nonbreed != null) {
      distWriter.set(ColdpTerm.taxonID, taxonId);
      distWriter.set(ColdpTerm.area, nonbreed);
      distWriter.set(ColdpTerm.remarks, "nonbreeding");
      distWriter.next();
    }
  }

  /**
   * Loads the multilingual Excel file and returns a map from full scientific name
   * to a list of [langCode, vernacularName] pairs. English names are excluded
   * (they are taken from the master list). Duplicate (langCode, name) pairs per
   * species are suppressed.
   */
  private Map<String, List<String[]>> loadMultilingual() throws Exception {
    File f;
    try {
      f = download(MULTILING_FN, multilingUri);
    } catch (Exception e) {
      LOG.warn("Could not download multilingual file from {}: {}", multilingUri, e.getMessage());
      return Collections.emptyMap();
    }

    Workbook mlWb = WorkbookFactory.create(f);
    try {
      Sheet sheet = mlWb.getSheetAt(0); // "List" sheet
      DataFormatter fmt = new DataFormatter(Locale.US);

      // Read language headers from row 0 to build col→langCode mapping.
      // This is done dynamically so the mapping remains correct if column order changes
      // in future IOC versions.
      Row headerRow = sheet.getRow(0);
      Map<Integer, String> colToLang = new LinkedHashMap<>();
      for (int c = ML_LANG_START; c < headerRow.getLastCellNum(); c++) {
        Cell cell = headerRow.getCell(c);
        if (cell == null) continue;
        String header = StringUtils.trimToNull(fmt.formatCellValue(cell));
        if (header == null) continue;
        String langCode = LANG_MAP.get(header);
        if (langCode != null && !"eng".equals(langCode)) { // English comes from master list
          colToLang.put(c, langCode);
        }
      }
      LOG.info("Multilingual file: {} language columns mapped", colToLang.size());

      Map<String, List<String[]>> result = new HashMap<>();
      for (Row row : sheet) {
        if (row.getRowNum() == 0) continue; // skip header
        Cell nameCell = row.getCell(ML_SCINAME);
        if (nameCell == null) continue;
        String sciName = StringUtils.trimToNull(fmt.formatCellValue(nameCell));
        if (sciName == null) continue;

        List<String[]> names = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (var entry : colToLang.entrySet()) {
          Cell cell = row.getCell(entry.getKey());
          if (cell == null) continue;
          String name = StringUtils.trimToNull(fmt.formatCellValue(cell));
          if (name != null) {
            // Deduplicate: skip if same (lang, name) pair already present for this species
            if (seen.add(entry.getValue() + "|" + name)) {
              names.add(new String[]{entry.getValue(), name});
            }
          }
        }
        if (!names.isEmpty()) result.put(sciName, names);
      }
      LOG.info("Multilingual file: {} species with vernacular names loaded", result.size());
      return result;
    } finally {
      mlWb.close();
    }
  }

  /** Strips extinct dagger markers (†, ††) and trims whitespace. */
  private static String stripDaggers(String s) {
    return s.replace("†", "").replace("‡", "").trim();
  }

  /** Converts ALL-CAPS string to Title Case (first letter upper, rest lower). */
  private static String toTitleCase(String s) {
    if (s == null || s.isEmpty()) return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    super.addMetadata();
  }
}
