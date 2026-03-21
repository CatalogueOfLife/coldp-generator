package org.catalogueoflife.data.grin;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.*;

/**
 * Germplasm Resources Information Network (GRIN) Taxonomy generator.
 * Downloads taxonomy_data.cab from https://npgsweb.ars-grin.gov/gringlobal/
 * Requires `cabextract` to be installed (e.g. `brew install cabextract`).
 */
public class Generator extends AbstractColdpGenerator {
  private static final String BASE      = "https://npgsweb.ars-grin.gov/gringlobal";
  private static final String LINK      = BASE + "/taxon/taxonomydetail?id=";
  private static final String CAB_FN   = "taxonomy_data.cab";
  private static final String CAB_URL  = BASE + "/uploads/documents/taxonomy_data.cab";

  // In-memory lookups built before writing ColDP records
  private final Map<Integer, String>  countryByGeoId        = new HashMap<>();
  private final Map<Integer, String>  refIdByLitId          = new HashMap<>();
  private final Map<Integer, String>  refIdBySpeciesId      = new HashMap<>();
  private final Map<Integer, Integer> basionymByAcceptedId  = new HashMap<>(); // accepted_id → basionym_id

  private static final Map<String, String> LANG_MAP = buildLangMap();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, Map.of(CAB_FN, URI.create(CAB_URL)));
  }

  // ── lifecycle ─────────────────────────────────────────────────────────────

  @Override
  protected void prepare() throws Exception {
    if (!sourceFile("taxonomy_species.txt").exists()) {
      LOG.info("Extracting {}", CAB_FN);
      var pb = new ProcessBuilder("cabextract", "-d", sources.getAbsolutePath(),
          sourceFile(CAB_FN).getAbsolutePath());
      pb.redirectErrorStream(true);
      int code = pb.start().waitFor();
      if (code != 0) throw new IOException("cabextract failed with exit code " + code +
          ". Install it with: brew install cabextract");
    } else {
      LOG.info("Reusing already-extracted GRIN data files");
    }
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.basionymID,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.status,
        ColdpTerm.nameReferenceID,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));

    initRefWriter(List.of(
        ColdpTerm.ID,
        ColdpTerm.author,
        ColdpTerm.title,
        ColdpTerm.year,
        ColdpTerm.publisher,
        ColdpTerm.publisherPlace,
        ColdpTerm.link
    ));

    var nomRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));
    var vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.name,
        ColdpTerm.language
    ));
    var distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.area,
        ColdpTerm.gazetteer,
        ColdpTerm.status
    ));
    var propWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.property,
        ColdpTerm.value
    ));

    // Phase 1: build lookup maps
    parseReferences();
    loadGeography();
    loadSpeciesCitations();
    collectBasionyms();

    // Phase 2: write ColDP records
    parseFamilies();
    parseGenera();
    parseSpecies(nomRelWriter);
    parseCommonNames(vernWriter);
    parseDistributions(distWriter);
    parseUses(propWriter);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

  // ── lookup builders ───────────────────────────────────────────────────────

  private void parseReferences() throws IOException {
    int count = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("literature.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var litId = intCol(row, idx, "literature_id");
      if (litId == null) continue;
      String refId = "ref:" + litId;
      refWriter.set(ColdpTerm.ID, refId);
      refWriter.set(ColdpTerm.author, col(row, idx, "editor_author_name"));
      refWriter.set(ColdpTerm.title, col(row, idx, "reference_title"));
      refWriter.set(ColdpTerm.year, col(row, idx, "publication_year"));
      refWriter.set(ColdpTerm.publisher, col(row, idx, "publisher_name"));
      refWriter.set(ColdpTerm.publisherPlace, col(row, idx, "publisher_location"));
      refWriter.set(ColdpTerm.link, col(row, idx, "url"));
      nextRef();
      refIdByLitId.put(litId, refId);
      count++;
    }
    parser.stopParsing();
    LOG.info("{} literature references loaded", count);
  }

  private void loadGeography() throws IOException {
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("geography.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var geoId   = intCol(row, idx, "geography_id");
      var country = col(row, idx, "country_code");
      if (geoId != null && country != null) countryByGeoId.put(geoId, country);
    }
    parser.stopParsing();
    LOG.info("{} geography entries loaded", countryByGeoId.size());
  }

  /** Stream the large citation.txt file and record the best literature ref per species. */
  private void loadSpeciesCitations() throws IOException {
    // species_id → ref from a non-primary citation (fallback if no Y citation found)
    Map<Integer, String> fallback = new HashMap<>();
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("citation.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var speciesId = intCol(row, idx, "taxonomy_species_id");
      if (speciesId == null) continue;
      var litId = intCol(row, idx, "literature_id");
      if (litId == null) continue;
      var refId = refIdByLitId.get(litId);
      if (refId == null) continue;
      if ("Y".equals(col(row, idx, "is_accepted_name"))) {
        refIdBySpeciesId.put(speciesId, refId);
      } else {
        fallback.putIfAbsent(speciesId, refId);
      }
    }
    parser.stopParsing();
    // fill in species that only have non-primary citations
    for (var e : fallback.entrySet()) {
      refIdBySpeciesId.putIfAbsent(e.getKey(), e.getValue());
    }
    LOG.info("{} species name citations loaded", refIdBySpeciesId.size());
  }

  /** Pass 1 over taxonomy_species: record basionym_id for each accepted species. */
  private void collectBasionyms() throws IOException {
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_species.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      if (!"B".equals(col(row, idx, "synonym_code"))) continue;
      var speciesId = intCol(row, idx, "taxonomy_species_id");
      var currentId = intCol(row, idx, "current_taxonomy_species_id");
      if (speciesId != null && currentId != null && !speciesId.equals(currentId)) {
        basionymByAcceptedId.putIfAbsent(currentId, speciesId);
      }
    }
    parser.stopParsing();
    LOG.info("{} basionym relations collected", basionymByAcceptedId.size());
  }

  // ── ColDP writers ─────────────────────────────────────────────────────────

  private void parseFamilies() throws IOException {
    int accepted = 0, synonyms = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_family.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var famId     = intCol(row, idx, "taxonomy_family_id");
      var currentId = intCol(row, idx, "current_taxonomy_family_id");
      if (famId == null) continue;

      writer.set(ColdpTerm.ID, "fam:" + famId);
      writer.set(ColdpTerm.scientificName, col(row, idx, "family_name"));
      writer.set(ColdpTerm.authorship, col(row, idx, "family_authority"));
      writer.set(ColdpTerm.rank, "family");

      if (currentId != null && !currentId.equals(famId)) {
        writer.set(ColdpTerm.parentID, "fam:" + currentId);
        writer.set(ColdpTerm.status, "synonym");
        synonyms++;
      } else {
        accepted++;
      }

      var altName = col(row, idx, "alternate_name");
      writer.set(ColdpTerm.remarks, altName != null ? "alt.: " + altName : null);
      writer.next();
    }
    parser.stopParsing();
    LOG.info("{} families ({} accepted, {} synonyms)", accepted + synonyms, accepted, synonyms);
  }

  private void parseGenera() throws IOException {
    int accepted = 0, synonyms = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_genus.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      if (!"Y".equals(col(row, idx, "is_web_visible"))) continue;
      var genusId   = intCol(row, idx, "taxonomy_genus_id");
      var currentId = intCol(row, idx, "current_taxonomy_genus_id");
      var famId     = intCol(row, idx, "taxonomy_family_id");
      if (genusId == null) continue;

      writer.set(ColdpTerm.ID, "gen:" + genusId);
      writer.set(ColdpTerm.scientificName, buildGenusName(row, idx));
      writer.set(ColdpTerm.authorship, col(row, idx, "genus_authority"));
      writer.set(ColdpTerm.rank, "genus");

      if (currentId != null && !currentId.equals(genusId)) {
        writer.set(ColdpTerm.parentID, "gen:" + currentId);
        writer.set(ColdpTerm.status, "synonym");
        synonyms++;
      } else {
        writer.set(ColdpTerm.parentID, famId != null ? "fam:" + famId : null);
        accepted++;
      }

      writer.set(ColdpTerm.remarks, col(row, idx, "note"));
      writer.next();
    }
    parser.stopParsing();
    LOG.info("{} genera ({} accepted, {} synonyms)", accepted + synonyms, accepted, synonyms);
  }

  /** Pass 2 over taxonomy_species: write NameUsage and NameRelation records. */
  private void parseSpecies(TermWriter nomRelWriter) throws IOException {
    int accepted = 0, synonyms = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_species.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      if (!"Y".equals(col(row, idx, "is_web_visible"))) continue;
      var speciesId = intCol(row, idx, "taxonomy_species_id");
      var currentId = intCol(row, idx, "current_taxonomy_species_id");
      var genusId   = intCol(row, idx, "taxonomy_genus_id");
      if (speciesId == null) continue;

      String id = "sp:" + speciesId;
      writer.set(ColdpTerm.ID, id);
      writer.set(ColdpTerm.scientificName, col(row, idx, "name"));
      writer.set(ColdpTerm.authorship, col(row, idx, "name_authority"));
      writer.set(ColdpTerm.rank, determineRank(row, idx));
      writer.set(ColdpTerm.nameReferenceID, refIdBySpeciesId.get(speciesId));
      writer.set(ColdpTerm.link, LINK + speciesId);
      writer.set(ColdpTerm.remarks, col(row, idx, "note"));

      boolean isSynonym = currentId != null && !currentId.equals(speciesId);
      if (isSynonym) {
        writer.set(ColdpTerm.parentID, "sp:" + currentId);
        String synCode = col(row, idx, "synonym_code");
        if ("B".equals(synCode)) {
          writer.set(ColdpTerm.status, "homotypic synonym");
          // NameRelation: currentId's name has this name as its basionym
          nomRelWriter.set(ColdpTerm.nameID, "sp:" + currentId);
          nomRelWriter.set(ColdpTerm.relatedNameID, id);
          nomRelWriter.set(ColdpTerm.type, "basionym");
          nomRelWriter.next();
        } else {
          writer.set(ColdpTerm.status, "heterotypic synonym");
        }
        synonyms++;
      } else {
        writer.set(ColdpTerm.parentID, genusId != null ? "gen:" + genusId : null);
        var basionymId = basionymByAcceptedId.get(speciesId);
        if (basionymId != null) writer.set(ColdpTerm.basionymID, "sp:" + basionymId);
        accepted++;
      }
      writer.next();
    }
    parser.stopParsing();
    LOG.info("{} species/infraspecies ({} accepted, {} synonyms)", accepted + synonyms, accepted, synonyms);
  }

  private void parseCommonNames(TermWriter vernWriter) throws IOException {
    int count = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_common_name.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var speciesId = intCol(row, idx, "taxonomy_species_id");
      var genusId   = intCol(row, idx, "taxonomy_genus_id");
      var name      = col(row, idx, "name");
      if (name == null) continue;
      String taxonId = speciesId != null ? "sp:" + speciesId
                     : genusId   != null ? "gen:" + genusId : null;
      if (taxonId == null) continue;
      vernWriter.set(ColdpTerm.taxonID, taxonId);
      vernWriter.set(ColdpTerm.name, name);
      vernWriter.set(ColdpTerm.language, toLangCode(col(row, idx, "language_description")));
      vernWriter.next();
      count++;
    }
    parser.stopParsing();
    LOG.info("{} common names written", count);
  }

  /** Streams the large taxonomy_geography_map.txt file to write Distribution records. */
  private void parseDistributions(TermWriter distWriter) throws IOException {
    int count = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_geography_map.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var speciesId = intCol(row, idx, "taxonomy_species_id");
      var geoId     = intCol(row, idx, "geography_id");
      if (speciesId == null || geoId == null) continue;
      var country = countryByGeoId.get(geoId);
      if (country == null) continue;
      distWriter.set(ColdpTerm.taxonID, "sp:" + speciesId);
      distWriter.set(ColdpTerm.area, country);
      distWriter.set(ColdpTerm.gazetteer, "ISO");
      distWriter.set(ColdpTerm.status, geoStatus(col(row, idx, "geography_status_code")));
      distWriter.next();
      count++;
    }
    parser.stopParsing();
    LOG.info("{} distribution records written", count);
  }

  private void parseUses(TermWriter propWriter) throws IOException {
    int count = 0;
    var parser = tsvParser();
    parser.beginParsing(UTF8IoUtils.readerFromFile(sourceFile("taxonomy_use.txt")));
    var idx = indexMap(parser.getContext().headers());
    String[] row;
    while ((row = parser.parseNext()) != null) {
      var speciesId = intCol(row, idx, "taxonomy_species_id");
      var usage     = col(row, idx, "economic_usage_code");
      var type      = col(row, idx, "usage_type");
      if (speciesId == null || usage == null) continue;
      propWriter.set(ColdpTerm.taxonID, "sp:" + speciesId);
      propWriter.set(ColdpTerm.property, "Economic Use - " + capitalise(usage));
      propWriter.set(ColdpTerm.value, type);
      propWriter.next();
      count++;
    }
    parser.stopParsing();
    LOG.info("{} economic use properties written", count);
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private TsvParser tsvParser() {
    var settings = new TsvParserSettings();
    settings.setNullValue(null);
    settings.setHeaderExtractionEnabled(true);
    settings.setLineSeparatorDetectionEnabled(true);
    settings.setMaxCharsPerColumn(65536);
    return new TsvParser(settings);
  }

  /** Build a column-name → index map, stripping BOM from the first header. */
  private static Map<String, Integer> indexMap(String[] headers) {
    var map = new HashMap<String, Integer>();
    if (headers == null) return map;
    for (int i = 0; i < headers.length; i++) {
      String h = headers[i];
      if (h != null) map.put(h.replace("\uFEFF", "").strip(), i);
    }
    return map;
  }

  /** Returns the column value, or null for \N or blank. */
  private static String col(String[] row, Map<String, Integer> idx, String col) {
    Integer i = idx.get(col);
    if (i == null || i >= row.length) return null;
    String v = row[i];
    return (v == null || v.equals("\\N") || v.isBlank()) ? null : v.strip();
  }

  private static Integer intCol(String[] row, Map<String, Integer> idx, String col) {
    String v = col(row, idx, col);
    return v == null ? null : Integer.parseInt(v);
  }

  private static String determineRank(String[] row, Map<String, Integer> idx) {
    if (col(row, idx, "forma_name") != null) {
      String ft = col(row, idx, "forma_rank_type");
      return ft != null ? ft.toLowerCase() : "form";
    }
    if (col(row, idx, "subvariety_name") != null) return "subvariety";
    if (col(row, idx, "variety_name")    != null) return "variety";
    if (col(row, idx, "subspecies_name") != null) return "subspecies";
    return "species";
  }

  private static String buildGenusName(String[] row, Map<String, Integer> idx) {
    String hybrid = col(row, idx, "hybrid_code");
    String name   = col(row, idx, "genus_name");
    if (name == null) return null;
    if ("+".equals(hybrid)) return "+" + name;
    if ("x".equalsIgnoreCase(hybrid) || "×".equals(hybrid)) return "×" + name;
    return name;
  }

  private static String geoStatus(String code) {
    if (code == null) return null;
    return switch (code) {
      case "n" -> "native";
      case "i" -> "introduced";
      case "c" -> "cultivated";
      case "a" -> "naturalised";
      default  -> null;
    };
  }

  private static String capitalise(String s) {
    if (s == null || s.isEmpty()) return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
  }

  private static String join(String a, String b) {
    if (a == null) return b;
    if (b == null) return a;
    return a + ", " + b;
  }

  private static String toLangCode(String lang) {
    if (lang == null) return null;
    // Normalize by taking the base language before any parenthesized region
    String base = lang.replaceAll("\\s*\\(.*\\)", "").strip().toLowerCase();
    String code = LANG_MAP.get(base);
    return code != null ? code : lang;
  }

  private static Map<String, String> buildLangMap() {
    var m = new HashMap<String, String>();
    m.put("english",                "eng");
    m.put("french",                 "fra");
    m.put("spanish",                "spa");
    m.put("castellano",             "spa");
    m.put("german",                 "deu");
    m.put("portuguese",             "por");
    m.put("chinese",                "zho");
    m.put("transcribed chinese",    "zho");
    m.put("japanese",               "jpn");
    m.put("japanese rōmaji",        "jpn");
    m.put("arabic",                 "ara");
    m.put("russian",                "rus");
    m.put("transliterated russian", "rus");
    m.put("korean",                 "kor");
    m.put("transcribed korean",     "kor");
    m.put("italian",                "ita");
    m.put("dutch",                  "nld");
    m.put("swedish",                "swe");
    m.put("danish",                 "dan");
    m.put("norwegian",              "nor");
    m.put("finnish",                "fin");
    m.put("polish",                 "pol");
    m.put("czech",                  "ces");
    m.put("hungarian",              "hun");
    m.put("romanian",               "ron");
    m.put("turkish",                "tur");
    m.put("thai",                   "tha");
    m.put("hindi",                  "hin");
    m.put("malay",                  "msa");
    m.put("indonesian",             "ind");
    m.put("afrikaans",              "afr");
    m.put("swahili",                "swa");
    m.put("malagasy",               "mlg");
    m.put("hawaiian",               "haw");
    m.put("india",                  "hin");
    m.put("india (hindi)",          "hin");
    return Collections.unmodifiableMap(m);
  }
}
