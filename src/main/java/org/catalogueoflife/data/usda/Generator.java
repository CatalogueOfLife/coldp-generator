package org.catalogueoflife.data.usda;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.univocity.parsers.csv.CsvParser;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.CsvUtils;
import org.gbif.nameparser.NameParserGBIF;
import org.gbif.nameparser.api.ParsedName;
import org.gbif.nameparser.api.Rank;
import org.gbif.nameparser.api.UnparsableNameException;
import org.gbif.nameparser.util.NameFormatter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

public class Generator extends AbstractColdpGenerator {

  private static final String TXT_FN      = "plantlst.txt";
  private static final URI    TXT_URI     = URI.create(
      "https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt");
  private static final String API_BASE    = "https://plantsservices.sc.egov.usda.gov/api/";
  private static final String PROFILE_URL = "https://plants.sc.egov.usda.gov/plant-profile/";
  private static final String IMG_URL     = "https://plants.sc.egov.usda.gov/ImageLibrary/standard/";
  private static final int    ENRICH_THREADS = 10;

  private static final NameParserGBIF NAME_PARSER = new NameParserGBIF();

  private static final int COL_SYMBOL  = 0;
  private static final int COL_SYN_SYM = 1;
  private static final int COL_NAME    = 2;
  private static final int COL_COMMON  = 3;
  private static final int COL_FAMILY  = 4;

  // built during first pass
  private final Map<String, String> genusToSymbol = new LinkedHashMap<>();
  private final Map<String, String> genusToFamily = new LinkedHashMap<>();
  private final Set<String>         familyNames   = new LinkedHashSet<>();
  private final List<String>        acceptedSymbols = new ArrayList<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    File txt = download(TXT_FN, TXT_URI);

    // ── Pass 1: build genus/family lookup maps ────────────────────────────────
    LOG.info("Pass 1: building genus/family lookup maps");
    CsvParser parser = CsvUtils.newCsvParser(512);
    parser.beginParsing(txt);
    parser.parseNext(); // skip header
    String[] row;
    while ((row = parser.parseNext()) != null) {
      String symbol   = col(row, COL_SYMBOL);
      String synSym   = col(row, COL_SYN_SYM);
      String nameAuth = col(row, COL_NAME);
      String family   = col(row, COL_FAMILY);
      if (symbol == null || nameAuth == null) continue;

      if (synSym == null) {
        String genusName = extractGenus(nameAuth);
        if (family != null) {
          familyNames.add(family);
          genusToFamily.put(genusName, family);
        }
        if (isUninomial(nameAuth)) {
          genusToSymbol.put(genusName, symbol);
        } else {
          acceptedSymbols.add(symbol);
        }
      }
    }
    parser.stopParsing();
    LOG.info("Pass 1 complete: {} families, {} genera ({} with symbols), {} species/infraspecifics",
        familyNames.size(), genusToFamily.size(), genusToSymbol.size(), acceptedSymbols.size());

    // ── Writers ───────────────────────────────────────────────────────────────
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID, ColdpTerm.parentID, ColdpTerm.rank,
        ColdpTerm.scientificName, ColdpTerm.authorship,
        ColdpTerm.family, ColdpTerm.status, ColdpTerm.link));
    TermWriter vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID, ColdpTerm.name, ColdpTerm.language));
    TermWriter distWriter  = null;
    TermWriter propWriter  = null;
    TermWriter mediaWriter = null;
    if (cfg.enrich) {
      distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
          ColdpTerm.taxonID, ColdpTerm.area, ColdpTerm.gazetteer, ColdpTerm.status));
      propWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
          ColdpTerm.taxonID, ColdpTerm.property, ColdpTerm.value));
      mediaWriter = additionalWriter(ColdpTerm.Media, List.of(
          ColdpTerm.taxonID, ColdpTerm.url, ColdpTerm.type));
    }

    // ── Write synthetic family rows ───────────────────────────────────────────
    for (String family : familyNames) {
      writer.set(ColdpTerm.ID,             "fam:" + family);
      writer.set(ColdpTerm.rank,           "family");
      writer.set(ColdpTerm.scientificName, family);
      writer.set(ColdpTerm.status,         "accepted");
      writer.next();
    }

    // ── Write synthetic genus rows for genera absent from file ────────────────
    Set<String> presentGenera = new HashSet<>(genusToSymbol.keySet());
    for (Map.Entry<String, String> e : genusToFamily.entrySet()) {
      String genusName = e.getKey();
      String family    = e.getValue();
      if (!presentGenera.contains(genusName)) {
        writer.set(ColdpTerm.ID,             "gen:" + genusName);
        writer.set(ColdpTerm.parentID,       "fam:" + family);
        writer.set(ColdpTerm.rank,           "genus");
        writer.set(ColdpTerm.scientificName, genusName);
        writer.set(ColdpTerm.status,         "accepted");
        writer.next();
      }
    }

    // ── Pass 2: write all file rows ───────────────────────────────────────────
    LOG.info("Pass 2: writing NameUsage and VernacularName");
    parser = CsvUtils.newCsvParser(512);
    parser.beginParsing(txt);
    parser.parseNext(); // skip header
    int accepted = 0, synonyms = 0;
    while ((row = parser.parseNext()) != null) {
      String symbol   = col(row, COL_SYMBOL);
      String synSym   = col(row, COL_SYN_SYM);
      String nameAuth = col(row, COL_NAME);
      String common   = col(row, COL_COMMON);
      String family   = col(row, COL_FAMILY);
      if (symbol == null || nameAuth == null) continue;

      if (synSym == null) {
        // accepted row
        ParseResult pr = parseName(nameAuth);
        String genusName = extractGenus(nameAuth);
        String parentId;
        if (isUninomial(nameAuth)) {
          parentId = family != null ? "fam:" + family : null;
        } else {
          parentId = genusToSymbol.containsKey(genusName)
              ? genusToSymbol.get(genusName)
              : "gen:" + genusName;
        }
        writer.set(ColdpTerm.ID,             symbol);
        writer.set(ColdpTerm.parentID,       parentId);
        writer.set(ColdpTerm.rank,           pr.rank);
        writer.set(ColdpTerm.scientificName, pr.name);
        writer.set(ColdpTerm.authorship,     pr.authorship);
        if (!isUninomial(nameAuth)) writer.set(ColdpTerm.family, family);
        writer.set(ColdpTerm.status,         "accepted");
        writer.set(ColdpTerm.link,           PROFILE_URL + symbol);
        writer.next();
        accepted++;

        if (common != null) {
          vernWriter.set(ColdpTerm.taxonID,  symbol);
          vernWriter.set(ColdpTerm.name,     common);
          vernWriter.set(ColdpTerm.language, "eng");
          vernWriter.next();
        }

      } else {
        // synonym row — symbol = accepted symbol, synSym = synonym's own symbol
        ParseResult pr = parseName(nameAuth);
        writer.set(ColdpTerm.ID,             synSym);
        writer.set(ColdpTerm.parentID,       symbol);
        writer.set(ColdpTerm.rank,           pr.rank);
        writer.set(ColdpTerm.scientificName, pr.name);
        writer.set(ColdpTerm.authorship,     pr.authorship);
        writer.set(ColdpTerm.status,         "synonym");
        writer.next();
        synonyms++;
      }
    }
    parser.stopParsing();
    LOG.info("Pass 2 complete: {} accepted, {} synonyms", accepted, synonyms);

    if (cfg.enrich) {
      enrich(distWriter, propWriter, mediaWriter);
    }
  }

  // ── Name parsing ─────────────────────────────────────────────────────────────

  record ParseResult(String name, String authorship, String rank) {}

  private static ParseResult parseName(String nameAuth) {
    if (nameAuth == null) return new ParseResult(null, null, null);
    try {
      ParsedName pn = NAME_PARSER.parse(nameAuth);
      String name = pn.canonicalNameWithoutAuthorship();
      String auth = NameFormatter.authorshipComplete(pn);
      String rank = rankLabel(pn.getRank());
      return new ParseResult(
          (name == null || name.isBlank()) ? nameAuth : name,
          (auth == null || auth.isBlank()) ? null : auth,
          rank);
    } catch (UnparsableNameException | InterruptedException e) {
      return new ParseResult(nameAuth, null, null);
    }
  }

  private static String rankLabel(Rank rank) {
    if (rank == null || rank == Rank.UNRANKED || rank == Rank.OTHER) return null;
    return rank.name().toLowerCase(Locale.ENGLISH);
  }

  private static String extractGenus(String nameAuth) {
    if (nameAuth == null) return null;
    String first = nameAuth.split("\\s+")[0];
    return first.startsWith("×") ? first.substring(1) : first;
  }

  /** True when the name is a uninomial (genus-rank entry):
   *  either a single token, or the second token starts with an uppercase letter (authorship). */
  private static boolean isUninomial(String nameAuth) {
    if (nameAuth == null) return false;
    String[] parts = nameAuth.split("\\s+");
    return parts.length < 2 || Character.isUpperCase(parts[1].charAt(0));
  }

  private static String col(String[] row, int index) {
    if (row == null || index >= row.length) return null;
    String v = row[index];
    return (v == null || v.isBlank()) ? null : v.strip();
  }

  // ── Enrichment (PlantProfile API) ────────────────────────────────────────────

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class PlantProfile {
    @JsonProperty("Durations")            List<String>       durations;
    @JsonProperty("GrowthHabits")         List<String>       growthHabits;
    @JsonProperty("Group")                String             group;
    @JsonProperty("NativeStatuses")       List<NativeStatus> nativeStatuses;
    @JsonProperty("ProfileImageFilename") String             profileImageFilename;
    @JsonProperty("OtherCommonNames")     List<String>       otherCommonNames;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class NativeStatus {
    @JsonProperty("Region") String region;
    @JsonProperty("Status") String status;
  }

  private void enrich(TermWriter distWriter, TermWriter propWriter, TermWriter mediaWriter)
      throws Exception {
    LOG.info("Enrichment: fetching {} profiles with {} threads", acceptedSymbols.size(), ENRICH_THREADS);
    ExecutorService exec = Executors.newFixedThreadPool(ENRICH_THREADS);
    try {
      List<CompletableFuture<Void>> fetches = new ArrayList<>();
      for (String symbol : acceptedSymbols) {
        fetches.add(CompletableFuture.runAsync(() -> {
          try { downloadProfile(symbol); }
          catch (Exception ex) { LOG.warn("Profile download failed for {}: {}", symbol, ex.getMessage()); }
        }, exec));
      }
      CompletableFuture.allOf(fetches.toArray(new CompletableFuture[0])).join();
      LOG.info("Enrichment downloads complete; writing ColDP output");

      for (String symbol : acceptedSymbols) {
        File cache = sourceFile("profile-" + symbol + ".json");
        if (!cache.exists()) continue;
        try {
          PlantProfile profile = mapper.readValue(cache, PlantProfile.class);
          writeEnrichment(symbol, profile, distWriter, propWriter, mediaWriter);
        } catch (Exception ex) {
          LOG.warn("Failed to process profile for {}: {}", symbol, ex.getMessage());
        }
      }
    } finally {
      exec.shutdown();
    }
  }

  private void downloadProfile(String symbol) throws IOException {
    File cache = sourceFile("profile-" + symbol + ".json");
    if (cache.exists() || cfg.noDownload) return;
    String json = http.getJSON(URI.create(API_BASE + "PlantProfile?symbol=" + symbol));
    try (var w = UTF8IoUtils.writerFromFile(cache)) {
      w.write(json);
    }
  }

  private void writeEnrichment(String symbol, PlantProfile p,
                                TermWriter distWriter, TermWriter propWriter,
                                TermWriter mediaWriter) throws IOException {
    if (p.nativeStatuses != null) {
      for (NativeStatus ns : p.nativeStatuses) {
        if (ns.region == null || ns.status == null) continue;
        distWriter.set(ColdpTerm.taxonID,   symbol);
        distWriter.set(ColdpTerm.area,      ns.region);
        distWriter.set(ColdpTerm.gazetteer, "text");
        distWriter.set(ColdpTerm.status,    nativeStatus(ns.status));
        distWriter.next();
      }
    }

    if (p.durations != null) {
      for (String d : p.durations) {
        if (d == null || d.isBlank()) continue;
        propWriter.set(ColdpTerm.taxonID,  symbol);
        propWriter.set(ColdpTerm.property, "duration");
        propWriter.set(ColdpTerm.value,    d);
        propWriter.next();
      }
    }

    if (p.growthHabits != null) {
      for (String g : p.growthHabits) {
        if (g == null || g.isBlank()) continue;
        propWriter.set(ColdpTerm.taxonID,  symbol);
        propWriter.set(ColdpTerm.property, "growth habit");
        propWriter.set(ColdpTerm.value,    g);
        propWriter.next();
      }
    }

    if (p.group != null && !p.group.isBlank()) {
      propWriter.set(ColdpTerm.taxonID,  symbol);
      propWriter.set(ColdpTerm.property, "group");
      propWriter.set(ColdpTerm.value,    p.group);
      propWriter.next();
    }

    if (p.profileImageFilename != null && !p.profileImageFilename.isBlank()) {
      mediaWriter.set(ColdpTerm.taxonID, symbol);
      mediaWriter.set(ColdpTerm.url,     IMG_URL + p.profileImageFilename);
      mediaWriter.set(ColdpTerm.type,    "image");
      mediaWriter.next();
    }
  }

  private static String nativeStatus(String code) {
    return switch (code) {
      case "N"  -> "native";
      case "I"  -> "introduced";
      case "N?" -> "native";
      case "I?" -> "introduced";
      default   -> "uncertain";
    };
  }
}
