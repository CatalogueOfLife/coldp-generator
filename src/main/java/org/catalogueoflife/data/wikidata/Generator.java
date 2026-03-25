package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.AltIdBuilder;

import java.io.*;
import java.net.URI;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.catalogueoflife.data.wikidata.WikidataDumpReader.*;

public class Generator extends AbstractColdpGenerator {
  // Swedish mirror — much faster from Copenhagen; has both dumps at stable "latest" paths.
  private static final String WIKIDATA_MIRROR_URL  = "https://ftp.acc.umu.se/mirror/wikimedia.org/other/wikibase/wikidatawiki/latest-all.json.gz";
  private static final String COMMONS_MIRROR_BASE  = "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/commonswiki/";
  // Main Wikimedia site — fallback when the mirror is unreachable or has no matching dump.
  private static final String WIKIDATA_FALLBACK_URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz";
  // Multistream format: the bzip2 file is composed of independent streams, enabling parallel decompression.
  // Companion index maps byte_offset → page titles so individual streams can be sought and processed concurrently.
  private static final String COMMONS_FALLBACK_URL       = "https://dumps.wikimedia.org/commonswiki/latest/commonswiki-latest-pages-articles-multistream.xml.bz2";
  private static final String COMMONS_INDEX_FALLBACK_URL = "https://dumps.wikimedia.org/commonswiki/latest/commonswiki-latest-pages-articles-multistream-index.txt.bz2";
  private static final String DUMP_FILENAME              = "latest-all.json.gz";
  private static final String COMMONS_DUMP_FILENAME      = "commonswiki-latest-pages-articles-multistream.xml.bz2";
  private static final String COMMONS_INDEX_FILENAME     = "commonswiki-latest-pages-articles-multistream-index.txt.bz2";

  private TermWriter vernWriter;
  private TermWriter distWriter;
  private TermWriter propWriter;
  private TermWriter nameRelWriter;
  private TermWriter mediaWriter;
  // File[0] = multistream dump, File[1] = multistream index
  private CompletableFuture<File[]> commonsDumpFuture;
  private PrintWriter duplicateLogWriter;
  private final Map<String, String> rankMap = new HashMap<>();
  private final Set<String> writtenRefs    = new HashSet<>();
  private final Set<String> usedExtIdPids  = new HashSet<>();

  // Maps for nom status (P1135) and gender (P2433) QIDs — resolved during pass 1 / SPARQL
  // Key: Wikidata label (lowercase) → ColDP nameStatus value
  private static final Map<String, String> NOM_STATUS_LABEL_MAP = buildNomStatusLabelMap();
  // Key: Wikidata label (lowercase) → ColDP gender value
  private static final Map<String, String> GENDER_LABEL_MAP = Map.of(
      "masculine", "masculine",
      "masculine gender", "masculine",
      "feminine", "feminine",
      "feminine gender", "feminine",
      "neuter", "neuter",
      "neuter gender", "neuter"
  );

  private static Map<String, String> buildNomStatusLabelMap() {
    Map<String, String> m = new HashMap<>();
    // nomen validum / established
    m.put("nomen validum", "established");
    m.put("nomen novum", "established");
    // nomen nudum / not established
    m.put("nomen nudum", "not established");
    m.put("not established", "not established");
    // nomen conservandum / conserved
    m.put("nomen conservandum", "conserved");
    m.put("conserved name", "conserved");
    // nomen rejectum / rejected
    m.put("nomen rejectum", "rejected");
    m.put("rejected name", "rejected");
    // nomen illegitimum / unacceptable
    m.put("nomen illegitimum", "unacceptable");
    m.put("illegitimate name", "unacceptable");
    // nomen ambiguum / nomen dubium → doubtful
    m.put("nomen ambiguum", "doubtful");
    m.put("nomen dubium", "doubtful");
    m.put("doubtful name", "doubtful");
    return Collections.unmodifiableMap(m);
  }

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
    initRankMap();
  }

  private void initRankMap() {
    rankMap.put("Q146481", "domain");
    rankMap.put("Q36732", "kingdom");
    rankMap.put("Q3491997", "superphylum");
    rankMap.put("Q38348", "phylum");
    rankMap.put("Q1153785", "subphylum");
    rankMap.put("Q3150876", "infraphylum");
    rankMap.put("Q5868149", "superclass");
    rankMap.put("Q37517", "class");
    rankMap.put("Q5867959", "subclass");
    rankMap.put("Q2361851", "infraclass");
    rankMap.put("Q2752679", "megaorder");
    rankMap.put("Q3504061", "superorder");
    rankMap.put("Q36602", "order");
    rankMap.put("Q5868144", "suborder");
    rankMap.put("Q2889003", "infraorder");
    rankMap.put("Q1799072", "parvorder");
    rankMap.put("Q5867051", "superfamily");
    rankMap.put("Q35409", "family");
    rankMap.put("Q2007442", "subfamily");
    rankMap.put("Q227936", "tribe");
    rankMap.put("Q3965313", "subtribe");
    rankMap.put("Q34740", "genus");
    rankMap.put("Q855769", "subgenus");
    rankMap.put("Q3181348", "section");
    rankMap.put("Q3025161", "subsection");
    rankMap.put("Q2007497", "series");
    rankMap.put("Q7432", "species");
    rankMap.put("Q68947", "subspecies");
    rankMap.put("Q767728", "variety");
    rankMap.put("Q630136", "subvariety");
    rankMap.put("Q3238261", "form");
    rankMap.put("Q14817220", "subform");
    rankMap.put("Q2455704", "cultivar");
    rankMap.put("Q1521892", "cohort");
  }

  @Override
  protected void prepare() throws Exception {
    // Wikidata dump — main thread; prefer Swedish mirror, fall back to main site.
    String wikidataUrl = resolveUrl("Wikidata", WIKIDATA_MIRROR_URL, WIKIDATA_FALLBACK_URL);
    File dumpFile = sourceFile(DUMP_FILENAME);
    if (dumpFile.exists()) {
      if (!cfg.noDownload && isRemoteNewer(dumpFile, wikidataUrl)) {
        LOG.info("Remote Wikidata dump is newer, re-downloading...");
        dumpFile.delete();
        downloadFile(wikidataUrl, dumpFile);
      } else {
        LOG.info("Reusing cached Wikidata dump: {}", dumpFile);
      }
    } else if (cfg.noDownload) {
      throw new IllegalStateException("--no-download set but Wikidata dump not found: " + dumpFile);
    } else {
      downloadFile(wikidataUrl, dumpFile);
    }

    // Commons dump + index — background thread (runs in parallel with Wikidata passes).
    // The mirror uses dated directories (e.g. 20260301/); we discover the latest one.
    // Both the multistream dump and its companion index are downloaded together.
    String[] commonsUrls = resolveCommonsDumpUrls();
    String commonsDumpUrl  = commonsUrls[0];
    String commonsIndexUrl = commonsUrls[1];
    File commonsDumpFile  = sourceFile(COMMONS_DUMP_FILENAME);
    File commonsIndexFile = sourceFile(COMMONS_INDEX_FILENAME);
    commonsDumpFuture = CompletableFuture.supplyAsync(() -> {
      try {
        if (cfg.noDownload || (commonsDumpFile.exists() && !isRemoteNewer(commonsDumpFile, commonsDumpUrl))) {
          LOG.info("Reusing cached Commons dump: {}", commonsDumpFile);
        } else {
          LOG.info("Downloading Commons dump (~106 GB, running in background)... {}", commonsDumpUrl);
          http.download(commonsDumpUrl, commonsDumpFile);
          LOG.info("Commons dump download complete: {}", commonsDumpFile);
        }
        if (cfg.noDownload || (commonsIndexFile.exists() && !isRemoteNewer(commonsIndexFile, commonsIndexUrl))) {
          LOG.info("Reusing cached Commons index: {}", commonsIndexFile);
        } else {
          LOG.info("Downloading Commons multistream index... {}", commonsIndexUrl);
          http.download(commonsIndexUrl, commonsIndexFile);
          LOG.info("Commons index download complete: {}", commonsIndexFile);
        }
        return new File[]{commonsDumpFile, commonsIndexFile};
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  /**
   * Returns the mirror URL if reachable (HTTP 2xx), otherwise the fallback URL.
   */
  private String resolveUrl(String label, String mirrorUrl, String fallbackUrl) {
    try {
      if (http.exists(mirrorUrl)) {
        LOG.info("{} dump: using Swedish mirror {}", label, mirrorUrl);
        return mirrorUrl;
      }
    } catch (Exception e) {
      LOG.warn("{} dump: mirror check failed ({}), falling back to main site", label, e.getMessage());
    }
    LOG.info("{} dump: using main Wikimedia site {}", label, fallbackUrl);
    return fallbackUrl;
  }

  /**
   * Discovers the latest Commons multistream dump and its index on the Swedish mirror.
   * Returns {@code String[]{dumpUrl, indexUrl}}.
   * Falls back to the main Wikimedia site if the mirror is unreachable or yields no valid date.
   */
  private String[] resolveCommonsDumpUrls() {
    try {
      String listing = http.get(URI.create(COMMONS_MIRROR_BASE));
      Pattern p = Pattern.compile("href=\"(\\d{8})/\"");
      Matcher m = p.matcher(listing);
      String latest = null;
      while (m.find()) {
        String date = m.group(1);
        if (latest == null || date.compareTo(latest) > 0) latest = date;
      }
      if (latest != null) {
        String base = COMMONS_MIRROR_BASE + latest + "/commonswiki-" + latest;
        String dumpUrl  = base + "-pages-articles-multistream.xml.bz2";
        String indexUrl = base + "-pages-articles-multistream-index.txt.bz2";
        if (http.exists(dumpUrl)) {
          LOG.info("Commons dump: using Swedish mirror, date={}, url={}", latest, dumpUrl);
          return new String[]{dumpUrl, indexUrl};
        }
        LOG.warn("Commons dump: mirror date {} found but multistream file not available, falling back", latest);
      } else {
        LOG.warn("Commons dump: no dated directories found in mirror listing");
      }
    } catch (Exception e) {
      LOG.warn("Commons dump: mirror resolution failed ({}), falling back to main site", e.getMessage());
    }
    LOG.info("Commons dump: using main Wikimedia site {}", COMMONS_FALLBACK_URL);
    return new String[]{COMMONS_FALLBACK_URL, COMMONS_INDEX_FALLBACK_URL};
  }

  private boolean isRemoteNewer(File localFile, String url) {
    try {
      var resp = http.head(url);
      var lastModHeader = resp.headers().firstValue("Last-Modified").orElse(null);
      if (lastModHeader != null) {
        var remoteDate = ZonedDateTime.parse(lastModHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
        long remoteMillis = remoteDate.toInstant().toEpochMilli();
        return remoteMillis > localFile.lastModified();
      }
    } catch (Exception e) {
      LOG.warn("Failed to check remote freshness for {}, will reuse local file", url, e);
    }
    return false;
  }

  private void downloadFile(String url, File target) throws IOException {
    LOG.info("Downloading {} to {} (this may take a while...)", url, target);
    http.download(url, target);
    LOG.info("Download complete: {}", target);
  }

  @Override
  protected void addData() throws Exception {
    initWriters();
    File dumpFile = sourceFile(DUMP_FILENAME);

    WikidataDumpReader reader = new WikidataDumpReader();

    LOG.info("Starting pass 1: collecting lookup maps...");
    reader.collectLookups(dumpFile, rankMap);

    resolveUnresolved(reader);

    LOG.info("Starting pass 2: emitting ColDP records...");
    try {
      emitColdpRecords(dumpFile, reader);
    } finally {
      if (duplicateLogWriter != null) {
        duplicateLogWriter.close();
      }
    }

    // Write only the identifier properties actually used in the generated archive
    writeIdentifierRegistry(reader);

    // Wait for Commons dump + index download (typically already done; Wikidata passes take many hours)
    File[] commonsFiles;
    try {
      commonsFiles = commonsDumpFuture.get();
    } catch (ExecutionException e) {
      LOG.error("Commons dump download failed, skipping media gallery phase", e.getCause());
      return;
    }
    crawlCommonsMedia(commonsFiles[0], commonsFiles[1], reader);
  }

  private void resolveUnresolved(WikidataDumpReader reader) {
    if (!reader.neededRankQids.isEmpty()) {
      LOG.info("Resolving {} unresolved rank QIDs via SPARQL...", reader.neededRankQids.size());
      resolveLabels(reader.neededRankQids, reader.rankLabels, true);
    }
    if (!reader.neededIucnQids.isEmpty()) {
      LOG.info("Resolving {} unresolved IUCN QIDs via SPARQL...", reader.neededIucnQids.size());
      resolveLabels(reader.neededIucnQids, reader.iucnLabels, false);
    }
    if (!reader.neededNomStatusQids.isEmpty()) {
      LOG.info("Resolving {} unresolved nom-status/gender QIDs via SPARQL...", reader.neededNomStatusQids.size());
      resolveLabels(reader.neededNomStatusQids, reader.nomStatusLabels, false);
    }
    if (!reader.neededAreaQids.isEmpty()) {
      LOG.info("Resolving {} unresolved area QIDs via SPARQL...", reader.neededAreaQids.size());
      resolveAreas(reader.neededAreaQids, reader.areaInfo);
    }
    if (!reader.neededJournalQids.isEmpty()) {
      LOG.info("Resolving {} unresolved journal QIDs via SPARQL...", reader.neededJournalQids.size());
      resolveLabels(reader.neededJournalQids, reader.journalLabels, false);
    }
    if (!reader.neededPubQids.isEmpty()) {
      LOG.info("Resolving {} unresolved publication QIDs via SPARQL...", reader.neededPubQids.size());
      resolvePubs(reader.neededPubQids, reader.pubInfo, reader.neededJournalQids, reader.journalLabels);
    }
  }

  /**
   * Resolve labels for a set of QIDs via SPARQL VALUES query in batches of 200.
   */
  private void resolveLabels(Set<String> qids, Map<String, String> target, boolean lowercase) {
    List<String> qidList = new ArrayList<>(qids);
    for (int i = 0; i < qidList.size(); i += 200) {
      List<String> batch = qidList.subList(i, Math.min(i + 200, qidList.size()));
      StringBuilder values = new StringBuilder();
      for (String qid : batch) {
        values.append(" wd:").append(qid);
      }
      String sparql = String.format("""
          PREFIX wd: <http://www.wikidata.org/entity/>
          PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          SELECT ?item ?label WHERE {
            VALUES ?item {%s}
            ?item rdfs:label ?label .
            FILTER(LANG(?label) = "en")
          }
          """, values);
      try {
        String result = querySparql(sparql);
        if (result != null) {
          parseSparqlLabels(result, target, lowercase);
        }
      } catch (Exception e) {
        LOG.warn("SPARQL label resolution failed for batch starting at {}: {}", i, e.getMessage());
      }
      sleepBriefly();
    }
    qids.clear();
  }

  private void resolveAreas(Set<String> qids, Map<String, WikidataDumpReader.AreaInfo> target) {
    List<String> qidList = new ArrayList<>(qids);
    for (int i = 0; i < qidList.size(); i += 200) {
      List<String> batch = qidList.subList(i, Math.min(i + 200, qidList.size()));
      StringBuilder values = new StringBuilder();
      for (String qid : batch) {
        values.append(" wd:").append(qid);
      }
      String sparql = String.format("""
          PREFIX wd: <http://www.wikidata.org/entity/>
          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
          PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          SELECT ?item ?label ?iso WHERE {
            VALUES ?item {%s}
            OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
            OPTIONAL { ?item wdt:P297 ?iso }
          }
          """, values);
      try {
        String result = querySparql(sparql);
        if (result != null) {
          parseSparqlAreas(result, target);
        }
      } catch (Exception e) {
        LOG.warn("SPARQL area resolution failed for batch starting at {}: {}", i, e.getMessage());
      }
      sleepBriefly();
    }
    qids.clear();
  }

  private void resolvePubs(Set<String> qids, Map<String, WikidataDumpReader.PubInfo> target,
                           Set<String> journalQids, Map<String, String> journalLabels) {
    List<String> qidList = new ArrayList<>(qids);
    for (int i = 0; i < qidList.size(); i += 200) {
      List<String> batch = qidList.subList(i, Math.min(i + 200, qidList.size()));
      StringBuilder values = new StringBuilder();
      for (String qid : batch) {
        values.append(" wd:").append(qid);
      }
      String sparql = String.format("""
          PREFIX wd: <http://www.wikidata.org/entity/>
          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
          PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          SELECT ?pub
            (SAMPLE(?title) AS ?pubTitle)
            (SAMPLE(?date) AS ?pubDate)
            (SAMPLE(?doi) AS ?pubDOI)
            (SAMPLE(?authorStr) AS ?pubAuthor)
            (SAMPLE(?journal) AS ?pubJournal)
            (SAMPLE(?journalName) AS ?pubJournalName)
            (SAMPLE(?vol) AS ?pubVolume)
            (SAMPLE(?iss) AS ?pubIssue)
            (SAMPLE(?pg) AS ?pubPage)
          WHERE {
            VALUES ?pub {%s}
            OPTIONAL { ?pub wdt:P1476 ?title }
            OPTIONAL { ?pub wdt:P577 ?date }
            OPTIONAL { ?pub wdt:P356 ?doi }
            OPTIONAL { ?pub wdt:P2093 ?authorStr }
            OPTIONAL { ?pub wdt:P1433 ?journal .
                       ?journal rdfs:label ?journalName .
                       FILTER(LANG(?journalName) = "en") }
            OPTIONAL { ?pub wdt:P478 ?vol }
            OPTIONAL { ?pub wdt:P433 ?iss }
            OPTIONAL { ?pub wdt:P304 ?pg }
          }
          GROUP BY ?pub
          """, values);
      try {
        String result = querySparql(sparql);
        if (result != null) {
          parseSparqlPubs(result, target, journalLabels);
        }
      } catch (Exception e) {
        LOG.warn("SPARQL pub resolution failed for batch starting at {}: {}", i, e.getMessage());
      }
      sleepBriefly();
    }
    qids.clear();
  }

  private String querySparql(String sparql) throws IOException {
    try {
      URI uri = URI.create("https://query.wikidata.org/sparql?format=json&query=" +
          java.net.URLEncoder.encode(sparql, "UTF-8"));
      return http.get(uri, Map.of("Accept", "application/sparql-results+json"));
    } catch (Exception e) {
      LOG.warn("SPARQL query failed: {}", e.getMessage());
      return null;
    }
  }

  private void parseSparqlLabels(String json, Map<String, String> target, boolean lowercase) {
    try {
      JsonNode root = mapper.readTree(json);
      JsonNode bindings = root.path("results").path("bindings");
      for (JsonNode binding : bindings) {
        String uri = binding.path("item").path("value").asText(null);
        String label = binding.path("label").path("value").asText(null);
        if (uri != null && label != null) {
          String qid = uri.substring(uri.lastIndexOf('/') + 1);
          target.put(qid, lowercase ? label.toLowerCase() : label);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse SPARQL label results: {}", e.getMessage());
    }
  }

  private void parseSparqlAreas(String json, Map<String, WikidataDumpReader.AreaInfo> target) {
    try {
      JsonNode root = mapper.readTree(json);
      JsonNode bindings = root.path("results").path("bindings");
      for (JsonNode binding : bindings) {
        String uri = binding.path("item").path("value").asText(null);
        if (uri == null) continue;
        String qid = uri.substring(uri.lastIndexOf('/') + 1);
        String label = binding.path("label").path("value").asText(null);
        String iso = binding.path("iso").path("value").asText(null);
        target.put(qid, new WikidataDumpReader.AreaInfo(label, iso));
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse SPARQL area results: {}", e.getMessage());
    }
  }

  private void parseSparqlPubs(String json, Map<String, WikidataDumpReader.PubInfo> target,
                               Map<String, String> journalLabels) {
    try {
      JsonNode root = mapper.readTree(json);
      JsonNode bindings = root.path("results").path("bindings");
      for (JsonNode binding : bindings) {
        String uri = binding.path("pub").path("value").asText(null);
        if (uri == null) continue;
        String qid = uri.substring(uri.lastIndexOf('/') + 1);
        String title = binding.path("pubTitle").path("value").asText(null);
        String date = binding.path("pubDate").path("value").asText(null);
        String doi = binding.path("pubDOI").path("value").asText(null);
        String author = binding.path("pubAuthor").path("value").asText(null);
        String volume = binding.path("pubVolume").path("value").asText(null);
        String issue = binding.path("pubIssue").path("value").asText(null);
        String pages = binding.path("pubPage").path("value").asText(null);
        String journalUri = binding.path("pubJournal").path("value").asText(null);
        String journalName = binding.path("pubJournalName").path("value").asText(null);
        String journalQid = null;
        if (journalUri != null) {
          journalQid = journalUri.substring(journalUri.lastIndexOf('/') + 1);
          if (journalName != null) {
            journalLabels.put(journalQid, journalName);
          }
        }
        target.put(qid, new WikidataDumpReader.PubInfo(title, doi, date, author, journalQid, volume, issue, pages));
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse SPARQL pub results: {}", e.getMessage());
    }
  }

  private void sleepBriefly() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Write a TSV registry of all discovered external identifier properties to the archive.
   * Columns: prefix, property, propertyLink, label, formatterUrl, formatRegex
   */
  private void writeIdentifierRegistry(WikidataDumpReader reader) throws IOException {
    File registryFile = new File(dir, "identifier-registry.tsv");
    LOG.info("Writing identifier registry with {}/{} used properties to {}",
        usedExtIdPids.size(), reader.extIdProperties.size(), registryFile);
    try (PrintWriter pw = new PrintWriter(new FileWriter(registryFile))) {
      pw.println("prefix\tproperty\tpropertyLink\tlabel\tformatterUrl\tformatRegex");
      for (var entry : reader.extIdProperties.entrySet()) {
        String pid = entry.getKey();
        if (!usedExtIdPids.contains(pid)) continue;
        WikidataDumpReader.ExtIdInfo info = entry.getValue();
        pw.printf("%s\t%s\t%s\t%s\t%s\t%s%n",
            info.prefix(),
            pid,
            "https://www.wikidata.org/wiki/Property:" + pid,
            nullToEmpty(info.label()),
            nullToEmpty(info.formatterUrl()),
            nullToEmpty(info.formatRegex()));
      }
    }
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  private void emitColdpRecords(File dumpFile, WikidataDumpReader reader) throws IOException {
    LOG.info("Pass 2: writing ColDP records...");
    int[] taxonCount = {0};
    int[] synCount = {0};
    int[] duplicateCount = {0};
    int[] vernCount = {0};
    int[] distCount = {0};
    int[] propCount = {0};
    int[] nameRelCount = {0};
    int[] mediaCount = {0};

    reader.streamDump(dumpFile, line -> line.contains("\"P225\""), entity -> {
      if (!hasClaim(entity, P225)) return;
      String qid = entity.path("id").asText(null);
      if (qid == null) return;

      // Skip Wikimedia duplicated pages (Q17362920) — log them instead
      if (isInstanceOf(entity, Q17362920)) {
        String name = getStringClaimValue(entity, P225);
        String link = "https://www.wikidata.org/wiki/" + qid;
        duplicateLogWriter.printf("%s\t%s\t%s%n", qid, link, name != null ? name : "");
        duplicateCount[0]++;
        return;
      }

      try {
        writeNameUsage(entity, qid, reader);
        taxonCount[0]++;
        if (hasClaim(entity, P1420)) synCount[0]++;

        vernCount[0] += writeVernacularNames(entity, qid);
        distCount[0] += writeInvasiveDistributions(entity, qid, reader);
        distCount[0] += writeTaxonRangeDistributions(entity, qid, reader);
        propCount[0] += writeIucnStatus(entity, qid, reader);
        nameRelCount[0] += writeNameRelations(entity, qid);
        mediaCount[0]  += writeP18Media(entity, qid);

        if (taxonCount[0] % 100_000 == 0) {
          LOG.info("Pass 2: {} taxa ({} synonyms), {} duplicates skipped, {} vernaculars, {} distributions, {} properties, {} name relations",
              taxonCount[0], synCount[0], duplicateCount[0], vernCount[0], distCount[0], propCount[0], nameRelCount[0]);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    writeReferences(reader);

    LOG.info("Pass 2 complete: {} taxa ({} synonyms), {} duplicates skipped, {} vernaculars, {} distributions, {} properties, {} name relations, {} references, {} P18 media",
        taxonCount[0], synCount[0], duplicateCount[0], vernCount[0], distCount[0], propCount[0], nameRelCount[0], writtenRefs.size(), mediaCount[0]);
  }

  private void crawlCommonsMedia(File dumpFile, File indexFile, WikidataDumpReader reader) throws Exception {
    Set<String> galleryNames = new HashSet<>(reader.galleryNames.values());
    LOG.info("Commons: {} gallery names to resolve; using {} threads for parallel decompression",
        galleryNames.size(), CommonsXmlDumpReader.DEFAULT_THREADS);

    Map<String, List<String>> galleryFiles = new LinkedHashMap<>();
    Map<String, CommonsXmlDumpReader.FileMetadata> fileMeta = new HashMap<>();

    new CommonsXmlDumpReader(dumpFile).streamAllParallel(
        indexFile,
        CommonsXmlDumpReader.DEFAULT_THREADS,
        galleryNames,
        galleryFiles::put,
        fileMeta::put
    );

    // Write Media records: taxon → gallery → files → metadata
    int count = 0;
    Set<String> writtenUrls = new HashSet<>(); // dedup vs P18 records
    for (var e : reader.galleryNames.entrySet()) {
      String qid = e.getKey();
      List<String> files = galleryFiles.get(e.getValue());
      if (files == null) continue;
      for (String filename : files) {
        String norm = filename.replace(' ', '_');
        String url  = "https://commons.wikimedia.org/wiki/Special:FilePath/" + norm;
        if (!writtenUrls.add(qid + "\t" + url)) continue; // dedup
        CommonsXmlDumpReader.FileMetadata meta = fileMeta.get(filename);
        String mime = CommonsXmlDumpReader.mimeFromFilename(filename);
        mediaWriter.set(ColdpTerm.taxonID, qid);
        mediaWriter.set(ColdpTerm.url,     url);
        mediaWriter.set(ColdpTerm.type,    CommonsXmlDumpReader.typeFromMime(mime));
        mediaWriter.set(ColdpTerm.format,  mime);
        mediaWriter.set(ColdpTerm.link,    "https://commons.wikimedia.org/wiki/File:" + norm);
        if (meta != null) {
          mediaWriter.set(ColdpTerm.title,   meta.title());
          mediaWriter.set(ColdpTerm.created, meta.created());
          mediaWriter.set(ColdpTerm.creator, meta.creator());
          mediaWriter.set(ColdpTerm.license, meta.license());
          mediaWriter.set(ColdpTerm.remarks, meta.remarks());
        }
        mediaWriter.next();
        count++;
      }
    }
    LOG.info("Commons media: {} records from {} galleries ({} files with metadata)",
        count, galleryFiles.size(), fileMeta.size());
  }

  private void writeNameUsage(JsonNode entity, String qid, WikidataDumpReader reader) throws IOException {
    JsonNode nameVal = getClaimValue(entity, P225);
    if (nameVal == null) return;
    String name = nameVal.asText(null);
    if (name == null) return;

    writer.set(ColdpTerm.ID, qid);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + qid);

    // Synonym status: P1420 = "taxon synonym of" (accepted name)
    JsonNode acceptedVal = getClaimValue(entity, P1420);
    if (acceptedVal != null) {
      String acceptedQid = getItemId(acceptedVal);
      if (acceptedQid != null) {
        writer.set(ColdpTerm.status, "synonym");
        writer.set(ColdpTerm.parentID, acceptedQid);
      } else {
        writer.set(ColdpTerm.status, "accepted");
      }
    } else {
      writer.set(ColdpTerm.status, "accepted");
      // Parent taxon (only for accepted names)
      JsonNode parentVal = getClaimValue(entity, P171);
      if (parentVal != null) {
        String parentQid = getItemId(parentVal);
        if (parentQid != null) {
          writer.set(ColdpTerm.parentID, parentQid);
        }
      }
    }

    // Rank
    JsonNode rankVal = getClaimValue(entity, P105);
    if (rankVal != null) {
      String rankQid = getItemId(rankVal);
      if (rankQid != null) {
        writer.set(ColdpTerm.rank, reader.rankLabels.get(rankQid));
      }
    }

    // Authorship (author citation string)
    String auth = getStringClaimValue(entity, P835);
    if (auth != null) {
      writer.set(ColdpTerm.authorship, auth);
    }

    // Basionym
    JsonNode basVal = getClaimValue(entity, P566);
    if (basVal != null) {
      String basQid = getItemId(basVal);
      if (basQid != null) {
        writer.set(ColdpTerm.basionymID, basQid);
      }
    }

    // Nomenclatural reference (from pass 1 — "first valid description" reference on P225)
    WikidataDumpReader.NomRef ref = reader.nomRefs.get(qid);
    String publishedInYear = null;
    if (ref != null) {
      writer.set(ColdpTerm.referenceID, ref.pubQid());
      writer.set(ColdpTerm.publishedInPage, ref.page());
      writer.set(ColdpTerm.publishedInPageLink, ref.bhlPageLink());
      WikidataDumpReader.PubInfo pub = reader.pubInfo.get(ref.pubQid());
      if (pub != null && pub.date() != null) {
        publishedInYear = extractYear(pub.date());
      }
    }

    // P225 statement qualifiers: year (P574), original spelling (P1353), nom status (P1135), gender (P2433)
    JsonNode p225Claims = entity.path("claims").path(P225);
    if (p225Claims.isArray() && !p225Claims.isEmpty()) {
      JsonNode qualifiers = p225Claims.get(0).path("qualifiers");

      // Year of publication (P574) — use if not already resolved from publication record
      if (publishedInYear == null) {
        String qualYear = getSnakStringValue(qualifiers, P574);
        if (qualYear != null) {
          publishedInYear = extractYear(qualYear);
        }
      }

      // Original combination / spelling (P1353)
      String origSpelling = getSnakStringValue(qualifiers, P1353);
      if (origSpelling != null) {
        writer.set(ColdpTerm.originalSpelling, origSpelling);
      }

      // Nomenclatural status (P1135)
      String nomStatQid = getSnakItemId(qualifiers, P1135);
      if (nomStatQid != null) {
        String nomLabel = reader.nomStatusLabels.get(nomStatQid);
        if (nomLabel != null) {
          String mapped = mapNomStatus(nomLabel);
          if (mapped != null) {
            writer.set(ColdpTerm.nameStatus, mapped);
          }
        }
      }

      // Gender (P2433) — first try as qualifier on P225
      String genderQid = getSnakItemId(qualifiers, P2433);
      if (genderQid != null) {
        String genderLabel = reader.nomStatusLabels.get(genderQid);
        if (genderLabel != null) {
          String mapped = mapGender(genderLabel);
          if (mapped != null) writer.set(ColdpTerm.gender, mapped);
        }
      }
    }

    // Gender (P2433) — also try as direct property if not set from qualifier
    if (p225Claims.isEmpty() || p225Claims.get(0).path("qualifiers").path(P2433).isMissingNode()) {
      JsonNode genderVal = getClaimValue(entity, P2433);
      if (genderVal != null) {
        String genderQid = getItemId(genderVal);
        if (genderQid != null) {
          String genderLabel = reader.nomStatusLabels.get(genderQid);
          if (genderLabel != null) {
            String mapped = mapGender(genderLabel);
            if (mapped != null) writer.set(ColdpTerm.gender, mapped);
          }
        }
      }
    }

    if (publishedInYear != null) {
      writer.set(ColdpTerm.publishedInYear, publishedInYear);
    }

    // English Wikipedia link → add to remarks
    String wikiTitle = entity.path("sitelinks").path("enwiki").path("title").asText(null);
    if (wikiTitle != null) {
      String wikiLink = "https://en.wikipedia.org/wiki/" + wikiTitle.replace(' ', '_');
      writer.set(ColdpTerm.remarks, wikiLink);
    }

    // External taxon identifiers → alternativeID as CURIEs
    AltIdBuilder altIds = new AltIdBuilder();
    JsonNode claims = entity.path("claims");
    claims.fieldNames().forEachRemaining(pid -> {
      WikidataDumpReader.ExtIdInfo extId = reader.extIdProperties.get(pid);
      if (extId != null) {
        String value = getStringClaimValue(entity, pid);
        if (value != null) {
          altIds.add(extId.prefix(), value);
          if (!value.isBlank() && !value.contains(",")) {
            usedExtIdPids.add(pid);
          }
        }
      }
    });
    String altIdStr = altIds.toString();
    if (altIdStr != null) {
      writer.set(ColdpTerm.alternativeID, altIdStr);
    }

    writer.next();
  }

  private String mapNomStatus(String label) {
    if (label == null) return null;
    return NOM_STATUS_LABEL_MAP.get(label.toLowerCase());
  }

  private String mapGender(String label) {
    if (label == null) return null;
    return GENDER_LABEL_MAP.get(label.toLowerCase());
  }

  private int writeVernacularNames(JsonNode entity, String qid) throws IOException {
    List<JsonNode> vernValues = getClaimValues(entity, P1843);
    int count = 0;
    for (JsonNode val : vernValues) {
      String[] monoText = WikidataDumpReader.getMonolingualText(val);
      if (monoText == null) continue;
      vernWriter.set(ColdpTerm.taxonID, qid);
      vernWriter.set(ColdpTerm.name, monoText[0]);
      vernWriter.set(ColdpTerm.language, monoText[1]);
      vernWriter.next();
      count++;
    }
    return count;
  }

  private int writeInvasiveDistributions(JsonNode entity, String qid, WikidataDumpReader reader) throws IOException {
    List<JsonNode> areaValues = getClaimValues(entity, P5588);
    int count = 0;
    for (JsonNode val : areaValues) {
      String areaQid = getItemId(val);
      if (areaQid == null) continue;
      writeDistribution(qid, areaQid, "invasive", null, reader);
      count++;
    }
    return count;
  }

  private int writeTaxonRangeDistributions(JsonNode entity, String qid, WikidataDumpReader reader) throws IOException {
    JsonNode rangeStmts = entity.path("claims").path(P9714);
    if (!rangeStmts.isArray()) return 0;
    int count = 0;
    for (JsonNode stmt : rangeStmts) {
      JsonNode val = stmt.path("mainsnak").path("datavalue").path("value");
      String areaQid = getItemId(val);
      if (areaQid == null) continue;

      String pubQid = null;
      JsonNode refs = stmt.path("references");
      if (refs.isArray()) {
        for (JsonNode ref : refs) {
          JsonNode pubSnaks = ref.path("snaks").path(P248);
          if (pubSnaks.isArray() && !pubSnaks.isEmpty()) {
            pubQid = getItemId(pubSnaks.get(0).path("datavalue").path("value"));
            break;
          }
        }
      }

      writeDistribution(qid, areaQid, "native", pubQid, reader);
      count++;
    }
    return count;
  }

  private void writeDistribution(String taxonQid, String areaQid, String establishmentMeans,
                                  String referenceID, WikidataDumpReader reader) throws IOException {
    WikidataDumpReader.AreaInfo area = reader.areaInfo.get(areaQid);
    distWriter.set(ColdpTerm.taxonID, taxonQid);
    if (area != null && area.isoCode() != null) {
      distWriter.set(ColdpTerm.areaID, area.isoCode());
      distWriter.set(ColdpTerm.gazetteer, "iso");
    } else {
      distWriter.set(ColdpTerm.areaID, areaQid);
      distWriter.set(ColdpTerm.gazetteer, "text");
    }
    distWriter.set(ColdpTerm.area, area != null ? area.label() : null);
    distWriter.set(ColdpTerm.establishmentMeans, establishmentMeans);
    if (referenceID != null) {
      distWriter.set(ColdpTerm.referenceID, referenceID);
    }
    distWriter.next();
  }

  private int writeIucnStatus(JsonNode entity, String qid, WikidataDumpReader reader) throws IOException {
    List<JsonNode> statusValues = getClaimValues(entity, P141);
    int count = 0;
    for (JsonNode val : statusValues) {
      String statusQid = getItemId(val);
      if (statusQid == null) continue;
      String label = reader.iucnLabels.get(statusQid);
      if (label == null) continue;

      propWriter.set(ColdpTerm.taxonID, qid);
      propWriter.set(ColdpTerm.property, "IUCN");
      propWriter.set(ColdpTerm.value, label);
      propWriter.next();
      count++;
    }
    return count;
  }

  /**
   * Write NameRelation records for P694 (replaced synonym → this name is a replacement for P694 value).
   */
  private int writeNameRelations(JsonNode entity, String qid) throws IOException {
    List<JsonNode> replacedSynonyms = getClaimValues(entity, P694);
    int count = 0;
    for (JsonNode val : replacedSynonyms) {
      String relatedQid = getItemId(val);
      if (relatedQid == null) continue;
      nameRelWriter.set(ColdpTerm.nameID, qid);
      nameRelWriter.set(ColdpTerm.relatedNameID, relatedQid);
      nameRelWriter.set(ColdpTerm.type, "replacement name");
      nameRelWriter.next();
      count++;
    }
    return count;
  }

  /**
   * Writes a col:Media record for the P18 (image) property of a taxon.
   * The Commons filename is available directly from the Wikidata dump — no HTTP calls needed.
   * URL format: https://commons.wikimedia.org/wiki/Special:FilePath/{filename}
   */
  private int writeP18Media(JsonNode entity, String qid) throws IOException {
    String filename = getStringClaimValue(entity, P18);
    if (filename == null) return 0;
    // MediaWiki convention: spaces → underscores in URLs
    String normalized = filename.replace(' ', '_');
    String url  = "https://commons.wikimedia.org/wiki/Special:FilePath/" + normalized;
    String link = "https://commons.wikimedia.org/wiki/File:" + normalized;
    mediaWriter.set(ColdpTerm.taxonID, qid);
    mediaWriter.set(ColdpTerm.url, url);
    mediaWriter.set(ColdpTerm.type, "image");
    mediaWriter.set(ColdpTerm.link, link);
    mediaWriter.next();
    return 1;
  }

  private void writeReferences(WikidataDumpReader reader) throws IOException {
    LOG.info("Writing {} reference records...", reader.pubInfo.size());
    Set<String> allPubQids = new HashSet<>();
    for (var ref : reader.nomRefs.values()) {
      allPubQids.add(ref.pubQid());
    }
    allPubQids.addAll(reader.pubInfo.keySet());

    for (String pubQid : allPubQids) {
      if (!writtenRefs.add(pubQid)) continue;

      WikidataDumpReader.PubInfo pub = reader.pubInfo.get(pubQid);
      refWriter.set(ColdpTerm.ID, pubQid);
      refWriter.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + pubQid);

      if (pub != null) {
        refWriter.set(ColdpTerm.title, pub.title());
        refWriter.set(ColdpTerm.doi, pub.doi());
        refWriter.set(ColdpTerm.author, pub.author());
        refWriter.set(ColdpTerm.volume, pub.volume());
        refWriter.set(ColdpTerm.issue, pub.issue());
        refWriter.set(ColdpTerm.page, pub.pages());

        if (pub.date() != null) {
          refWriter.set(ColdpTerm.issued, extractYear(pub.date()));
        }
        if (pub.journalQid() != null) {
          String journalName = reader.journalLabels.get(pub.journalQid());
          refWriter.set(ColdpTerm.containerTitle, journalName);
        }
      }

      refWriter.next();
    }
    LOG.info("Wrote {} references", writtenRefs.size());
  }

  private static String extractYear(String date) {
    if (date == null) return null;
    String clean = date.startsWith("+") ? date.substring(1) : date;
    return clean.length() >= 4 ? clean.substring(0, 4) : clean;
  }

  private void initWriters() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.alternativeID,
        ColdpTerm.parentID,
        ColdpTerm.basionymID,
        ColdpTerm.status,
        ColdpTerm.nameStatus,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.originalSpelling,
        ColdpTerm.gender,
        ColdpTerm.link,
        ColdpTerm.referenceID,
        ColdpTerm.publishedInYear,
        ColdpTerm.publishedInPage,
        ColdpTerm.publishedInPageLink,
        ColdpTerm.remarks
    ));
    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.language,
        ColdpTerm.name,
        ColdpTerm.referenceID
    ));
    distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.areaID,
        ColdpTerm.area,
        ColdpTerm.gazetteer,
        ColdpTerm.establishmentMeans,
        ColdpTerm.threatStatus,
        ColdpTerm.referenceID,
        ColdpTerm.remarks
    ));
    propWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.property,
        ColdpTerm.value
    ));
    nameRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
        ColdpTerm.ID,
        ColdpTerm.doi,
        ColdpTerm.citation,
        ColdpTerm.type,
        ColdpTerm.author,
        ColdpTerm.title,
        ColdpTerm.containerTitle,
        ColdpTerm.issued,
        ColdpTerm.volume,
        ColdpTerm.issue,
        ColdpTerm.edition,
        ColdpTerm.page,
        ColdpTerm.publisher,
        ColdpTerm.link
    ));

    mediaWriter = additionalWriter(ColdpTerm.Media, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.url,
        ColdpTerm.type,
        ColdpTerm.format,
        ColdpTerm.title,
        ColdpTerm.created,
        ColdpTerm.creator,
        ColdpTerm.license,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));

    // Duplicate page debug log (written to the archive directory as a plain TSV)
    File duplicateLog = new File(dir, "wikidata-duplicates.tsv");
    duplicateLogWriter = new PrintWriter(new FileWriter(duplicateLog));
    duplicateLogWriter.println("qid\tlink\tname");
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }
}
