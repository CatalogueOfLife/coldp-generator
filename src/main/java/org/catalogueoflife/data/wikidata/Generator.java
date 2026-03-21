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
import java.util.concurrent.atomic.AtomicInteger;

import static org.catalogueoflife.data.wikidata.WikidataDumpReader.*;

public class Generator extends AbstractColdpGenerator {
  private static final String DUMP_URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz";
  private static final String DUMP_FILENAME = "latest-all.json.gz";
  private final boolean PREVENT_DOWNLOAD = true;

  private TermWriter vernWriter;
  private TermWriter distWriter;
  private TermWriter propWriter;
  private TermWriter nameRelWriter;
  private TermWriter mediaWriter; // null when media crawling is disabled (--media-threads 0)
  private PrintWriter duplicateLogWriter;
  private final Map<String, String> rankMap = new HashMap<>();
  private final Set<String> writtenRefs = new HashSet<>();

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
    File dumpFile = sourceFile(DUMP_FILENAME);
    if (dumpFile.exists()) {
      if (!PREVENT_DOWNLOAD && isRemoteNewer(dumpFile)) {
        LOG.info("Remote dump is newer, re-downloading...");
        dumpFile.delete();
        downloadDump(dumpFile);
      } else {
        LOG.info("Reusing cached dump file: {}", dumpFile);
      }
    } else {
      downloadDump(dumpFile);
    }
  }

  private boolean isRemoteNewer(File localFile) {
    try {
      var resp = http.head(DUMP_URL);
      var lastModHeader = resp.headers().firstValue("Last-Modified").orElse(null);
      if (lastModHeader != null) {
        var remoteDate = ZonedDateTime.parse(lastModHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
        long remoteMillis = remoteDate.toInstant().toEpochMilli();
        return remoteMillis > localFile.lastModified();
      }
    } catch (Exception e) {
      LOG.warn("Failed to check remote dump freshness, will reuse local file", e);
    }
    return false;
  }

  private void downloadDump(File target) throws IOException {
    LOG.info("Downloading Wikidata dump from {} to {} (this may take a while...)", DUMP_URL, target);
    http.download(DUMP_URL, target);
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

    // Write the identifier registry TSV to the archive
    writeIdentifierRegistry(reader);

    LOG.info("Starting pass 2: emitting ColDP records...");
    Map<String, String> pendingGalleries = new LinkedHashMap<>();
    try {
      emitColdpRecords(dumpFile, reader, pendingGalleries);
    } finally {
      if (duplicateLogWriter != null) {
        duplicateLogWriter.close();
      }
    }

    if (cfg.mediaThreads > 0 && !pendingGalleries.isEmpty()) {
      LOG.info("Starting media crawl: {} galleries, {} threads", pendingGalleries.size(), cfg.mediaThreads);
      crawlMedia(pendingGalleries);
    }
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
    LOG.info("Writing identifier registry with {} properties to {}", reader.extIdProperties.size(), registryFile);
    try (PrintWriter pw = new PrintWriter(new FileWriter(registryFile))) {
      pw.println("prefix\tproperty\tpropertyLink\tlabel\tformatterUrl\tformatRegex");
      for (var entry : reader.extIdProperties.entrySet()) {
        String pid = entry.getKey();
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

  private void emitColdpRecords(File dumpFile, WikidataDumpReader reader,
                                Map<String, String> pendingGalleries) throws IOException {
    LOG.info("Pass 2: writing ColDP records...");
    int[] taxonCount = {0};
    int[] synCount = {0};
    int[] duplicateCount = {0};
    int[] vernCount = {0};
    int[] distCount = {0};
    int[] propCount = {0};
    int[] nameRelCount = {0};

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

        // Collect gallery name for later media crawling
        String gallery = reader.galleryNames.get(qid);
        if (gallery != null && cfg.mediaThreads > 0) {
          pendingGalleries.put(qid, gallery);
        }

        if (taxonCount[0] % 100_000 == 0) {
          LOG.info("Pass 2: {} taxa ({} synonyms), {} duplicates skipped, {} vernaculars, {} distributions, {} properties, {} name relations",
              taxonCount[0], synCount[0], duplicateCount[0], vernCount[0], distCount[0], propCount[0], nameRelCount[0]);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    writeReferences(reader);

    LOG.info("Pass 2 complete: {} taxa ({} synonyms), {} duplicates skipped, {} vernaculars, {} distributions, {} properties, {} name relations, {} references",
        taxonCount[0], synCount[0], duplicateCount[0], vernCount[0], distCount[0], propCount[0], nameRelCount[0], writtenRefs.size());
  }

  private void crawlMedia(Map<String, String> galleries) throws InterruptedException {
    CommonsMediaCrawler crawler = new CommonsMediaCrawler(cfg.mediaThreads, http);
    AtomicInteger mediaCount = new AtomicInteger();

    crawler.submitAll(galleries);
    crawler.awaitAndDrain(r -> {
      try {
        mediaWriter.set(ColdpTerm.taxonID, r.taxonID());
        mediaWriter.set(ColdpTerm.url, r.url());
        mediaWriter.set(ColdpTerm.type, r.type());
        mediaWriter.set(ColdpTerm.format, r.format());
        mediaWriter.set(ColdpTerm.title, r.title());
        mediaWriter.set(ColdpTerm.created, r.created());
        mediaWriter.set(ColdpTerm.creator, r.creator());
        mediaWriter.set(ColdpTerm.license, r.license());
        mediaWriter.set(ColdpTerm.link, r.link());
        mediaWriter.next();
        mediaCount.incrementAndGet();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    LOG.info("Media records written: {}", mediaCount.get());
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

    if (cfg.mediaThreads > 0) {
      mediaWriter = additionalWriter(ColdpTerm.Media, List.of(
          ColdpTerm.taxonID,
          ColdpTerm.url,
          ColdpTerm.type,
          ColdpTerm.format,
          ColdpTerm.title,
          ColdpTerm.created,
          ColdpTerm.creator,
          ColdpTerm.license,
          ColdpTerm.link
      ));
    }

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
