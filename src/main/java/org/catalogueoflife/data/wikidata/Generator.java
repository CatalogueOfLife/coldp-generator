package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.catalogueoflife.data.wikidata.WikidataDumpReader.*;

public class Generator extends AbstractColdpGenerator {
  private static final String DUMP_URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz";
  private static final String DUMP_FILENAME = "latest-all.json.gz";

  private TermWriter vernWriter;
  private TermWriter distWriter;
  private TermWriter propWriter;
  private final Map<String, String> rankMap = new HashMap<>();
  private final Set<String> writtenRefs = new HashSet<>();

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
      // Check freshness via HTTP HEAD
      if (isRemoteNewer(dumpFile)) {
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

    // Pass 1: collect lookup maps
    LOG.info("Starting pass 1: collecting lookup maps...");
    reader.collectLookups(dumpFile, rankMap);

    // Resolve any remaining unresolved QIDs via small SPARQL VALUES queries
    resolveUnresolved(reader);

    // Pass 2: emit ColDP records
    LOG.info("Starting pass 2: emitting ColDP records...");
    emitColdpRecords(dumpFile, reader);
  }

  private void resolveUnresolved(WikidataDumpReader reader) {
    // Resolve unresolved rank QIDs
    if (!reader.neededRankQids.isEmpty()) {
      LOG.info("Resolving {} unresolved rank QIDs via SPARQL...", reader.neededRankQids.size());
      resolveLabels(reader.neededRankQids, reader.rankLabels, true);
    }
    // Resolve unresolved IUCN status QIDs
    if (!reader.neededIucnQids.isEmpty()) {
      LOG.info("Resolving {} unresolved IUCN QIDs via SPARQL...", reader.neededIucnQids.size());
      resolveLabels(reader.neededIucnQids, reader.iucnLabels, false);
    }
    // Resolve unresolved area QIDs
    if (!reader.neededAreaQids.isEmpty()) {
      LOG.info("Resolving {} unresolved area QIDs via SPARQL...", reader.neededAreaQids.size());
      resolveAreas(reader.neededAreaQids, reader.areaInfo);
    }
    // Resolve unresolved journal QIDs
    if (!reader.neededJournalQids.isEmpty()) {
      LOG.info("Resolving {} unresolved journal QIDs via SPARQL...", reader.neededJournalQids.size());
      resolveLabels(reader.neededJournalQids, reader.journalLabels, false);
    }
    // Resolve unresolved publication QIDs
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

  /**
   * Execute a SPARQL query against the Wikidata Query Service.
   * Returns the raw JSON response.
   */
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

  private void emitColdpRecords(File dumpFile, WikidataDumpReader reader) throws IOException {
    LOG.info("Pass 2: writing ColDP records...");
    int[] taxonCount = {0};
    int[] vernCount = {0};
    int[] distCount = {0};
    int[] propCount = {0};

    reader.streamDump(dumpFile, line -> line.contains("\"P225\""), entity -> {
      if (!hasClaim(entity, P225)) return;
      String qid = entity.path("id").asText(null);
      if (qid == null) return;

      try {
        // Write NameUsage
        writeNameUsage(entity, qid, reader);
        taxonCount[0]++;

        // Write VernacularNames
        vernCount[0] += writeVernacularNames(entity, qid);

        // Write Distributions (invasive)
        distCount[0] += writeInvasiveDistributions(entity, qid, reader);

        // Write Distributions (taxon range)
        distCount[0] += writeTaxonRangeDistributions(entity, qid, reader);

        // Write IUCN status as TaxonProperty
        propCount[0] += writeIucnStatus(entity, qid, reader);

        if (taxonCount[0] % 100_000 == 0) {
          LOG.info("Pass 2: {} taxa, {} vernaculars, {} distributions, {} properties",
              taxonCount[0], vernCount[0], distCount[0], propCount[0]);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    // Write Reference records for all collected publications
    writeReferences(reader);

    LOG.info("Pass 2 complete: {} taxa, {} vernaculars, {} distributions, {} properties, {} references",
        taxonCount[0], vernCount[0], distCount[0], propCount[0], writtenRefs.size());
  }

  private void writeNameUsage(JsonNode entity, String qid, WikidataDumpReader reader) throws IOException {
    JsonNode nameVal = getClaimValue(entity, P225);
    if (nameVal == null) return;
    String name = nameVal.asText(null);
    if (name == null) return;

    writer.set(ColdpTerm.ID, qid);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.status, "accepted");
    writer.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + qid);

    // Rank
    JsonNode rankVal = getClaimValue(entity, P105);
    if (rankVal != null) {
      String rankQid = getItemId(rankVal);
      if (rankQid != null) {
        writer.set(ColdpTerm.rank, reader.rankLabels.get(rankQid));
      }
    }

    // Parent
    JsonNode parentVal = getClaimValue(entity, P171);
    if (parentVal != null) {
      String parentQid = getItemId(parentVal);
      if (parentQid != null) {
        writer.set(ColdpTerm.parentID, parentQid);
      }
    }

    // Authorship
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

    // Nomenclatural reference
    WikidataDumpReader.NomRef ref = reader.nomRefs.get(qid);
    if (ref != null) {
      writer.set(ColdpTerm.referenceID, ref.pubQid());
      writer.set(ColdpTerm.publishedInPage, ref.page());
      writer.set(ColdpTerm.publishedInPageLink, ref.bhlPageLink());
      // Get year from pub info
      WikidataDumpReader.PubInfo pub = reader.pubInfo.get(ref.pubQid());
      if (pub != null && pub.date() != null) {
        String year = extractYear(pub.date());
        writer.set(ColdpTerm.publishedInYear, year);
      }
    }

    writer.next();
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

      // Check for reference publication
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

  private void writeReferences(WikidataDumpReader reader) throws IOException {
    LOG.info("Writing {} reference records...", reader.pubInfo.size());
    // Write references for all publications we have info for
    Set<String> allPubQids = new HashSet<>();
    // Collect all pub QIDs referenced by taxa
    for (var ref : reader.nomRefs.values()) {
      allPubQids.add(ref.pubQid());
    }
    // Also include any pub QIDs from the original needed set that got resolved
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
    // Wikidata time format: +2023-01-15T00:00:00Z or just 2023
    String clean = date.startsWith("+") ? date.substring(1) : date;
    return clean.length() >= 4 ? clean.substring(0, 4) : clean;
  }

  private void initWriters() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.basionymID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.link,
        ColdpTerm.referenceID,
        ColdpTerm.publishedInYear,
        ColdpTerm.publishedInPage,
        ColdpTerm.publishedInPageLink,
        ColdpTerm.remarks
    ));
    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.ID,
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
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }
}
