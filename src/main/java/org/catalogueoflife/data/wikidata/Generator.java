package org.catalogueoflife.data.wikidata;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.engine.http.QueryExceptionHTTP;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;

public class Generator extends AbstractColdpGenerator {
  private static final String SPARQL_ENDPOINT = "https://query.wikidata.org/sparql";
  private static final int BATCH_SIZE = 5000;
  private static final int PUB_BATCH_SIZE = 200;
  private static final int MAX_RETRIES = 3;
  private static final String USER_AGENT = "ColdpGenerator/1.0 (https://github.com/CatalogueOfLife/coldp-generator) Apache-Jena";

  private static final String PREFIXES = """
      PREFIX wd: <http://www.wikidata.org/entity/>
      PREFIX wdt: <http://www.wikidata.org/prop/direct/>
      PREFIX wikibase: <http://wikiba.se/ontology#>
      PREFIX bd: <http://www.bigdata.com/rdf#>
      PREFIX p: <http://www.wikidata.org/prop/>
      PREFIX ps: <http://www.wikidata.org/prop/statement/>
      PREFIX pr: <http://www.wikidata.org/prop/reference/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      """;

  private TermWriter vernWriter;
  private TermWriter distWriter;
  private TermWriter propWriter;
  private final Map<String, String> rankMap = new HashMap<>();
  // taxon QID -> nomenclatural reference info
  private final Map<String, NomRef> nomRefMap = new HashMap<>();
  // publication QIDs already written as Reference records
  private final Set<String> writtenRefs = new HashSet<>();

  private record NomRef(String refID, String year, String page, String bhlPageLink) {}

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
  protected void addData() throws Exception {
    initWriters();
    LOG.info("Starting WikiData SPARQL queries");
    loadAdditionalRanks();
    crawlNomenclaturalReferences();
    crawlTaxa();
    crawlVernacularNames();
    crawlInvasiveDistributions();
    crawlTaxonRangeDistributions();
    crawlIUCNStatus();
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

  /**
   * Query all distinct taxon ranks directly from items that are instance of "taxonomic rank" (Q427626),
   * avoiding a full scan of all taxa.
   */
  private void loadAdditionalRanks() {
    String sparql = PREFIXES + """
        SELECT ?rank ?rankLabel WHERE {
          ?rank wdt:P31 wd:Q427626 .
          ?rank rdfs:label ?rankLabel .
          FILTER(LANG(?rankLabel) = "en")
        }
        """;
    try {
      Query query = QueryFactory.create(sparql);
      executeQuery(query, sol -> {
        String qid = qid(sol.getResource("rank"));
        if (qid != null && !rankMap.containsKey(qid)) {
          String label = sol.getLiteral("rankLabel").getString().toLowerCase();
          rankMap.put(qid, label);
          LOG.debug("Discovered rank {} -> {}", qid, label);
        }
      });
      LOG.info("Total rank mappings: {}", rankMap.size());
    } catch (Exception e) {
      LOG.warn("Failed to load dynamic rank mappings, using static defaults", e);
    }
  }

  /**
   * Phase 1: Query taxon→publication mappings from P225 reference nodes with role "first valid description".
   * Phase 2: Enrich unique publications with their bibliographic properties in batches.
   */
  private void crawlNomenclaturalReferences() throws Exception {
    // Phase 1: get taxon→pub mappings (lightweight query, no pub property joins)
    LOG.info("Crawling nomenclatural reference mappings...");
    Set<String> pubQids = new LinkedHashSet<>();
    int total = crawlPaginated("""
        SELECT ?item ?pub ?refPg ?bhl WHERE {
          ?item p:P225 ?stmt .
          ?stmt prov:wasDerivedFrom ?refnode .
          ?refnode pr:P248 ?pub .
          ?refnode pr:P6184 wd:Q1361864 .
          OPTIONAL { ?refnode pr:P304 ?refPg }
          OPTIONAL { ?refnode pr:P687 ?bhl }
        }
        """, sol -> {
      String taxonQid = qid(sol.getResource("item"));
      String pubQid = qid(sol.getResource("pub"));
      if (taxonQid == null || pubQid == null) return;

      pubQids.add(pubQid);

      if (!nomRefMap.containsKey(taxonQid)) {
        String page = literal(sol, "refPg");
        String bhlPageId = literal(sol, "bhl");
        String bhlPageLink = bhlPageId != null ? "https://www.biodiversitylibrary.org/page/" + bhlPageId : null;
        nomRefMap.put(taxonQid, new NomRef(pubQid, null, page, bhlPageLink));
      }
    });
    LOG.info("Phase 1: {} taxon→pub mappings, {} unique publications", total, pubQids.size());

    // Phase 2: enrich publications with bibliographic metadata in batches
    LOG.info("Enriching publication metadata...");
    Map<String, String> pubYears = new HashMap<>();
    List<String> pubList = new ArrayList<>(pubQids);
    for (int i = 0; i < pubList.size(); i += PUB_BATCH_SIZE) {
      List<String> batch = pubList.subList(i, Math.min(i + PUB_BATCH_SIZE, pubList.size()));
      StringBuilder values = new StringBuilder();
      for (String qid : batch) {
        values.append(" wd:").append(qid);
      }

      String sparql = PREFIXES + String.format("""
          SELECT ?pub
            (SAMPLE(?title) AS ?pubTitle)
            (SAMPLE(?date) AS ?pubDate)
            (SAMPLE(?doi) AS ?pubDOI)
            (SAMPLE(?authorStr) AS ?pubAuthor)
            (SAMPLE(?journalName) AS ?pubJournal)
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

      Query query = QueryFactory.create(sparql);
      executeQuery(query, sol -> {
        String pubQid = qid(sol.getResource("pub"));
        if (pubQid == null || writtenRefs.contains(pubQid)) return;

        writtenRefs.add(pubQid);
        try {
          refWriter.set(ColdpTerm.ID, pubQid);
          refWriter.set(ColdpTerm.doi, literal(sol, "pubDOI"));
          refWriter.set(ColdpTerm.title, literal(sol, "pubTitle"));
          refWriter.set(ColdpTerm.author, literal(sol, "pubAuthor"));
          refWriter.set(ColdpTerm.containerTitle, literal(sol, "pubJournal"));
          refWriter.set(ColdpTerm.volume, literal(sol, "pubVolume"));
          refWriter.set(ColdpTerm.issue, literal(sol, "pubIssue"));
          refWriter.set(ColdpTerm.page, literal(sol, "pubPage"));
          String date = literal(sol, "pubDate");
          if (date != null) {
            String year = date.length() >= 4 ? date.substring(0, 4) : date;
            refWriter.set(ColdpTerm.issued, year);
            pubYears.put(pubQid, year);
          }
          refWriter.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + pubQid);
          refWriter.next();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      LOG.info("Enriched publications {}/{}", Math.min(i + PUB_BATCH_SIZE, pubList.size()), pubList.size());
      Thread.sleep(500);
    }

    // Write minimal Reference records for any publications not returned by the enrichment query
    for (String pubQid : pubQids) {
      if (!writtenRefs.contains(pubQid)) {
        writtenRefs.add(pubQid);
        refWriter.set(ColdpTerm.ID, pubQid);
        refWriter.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + pubQid);
        refWriter.next();
      }
    }

    // Update nomRefMap with publication years
    for (var entry : nomRefMap.entrySet()) {
      NomRef ref = entry.getValue();
      String year = pubYears.get(ref.refID());
      if (year != null && ref.year() == null) {
        entry.setValue(new NomRef(ref.refID(), year, ref.page(), ref.bhlPageLink()));
      }
    }
    LOG.info("Total nomenclatural references: {}, unique publications: {}", nomRefMap.size(), writtenRefs.size());
  }

  /**
   * Query all Wikidata taxa using a subquery pattern: the inner query paginates over
   * item IDs efficiently, the outer query enriches with optional properties.
   * This avoids GROUP BY + ORDER BY over millions of rows which causes timeouts.
   */
  private void crawlTaxa() throws Exception {
    int total = 0;
    int offset = 0;
    boolean hasMore = true;
    Set<String> seen = new HashSet<>();

    while (hasMore) {
      String sparql = PREFIXES + String.format("""
          SELECT ?item ?name ?r ?p ?auth ?basionym WHERE {
            {
              SELECT ?item WHERE {
                ?item wdt:P225 [] .
              }
              LIMIT %d
              OFFSET %d
            }
            ?item wdt:P225 ?name .
            OPTIONAL { ?item wdt:P105 ?r }
            OPTIONAL { ?item wdt:P171 ?p }
            OPTIONAL { ?item wdt:P835 ?auth }
            OPTIONAL { ?item wdt:P566 ?basionym }
          }
          """, BATCH_SIZE, offset);

      Query query = QueryFactory.create(sparql);
      int batchItems = 0;
      int batchCount = executeQuery(query, sol -> {
        try {
          Resource item = sol.getResource("item");
          String qid = qid(item);
          // skip duplicate rows from multiple optional values
          if (!seen.add(qid)) return;

          writer.set(ColdpTerm.ID, qid);
          writer.set(ColdpTerm.scientificName, sol.getLiteral("name").getString());

          if (sol.getResource("r") != null) {
            writer.set(ColdpTerm.rank, rankMap.get(qid(sol.getResource("r"))));
          }
          if (sol.getResource("p") != null) {
            writer.set(ColdpTerm.parentID, qid(sol.getResource("p")));
          }
          if (sol.get("auth") != null) {
            writer.set(ColdpTerm.authorship, sol.getLiteral("auth").getString());
          }
          if (sol.getResource("basionym") != null) {
            writer.set(ColdpTerm.basionymID, qid(sol.getResource("basionym")));
          }
          writer.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + qid);
          writer.set(ColdpTerm.status, "accepted");

          NomRef ref = nomRefMap.get(qid);
          if (ref != null) {
            writer.set(ColdpTerm.referenceID, ref.refID());
            writer.set(ColdpTerm.publishedInYear, ref.year());
            writer.set(ColdpTerm.publishedInPage, ref.page());
            writer.set(ColdpTerm.publishedInPageLink, ref.bhlPageLink());
          }

          writer.next();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      // The inner subquery returns BATCH_SIZE items, but the outer query may produce
      // more rows (from OPTIONALs). Track unique items written to determine batch size.
      int prevTotal = total;
      total = seen.size();
      batchItems = total - prevTotal;
      offset += BATCH_SIZE;
      LOG.info("Processed {} taxa (batch {})", total, batchItems);
      // Stop when inner subquery returned fewer than BATCH_SIZE items
      hasMore = batchCount > 0 && batchItems > 0;

      if (hasMore) {
        Thread.sleep(1000);
      }
    }
    LOG.info("Total taxa: {}", total);
  }

  /**
   * Query all vernacular names (P1843) for Wikidata taxa.
   */
  private void crawlVernacularNames() throws Exception {
    int total = crawlPaginated("""
        SELECT ?item ?name WHERE {
          ?item wdt:P225 [] ;
                wdt:P1843 ?name .
        }
        """, sol -> {
      try {
        String qid = qid(sol.getResource("item"));
        Literal nameLit = sol.getLiteral("name");

        vernWriter.set(ColdpTerm.taxonID, qid);
        vernWriter.set(ColdpTerm.name, nameLit.getString());
        vernWriter.set(ColdpTerm.language, nameLit.getLanguage());
        vernWriter.next();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    LOG.info("Total vernacular names: {}", total);
  }

  /**
   * Query P5588 (invasive to) distributions.
   */
  private void crawlInvasiveDistributions() throws Exception {
    int total = crawlPaginated("""
        SELECT ?item ?area ?areaLabel ?iso WHERE {
          ?item wdt:P225 [] ;
                wdt:P5588 ?area .
          OPTIONAL { ?area rdfs:label ?areaLabel . FILTER(LANG(?areaLabel) = "en") }
          OPTIONAL { ?area wdt:P297 ?iso }
        }
        """, sol -> {
      try {
        writeDistribution(sol, "invasive", null);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    LOG.info("Total invasive distributions: {}", total);
  }

  /**
   * Query P9714 (taxon range) distributions with optional reference.
   */
  private void crawlTaxonRangeDistributions() throws Exception {
    int total = crawlPaginated("""
        SELECT ?item ?area ?areaLabel ?iso ?pub WHERE {
          ?item wdt:P225 [] .
          ?item p:P9714 ?rangeStmt .
          ?rangeStmt ps:P9714 ?area .
          OPTIONAL { ?area rdfs:label ?areaLabel . FILTER(LANG(?areaLabel) = "en") }
          OPTIONAL { ?area wdt:P297 ?iso }
          OPTIONAL {
            ?rangeStmt prov:wasDerivedFrom ?refnode .
            ?refnode pr:P248 ?pub .
          }
        }
        """, sol -> {
      try {
        String pubQid = qid(sol.getResource("pub"));
        if (pubQid != null && !writtenRefs.contains(pubQid)) {
          writtenRefs.add(pubQid);
          refWriter.set(ColdpTerm.ID, pubQid);
          refWriter.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + pubQid);
          refWriter.next();
        }
        writeDistribution(sol, "native", pubQid);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    LOG.info("Total taxon range distributions: {}", total);
  }

  private void writeDistribution(QuerySolution sol, String establishmentMeans, String referenceID) throws IOException {
    String taxonQid = qid(sol.getResource("item"));
    String areaQid = qid(sol.getResource("area"));
    if (taxonQid == null || areaQid == null) return;

    String areaLabel = literal(sol, "areaLabel");
    String iso = literal(sol, "iso");

    distWriter.set(ColdpTerm.taxonID, taxonQid);
    if (iso != null) {
      distWriter.set(ColdpTerm.areaID, iso);
      distWriter.set(ColdpTerm.gazetteer, "iso");
    } else {
      distWriter.set(ColdpTerm.areaID, areaQid);
      distWriter.set(ColdpTerm.gazetteer, "text");
    }
    distWriter.set(ColdpTerm.area, areaLabel);
    distWriter.set(ColdpTerm.establishmentMeans, establishmentMeans);
    if (referenceID != null) {
      distWriter.set(ColdpTerm.referenceID, referenceID);
    }
    distWriter.next();
  }

  /**
   * Query IUCN conservation status (P141) and write as TaxonProperty records.
   */
  private void crawlIUCNStatus() throws Exception {
    int total = crawlPaginated("""
        SELECT ?item ?statusLabel WHERE {
          ?item wdt:P225 [] ;
                wdt:P141 ?status .
          ?status rdfs:label ?statusLabel .
          FILTER(LANG(?statusLabel) = "en")
        }
        """, sol -> {
      try {
        String taxonQid = qid(sol.getResource("item"));
        String statusLabel = literal(sol, "statusLabel");
        if (taxonQid == null || statusLabel == null) return;

        propWriter.set(ColdpTerm.taxonID, taxonQid);
        propWriter.set(ColdpTerm.property, "IUCN");
        propWriter.set(ColdpTerm.value, statusLabel);
        propWriter.next();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    LOG.info("Total IUCN statuses: {}", total);
  }

  // --- Shared infrastructure ---

  /**
   * Paginated SPARQL crawl using LIMIT/OFFSET without ORDER BY.
   * Blazegraph (used by WDQS) iterates in deterministic internal storage order,
   * making OFFSET without ORDER BY reliable and much faster than with ORDER BY
   * which forces a full sort of millions of results before applying LIMIT.
   */
  private int crawlPaginated(String queryBody, Consumer<QuerySolution> handler) throws Exception {
    int total = 0;
    int offset = 0;
    boolean hasMore = true;

    while (hasMore) {
      String sparql = PREFIXES + queryBody + String.format("""
          LIMIT %d
          OFFSET %d
          """, BATCH_SIZE, offset);

      Query query = QueryFactory.create(sparql);
      int batchCount = executeQuery(query, handler);

      total += batchCount;
      offset += BATCH_SIZE;
      LOG.info("Processed {} rows (batch {})", total, batchCount);
      hasMore = batchCount == BATCH_SIZE;

      if (hasMore) {
        Thread.sleep(1000);
      }
    }
    return total;
  }

  /**
   * Execute a SPARQL SELECT query with retry on transient HTTP errors (504, 429).
   * Returns the number of results processed.
   */
  private int executeQuery(Query query, Consumer<QuerySolution> handler) throws Exception {
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        int count = 0;
        try (var qexec = buildQuery(query)) {
          ResultSet results = qexec.execSelect();
          while (results.hasNext()) {
            handler.accept(results.next());
            count++;
          }
        }
        return count;
      } catch (QueryExceptionHTTP e) {
        if (attempt == MAX_RETRIES) throw e;
        long wait = attempt * 10_000L;
        LOG.warn("SPARQL query failed (attempt {}/{}), retrying in {}s: {}",
            attempt, MAX_RETRIES, wait / 1000, e.getMessage());
        Thread.sleep(wait);
      }
    }
    return 0; // unreachable
  }

  private QueryExecution buildQuery(Query query) {
    return QueryExecution.service(SPARQL_ENDPOINT)
        .query(query)
        .httpHeader("User-Agent", USER_AGENT)
        .build();
  }

  private static String qid(Resource r) {
    if (r == null) return null;
    String uri = r.getURI();
    return uri.substring(uri.lastIndexOf('/') + 1);
  }

  private static String literal(QuerySolution sol, String var) {
    RDFNode node = sol.get(var);
    if (node == null || !node.isLiteral()) return null;
    return node.asLiteral().getString();
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }
}
