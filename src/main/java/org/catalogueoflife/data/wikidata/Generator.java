package org.catalogueoflife.data.wikidata;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

public class Generator extends AbstractColdpGenerator {
  private static final String SPARQL_ENDPOINT = "https://query.wikidata.org/sparql";
  private static final int BATCH_SIZE = 10000;
  private static final String USER_AGENT = "ColdpGenerator/1.0 (https://github.com/CatalogueOfLife/coldp-generator) Apache-Jena";

  private static final String PREFIXES = """
      PREFIX wd: <http://www.wikidata.org/entity/>
      PREFIX wdt: <http://www.wikidata.org/prop/direct/>
      PREFIX wikibase: <http://wikiba.se/ontology#>
      PREFIX bd: <http://www.bigdata.com/rdf#>
      """;

  private TermWriter vernWriter;
  private final Map<String, String> rankMap = new HashMap<>();

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
    crawlTaxa();
    crawlVernacularNames();
  }

  private void initWriters() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.ID,
        ColdpTerm.language,
        ColdpTerm.name
    ));
  }

  /**
   * Query all distinct taxon ranks from Wikidata and add any missing ones to the rank map.
   */
  private void loadAdditionalRanks() {
    String sparql = PREFIXES + """
        SELECT DISTINCT ?rank ?rankLabel WHERE {
          ?item wdt:P105 ?rank .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """;
    try {
      Query query = QueryFactory.create(sparql);
      try (var qexec = buildQuery(query)) {
        ResultSet results = qexec.execSelect();
        while (results.hasNext()) {
          QuerySolution sol = results.next();
          String qid = qid(sol.getResource("rank"));
          if (!rankMap.containsKey(qid)) {
            String label = sol.getLiteral("rankLabel").getString().toLowerCase();
            rankMap.put(qid, label);
            LOG.debug("Discovered rank {} -> {}", qid, label);
          }
        }
      }
      LOG.info("Total rank mappings: {}", rankMap.size());
    } catch (Exception e) {
      LOG.warn("Failed to load dynamic rank mappings, using static defaults", e);
    }
  }

  /**
   * Query all Wikidata taxa (items with P225 taxon name) in paginated batches
   * and write them as ColDP NameUsage records.
   */
  private void crawlTaxa() throws Exception {
    int total = 0;
    int offset = 0;
    boolean hasMore = true;

    while (hasMore) {
      String sparql = PREFIXES + String.format("""
          SELECT ?item
            (SAMPLE(?taxonName) AS ?name)
            (SAMPLE(?rank) AS ?r)
            (SAMPLE(?parent) AS ?p)
            (SAMPLE(?authorName) AS ?auth)
          WHERE {
            ?item wdt:P225 ?taxonName .
            OPTIONAL { ?item wdt:P105 ?rank }
            OPTIONAL { ?item wdt:P171 ?parent }
            OPTIONAL { ?item wdt:P835 ?authorName }
          }
          GROUP BY ?item
          ORDER BY ?item
          LIMIT %d
          OFFSET %d
          """, BATCH_SIZE, offset);

      Query query = QueryFactory.create(sparql);
      int batchCount = 0;

      try (var qexec = buildQuery(query)) {
        ResultSet results = qexec.execSelect();
        while (results.hasNext()) {
          QuerySolution sol = results.next();
          batchCount++;

          Resource item = sol.getResource("item");
          String qid = qid(item);

          String taxonName = sol.getLiteral("name").getString();

          String rank = null;
          if (sol.getResource("r") != null) {
            rank = rankMap.get(qid(sol.getResource("r")));
          }

          String parentID = null;
          if (sol.getResource("p") != null) {
            parentID = qid(sol.getResource("p"));
          }

          String authorship = null;
          if (sol.get("auth") != null) {
            authorship = sol.getLiteral("auth").getString();
          }

          writer.set(ColdpTerm.ID, qid);
          writer.set(ColdpTerm.parentID, parentID);
          writer.set(ColdpTerm.rank, rank);
          writer.set(ColdpTerm.scientificName, taxonName);
          writer.set(ColdpTerm.authorship, authorship);
          writer.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + qid);
          writer.set(ColdpTerm.status, "accepted");
          writer.next();
        }
      }

      total += batchCount;
      offset += BATCH_SIZE;
      LOG.info("Processed {} taxa (batch {})", total, batchCount);
      hasMore = batchCount == BATCH_SIZE;

      if (hasMore) {
        Thread.sleep(1000);
      }
    }
    LOG.info("Total taxa: {}", total);
  }

  /**
   * Query all vernacular names (P1843) for Wikidata taxa and write them as ColDP VernacularName records.
   */
  private void crawlVernacularNames() throws Exception {
    int total = 0;
    int offset = 0;
    boolean hasMore = true;

    while (hasMore) {
      String sparql = PREFIXES + String.format("""
          SELECT ?item ?name WHERE {
            ?item wdt:P225 [] ;
                  wdt:P1843 ?name .
          }
          ORDER BY ?item
          LIMIT %d
          OFFSET %d
          """, BATCH_SIZE, offset);

      Query query = QueryFactory.create(sparql);
      int batchCount = 0;

      try (var qexec = buildQuery(query)) {
        ResultSet results = qexec.execSelect();
        while (results.hasNext()) {
          QuerySolution sol = results.next();
          batchCount++;

          Resource item = sol.getResource("item");
          String qid = qid(item);
          Literal nameLit = sol.getLiteral("name");

          vernWriter.set(ColdpTerm.taxonID, qid);
          vernWriter.set(ColdpTerm.name, nameLit.getString());
          vernWriter.set(ColdpTerm.language, nameLit.getLanguage());
          vernWriter.next();
        }
      }

      total += batchCount;
      offset += BATCH_SIZE;
      LOG.info("Processed {} vernacular names (batch {})", total, batchCount);
      hasMore = batchCount == BATCH_SIZE;

      if (hasMore) {
        Thread.sleep(1000);
      }
    }
    LOG.info("Total vernacular names: {}", total);
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

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }
}
