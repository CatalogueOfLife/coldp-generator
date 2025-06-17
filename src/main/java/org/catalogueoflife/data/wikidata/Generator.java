package org.catalogueoflife.data.wikidata;

import life.catalogue.coldp.ColdpTerm;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

public class Generator extends AbstractColdpGenerator {
  private static final String API = "https://query.wikidata.org/sparql";

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    initWriters();
    System.out.println("\nStart WikiData queries");
    crawl();
  }


  private void initWriters() throws Exception {
    initRefWriter(List.of(
            ColdpTerm.ID,
            ColdpTerm.citation,
            ColdpTerm.doi
    ));
    newWriter(ColdpTerm.NameUsage, List.of(
            ColdpTerm.ID,
            ColdpTerm.parentID,
            ColdpTerm.status,
            ColdpTerm.nameStatus,
            ColdpTerm.rank,
            ColdpTerm.uninomial,
            ColdpTerm.genericName,
            ColdpTerm.specificEpithet,
            ColdpTerm.infraspecificEpithet,
            ColdpTerm.authorship,
            ColdpTerm.nameReferenceID,
            ColdpTerm.publishedInPage
    ));
  }
  private void crawl() throws Exception {
    var wdqs = QueryExecution.service(API);

    String q = "select distinct ?Concept where {[] a ?Concept } LIMIT 10";
    Query query = QueryFactory.create(q);

    try (var qexec = wdqs.query(query).build()) {
      ResultSet results = qexec.execSelect() ;
      while (results.hasNext()) {
        QuerySolution soln = results.nextSolution() ;
        RDFNode x = soln.get("Concept") ;       // Get a result variable by name.
        Resource r = soln.getResource("Concept") ; // Get a result variable - must be a resource
        //Literal l = soln.getLiteral("Concept") ;
      }
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", "Version 23.0");
    super.addMetadata();
  }

}
