package org.catalogueoflife.data.wikidata;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GeneratorParseTaxonPropsTest {

  @Test public void parseAndFilter() {
    // habitat (kept, WikibaseItem), gestation (kept, Quantity),
    // P523 temporal (skipped — special-cased), an ExternalId (skipped — datatype)
    String json = "{\"results\":{\"bindings\":[" +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P2974\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#WikibaseItem\"},\"pLabel\":{\"value\":\"habitat\"}}," +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P3063\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#Quantity\"},\"pLabel\":{\"value\":\"gestation period\"}}," +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P523\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#WikibaseItem\"},\"pLabel\":{\"value\":\"temporal range start\"}}," +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P3151\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#ExternalId\"},\"pLabel\":{\"value\":\"iNaturalist taxon ID\"}}" +
        "]}}";
    Map<String, WikidataDumpReader.TaxonPropInfo> target = new HashMap<>();
    Generator.parseSparqlTaxonProps(json, target);

    assertEquals(2, target.size());
    assertEquals("habitat", target.get("P2974").label());
    assertEquals("WikibaseItem", target.get("P2974").datatype());
    assertEquals("Quantity", target.get("P3063").datatype());
    assertNull(target.get("P523"));   // special-cased
    assertNull(target.get("P3151"));  // ExternalId datatype dropped
  }
}
