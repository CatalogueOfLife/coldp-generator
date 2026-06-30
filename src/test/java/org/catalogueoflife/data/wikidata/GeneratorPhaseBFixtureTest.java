package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class GeneratorPhaseBFixtureTest {
  private static final ObjectMapper M = new ObjectMapper();

  @Test public void taxonPropertyRows() throws Exception {
    Map<String, WikidataDumpReader.TaxonPropInfo> tp = new HashMap<>();
    tp.put("P2974", new WikidataDumpReader.TaxonPropInfo("habitat", "WikibaseItem"));
    tp.put("P2067", new WikidataDumpReader.TaxonPropInfo("mass", "Quantity"));
    Map<String, String> labels = new HashMap<>();
    labels.put("Q123", "forest");
    labels.put("Q11570", "kilogram");
    String json = "{\"id\":\"Q1\",\"claims\":{"
        + "\"P2974\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q123\"}}}}],"
        + "\"P2067\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"amount\":\"+250\",\"unit\":\"http://www.wikidata.org/entity/Q11570\"}}}}]"
        + "}}";
    List<String[]> rows = WikidataDumpReader.taxonPropertyRows(M.readTree(json), tp, labels);
    Set<String> got = new HashSet<>();
    for (String[] r : rows) got.add(r[0] + "=" + r[1]);
    assertTrue(got.contains("habitat=forest"));
    assertTrue(got.contains("mass=250 kilogram"));
    assertEquals(2, rows.size());
  }

  @Test public void speciesInteractionRows() throws Exception {
    String json = "{\"id\":\"Q1\",\"claims\":{"
        + "\"P1034\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q1231177\"}}}}]"
        + "}}";
    List<String[]> rows = WikidataDumpReader.speciesInteractionRows(M.readTree(json));
    assertEquals(1, rows.size());
    assertEquals("eats", rows.get(0)[0]);
    assertEquals("Q1231177", rows.get(0)[1]);
  }
}
