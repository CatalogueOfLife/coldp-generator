package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WikidataDumpReaderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonNode parse(String json) throws Exception {
    return MAPPER.readTree(json);
  }

  /**
   * P1420 "taxon synonym" points from the accepted name to its synonyms.
   * For Quercus alba (Q469555) with P1420 → Quercus candida (Q123),
   * the synonym is Q123 and its accepted name is Q469555 — NOT the reverse.
   */
  @Test
  public void collectSynonymLinksDirection() throws Exception {
    String json = "{"
        + "\"id\":\"Q469555\","
        + "\"claims\":{"
        + "  \"P225\":[{\"mainsnak\":{\"datavalue\":{\"value\":\"Quercus alba\"}}}],"
        + "  \"P1420\":["
        + "    {\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q123\"}}}},"
        + "    {\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q456\"}}}}"
        + "  ]"
        + "}}";
    Map<String, String> out = new HashMap<>();
    WikidataDumpReader.collectSynonymLinks(parse(json), "Q469555", out);

    assertEquals("Q469555", out.get("Q123"));
    assertEquals("Q469555", out.get("Q456"));
    // the accepted name itself must not be recorded as a synonym
    assertNull(out.get("Q469555"));
    assertEquals(2, out.size());
  }

  @Test
  public void collectSynonymLinksNoP1420() throws Exception {
    String json = "{"
        + "\"id\":\"Q469555\","
        + "\"claims\":{"
        + "  \"P225\":[{\"mainsnak\":{\"datavalue\":{\"value\":\"Quercus alba\"}}}]"
        + "}}";
    Map<String, String> out = new HashMap<>();
    WikidataDumpReader.collectSynonymLinks(parse(json), "Q469555", out);
    assertEquals(0, out.size());
  }

  @Test
  public void extractAuthorshipRecombination() throws Exception {
    // Panthera tigris: P405=Q1043, P574=1758, P3831=Q14594740 (recombination)
    String json = "{"
        + "\"id\":\"Q19939\",\"claims\":{\"P225\":[{"
        + "  \"mainsnak\":{\"datavalue\":{\"value\":\"Panthera tigris\"}},"
        + "  \"qualifiers\":{"
        + "    \"P405\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q1043\"}}}],"
        + "    \"P574\":[{\"datavalue\":{\"value\":{\"time\":\"+1758-01-01T00:00:00Z\"}}}],"
        + "    \"P3831\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q14594740\"}}}]"
        + "  }}]}}";
    WikidataDumpReader.NameAuthorship na = WikidataDumpReader.extractAuthorship(parse(json));
    assertEquals(java.util.List.of("Q1043"), na.authorQids());
    assertEquals("1758", na.year());
    assertTrue(na.recombination());
  }

  @Test
  public void extractAuthorshipMultiAuthorOriginal() throws Exception {
    // Loxosceles reclusa: two authors, no recombination
    String json = "{"
        + "\"id\":\"Q284352\",\"claims\":{\"P225\":[{"
        + "  \"mainsnak\":{\"datavalue\":{\"value\":\"Loxosceles reclusa\"}},"
        + "  \"qualifiers\":{"
        + "    \"P405\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q955086\"}}},"
        + "             {\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q22114001\"}}}],"
        + "    \"P574\":[{\"datavalue\":{\"value\":{\"time\":\"+1940-01-01T00:00:00Z\"}}}]"
        + "  }}]}}";
    WikidataDumpReader.NameAuthorship na = WikidataDumpReader.extractAuthorship(parse(json));
    assertEquals(java.util.List.of("Q955086", "Q22114001"), na.authorQids());
    assertEquals("1940", na.year());
    assertFalse(na.recombination());
  }

  @Test
  public void collectAuthorRefs() throws Exception {
    String json = "{"
        + "\"id\":\"Q284352\",\"claims\":{\"P225\":[{"
        + "  \"mainsnak\":{\"datavalue\":{\"value\":\"Loxosceles reclusa\"}},"
        + "  \"qualifiers\":{\"P405\":["
        + "    {\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q955086\"}}},"
        + "    {\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q22114001\"}}}]}}]}}";
    java.util.Set<String> out = new java.util.HashSet<>();
    WikidataDumpReader.collectAuthorRefs(parse(json), out);
    assertEquals(2, out.size());
    assertTrue(out.contains("Q955086"));
    assertTrue(out.contains("Q22114001"));
  }

  @Test
  public void quantityExtraction() throws Exception {
    String json = "{\"amount\":\"+250\",\"unit\":\"http://www.wikidata.org/entity/Q11570\"}";
    assertEquals("+250", WikidataDumpReader.quantityAmount(parse(json)));
    assertEquals("Q11570", WikidataDumpReader.quantityUnitQid(parse(json)));

    String dimensionless = "{\"amount\":\"+3.5\",\"unit\":\"1\"}";
    assertEquals("+3.5", WikidataDumpReader.quantityAmount(parse(dimensionless)));
    assertNull(WikidataDumpReader.quantityUnitQid(parse(dimensionless)));
  }

  @Test
  public void collectPropertyLabelRefs() throws Exception {
    Map<String, WikidataDumpReader.TaxonPropInfo> tp = new java.util.HashMap<>();
    tp.put("P2974", new WikidataDumpReader.TaxonPropInfo("habitat", "WikibaseItem"));
    tp.put("P3063", new WikidataDumpReader.TaxonPropInfo("gestation period", "Quantity"));
    String json = "{\"id\":\"Q1\",\"claims\":{"
        + "\"P2974\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q123\"}}}}],"
        + "\"P3063\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"amount\":\"+100\",\"unit\":\"http://www.wikidata.org/entity/Q573\"}}}}],"
        + "\"P523\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q789\"}}}}]"
        + "}}";
    java.util.Set<String> out = new java.util.HashSet<>();
    WikidataDumpReader.collectPropertyLabelRefs(parse(json), tp, out);
    assertTrue(out.contains("Q123")); // habitat item value
    assertTrue(out.contains("Q573")); // gestation unit
    assertTrue(out.contains("Q789")); // temporal period
    assertEquals(3, out.size());
  }
}
