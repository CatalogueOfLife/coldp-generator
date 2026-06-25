package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
}
