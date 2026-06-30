package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GeneratorAuthorshipFixtureTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static Map<String, WikidataDumpReader.AuthorInfo> authors() {
    var m = new HashMap<String, WikidataDumpReader.AuthorInfo>();
    m.put("Q1043", new WikidataDumpReader.AuthorInfo("Carl", "Linnaeus", "L.", "1707", "1778", "SE", "male", null, null));
    return m;
  }

  @Test public void tigerRecombinationEndToEnd() throws Exception {
    String json = "{\"id\":\"Q19939\",\"claims\":{\"P225\":[{"
        + "\"mainsnak\":{\"datavalue\":{\"value\":\"Panthera tigris\"}},"
        + "\"qualifiers\":{"
        + "  \"P405\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q1043\"}}}],"
        + "  \"P574\":[{\"datavalue\":{\"value\":{\"time\":\"+1758-01-01T00:00:00Z\"}}}],"
        + "  \"P3831\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q14594740\"}}}]"
        + "}}]}}";
    var na = WikidataDumpReader.extractAuthorship(M.readTree(json));
    var a = WikidataMappings.assembleAuthorship(na, authors());
    assertEquals("(Linnaeus, 1758)", a.flat());
    assertEquals("Linnaeus", a.basionymAuthorship());
    assertEquals("1758", a.basionymAuthorshipYear());
    assertNull(a.combinationAuthorship());
  }
}
