package org.catalogueoflife.data.wikidata;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GeneratorParseAuthorsTest {

  @Test public void parseAuthors() throws Exception {
    // Linnaeus: P734/P735 absent → family falls back to P835 citation, given from label
    String json = "{\"results\":{\"bindings\":[{"
        + "\"a\":{\"value\":\"http://www.wikidata.org/entity/Q1043\"},"
        + "\"lab\":{\"value\":\"Carl Linnaeus\"},"
        + "\"ab\":{\"value\":\"L.\"},"
        + "\"c\":{\"value\":\"Linnaeus\"},"
        + "\"b\":{\"value\":\"1707-05-23T00:00:00Z\"},"
        + "\"d\":{\"value\":\"1778-01-10T00:00:00Z\"},"
        + "\"s\":{\"value\":\"male\"},"
        + "\"cc\":{\"value\":\"SE\"}"
        + "}]}}";
    Map<String, WikidataDumpReader.AuthorInfo> target = new HashMap<>();
    Generator.parseSparqlAuthors(json, target);

    WikidataDumpReader.AuthorInfo a = target.get("Q1043");
    assertNotNull(a);
    assertEquals("Linnaeus", a.family());
    assertEquals("Carl", a.given());
    assertEquals("L.", a.abbreviationBotany());
    assertEquals("1707", a.birth());
    assertEquals("1778", a.death());
    assertEquals("male", a.sex());
    assertEquals("SE", a.country());
  }
}
