package org.catalogueoflife.data.wikidata;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class WikidataMappingsTest {

  @Test public void extractYear() {
    assertEquals("1758", WikidataMappings.extractYear("+1758-01-01T00:00:00Z"));
    assertEquals("1753", WikidataMappings.extractYear("1753"));
    assertNull(WikidataMappings.extractYear(null));
  }

  @Test public void joinFamilies() {
    assertEquals("Linnaeus", WikidataMappings.joinFamilies(List.of("Linnaeus")));
    assertEquals("Gertsch & Mulaik", WikidataMappings.joinFamilies(List.of("Gertsch", "Mulaik")));
    assertEquals("A, B & C", WikidataMappings.joinFamilies(List.of("A", "B", "C")));
    assertNull(WikidataMappings.joinFamilies(List.of()));
  }

  @Test public void flatAuthorship() {
    assertEquals("Gertsch & Mulaik, 1940",
        WikidataMappings.flatAuthorship(List.of("Gertsch", "Mulaik"), "1940", false));
    assertEquals("(Linnaeus, 1758)",
        WikidataMappings.flatAuthorship(List.of("Linnaeus"), "1758", true));
    assertEquals("Linnaeus",
        WikidataMappings.flatAuthorship(List.of("Linnaeus"), null, false));
    assertNull(WikidataMappings.flatAuthorship(List.of(), "1940", false));
  }

  @Test public void labelSplit() {
    assertEquals("Linnaeus", WikidataMappings.lastToken("Carl Linnaeus"));
    assertEquals("Carl", WikidataMappings.givenFromLabel("Carl Linnaeus", "Linnaeus"));
    assertNull(WikidataMappings.givenFromLabel("Linnaeus", "Linnaeus"));
  }

  @Test public void sex() {
    assertEquals("male", WikidataMappings.mapSex("Q6581097"));
    assertEquals("female", WikidataMappings.mapSex("Q6581072"));
    assertNull(WikidataMappings.mapSex("Q1"));
  }
}
