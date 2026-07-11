package org.catalogueoflife.data.lpsn;

import life.catalogue.common.io.Resources;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Extraction tests for {@link LpsnPage} against saved LPSN website fixtures.
 * The fixtures reproduce the real {@code #detail-page} markup captured from
 * https://lpsn.dsmz.de for a genus, species, subspecies, accepted family and a synonym.
 */
public class LpsnPageTest {

  private LpsnPage parse(String fixture) throws IOException {
    return LpsnPage.parse(Resources.toString("lpsn/" + fixture));
  }

  @Test
  public void genus() throws IOException {
    var p = parse("genus-aerofilum.html");
    assertEquals(Integer.valueOf(12732), p.recordNo);
    assertEquals("Aerofilum", p.name);
    assertEquals("Chakraborty and Mukherjee 2021", p.author);
    assertEquals("genus", p.rank);
    assertEquals("validly published under the ICN (Botanical Code)", p.nomStatus);
    assertEquals("correct name", p.taxStatus);
    assertEquals("/species/aerofilum-fasciculatum", p.typeLink);
    assertEquals("/family/oculatellaceae", p.parentTaxonLink);
    assertEquals(1, p.childTaxaLinks.size());
    assertEquals("/species/aerofilum-fasciculatum", p.childTaxaLinks.get(0));
    assertTrue(p.synonymLinks.isEmpty());
    assertNull(p.correctNameLink);
  }

  @Test
  public void species() throws IOException {
    var p = parse("species-aerofilum-fasciculatum.html");
    assertEquals(Integer.valueOf(12733), p.recordNo);
    assertEquals("Aerofilum fasciculatum", p.name);
    assertEquals("Chakraborty and Mukherjee 2021", p.author);
    assertEquals("species", p.rank);
    assertTrue(p.childTaxaLinks.isEmpty());
  }

  @Test
  public void subspeciesKeepsConnectorInName() throws IOException {
    var p = parse("subspecies-acetobacter-aceti-aceti.html");
    assertEquals("Acetobacter aceti subsp. aceti", p.name);
    assertEquals("(Pasteur 1864) De Ley and Frateur 1974 (Approved Lists 1980)", p.author);
    assertEquals("subspecies", p.rank);
    assertEquals("/species/acetobacter-aceti", p.basionymLink);
  }

  @Test
  public void synonymWithItalicEtAlInAuthor() throws IOException {
    var p = parse("synonym-elainellaceae.html");
    assertEquals(Integer.valueOf(43552), p.recordNo);
    assertEquals("Elainellaceae", p.name);
    assertEquals("Chuvochina et al. 2024", p.author);
    assertEquals("family", p.rank);
    assertEquals("synonym", p.taxStatus);
    assertEquals("/family/oculatellaceae", p.correctNameLink);
    assertTrue(p.childTaxaLinks.isEmpty());
  }

  @Test
  public void acceptedFamilyChildAndSynonymLinks() throws IOException {
    var p = parse("family-oculatellaceae.html");
    assertEquals(Integer.valueOf(6116), p.recordNo);
    assertEquals("Oculatellaceae", p.name);
    assertEquals("Mai and Johansen 2018", p.author);
    // child taxa are the genera, NOT the parent link or synonyms
    assertEquals(2, p.childTaxaLinks.size());
    assertTrue(p.childTaxaLinks.contains("/genus/aerofilum"));
    assertTrue(p.childTaxaLinks.contains("/genus/oculatella"));
    // synonyms section is separate
    assertEquals(1, p.synonymLinks.size());
    assertEquals("/family/elainellaceae", p.synonymLinks.get(0));
  }
}
