package org.catalogueoflife.data.ipni;

import org.junit.Test;

import static org.junit.Assert.*;

public class GeneratorTest {

  @Test
  public void collations() {
    assertEquals(new Generator.Collation("23", "8", "123-145"), Generator.parseCollation("23(8): 123-145"));
    assertEquals(new Generator.Collation("2020", "93", "1"), Generator.parseCollation("2020-93: 1"));
    assertEquals(new Generator.Collation("75(4)-64", null, "6"), Generator.parseCollation("75(4)-64: 6"));
    assertEquals(new Generator.Collation("52", "2-4", "339"), Generator.parseCollation("52(2-4): 339"));
    assertEquals(new Generator.Collation("n.s., 107(1)", null, "26"), Generator.parseCollation("n.s., 107(1): 26"));
    assertEquals(new Generator.Collation("74(4)-52", null, "9"), Generator.parseCollation("74(4)-52: 9"));
    assertEquals(new Generator.Collation("176", null, null), Generator.parseCollation("176"));
    assertEquals(new Generator.Collation("7(e6528)", null, "22"), Generator.parseCollation("7(e6528): 22"));
  }

  @Test
  public void lsid() {
    assertEquals("17541030-1", Generator.idFromLsid("urn:lsid:ipni.org:names:17541030-1"));
  }

  @Test
  public void doi() {
    assertEquals("10.1093/botlinnean/boaa099", Generator.extractDOI("doi:10.1093/botlinnean/boaa099"));
    assertEquals("10.11646/phytotaxa.523.1.9", Generator.extractDOI("doi:10.11646/phytotaxa.523.1.9"));
    assertEquals("10.11646/phytotaxa.523.1.9", Generator.extractDOI("http://doi.org/10.11646/phytotaxa.523.1.9"));
    assertEquals("10.11646/phytotaxa.522.2.5", Generator.extractDOI("doi:10.11646/phytotaxa.522.2.5. The specific epithet refers to the non-maculated purple color of the new and older stems under dry conditions."));
    assertEquals("10.1093/botlinnean/boab013", Generator.extractDOI("doi:10.1093/botlinnean/boab013 The name is dedicated to Dr Frédéric Achille of the Museum of Natural History, Paris, in recognition of his contributions that have provided significant insights into the systematics of Guettardeae."));
    assertNull(Generator.extractDOI(null));
    assertNull(Generator.extractDOI(""));
    assertNull(Generator.extractDOI("  "));
    assertNull(Generator.extractDOI("10.7"));
    assertNull(Generator.extractDOI("doi:gbif.org"));
  }
}