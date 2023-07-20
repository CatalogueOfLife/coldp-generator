package org.catalogueoflife.data.ipni;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class GeneratorTest {

  @Test
  public void lsid() {
    assertEquals("17541030-1", Generator.idFromLsid("urn:lsid:ipni.org:names:17541030-1"));
  }

  @Test
  public void uniqueReference() {
    Set<Generator.Reference> refs = new HashSet<>();
    refs.add(new Generator.Reference("12345"));
    refs.add(new Generator.Reference("12345"));
    assertEquals(1, refs.size());

    refs.add(new Generator.Reference("12345", "23(8): 123-145"));
    assertEquals(2, refs.size());
    refs.add(new Generator.Reference("12345", "23(8): 123-145"));
    assertEquals(2, refs.size());
    refs.add(new Generator.Reference("12345", "23(8): 33"));
    assertEquals(3, refs.size());
    refs.add(new Generator.Reference("12345", "21(8): 33"));
    assertEquals(4, refs.size());
    refs.add(new Generator.Reference("vcece", "21(8): 33"));
    assertEquals(5, refs.size());
  }

  @Test
  public void collations() {
    assertRef("23", "8", "123-145", "23(8): 123-145");
    assertRef("2020", "93", "1", "2020-93: 1");
    assertRef("75(4)-64", null, "6", "75(4)-64: 6");
    assertRef("52", "2-4", "339", "52(2-4): 339");
    assertRef("n.s., 107(1)", null, "26", "n.s., 107(1): 26");
    assertRef("74(4)-52", null, "9", "74(4)-52: 9");
    assertRef("176", null, null, "176");
    assertRef("7(e6528)", null, "22", "7(e6528): 22");
  }

  void assertRef(String vol, String issue, String pages, String collation) {
    var ref = new Generator.Reference("1", collation);
    assertEquals(vol, ref.volume);
    assertEquals(issue, ref.issue);
    assertEquals(pages, ref.pages);
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

  @Test
  public void buildCollectionDate() {
    assertEquals("1989-8-23", Generator.buildCollectionDate("1989", "8", "23"));
    assertEquals("1989-8", Generator.buildCollectionDate("1989", "8", null));
    assertEquals("1989", Generator.buildCollectionDate("1989", "", null));
  }

}