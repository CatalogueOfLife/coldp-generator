package org.catalogueoflife.data.lcvp;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.catalogueoflife.data.lcvp.LcvpMappings.*;
import static org.junit.Assert.*;

public class LcvpMappingsTest {

  @Test
  public void hybridPart() {
    assertEquals(new Part("kentiana", true), Part.of("kentiana_x"));
    assertEquals(new Part("borisii-regis", true), Part.of("borisii-regis_x"));
    assertEquals(new Part("macrostolonum", true), Part.of("macrostolonum_x_x"));
    assertEquals(new Part("Serapicamptis", true), Part.of("Serapicamptis_x"));
    assertEquals(new Part("argyrolepis", false), Part.of("argyrolepis"));
    assertEquals(new Part(null, false), Part.of(null));
  }

  @Test
  public void cleanPlaceholders() {
    assertNull(clean("nil"));
    assertNull(clean("---"));
    assertNull(clean("  "));
    assertEquals("Aa argyrolepis", clean("Aa argyrolepis "));
  }

  @Test
  public void names() {
    assertEquals("Aa argyrolepis", scientificName(Part.of("Aa"), Part.of("argyrolepis"), "species", Part.of(null)));
    assertEquals("Abies ×kentiana", scientificName(Part.of("Abies"), Part.of("kentiana_x"), "species", Part.of(null)));
    assertEquals("×Serapicamptis rousii", scientificName(Part.of("Serapicamptis_x"), Part.of("rousii"), "species", Part.of(null)));
    assertEquals("Abies koreana f. nigrocarpa", scientificName(Part.of("Abies"), Part.of("koreana"), "forma", Part.of("nigrocarpa")));
    assertEquals("Achyranthes aspera var. pubescens", scientificName(Part.of("Achyranthes"), Part.of("aspera"), "var.", Part.of("pubescens")));
    assertEquals("Salix ambigua nothovar. maritima", scientificName(Part.of("Salix"), Part.of("ambigua"), "var.", Part.of("maritima_x")));
    assertEquals("Ophrys ×godferyana nothosubsp. fayencensis",
        scientificName(Part.of("Ophrys"), Part.of("godferyana_x"), "subsp.", Part.of("fayencensis_x")));
  }

  @Test
  public void nothoValues() {
    assertNull(notho(Part.of("Abies"), Part.of("alba"), Part.of(null)));
    assertEquals("generic", notho(Part.of("Agropogon_x"), Part.of("littoralis"), Part.of(null)));
    assertEquals("specific", notho(Part.of("Abies"), Part.of("kentiana_x"), Part.of(null)));
    assertEquals("infraspecific", notho(Part.of("Salix"), Part.of("ambigua"), Part.of("maritima_x")));
  }

  @Test
  public void statuses() {
    assertEquals("accepted", status("accepted"));
    assertEquals("synonym", status("synonym"));
    assertEquals("synonym", status("unresolved"));
    assertEquals("external", status("external"));
    assertNull(status(null));
  }

  @Test
  public void literatureSplit() {
    assertEquals(List.of(), literature(null));
    assertEquals(List.of("Lankesteriana 2011.11.1 1-8"), literature("Lankesteriana 2011.11.1 1-8; "));
    assertEquals(List.of("Mol Phyl Evol 2012.62.1 263-274", "Mol Phyl Evol 2018.125 14-28"),
        literature("Mol Phyl Evol 2012.62.1 263-274; Mol Phyl Evol 2018.125 14-28; "));
  }

  @Test
  public void acceptedChains() {
    Set<String> accepted = Set.of("1", "2");
    // 3 synonym of 1, 4 unresolved pointing to 2, 5 synonym pointing to unresolved 4, 6 missing, 7/8 loop
    Map<String, String> output = Map.of("3", "1", "4", "2", "5", "4", "7", "8", "8", "7");
    assertEquals("1", resolveAccepted("1", accepted, output));
    assertEquals("1", resolveAccepted("3", accepted, output));
    assertEquals("2", resolveAccepted("5", accepted, output));
    assertNull(resolveAccepted("6", accepted, output));
    assertNull(resolveAccepted("7", accepted, output));
    assertNull(resolveAccepted(null, accepted, output));
  }
}
