package org.catalogueoflife.data.colac;

import org.gbif.nameparser.api.NomCode;
import org.junit.Test;

import java.util.List;

import static org.catalogueoflife.data.colac.ColacMappings.*;
import static org.junit.Assert.*;

public class ColacMappingsTest {

  @Test
  public void testNomCode() {
    assertEquals(NomCode.ZOOLOGICAL.getAcronym(), nomCode("Animalia"));
    assertEquals(NomCode.ZOOLOGICAL.getAcronym(), nomCode("Protozoa"));
    assertEquals(NomCode.BOTANICAL.getAcronym(), nomCode("Plantae"));
    assertEquals(NomCode.BOTANICAL.getAcronym(), nomCode("Fungi"));
    assertEquals(NomCode.BOTANICAL.getAcronym(), nomCode("Chromista"));
    assertEquals(NomCode.BACTERIAL.getAcronym(), nomCode("Bacteria"));
    assertEquals(NomCode.BACTERIAL.getAcronym(), nomCode("Archaea"));
    assertEquals(NomCode.VIRUS.getAcronym(), nomCode("Viruses"));
    assertNull(nomCode("Something"));
    assertNull(nomCode(null));
  }

  @Test
  public void testStatus() {
    assertEquals("accepted", status("accepted name"));
    assertEquals("provisionally accepted", status("provisionally accepted name"));
    assertEquals("synonym", status("synonym"));
    assertEquals("ambiguous synonym", status("ambiguous synonym"));
    assertEquals("misapplied", status("misapplied name"));
    // tolerant of case + surrounding whitespace
    assertEquals("accepted", status("  Accepted Name "));
    assertNull(status(null));
    assertNull(status(""));
  }

  @Test
  public void testEstablishment() {
    assertEquals("native", establishment("native"));
    assertEquals("native", establishment("native-domesticated"));
    assertEquals("introduced", establishment("alien"));
    assertEquals("introduced", establishment("alien-domesticated"));
    assertEquals("introduced", establishment("domesticated"));
    assertNull(establishment("uncertain"));
    assertNull(establishment(null));
  }

  @Test
  public void testSynonymName() {
    assertEquals("Helix bombycina", synonymName("Helix", "bombycina", "", ""));
    assertEquals("Helix spaldingi carinata", synonymName("Helix", "spaldingi", "", "carinata"));
    assertEquals("Helix spaldingi var. carinata", synonymName("Helix", "spaldingi", "var.", "carinata"));
    assertEquals("Salinator", synonymName("Salinator", "", "", ""));
    // collapse blanks / trim
    assertEquals("Helix bombycina", synonymName(" Helix ", " bombycina ", null, null));
    assertNull(synonymName(null, null, null, null));
    assertNull(synonymName("", "", "", ""));
  }

  @Test
  public void testJoinRefs() {
    assertEquals("r1,r2,r3", joinRefs(List.of("r1", "r2", "r3")));
    // de-duplicate, preserve first-seen order
    assertEquals("r1,r2", joinRefs(List.of("r1", "r1", "r2", "r1")));
    assertNull(joinRefs(List.of()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJoinRefsRejectsComma() {
    // a value containing the separator would corrupt the multi-valued field
    joinRefs(List.of("r1", "r2,r3"));
  }
}
