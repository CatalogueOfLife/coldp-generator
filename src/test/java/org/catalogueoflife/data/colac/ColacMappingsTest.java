package org.catalogueoflife.data.colac;

import org.gbif.nameparser.api.NomCode;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

  @Test
  public void testFirstRef() {
    // single-valued fields (VernacularName, …) take the first ref only
    assertEquals("r1", firstRef(List.of("r1", "r2", "r3")));
    // skips leading blanks
    assertEquals("r2", firstRef(Arrays.asList("", "  ", "r2", "r3")));
    assertNull(firstRef(List.of()));
    assertNull(firstRef(null));
    assertNull(firstRef(Arrays.asList("", "  ")));
  }

  @Test
  public void testParseEditors() {
    assertEquals("F.A.|Bisby; Y.R.|Roskov; T.M.|Orrell",
        fmt(parseEditors("Bisby F.A., Roskov Y.R., Orrell T.M.")));
    // single-initial surname
    assertEquals("Y.|Roskov", fmt(parseEditors("Roskov Y.")));
    // multi-part surname (no particle to move)
    assertEquals("A.|De Wever", fmt(parseEditors("De Wever A.")));
    // trailing lowercase particle moves before the surname
    assertEquals("E.|van Nieukerken", fmt(parseEditors("Nieukerken E. van")));
    // compound initials and camelCase surname
    assertEquals("R.E.|DeWalt", fmt(parseEditors("DeWalt R.E.")));
    assertTrue(parseEditors(null).isEmpty());
    assertTrue(parseEditors("  ").isEmpty());
  }

  // accepted name_code → emitted taxon id
  private static final Map<String, String> ACC = Map.of(
      "A-acc", "t100",   // an accepted species
      "B-acc", "t200");  // another accepted species
  // synonym name_code → its declared accepted_name_code (may itself be a synonym = a chain)
  private static final Map<String, String> SYN = new HashMap<>() {{
    put("S1", "A-acc");  // direct: synonym of an accepted name
    put("S2", "S1");     // chain: synonym of a synonym (→ S1 → A-acc)
    put("S3", "S2");     // longer chain (→ S2 → S1 → A-acc)
    put("X1", "X-none"); // dead end: accepted_name_code never resolves
    put("C1", "C2");     // cycle
    put("C2", "C1");
  }};

  @Test
  public void testResolveAcceptedTaxon() {
    // an accepted name_code maps straight to its emitted taxon
    assertEquals("t100", resolveAcceptedTaxon("A-acc", ACC, SYN));
    // a synonym resolves to its accepted taxon
    assertEquals("t100", resolveAcceptedTaxon("S1", ACC, SYN));
    // synonym→synonym chains are followed to the accepted terminus
    assertEquals("t100", resolveAcceptedTaxon("S2", ACC, SYN));
    assertEquals("t100", resolveAcceptedTaxon("S3", ACC, SYN));
  }

  @Test
  public void testResolveAcceptedTaxonUnresolvable() {
    // accepted_name_code points to an unknown code → null (reference dropped, not dangling)
    assertNull(resolveAcceptedTaxon("X1", ACC, SYN));
    // a name_code that is neither accepted nor a known synonym → null
    assertNull(resolveAcceptedTaxon("ghost", ACC, SYN));
    assertNull(resolveAcceptedTaxon(null, ACC, SYN));
    // a cycle terminates via the guard and yields null
    assertNull(resolveAcceptedTaxon("C1", ACC, SYN));
  }

  @Test
  public void testNormCodeIsCaseInsensitive() {
    // the 2005 source is case-inconsistent (a synonym's accepted_name_code "Mos-35136210" vs the
    // accepted name's name_code "MOS-35136210"); both must collapse to one key
    assertEquals(normCode("Mos-35136210"), normCode("MOS-35136210"));
    assertEquals("ARA-19298", normCode("  Ara-19298 "));
    assertNull(normCode(null));
    assertNull(normCode("   "));
  }

  @Test
  public void testStatusEarlyVocabulary() {
    assertEquals("synonym", status("unambiguous synonym"));
    assertEquals("synonym", status("unambiguous synonyms"));
    assertEquals("ambiguous synonym", status("ambiguous synonyms"));
  }

  private static String fmt(List<String[]> eds) {
    return eds.stream().map(e -> e[0] + "|" + e[1]).collect(Collectors.joining("; "));
  }

  @Test
  public void testParseAgents() {
    var a = parseAgents("Vignes-Lebbe R. & Gallut C.");
    assertEquals("R.|Vignes-Lebbe; C.|Gallut", fmt(a.authors()));
    assertEquals("", fmt(a.editors()));

    // trailing (eds) marks the whole clause as editors
    var b = parseAgents("Shimura J.,  Hiraki K. &  Garrity G.M. (eds)");
    assertEquals("", fmt(b.authors()));
    assertEquals("J.|Shimura; K.|Hiraki; G.M.|Garrity", fmt(b.editors()));

    // leading © is dropped
    assertEquals("M.|Guiry", fmt(parseAgents("© Guiry M.").authors()));

    // scope tag "(Pulmonata)" is dropped (authors), separate "(ed)" clause is editors
    var c = parseAgents("Smith B.J., Reid S. & Ponder W.F. (Pulmonata); Wells A. (ed)");
    assertEquals("B.J.|Smith; S.|Reid; W.F.|Ponder", fmt(c.authors()));
    assertEquals("A.|Wells", fmt(c.editors()));

    var empty = parseAgents(null);
    assertTrue(empty.authors().isEmpty() && empty.editors().isEmpty());
  }
}
