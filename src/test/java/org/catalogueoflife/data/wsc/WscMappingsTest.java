package org.catalogueoflife.data.wsc;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class WscMappingsTest {

  @Test
  public void status() {
    assertEquals(WscMappings.ACCEPTED, WscMappings.status("VALID"));
    assertEquals(WscMappings.SYNONYM, WscMappings.status("SYNONYM"));
    // the bug: HOMONYM_REPLACED is not a ColDP status, so CLB fell back to accepted and the
    // record ended up as an accepted species parented to another accepted species
    assertEquals(WscMappings.SYNONYM, WscMappings.status("HOMONYM_REPLACED"));
    assertEquals(WscMappings.BARE_NAME, WscMappings.status("NOMEN_DUBIUM"));
    assertEquals(WscMappings.BARE_NAME, WscMappings.status("NOMEN_NUDUM"));
    assertEquals(WscMappings.BARE_NAME, WscMappings.status("DELETED"));
    assertEquals(WscMappings.ACCEPTED, WscMappings.status(" valid "));
    assertNull(WscMappings.status(null));
    // unknown values are passed through so a new WSC status surfaces in CLB instead of vanishing
    assertEquals("SOMETHING_NEW", WscMappings.status("SOMETHING_NEW"));
  }

  @Test
  public void nameStatus() {
    // only genuinely nomenclatural values belong here; SYNONYM & friends are taxonomic statuses
    // and produced 10.6k "nomenclatural status invalid" issues
    assertEquals("NOMEN_DUBIUM", WscMappings.nameStatus("NOMEN_DUBIUM"));
    assertEquals("NOMEN_NUDUM", WscMappings.nameStatus("NOMEN_NUDUM"));
    assertEquals("VALID", WscMappings.nameStatus("VALID"));
    assertNull(WscMappings.nameStatus("SYNONYM"));
    assertNull(WscMappings.nameStatus("HOMONYM_REPLACED"));
    assertNull(WscMappings.nameStatus("DELETED"));
    assertNull(WscMappings.nameStatus(null));
  }

  @Test
  public void speciesKey() {
    assertEquals("gen1|theisi", WscMappings.speciesKey("gen1", "Theisi"));
    assertNull(WscMappings.speciesKey(null, "theisi"));
    assertNull(WscMappings.speciesKey("gen1", null));
  }

  private static Generator.Taxon taxon(String rank, String genusLsid, String species, String famLsid) {
    var t = new Generator.Taxon();
    t.taxonRank = rank;
    t.species = species;
    if (genusLsid != null) {
      t.genusObject = new Generator.TaxonObject();
      t.genusObject.genLsid = genusLsid;
    }
    if (famLsid != null) {
      t.familyObject = new Generator.TaxonObject();
      t.familyObject.famLsid = famLsid;
    }
    return t;
  }

  @Test
  public void parentID() {
    Map<String, String> species = new HashMap<>();
    species.put("gen1|theisi", "sp:theisi");

    // the bug: a subspecies was parented to its genus, skipping the species level entirely
    assertEquals("sp:theisi",
        WscMappings.parentID(taxon("subspecies", "gen1", "theisi", "fam1"), species, "root"));
    // no accepted parent species known -> fall back to the genus rather than emitting nothing
    assertEquals("gen1",
        WscMappings.parentID(taxon("subspecies", "gen1", "dubia", "fam1"), species, "root"));

    assertEquals("gen1", WscMappings.parentID(taxon("species", "gen1", "theisi", "fam1"), species, "root"));
    assertEquals("fam1", WscMappings.parentID(taxon("genus", null, null, "fam1"), species, "root"));
    assertEquals("root", WscMappings.parentID(taxon("family", null, null, null), species, "root"));
  }
}
