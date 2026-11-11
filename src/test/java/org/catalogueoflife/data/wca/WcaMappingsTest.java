package org.catalogueoflife.data.wca;

import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.catalogueoflife.data.wca.WcaMappings.*;
import static org.junit.Assert.*;

public class WcaMappingsTest {

  @Test
  public void id() {
    assertEquals("musa-textilis", WcaMappings.id("Musa textilis"));
    assertEquals("xtriticosecale", WcaMappings.id("×Triticosecale"));
    assertEquals("brassica-oleracea-var-botrytis", WcaMappings.id("Brassica oleracea var. botrytis"));
  }

  @Test
  public void rank() {
    assertEquals("genus", WcaMappings.rank("Rubus"));
    assertEquals("genus", WcaMappings.rank("×Triticosecale"));
    assertNull(WcaMappings.rank("Rubus idaeus"));
  }

  @Test
  public void cpc() {
    var codes = WcaMappings.cpc("01919.90* (fodder), 01259* (red), 01801 (sugar), 01803 (sugar for seeds)");
    assertEquals(4, codes.size());
    assertEquals(new CpcCode("01919.90", true, "fodder"), codes.get(0));
    assertEquals(new CpcCode("01801", false, "sugar"), codes.get(2));
    assertEquals(new CpcCode("01343", false, null), WcaMappings.cpc("01343").getFirst());
    assertTrue(WcaMappings.cpc("").isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void badCpc() {
    WcaMappings.cpc("01930.90* 03249");
  }

  /** The curated tables must be consistent: every crop has a known ICC 1.2 class and parseable CPC codes. */
  @Test
  public void curatedTables() throws Exception {
    Map<String, IccClass> icc = readIcc();
    List<Crop> crops = readCrops();
    assertEquals(198, icc.size());
    assertEquals(312, crops.size());
    for (IccClass c : icc.values()) {
      assertTrue(c.code(), c.parent().isEmpty() || icc.containsKey(c.parent()));
    }
    Set<String> kingdoms = new HashSet<>();
    for (Crop c : crops) {
      assertTrue(c.crop(), icc.containsKey(c.icc12()));
      assertFalse(c.crop(), c.scientificName().isBlank());
      WcaMappings.cpc(c.cpc());
      kingdoms.add(c.kingdom());
    }
    assertEquals(Set.of("Plantae", "Fungi"), kingdoms);

    assertEquals("Cereals > Wheat", path(icc, "1.01"));
    assertEquals("Other crops > Fibre crops > Temporary fibre crops > Cotton", path(icc, "9.02.01.01"));
    assertEquals("temporary", cropType(icc, "1.01"));
    assertEquals("permanent", cropType(icc, "4.04.01"));
    // the spice class is mixed, its subclasses decide
    assertNull(cropType(icc, "6.02"));
    assertEquals("permanent", cropType(icc, "6.02.02.01"));
  }
}
