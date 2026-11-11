package org.catalogueoflife.data.dadis;

import org.junit.Test;

import java.util.Map;

import static org.catalogueoflife.data.dadis.DadisMappings.*;
import static org.junit.Assert.*;

public class DadisMappingsTest {

  @Test
  public void key() {
    assertEquals(DadisMappings.key("Blonde d'Aquitaine"), DadisMappings.key("Blonde D’aquitaine"));
    assertEquals(DadisMappings.key("Aberdeen Angus"), DadisMappings.key("Aberdeen-Angus"));
    assertEquals(DadisMappings.key("Bérabiche"), DadisMappings.key("Berabiche"));
    assertEquals("yak domestic", DadisMappings.key("Yak (domestic)"));
    assertEquals("aberdeen-angus", slug("Aberdeen  Angus"));
  }

  @Test
  public void bestSpelling() {
    assertEquals("Jersey", DadisMappings.bestSpelling(Map.of("Jersey", 3, "jersey", 1)));
    assertEquals("Tibesti", DadisMappings.bestSpelling(Map.of("TIBESTI", 1, "Tibesti", 1)));
    assertEquals("Aberdeen Angus", DadisMappings.bestSpelling(Map.of("Aberdeen-Angus", 1, "Aberdeen Angus", 1)));
  }

  @Test
  public void iso2() {
    assertEquals("DE", DadisMappings.iso2("DEU"));
    assertEquals("GB", DadisMappings.iso2("GBR"));
    assertNull(DadisMappings.iso2("680"));
  }

  @Test
  public void language() {
    assertEquals("deu", DadisMappings.language("ger."));
    assertEquals("deu", DadisMappings.language("(ger.)"));
    assertEquals("deu", DadisMappings.language("ger,"));
    assertEquals("eng", DadisMappings.language("En"));
    assertEquals("nld", DadisMappings.language("neth."));
    assertEquals("spa", DadisMappings.language("span."));
    assertNull(DadisMappings.language(""));
    assertNull(DadisMappings.language("Kiswahili local"));
  }

  @Test
  public void statuses() {
    assertEquals("native", establishmentMeans("Native"));
    assertEquals("introduced", establishmentMeans("Exotic"));
    assertNull(establishmentMeans(" "));
    assertEquals("critically endangered", threatStatus("criticalMaintained"));
    assertEquals("least concern", threatStatus("notAtRisk"));
    assertEquals("extinct in the wild", threatStatus("cryoConservedOnly"));
    assertNull(threatStatus("whatever"));
    assertEquals("draught power", humanize("DraughtPower"));
    assertEquals("cryo conserved only", humanize("cryoConservedOnly"));
  }

  @Test
  public void breedName() {
    assertEquals("Bos taurus 'Jersey'", DadisMappings.breedName("Bos taurus", "Jersey"));
    assertEquals("Bos taurus 'Blonde d’Aquitaine'", DadisMappings.breedName("Bos taurus", "Blonde d'Aquitaine"));
  }

  @Test
  public void beeName() {
    assertEquals(new BeeName("Apis cerana", null), DadisMappings.beeName("Apis cerana (Unspecified)"));
    assertEquals(new BeeName("Melipona", null), DadisMappings.beeName("Melipona (Unspecified)"));
    assertEquals(new BeeName("Liotrigona", null), DadisMappings.beeName("Liotrigona sp"));
    assertEquals(new BeeName("Apis mellifera jemenitica", null), DadisMappings.beeName("Apis mellifera jemenitica (nubica)"));
    assertEquals(new BeeName("Apis mellifera simensis", null),
        DadisMappings.beeName("Apis mellifera simensis (woyi-gambella | bandasii)"));
    assertEquals(new BeeName("Scaptotrigona jujuyensis", null), DadisMappings.beeName("Scaptotrigona Jujuyensis"));
    assertEquals(new BeeName("Apis mellifera", "Buckfast"), DadisMappings.beeName("Apis mellifera buckfast hybrid"));
    assertEquals(new BeeName("Apis mellifera", null), DadisMappings.beeName("Apis mellifera other hybrids"));
    assertEquals("genus", DadisMappings.rank("Melipona"));
    assertEquals("species", DadisMappings.rank("Apis cerana"));
    assertEquals("subspecies", DadisMappings.rank("Apis cerana indica"));
  }
}
