package org.catalogueoflife.data.dadis;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    // letters NFKD does not decompose
    assertEquals(DadisMappings.key("Kłusak"), DadisMappings.key("Klusak"));
    assertEquals("deutsches sattelschwein", DadisMappings.key("Deutsches Sattelschwein"));
    assertEquals("weisse gebirgsziege", DadisMappings.key("Weiße Gebirgsziege"));
    assertEquals("faeroe", DadisMappings.key("Færøe"));
    // Cyrillic is transliterated
    assertEquals("azerbaidzhanskii-kavkazskii-buivol", slug("Азербайджанский (кавказский) буйвол"));
    assertEquals("shvedish-red", slug("Шведиш Ред"));
    // compatibility forms
    assertEquals("pin2zyl", slug("Pin²zyl"));
    // breeds ignore spacing and hyphens altogether
    assertEquals(breedKey("Hy-line"), breedKey("Hyline"));
    assertEquals(breedKey("Satsuma-Dori"), breedKey("Satsumadori"));
    assertNotEquals(breedKey("Limousin"), breedKey("limousine"));
  }

  @Test
  public void clean() {
    assertEquals("Criollo", DadisMappings.clean("\"Criollo\""));
    assertEquals("Commercial strain STAR-53", DadisMappings.clean("Commercial strain \"STAR-53\""));
    assertEquals("Muscovy Duck (Makou)", DadisMappings.clean("Muscovy Duck (\"Makou\")"));
    assertEquals("Jersey", DadisMappings.clean("  Jersey "));
    assertNull(DadisMappings.clean(" \" "));
    // Cyrillic homoglyphs in Latin names
    assertEquals("LINE E", DadisMappings.clean("LINE Е"));
    assertEquals("LINE C3 - 80 B", DadisMappings.clean("LINE СЗ – 80 B"));
    assertEquals("Gunsi - Alpha", DadisMappings.clean("Gunsi — Alpha"));
    assertEquals("SYNTHETIC LINE C", DadisMappings.clean("SYNTHETIC LINE С"));
    // Cyrillic names stay
    assertEquals("Шведиш Ред", DadisMappings.clean("Шведиш Ред"));
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
    // a word-final apostrophe is a letter
    assertEquals("Ovis aries 'Kazakhskaya Korridel’'", DadisMappings.breedName("Ovis aries", "Kazakhskaya Korridel'"));
    assertEquals("Gallus gallus domesticus 'Gaga’'", DadisMappings.breedName("Gallus gallus domesticus", "Gaga'"));
    assertEquals("Ovis aries 'Korridel’ Mutton'", DadisMappings.breedName("Ovis aries", "Korridel’ Mutton"));
    assertEquals("Ts´üdi Xirgo", epithet("Ts´üdi Xirgo"));
  }

  /** Every refile entry names DAD-IS species of species.tsv. */
  @Test
  public void refiles() throws Exception {
    Set<String> species = new HashSet<>();
    try (var in = getClass().getResourceAsStream("/dadis/species.tsv")) {
      new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().skip(1)
          .forEach(l -> species.add(l.split("\t")[0]));
    }
    var refiles = readRefiles();
    assertEquals(9, refiles.size());
    for (Refile r : refiles.values()) {
      assertTrue(r.toString(), species.contains(r.species()));
      assertTrue(r.toString(), species.contains(r.target()));
    }
    Refile r = refiles.get(refileKey("Duck(domestic) Muscovy duck", "Muscovy Duck (\"Makou\")"));
    assertEquals("Muscovy duck", r.target());
    assertTrue(r.speciesOnly());
    assertFalse(refiles.get(refileKey("Duck(domestic) Muscovy duck", "Svensk Myskanka")).speciesOnly());
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
