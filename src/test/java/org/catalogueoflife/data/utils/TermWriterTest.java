package org.catalogueoflife.data.utils;

import life.catalogue.api.datapackage.ColdpTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TermWriterTest {
  File dir;

  @BeforeEach
  public void init() throws Exception {
    dir = Files.createTempDirectory("tw-").toFile();
    dir.deleteOnExit();
  }

  @Test
  public void next() throws Exception {
    TermWriter tw = new TermWriter(dir, ColdpTerm.NameUsage, ColdpTerm.taxonID, List.of(
        ColdpTerm.scientificName, ColdpTerm.rank, ColdpTerm.family
    ));

    tw.set(ColdpTerm.taxonID, "567");
    tw.set(ColdpTerm.scientificName, "Abies alba");
    assertEquals("567", tw.next());

    tw.set(ColdpTerm.taxonID, "56");
    tw.set(ColdpTerm.rank, "species");
    tw.set(ColdpTerm.family, "Asteraceae");
    assertEquals("56", tw.get(ColdpTerm.taxonID));
    assertEquals("species", tw.get(ColdpTerm.rank));
    assertNull(tw.get(ColdpTerm.scientificName));
    assertEquals("56", tw.next());
  }

  @Test()
  public void badTerm() throws Exception {
    TermWriter tw = new TermWriter(dir, ColdpTerm.NameUsage, ColdpTerm.taxonID, List.of(
        DwcTerm.scientificName
    ));

    tw.set(ColdpTerm.taxonID, "567");
    tw.set(DwcTerm.scientificName, "Abies alba");
    assertEquals("Abies alba", tw.get(DwcTerm.scientificName));
    assertNull(tw.get(ColdpTerm.scientificName));
    assertEquals("567", tw.next());

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      tw.set(ColdpTerm.scientificName, "56");
    });
  }
}