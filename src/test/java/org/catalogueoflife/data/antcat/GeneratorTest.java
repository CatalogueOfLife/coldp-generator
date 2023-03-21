package org.catalogueoflife.data.antcat;

import org.catalogueoflife.data.GeneratorConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GeneratorTest {

  @Test
  public void testRefDate() {
    Generator.Reference r = new Generator.Reference();
    r.date = "19820428";
    Assert.assertEquals("1982-04-28", r.getDate());
    r.date = "199506";
    assertEquals("1995-06", r.getDate());
    r.date = "1995";
    assertEquals("1995", r.getDate());
  }

  @Test
  public void replVars() throws IOException {
    var cfg = new GeneratorConfig();
    cfg.source="test";
    Generator g = new Generator(cfg);

    assertNull(g.replVars(null));

    var x = g.replVars("Primary type material: lectotype minor worker");
    assertEquals("Primary type material: lectotype minor worker", x);

    x = g.replVars("Primary type material: lectotype minor worker (by designation of {ref 143981}: 53). Primary type locality");
    assertEquals("Primary type material: lectotype minor worker (by designation of ???: 53). Primary type locality", x);

    x = g.replVars("Palearctic. Forms: queen, male. {ref 143117}: 146, retain the paraphyletic genus {tax 429801}. Secondary type material: 16 paratype queens");
    assertEquals("Palearctic. Forms: queen, male. ???: 146, retain the paraphyletic genus ???. Secondary type material: 16 paratype queens", x);

  }
}