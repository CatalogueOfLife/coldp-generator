package org.catalogueoflife.data.antcat;

import junit.framework.TestCase;

public class GeneratorTest extends TestCase {

  public void testRefDate() {
    Generator.Reference r = new Generator.Reference();
    r.date = "19820428";
    assertEquals("1982-04-28", r.getDate());
    r.date = "199506";
    assertEquals("1995-06", r.getDate());
    r.date = "1995";
    assertEquals("1995", r.getDate());
  }
}