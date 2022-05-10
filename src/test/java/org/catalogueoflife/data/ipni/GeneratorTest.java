package org.catalogueoflife.data.ipni;

import org.junit.Test;

import static org.junit.Assert.*;

public class GeneratorTest {

  @Test
  public void collations() {
    assertEquals(new Generator.Collation("23", "8", "123-145"), Generator.parseCollation("23(8): 123-145"));
    assertEquals(new Generator.Collation("2020", "93", "1"), Generator.parseCollation("2020-93: 1"));
    assertEquals(new Generator.Collation("75(4)-64", null, "6"), Generator.parseCollation("75(4)-64: 6"));
    assertEquals(new Generator.Collation("52", "2-4", "339"), Generator.parseCollation("52(2-4): 339"));
    assertEquals(new Generator.Collation("n.s., 107(1)", null, "26"), Generator.parseCollation("n.s., 107(1): 26"));
    assertEquals(new Generator.Collation("74(4)-52", null, "9"), Generator.parseCollation("74(4)-52: 9"));
    assertEquals(new Generator.Collation("176", null, null), Generator.parseCollation("176"));
    assertEquals(new Generator.Collation("7(e6528)", null, "22"), Generator.parseCollation("7(e6528): 22"));
  }

  @Test
  public void lsid() {
    assertEquals("17541030-1", Generator.idFromLsid("urn:lsid:ipni.org:names:17541030-1"));
  }
}