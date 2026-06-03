package org.catalogueoflife.data.colac;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

public class EarlySchemaReaderTest {
  @Test
  public void testPathId() {
    assertEquals("h|Plantae", EarlySchemaReader.pathId(List.of("Plantae")));
    assertEquals("h|Plantae|Rhodophyta|Palmariaceae|Palmaria",
        EarlySchemaReader.pathId(List.of("Plantae", "Rhodophyta", "Palmariaceae", "Palmaria")));
    assertNull(EarlySchemaReader.pathId(List.of()));
  }
  @Test
  public void testParentPathId() {
    assertEquals("h|Plantae|Rhodophyta",
        EarlySchemaReader.parentPathId("h|Plantae|Rhodophyta|Palmariaceae"));
    assertNull(EarlySchemaReader.parentPathId("h|Plantae")); // kingdom is a root
    assertNull(EarlySchemaReader.parentPathId(null));
  }
  @Test
  public void testCleanLanguage() {
    // real language names pass through for CLB to map to an ISO code
    assertEquals("English", EarlySchemaReader.cleanLanguage("English"));
    assertEquals("Creole, French", EarlySchemaReader.cleanLanguage("Creole, French"));
    // "no language" sentinels become null
    assertNull(EarlySchemaReader.cleanLanguage("Not specified"));
    assertNull(EarlySchemaReader.cleanLanguage("missing"));
    assertNull(EarlySchemaReader.cleanLanguage("Other"));
    assertNull(EarlySchemaReader.cleanLanguage("  "));
    assertNull(EarlySchemaReader.cleanLanguage(null));
  }

  @Test
  public void testNameKey() {
    // case-insensitive; the "none" infra sentinel and a blank infra collapse to the same key
    assertEquals(EarlySchemaReader.nameKey("Animalia-Mollusca", "Nautilus", "pompilius", "none"),
                 EarlySchemaReader.nameKey("animalia-mollusca", "NAUTILUS", "Pompilius", ""));
    // a different species epithet is a different key
    assertNotEquals(EarlySchemaReader.nameKey("H", "Nautilus", "pompilius", null),
                    EarlySchemaReader.nameKey("H", "Nautilus", "belauensis", null));
    // an infraspecies key differs from the bare species key
    assertNotEquals(EarlySchemaReader.nameKey("H", "Aus", "bus", "cus"),
                    EarlySchemaReader.nameKey("H", "Aus", "bus", "none"));
  }
}
