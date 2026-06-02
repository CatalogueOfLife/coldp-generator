package org.catalogueoflife.data.colac;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

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
}
