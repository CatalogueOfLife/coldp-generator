package org.catalogueoflife.data.ncbi;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class GeneratorTest {

  // ── split() ───────────────────────────────────────────────────────────────

  @Test
  public void testSplitTypicalLine() {
    // Standard nodes.dmp / names.dmp record ending with \t|
    String line = "1\t|\t1\t|\tno rank\t|\t\t|\t8\t|\t";
    String[] row = Generator.split(line);
    assertNotNull(row);
    assertEquals("1",       row[0].trim());
    assertEquals("1",       row[1].trim());
    assertEquals("no rank", row[2].trim());
  }

  @Test
  public void testSplitNameLine() {
    // Typical names.dmp line
    String line = "9606\t|\tHomo sapiens\t|\t\t|\tscientific name\t|";
    String[] row = Generator.split(line);
    assertNotNull(row);
    assertEquals("9606",          row[0].trim());
    assertEquals("Homo sapiens",  row[1].trim());
    assertEquals("",              row[2].trim());
    assertEquals("scientific name", row[3].trim());
  }

  @Test
  public void testSplitBlankLine() {
    assertNull(Generator.split(""));
    assertNull(Generator.split("   "));
    assertNull(Generator.split(null));
  }

  @Test
  public void testSplitTrailingPipeVariants() {
    // Lines may end with just | (without leading tab)
    String line = "42\t|\tHomo\t|\tscientific name\t|";
    String[] row = Generator.split(line);
    assertNotNull(row);
    assertEquals("42",             row[0].trim());
    assertEquals("Homo",           row[1].trim());
    assertEquals("scientific name", row[2].trim());
  }

  // ── col() ─────────────────────────────────────────────────────────────────

  @Test
  public void testColNormal() {
    String[] row = {"9606", "Homo sapiens", "", "scientific name"};
    assertEquals("9606",            Generator.col(row, 0));
    assertEquals("Homo sapiens",    Generator.col(row, 1));
    assertNull("blank col should be null", Generator.col(row, 2));
    assertEquals("scientific name", Generator.col(row, 3));
  }

  @Test
  public void testColOutOfBounds() {
    String[] row = {"1", "2"};
    assertNull(Generator.col(row, 5));
    assertNull(Generator.col(null, 0));
  }

  @Test
  public void testColTrimsWhitespace() {
    String[] row = {"  9606  ", "  Homo sapiens  "};
    assertEquals("9606",         Generator.col(row, 0));
    assertEquals("Homo sapiens", Generator.col(row, 1));
  }

  // ── parseInt() ───────────────────────────────────────────────────────────

  @Test
  public void testParseIntValid() {
    String[] row = {"9606", "1"};
    assertEquals(9606, Generator.parseInt(row, 0));
    assertEquals(1,    Generator.parseInt(row, 1));
  }

  @Test
  public void testParseIntBlankOrMissing() {
    String[] row = {"", "abc"};
    assertEquals(0, Generator.parseInt(row, 0));
    assertEquals(0, Generator.parseInt(row, 1));
    assertEquals(0, Generator.parseInt(row, 5));
  }

  // ── extractAuthorship() ───────────────────────────────────────────────────

  @Test
  public void testExtractAuthorshipNormal() {
    assertEquals("L., 1753",
        Generator.extractAuthorship("Quercus robur", List.of("Quercus robur L., 1753")));
  }

  @Test
  public void testExtractAuthorshipWithParentheses() {
    assertEquals("(Rehder & E.H.Wilson) W.E.Manning, 1975",
        Generator.extractAuthorship(
            "Pterocarya macroptera var. insignis",
            List.of("Pterocarya macroptera var. insignis (Rehder & E.H.Wilson) W.E.Manning, 1975")));
  }

  @Test
  public void testExtractAuthorshipNullInputs() {
    assertNull(Generator.extractAuthorship(null, List.of("Homo sapiens L., 1758")));
    assertNull(Generator.extractAuthorship("Homo sapiens", null));
    assertNull(Generator.extractAuthorship(null, null));
  }

  @Test
  public void testExtractAuthorshipNoMatch() {
    // No entry matches sciName → null
    assertNull(Generator.extractAuthorship("Quercus robur", List.of("Some unrelated string")));
  }

  @Test
  public void testExtractAuthorshipSameString() {
    // Authority equals the scientific name exactly → no authorship
    assertNull(Generator.extractAuthorship("Homo sapiens", List.of("Homo sapiens")));
  }

  @Test
  public void testExtractAuthorshipQuotedNameMatches() {
    // Quoted name matches sciName → strip prefix, return authorship
    assertEquals("Cavalier-Smith 1987",
        Generator.extractAuthorship("Bacteria", List.of("\"Bacteria\" Cavalier-Smith 1987")));
  }

  @Test
  public void testExtractAuthorshipQuotedNameMismatch() {
    // Quoted name differs from sciName → authorship belongs to another name, discard
    assertNull(Generator.extractAuthorship("Bacteria", List.of("\"Bacteriobiota\" Luketa 2012")));
    assertNull(Generator.extractAuthorship("Cellulomonas gilvus", List.of("\"Cellvibrio gilvus\" Hulcher & King, 1958")));
  }

  @Test
  public void testExtractAuthorshipMultipleEntries() {
    // taxId=2 scenario: two authority entries, only the one matching sciName should be used
    assertEquals("Cavalier-Smith 1987",
        Generator.extractAuthorship("Bacteria",
            List.of("\"Bacteria\" Cavalier-Smith 1987", "\"Bacteriobiota\" Luketa 2012")));
    // reversed order — same result
    assertEquals("Cavalier-Smith 1987",
        Generator.extractAuthorship("Bacteria",
            List.of("\"Bacteriobiota\" Luketa 2012", "\"Bacteria\" Cavalier-Smith 1987")));
  }
}
