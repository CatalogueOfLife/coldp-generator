package org.catalogueoflife.data.bats;

import org.junit.Test;

import static org.junit.Assert.*;

public class GeneratorTest {

  @Test
  public void parseAuthorYear() {
    // Standard "Author, Year. Publication" format — ayp[0] is full authorship including year
    String[] ayp = Generator.parseAuthorYear("Miller, 1907. Proc. U.S. Nat. Mus. 31:62.");
    assertEquals("Miller, 1907", ayp[0]);
    assertEquals("1907", ayp[1]);
    assertEquals("Proc. U.S. Nat. Mus. 31:62.", ayp[2]);

    // Multi-author
    ayp = Generator.parseAuthorYear("Lack, Roehrs, Stanley, Ruedi & Van Den Bussche, 2010. J. Mamm. Evol. 17:135.");
    assertEquals("Lack, Roehrs, Stanley, Ruedi & Van Den Bussche, 2010", ayp[0]);
    assertEquals("2010", ayp[1]);

    // No publication
    ayp = Generator.parseAuthorYear("Thomas, 1915");
    assertEquals("Thomas, 1915", ayp[0]);
    assertEquals("1915", ayp[1]);
    assertNull(ayp[2]);

    // No year
    ayp = Generator.parseAuthorYear("Simmons");
    assertEquals("Simmons", ayp[0]);
    assertNull(ayp[1]);
    assertNull(ayp[2]);

    // Parenthetical authorship — closing paren must be preserved inside authorship
    ayp = Generator.parseAuthorYear("(Gerbe, 1880).");
    assertEquals("(Gerbe, 1880)", ayp[0]);
    assertEquals("1880", ayp[1]);

    // Parenthetical multi-author
    ayp = Generator.parseAuthorYear("(Le Conte, 1831).");
    assertEquals("(Le Conte, 1831)", ayp[0]);
    assertEquals("1831", ayp[1]);

    // Blank input
    ayp = Generator.parseAuthorYear(null);
    assertNull(ayp[0]);
    assertNull(ayp[1]);
    assertNull(ayp[2]);
  }

  @Test
  public void clean() {
    assertEquals("hello world", Generator.clean("hello\u00a0world"));
    assertEquals("foo bar", Generator.clean("  foo   bar  "));
    assertNull(Generator.clean(null));
  }

  @Test
  public void extractBrLines() {
    org.jsoup.nodes.Element div = org.jsoup.Jsoup.parse(
        "<div><b><i>Myotis lucifugus</i></b> (Le Conte, 1831).<br/>Ann. Lyc. Nat. Hist. N.Y. 30.<br/>Little Brown Bat</div>"
    ).selectFirst("div");
    String[] lines = Generator.extractBrLines(div);
    assertEquals(3, lines.length);
    assertTrue(lines[0].contains("Myotis lucifugus"));
    assertTrue(lines[0].contains("Le Conte, 1831"));
    assertEquals("Ann. Lyc. Nat. Hist. N.Y. 30.", lines[1]);
    assertEquals("Little Brown Bat", lines[2]);
  }
}
