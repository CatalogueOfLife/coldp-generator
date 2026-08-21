package org.catalogueoflife.data;

import org.gbif.nameparser.api.NameParser;
import org.gbif.nameparser.api.NameType;
import org.gbif.nameparser.api.Rank;
import org.gbif.nameparser.rust.NameParserRust;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Guards the two dependencies that only break at runtime, never at compile time:
 * the name-parser Rust FFM binding (needs its platform cdylib on the classpath and
 * native access enabled) and the clb parsers, whose static initialisers blow up when
 * the pinned name-parser and the clb release disagree about enums.
 */
public class ParserRuntimeTest {

  @Test
  public void rustNameParser() throws Exception {
    NameParser np = new NameParserRust();
    var pn = np.parse("Abies alba subsp. alpina Mill., 1876").orElseThrow();
    assertEquals(NameType.SCIENTIFIC, pn.getType());
    assertEquals("Abies", pn.getGenus());
    assertEquals("alba", pn.getSpecificEpithet());
    assertEquals("alpina", pn.getInfraspecificEpithet());
    assertEquals(Rank.SUBSPECIES, pn.getRank());
  }

  @Test
  public void rustNameParserUnparsable() {
    assertFalse(new NameParserRust().parse("?").parsed().isPresent());
  }

  @Test
  public void clbParsers() throws Exception {
    assertTrue(life.catalogue.parser.NameParser.PARSER.parse("Abies alba Mill.").isPresent());
    assertEquals("(Miller, 1876)",
        life.catalogue.parser.NameParser.PARSER.parseAuthorship("(Miller, 1876)").orElseThrow().toString());
    assertEquals(Rank.SUBSPECIES,
        life.catalogue.parser.RankParser.PARSER.parse(null, "subspecies").orElseThrow());
    assertTrue(life.catalogue.parser.LanguageParser.PARSER.parse("English").isPresent());
  }
}
