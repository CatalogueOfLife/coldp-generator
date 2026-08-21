package org.catalogueoflife.data;

import org.gbif.nameparser.api.NameParser;
import org.gbif.nameparser.api.NameType;
import org.gbif.nameparser.api.Rank;
import org.gbif.nameparser.rust.NameParserRust;
import org.gbif.txtree.Tree;
import org.gbif.txtree.parsed.ParsedTree;
import org.gbif.txtree.parsed.ParsedTreeNode;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

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

  /**
   * text-tree 1.7 shipped the removed pure-Java name-parser 4.x, so this path threw
   * NoSuchMethodError against the 5.0 api. 2.0.0 split it out and takes the parser as an argument.
   */
  @Test
  public void parsedTextTree() throws Exception {
    String txt = "Animalia [kingdom]\n  Insecta [class]\n    Abies alba Mill. [species]\n";
    Tree<ParsedTreeNode> tree = ParsedTree.parse(
        new ByteArrayInputStream(txt.getBytes(StandardCharsets.UTF_8)), new NameParserRust());
    assertEquals(3, tree.size());
    var root = tree.getRoot().getFirst();
    assertEquals("Animalia", root.name);
    var species = root.children.getFirst().children.getFirst();
    assertEquals("Abies alba Mill.", species.name);
    assertEquals("Mill.", species.parsedName.getCombinationAuthorship().toString());
  }

  /** The un-parsed tree must keep working without any name parser on the classpath at all. */
  @Test
  public void simpleTextTree() throws Exception {
    String txt = "Animalia [kingdom]\n  Insecta [class]\n";
    var tree = Tree.simple(new ByteArrayInputStream(txt.getBytes(StandardCharsets.UTF_8)));
    assertEquals(2, tree.size());
    assertEquals("kingdom", tree.getRoot().getFirst().rank);
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
