package org.catalogueoflife.data.wikispecies;

import org.junit.Test;
import org.sweble.wikitext.parser.WikitextParser;
import org.sweble.wikitext.parser.nodes.*;
import org.sweble.wikitext.parser.utils.SimpleParserConfig;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class GeneratorTest {

  @Test
  public void testSectionKeyParsing() throws Exception {
    WikitextParser parser = new WikitextParser(new SimpleParserConfig());
    String wikitext =
        "=={{int:Taxonavigation}}==\n" +
        "{{Homo sapiens}}\n\n" +
        "=={{int:Name}}==\n" +
        "''Homo sapiens'' {{a|Linnaeus}}, 1758\n\n" +
        "=={{int:References}}==\n" +
        "=={{int:Vernacular names}}==\n" +
        "{{VN |en=Human |de=Mensch}}\n";
    WtNode article = parser.parseArticle(wikitext, "Homo sapiens");

    List<String> keys = new ArrayList<>();
    for (WtNode node : article) {
      if (!(node instanceof WtSection)) continue;
      WtSection sect = (WtSection) node;
      if (!sect.hasBody()) continue;
      keys.add(WtUtils.sectionKey(sect.getHeading()));
    }
    assertTrue("Must resolve taxonavigation key", keys.contains("taxonavigation"));
    assertTrue("Must resolve vernacular names key", keys.contains("vernacular names"));
  }

  @Test
  public void testVernacularExtractionSingleLine() throws Exception {
    WikitextParser parser = new WikitextParser(new SimpleParserConfig());
    WtNode article = parser.parseArticle(
        "=={{int:Vernacular names}}==\n{{VN |en=Human |de=Mensch |fr=Humain}}\n",
        "Homo sapiens");

    List<VernacularExtractor.VernacularName> vn = extractVernaculars(parser, article);
    assertFalse("Must extract vernacular names", vn.isEmpty());
    assertTrue("Must have English", vn.stream().anyMatch(v -> "en".equals(v.language()) && "Human".equals(v.name())));
    assertTrue("Must have German",  vn.stream().anyMatch(v -> "de".equals(v.language()) && "Mensch".equals(v.name())));
    assertTrue("Must have French",  vn.stream().anyMatch(v -> "fr".equals(v.language()) && "Humain".equals(v.name())));
  }

  @Test
  public void testVernacularExtractionMultiLine() throws Exception {
    WikitextParser parser = new WikitextParser(new SimpleParserConfig());
    WtNode article = parser.parseArticle(
        "=={{int:Vernacular names}}==\n{{VN\n|en=Parazoans\n|de=Gewebelose\n|fr=Parazoaires\n}}\n",
        "Parazoa");

    List<VernacularExtractor.VernacularName> vn = extractVernaculars(parser, article);
    assertFalse("Must extract multiline VN", vn.isEmpty());
    assertTrue("Must have English", vn.stream().anyMatch(v -> "en".equals(v.language())));
    assertTrue("Must have German",  vn.stream().anyMatch(v -> "de".equals(v.language())));
  }

  private List<VernacularExtractor.VernacularName> extractVernaculars(WikitextParser parser, WtNode article) {
    for (WtNode node : article) {
      if (!(node instanceof WtSection)) continue;
      WtSection sect = (WtSection) node;
      if (!sect.hasBody()) continue;
      if ("vernacular names".equals(WtUtils.sectionKey(sect.getHeading()))) {
        return VernacularExtractor.extract(sect.getBody());
      }
    }
    return List.of();
  }
}
