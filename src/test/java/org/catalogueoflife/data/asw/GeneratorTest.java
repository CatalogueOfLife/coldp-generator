package org.catalogueoflife.data.asw;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class GeneratorTest {

  @Test
  public void testParseH1Species() {
    Element h1 = Jsoup.parse("<h1><i>Arthroleptis adolfifriederici</i> Nieden, 1911</h1>").selectFirst("h1");
    String[] result = Generator.parseH1(h1);
    assertEquals("Arthroleptis adolfifriederici", result[0]);
    assertEquals("Nieden, 1911", result[1]);
  }

  @Test
  public void testParseH1Genus() {
    Element h1 = Jsoup.parse("<h1><i>Arthroleptis</i> Smith, 1849</h1>").selectFirst("h1");
    String[] result = Generator.parseH1(h1);
    assertEquals("Arthroleptis", result[0]);
    assertEquals("Smith, 1849", result[1]);
  }

  @Test
  public void testParseH1Subfamily() {
    Element h1 = Jsoup.parse("<h1>Arthroleptinae Mivart, 1869</h1>").selectFirst("h1");
    String[] result = Generator.parseH1(h1);
    assertEquals("Arthroleptinae", result[0]);
    assertEquals("Mivart, 1869", result[1]);
  }

  @Test
  public void testParseH1NoAuthorship() {
    Element h1 = Jsoup.parse("<h1>Anura</h1>").selectFirst("h1");
    String[] result = Generator.parseH1(h1);
    assertEquals("Anura", result[0]);
    assertNull(result[1]);
  }

  @Test
  public void testExtractAuthorship() {
    assertEquals("Mivart, 1869", Generator.extractAuthorship("Mivart, 1869, Proc. Zool. Soc. London, 1869"));
    assertEquals("Nieden, 1911 \"1910\"", Generator.extractAuthorship("Nieden, 1911 \"1910\", Sitzungsber. Ges. Naturforsch."));
    assertEquals("Smith, 1849", Generator.extractAuthorship("Smith, 1849, Illust. Zool. S. Afr., 3 (Appendix)"));
    assertNull(Generator.extractAuthorship(null));
    assertNull(Generator.extractAuthorship(""));
  }

  @Test
  public void testExtractContainerTitleShort() {
    assertEquals("Proc. Zool. Soc. London",
        Generator.extractContainerTitleShort("Günther, 1858, Proc. Zool. Soc. London, 1858"));
    assertEquals("Proc. Zool. Soc. London",
        Generator.extractContainerTitleShort("Mivart, 1869, Proc. Zool. Soc. London, 1869"));
    assertEquals("Ann. Mag. Nat. Hist., Ser. 3, 3",
        Generator.extractContainerTitleShort("Günther, 1859, Ann. Mag. Nat. Hist., Ser. 3, 3"));
    assertEquals("Cat. Batr. Sal. Coll. Brit. Mus.",
        Generator.extractContainerTitleShort("Günther, 1859 \"1858\", Cat. Batr. Sal. Coll. Brit. Mus."));
    assertNull(Generator.extractContainerTitleShort(null));
  }

  @Test
  public void testExtractPageFromText() {
    assertEquals("347", Generator.extractPageFromText(": 347. Coined as a Section"));
    assertEquals("341", Generator.extractPageFromText(": 341; Günther, 1859,"));
    assertEquals("69", Generator.extractPageFromText(": 69. A series"));
    assertEquals("339-352", Generator.extractPageFromText(": 339-352."));
    assertNull(Generator.extractPageFromText(null));
    assertNull(Generator.extractPageFromText("   "));
  }

  @Test
  public void testCleanRemarks() {
    assertEquals("Coined as a Section, apparently above the family-group",
        Generator.cleanRemarks("347", ": 347. Coined as a Section, apparently above the family-group."));
    assertEquals("A series within Opisthoglossa",
        Generator.cleanRemarks("69", ": 69. A series within Opisthoglossa."));
    // Type text should be stripped
    assertNull(Generator.cleanRemarks(null, ": 341. Type genus: Foo."));
    assertEquals("Subsequent usage of Salientia Gray, 1850",
        Generator.cleanRemarks("1", ": 1. Subsequent usage of Salientia Gray, 1850."));
  }

  @Test
  public void testSplitCountriesSimple() {
    List<String> result = Generator.splitCountries("Rwanda, Uganda, Burundi");
    assertEquals(List.of("Rwanda", "Uganda", "Burundi"), result);
  }

  @Test
  public void testSplitCountriesWithCommaName() {
    List<String> result = Generator.splitCountries("Congo, Democratic Republic of the, Rwanda, Uganda");
    assertEquals(List.of("Congo, Democratic Republic of the", "Rwanda", "Uganda"), result);
  }

  @Test
  public void testSplitCountriesBothComma() {
    List<String> result = Generator.splitCountries(
        "Congo, Democratic Republic of the, Congo, Republic of the, Cameroon");
    assertEquals(List.of("Congo, Democratic Republic of the", "Congo, Republic of the", "Cameroon"), result);
  }
}
