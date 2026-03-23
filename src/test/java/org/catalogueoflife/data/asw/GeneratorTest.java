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
