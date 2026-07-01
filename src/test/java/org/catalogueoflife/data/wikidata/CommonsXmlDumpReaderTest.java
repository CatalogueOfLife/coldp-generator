package org.catalogueoflife.data.wikidata;

import org.catalogueoflife.data.wikidata.CommonsXmlDumpReader.FileMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CommonsXmlDumpReaderTest {

  /** The exact wikitext of https://commons.wikimedia.org/wiki/File:Adult_male_Royal_Bengal_tiger.jpg */
  @Test
  public void parseRoyalBengalTiger() {
    String text = """
        =={{int:filedesc}}==
        {{Information
        |description={{en|1=A tiger in Kanha National Park}}
        |date=2015-05-21 18:22:48
        |source={{own}}
        |author=[[User:Seemaleena|Seemaleena]]
        |permission=
        |other versions=
        }}

        =={{int:license-header}}==
        {{self|cc-by-sa-4.0}}

        {{Wiki Loves Earth 2016|in}}
        """;
    FileMetadata m = CommonsXmlDumpReader.parseFileMetadata(text);
    assertEquals("A tiger in Kanha National Park", m.title());
    assertEquals("2015-05-21 18:22:48", m.created());
    assertEquals("Seemaleena", m.creator());
    assertEquals("CC BY-SA 4.0", m.license());
    assertNull(m.remarks());
  }

  /** {{self|cc-by-sa-4.0|author=…}} — license is an argument, author is a named arg to skip. */
  @Test
  public void selfLicenseWithAuthorArg() {
    String text = """
        {{Information|description={{en|A frog}}|date=2020|author=Jane Doe}}
        {{self|cc-by-4.0|author=[[User:Jane|Jane Doe]]}}
        """;
    FileMetadata m = CommonsXmlDumpReader.parseFileMetadata(text);
    assertEquals("A frog", m.title());
    assertEquals("CC BY 4.0", m.license());
  }

  /** Standalone license template (no {{self}}) still works. */
  @Test
  public void standaloneLicenseTemplate() {
    String text = """
        {{Information|description=A beetle|date=2019|author=Someone}}
        {{cc-by-sa-3.0}}
        """;
    FileMetadata m = CommonsXmlDumpReader.parseFileMetadata(text);
    assertEquals("A beetle", m.title());
    assertEquals("CC BY-SA 3.0", m.license());
  }

  /** A long description spills into remarks with a shortened title. */
  @Test
  public void longDescriptionSpillsToRemarks() {
    String desc = "A very long detailed description of the specimen that keeps going well past "
                + "eighty characters. Extra sentence here.";
    String text = "{{Information|description={{en|" + desc + "}}|date=2018|author=X}}\n{{cc0}}";
    FileMetadata m = CommonsXmlDumpReader.parseFileMetadata(text);
    assertTrue(m.title().length() <= desc.length());
    assertEquals(desc, m.remarks());
    assertEquals("CC0", m.license());
  }

  @Test
  public void noMetadata() {
    FileMetadata m = CommonsXmlDumpReader.parseFileMetadata("just some text, no templates");
    assertNull(m.title());
    assertNull(m.creator());
    assertNull(m.license());
  }
}
