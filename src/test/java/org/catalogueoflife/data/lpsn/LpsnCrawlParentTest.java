package org.catalogueoflife.data.lpsn;

import org.apache.commons.io.FileUtils;
import org.catalogueoflife.data.GeneratorConfig;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.*;

/**
 * No-network reproduction of the mis-parenting bug: LPSN's "Synonyms" section is symmetric
 * across the whole name-group, so a synonym page (Phormidiaceae) lists the accepted name
 * (Oscillatoriaceae) among its synonyms. When the crawl reaches the accepted family via that
 * synonym's list, it must NOT parent the accepted family to the synonym — an accepted taxon's
 * parent is its "Parent taxon" (the accepted order), and a synonym's parent is its "Correct name".
 *
 * <p>Fixtures are written to the source cache and read with {@code --no-download}, and the child
 * order is deliberately [phormidiaceae, oscillatoriaceae] so the synonym is visited first.</p>
 */
public class LpsnCrawlParentTest {

  private static String page(int rec, String name, String category, String taxStatus,
                             String parentLink, String correctNameLink, String sectionLabel, String... sectionLinks) {
    StringBuilder sb = new StringBuilder("<html><body><div id=\"detail-page\">");
    sb.append("<p><b>Record number:</b> ").append(rec).append("</p>");
    sb.append("<p><b>Name:</b> <I>").append(name).append("</I> Author 2023</p>");
    sb.append("<p><b>Category:</b> ").append(category).append("</p>");
    sb.append("<p><b>Taxonomic status:</b> ").append(taxStatus).append("</p>");
    if (parentLink != null) {
      sb.append("<p><b>Parent taxon:</b> <a href=\"").append(parentLink).append("\">p</a></p>");
    }
    if (correctNameLink != null) {
      sb.append("<p class=\"corr-name\"><b>Correct name:</b> <a href=\"").append(correctNameLink).append("\">c</a></p>");
    }
    if (sectionLinks.length > 0) {
      sb.append("<p><span class=\"open\"><b>").append(sectionLabel).append("</b></span><ul><li><table><tbody>");
      for (String l : sectionLinks) {
        sb.append("<tr><td><a href=\"").append(l).append("\">x</a></td></tr>");
      }
      sb.append("</tbody></table></li></ul></p>");
    }
    return sb.append("</div></body></html>").toString();
  }

  @Test
  public void acceptedNotParentedToSynonymSibling() throws Exception {
    File tmp = Files.createTempDirectory("lpsn-crawl").toFile();
    var cfg = new GeneratorConfig();
    cfg.source = "lpsn";
    cfg.tmpSourceDir = tmp;
    cfg.noDownload = true;
    var gen = new Generator(cfg);
    File dir = new File(tmp, "lpsn");
    dir.mkdirs();

    // order Oscillatoriales (accepted) lists the synonym family FIRST, then the accepted family
    FileUtils.write(new File(dir, "web-order-oscillatoriales.html"),
      page(6539, "Oscillatoriales", "Order", "correct name", "/class/chroococcophyceae", null,
        "Child taxa:", "/family/phormidiaceae", "/family/oscillatoriaceae"), StandardCharsets.UTF_8);
    // synonym family Phormidiaceae: correct name is the accepted family; Synonyms list it too
    FileUtils.write(new File(dir, "web-family-phormidiaceae.html"),
      page(6124, "Phormidiaceae", "Family", "synonym", "/order/oscillatoriales", "/family/oscillatoriaceae",
        "Synonyms:", "/family/oscillatoriaceae"), StandardCharsets.UTF_8);
    // accepted family Oscillatoriaceae: parent is the accepted order; Synonyms list the synonym
    FileUtils.write(new File(dir, "web-family-oscillatoriaceae.html"),
      page(6108, "Oscillatoriaceae", "Family", "correct name", "/order/oscillatoriales", null,
        "Synonyms:", "/family/phormidiaceae"), StandardCharsets.UTF_8);

    gen.crawlSubtree("/order/oscillatoriales");

    var accepted = gen.record(6108);
    assertNotNull(accepted);
    assertEquals("correct name", accepted.lpsn_taxonomic_status);
    assertEquals("accepted family parents to the accepted order, not the synonym sibling",
      Integer.valueOf(6539), accepted.lpsn_parent_id);

    var synonym = gen.record(6124);
    assertNotNull(synonym);
    assertEquals("synonym", synonym.lpsn_taxonomic_status);
    assertEquals("synonym parents to its correct name",
      Integer.valueOf(6108), synonym.lpsn_correct_name_id);

    // defence in depth: if an accepted taxon's parent is a synonym, redirect to its correct name
    assertEquals("accepted terminus is itself", Integer.valueOf(6539), gen.acceptedAncestor(6539, 1));
    assertEquals("synonym parent redirects to its accepted correct name",
      Integer.valueOf(6108), gen.acceptedAncestor(6124, 1));
    assertNull("redirect that loops back to the child yields no parent",
      gen.acceptedAncestor(6124, 6108));
    assertNull("unknown parent yields no parent", gen.acceptedAncestor(999999, 1));
  }
}
