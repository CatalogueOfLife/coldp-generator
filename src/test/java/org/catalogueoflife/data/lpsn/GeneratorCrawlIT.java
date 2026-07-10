package org.catalogueoflife.data.lpsn;

import org.catalogueoflife.data.GeneratorConfig;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Live integration test for the cyanobacteria website crawl. Hits https://lpsn.dsmz.de
 * (no auth required) and caches pages under the source dir, so it is {@link Ignore}d by
 * default. Crawls the small Oculatellaceae family subtree rather than the whole phylum.
 */
@Ignore("hits the live LPSN website")
public class GeneratorCrawlIT {

  @Test
  public void crawlOculatellaceae() throws Exception {
    var cfg = new GeneratorConfig();
    cfg.source = "lpsn";
    var gen = new Generator(cfg);

    // Oculatellaceae (6116) and its genus Aerofilum (12732) are cyanobacteria the API omits;
    // the synonym Elainellaceae (43552) must resolve to Oculatellaceae as its accepted parent.
    gen.crawlSubtree("/family/oculatellaceae");

    var family = gen.record(6116);
    assertNotNull("Oculatellaceae family must be recovered", family);
    assertEquals("Oculatellaceae", family.full_name);
    assertEquals("family", family.category);

    var genus = gen.record(12732);
    assertNotNull("Aerofilum genus must be recovered", genus);
    assertEquals(Integer.valueOf(6116), genus.lpsn_parent_id); // child of the family

    var synonym = gen.record(43552);
    assertNotNull("Elainellaceae synonym must be recovered", synonym);
    assertEquals("synonym", synonym.lpsn_taxonomic_status);
    assertEquals("its accepted parent is Oculatellaceae",
      Integer.valueOf(6116), synonym.lpsn_correct_name_id);
  }
}
