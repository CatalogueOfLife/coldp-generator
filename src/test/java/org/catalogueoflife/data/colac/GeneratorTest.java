package org.catalogueoflife.data.colac;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.Utils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

/**
 * Integration test for the colac historical CoL Annual Checklist generator.
 * Requires a local MariaDB with the col{year}ac dumps loaded — see CLAUDE.md.
 */
@Ignore("manual / integration test — requires a local MariaDB with col{year}ac dumps")
public class GeneratorTest {

  @Test
  public void testOldSchema2008() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "colac";
    cfg.year = 2008;
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }

  @Test
  public void testNewSchema2015() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "colac";
    cfg.year = 2015;
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }

  @Test
  public void testEarlySchema2004() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "colac";
    cfg.year = 2004;
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }
}
