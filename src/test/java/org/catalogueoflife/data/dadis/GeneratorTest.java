package org.catalogueoflife.data.dadis;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.Utils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore("manual / integration test — downloads all national breed populations from DAD-IS")
public class GeneratorTest {

  @Test
  public void testGenerate() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "dadis";
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }
}
