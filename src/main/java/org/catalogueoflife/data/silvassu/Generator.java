package org.catalogueoflife.data.silvassu;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.silva.BaseGenerator;

import java.io.IOException;

public class Generator extends BaseGenerator {
  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, "ssu");
  }
}
