package org.catalogueoflife.data;

import life.catalogue.common.io.CompressionUtil;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public abstract class AbstractZipSrcGenerator extends AbstractGenerator {
  protected final File srcDir; // working directory for

  public AbstractZipSrcGenerator(GeneratorConfig cfg, boolean addMetadata, @Nullable URI downloadUri) throws IOException {
    super(cfg, addMetadata, downloadUri);
    srcDir = new File("/tmp/" + name);
    if (srcDir.exists()) {
      org.apache.commons.io.FileUtils.cleanDirectory(srcDir);
    } else {
      srcDir.mkdirs();
    }
  }

  @Override
  protected void prepare() throws IOException {
    // unzip
    CompressionUtil.unzipFile(srcDir, src);
  }
}
