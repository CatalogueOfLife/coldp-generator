package org.catalogueoflife.data;

import life.catalogue.common.io.Resources;
import life.catalogue.common.io.UTF8IoUtils;
import life.catalogue.common.text.SimpleTemplate;
import org.gbif.txtree.SimpleTreeNode;
import org.gbif.txtree.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public abstract class AbstractTextTreeGenerator extends AbstractGenerator {
  protected static Logger LOG = LoggerFactory.getLogger(AbstractTextTreeGenerator.class);
  protected final Tree<SimpleTreeNode> tree = new Tree<>();

  public AbstractTextTreeGenerator(GeneratorConfig cfg, boolean addMetadata) throws IOException {
    super(cfg, addMetadata, "TextTree");
  }

  @Override
  protected void addDataFiles() throws Exception {
    populateTree();
    // write tree
    tree.print(new File(dir, "taxonomy.txtree"));
  }

  protected abstract void populateTree() throws Exception;

  protected void addMetadata() throws Exception {
    if (addMetadata) {
      // do we have sources?
      citAsYaml(sourceCitations).ifPresent(yaml -> {
        metadata.put("sources", "source: \n" + yaml);
      });

      // use metadata to format
      String template = UTF8IoUtils.readString(Resources.stream(cfg.source+"/metadata.yaml"));
      try (var mw = UTF8IoUtils.writerFromFile(new File(dir, "metadata.yaml"))) {
        mw.write(SimpleTemplate.render(template, metadata));
      }
    }
  }

}
