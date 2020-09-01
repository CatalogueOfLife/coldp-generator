package org.catalogueoflife.data;

import life.catalogue.api.datapackage.ColdpTerm;
import life.catalogue.common.io.CompressionUtil;
import life.catalogue.common.io.Resources;
import life.catalogue.common.io.UTF8IoUtils;
import life.catalogue.common.text.SimpleTemplate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.catalogueoflife.data.utils.ExcelUtils;
import org.catalogueoflife.data.utils.HttpUtils;
import org.catalogueoflife.data.utils.TermWriter;
import org.gbif.dwc.terms.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractBuilder implements Runnable {
  protected static Logger LOG = LoggerFactory.getLogger(AbstractBuilder.class);
  protected final BuilderConfig cfg;
  protected final HttpUtils http;
  protected final File dir;
  protected final Map<String, Object> metadata = new HashMap<>();

  public AbstractBuilder(BuilderConfig cfg) {
    this(cfg, null, null);
  }

  public AbstractBuilder(BuilderConfig cfg, @Nullable String username, @Nullable String password) {
    this.cfg = cfg;
    http = new HttpUtils(username, password);
    dir = cfg.archiveDir();
  }

  @Override
  public void run() {
    try {
      if (dir.exists()) {
        LOG.info("Clear archive dir {}", dir.getAbsolutePath());
        FileUtils.deleteDirectory(dir);
      }
      dir.mkdirs();
      parseData();
      addMetadata();
      // finish archive and zip it
      LOG.info("Bundling archive at {}", dir.getAbsolutePath());
      File zip = new File(dir.getParentFile(), dir.getName() + ".zip");
      CompressionUtil.zipDir(dir, zip);
      LOG.info("Archive completed at {} !", zip);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void parseData() throws Exception;

  void addMetadata() throws Exception {
    String tmpl = Resources.toString(cfg.source + "/metadata.yaml");
    String yaml = SimpleTemplate.render(tmpl, metadata);
    File fo = new File(dir, "metadata.yaml");
    BufferedWriter writer = UTF8IoUtils.writerFromFile(fo);
    writer.write(yaml);
    writer.close();

    InputStream logo = Resources.stream(cfg.source + "/logo.png");
    if (logo != null) {
      FileOutputStream out = new FileOutputStream(new File(dir, "logo.png"));
      IOUtils.copy(logo, out);
      out.close();
    }
  }

  public TermWriter newDataFile(ColdpTerm rowType, ColdpTerm idTerm, Term... header) throws IOException {
    return new TermWriter(dir, rowType, idTerm, List.of(header));
  }

  protected String col(Row row, int column) {
    return ExcelUtils.col(row, column);
  }

  protected String link(Row row, int column) {
    return ExcelUtils.link(row, column);
  }

  protected static String buildCitation(String author, String year, String title, String journal) {
    StringBuilder sb = new StringBuilder();
    sb.append(trimOrDefault(author, "???"));
    sb.append(" (");
    sb.append(trimOrDefault(year, "???"));
    sb.append("). ");
    sb.append(appendDotIfNotEmpty(title));
    sb.append(trimOrDefault(journal, ""));
    return sb.toString().trim();
  }

  private static String appendDotIfNotEmpty(String x) {
    return x == null || x.isEmpty() ? " " : StringUtils.strip(x, " .") + ". ";
  }

  private static String trimOrDefault(String x, String defaultValue) {
    x = StringUtils.strip(x, " .");
    return StringUtils.isEmpty(x) ? defaultValue : x;
  }


  /**
   * Silently swallows errors converting a string to a URI
   */
  URI uri(String uri) {
    if (!StringUtils.isBlank(uri)) {
      try {
        return URI.create(uri);
      } catch (IllegalArgumentException e) {
        LOG.debug("Bogus URI {}", uri);
      }
    }
    return null;
  }

}
