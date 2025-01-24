package org.catalogueoflife.data;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import life.catalogue.api.model.Citation;
import life.catalogue.api.model.CslName;
import life.catalogue.api.model.DOI;
import life.catalogue.api.model.IssueContainer;
import life.catalogue.common.io.CompressionUtil;
import life.catalogue.common.io.Resources;
import life.catalogue.common.io.UTF8IoUtils;
import life.catalogue.common.text.SimpleTemplate;
import life.catalogue.metadata.DoiResolver;
import life.catalogue.metadata.coldp.YamlMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.catalogueoflife.data.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractGenerator implements Runnable {
  protected static Logger LOG = LoggerFactory.getLogger(AbstractColdpGenerator.class);
  protected final GeneratorConfig cfg;
  protected final HttpUtils http;
  protected final boolean addMetadata;
  protected final Map<String, Object> metadata = new HashMap<>();
  protected final List<Citation> sourceCitations = new ArrayList<>();
  protected final String name;
  protected final File dir; // working directory
  protected final CloseableHttpClient hc;
  protected final DoiResolver doiResolver;
  private final String archiveType;

  public AbstractGenerator(GeneratorConfig cfg, boolean addMetadata, String archiveType) throws IOException {
    this.archiveType = archiveType;
    this.cfg = cfg;
    this.http = new HttpUtils();
    this.addMetadata = addMetadata;
    name = getClass().getPackageName().replaceFirst(AbstractColdpGenerator.class.getPackageName(), "");
    this.dir = cfg.archiveDir();
    dir.mkdirs();
    LOG.info("Build archive at {}", dir);
    FileUtils.cleanDirectory(dir);
    HttpClientBuilder htb = HttpClientBuilder.create();
    hc = htb.build();
    doiResolver = new DoiResolver(hc);
    if (addMetadata) {
      initMapper();
    }
  }

  private void initMapper() {
    YamlMapper.MAPPER.addMixIn(Citation.class, CitationMixin.class);
  }
  public abstract class CitationMixin {
    @JsonProperty("DOI")
    private DOI doi;
    @JsonProperty("container-author")
    private List<CslName> containerAuthor;
    @JsonProperty("container-title")
    private String containerTitle;
    @JsonProperty("collection-title")
    private String collectionTitle;
    @JsonProperty("collection-editor")
    private List<CslName> collectionEditor;
    @JsonProperty("publisher-place")
    private String publisherPlace;
    @JsonProperty("ISBN")
    private String isbn;
    @JsonProperty("ISSN")
    private String issn;
    @JsonProperty("URL")
    private String url;
  }
  protected static Optional<String> asYaml(List<?> items) throws JsonProcessingException {
    StringBuilder yaml = new StringBuilder();
    for (Object item : items) {
      yaml.append(" - \n");
      String itemYaml = YamlMapper.MAPPER.writeValueAsString(item).replaceFirst("---\n", "").trim();
      String indented = new BufferedReader(new StringReader(itemYaml)).lines()
                                                                      .map(l -> "   " + l)
                                                                      .collect(Collectors.joining("\n"));
      yaml.append(indented);
      yaml.append("\n");
    }
    return yaml.length() > 0 ? Optional.of(yaml.toString()) : Optional.empty();
  }

  @Override
  public void run() {
    try {
      addDataFiles();
      addMetadata();

      // finish archive and zip it
      LOG.info("Bundling archive at {}", dir.getAbsolutePath());
      File zip = new File(dir.getParentFile(), dir.getName() + ".zip");
      CompressionUtil.zipDir(dir, zip);
      LOG.info("{} archive completed at {} !", archiveType, zip);

    } catch (Exception e) {
      LOG.error("Error building {} archive for {}", archiveType, cfg.source, e);
      throw new RuntimeException(e);

    } finally {
      try {
        hc.close();
      } catch (IOException e) {
        LOG.error("Failed to close http client", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Adds a new source entry to the metadata map by resolving a DOI.
   * @param doi
   */
  protected void addSource(DOI doi) throws IOException {
    IssueContainer issues = IssueContainer.simple();
    var data = doiResolver.resolve(doi, issues);
    if (data != null) {
      LOG.info("Add source DOI {}: {}", doi, data.getCitationText());
      sourceCitations.add(data);
    }
    for (var iss : issues.getIssues()) {
      LOG.warn("Resolution of DOI {} caused {}", doi, iss);
    }
  }

  protected abstract void addDataFiles() throws Exception;

  protected void addMetadata() throws Exception {
    if (addMetadata) {
      // do we have sources?
      AbstractGenerator.asYaml(sourceCitations).ifPresent(yaml -> {
        metadata.put("sources", "source: \n" + yaml);
      });

      // use metadata to format
      String template = UTF8IoUtils.readString(Resources.stream(cfg.source + "/metadata.yaml"));
      try (var mw = UTF8IoUtils.writerFromFile(new File(dir, "metadata.yaml"))) {
        mw.write(SimpleTemplate.render(template, metadata));
      }

      // look for logo.png
      var img = Resources.stream(cfg.source + "/logo.png");
      if (img != null) {
        var f = new File(dir, "logo.png");
        LOG.info("Copy logo.png from resources");
        try (OutputStream out = new FileOutputStream(f)) {
          IOUtils.copy(img, out);
        }
      }
      try (var mw = UTF8IoUtils.writerFromFile(new File(dir, "metadata.yaml"))) {
        mw.write(SimpleTemplate.render(template, metadata));
      }
    }
  }
}
