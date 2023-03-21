package org.catalogueoflife.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import life.catalogue.api.model.Citation;
import life.catalogue.api.model.DOI;
import life.catalogue.api.model.IssueContainer;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.*;
import life.catalogue.common.text.SimpleTemplate;
import life.catalogue.metadata.DoiResolver;
import life.catalogue.metadata.coldp.YamlMapper;
import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.catalogueoflife.data.utils.HttpException;
import org.catalogueoflife.data.utils.HttpUtils;
import org.gbif.dwc.terms.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractGenerator implements Runnable {
  protected static Logger LOG = LoggerFactory.getLogger(AbstractGenerator.class);
  protected final GeneratorConfig cfg;
  protected final DownloadUtil download;
  protected final HttpUtils http;
  private final boolean addMetadata;
  protected final Map<String, Object> metadata = new HashMap<>();
  protected final List<Citation> sources = new ArrayList<>();
  protected final String name;
  protected final File dir; // working directory
  protected final File src; // optional download
  protected final URI srcUri;
  protected TermWriter writer;
  protected TermWriter refWriter;
  private int refCounter = 1;
  protected final CloseableHttpClient hc;
  private final DoiResolver doiResolver;
  protected final static ObjectMapper mapper = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);

  public AbstractGenerator(GeneratorConfig cfg, boolean addMetadata) throws IOException {
    this(cfg, addMetadata, null);
  }

  public AbstractGenerator(GeneratorConfig cfg, boolean addMetadata, @Nullable URI downloadUri) throws IOException {
    this.cfg = cfg;
    this.addMetadata = addMetadata;
    this.dir = cfg.archiveDir();
    dir.mkdirs();
    LOG.info("Build archive at {}", dir);
    FileUtils.cleanDirectory(dir);
    name = getClass().getPackageName().replaceFirst(AbstractGenerator.class.getPackageName(), "");
    src = new File("/tmp/" + name + ".src");
    src.deleteOnExit();
    HttpClientBuilder htb = HttpClientBuilder.create();
    hc = htb.build();
    this.download = new DownloadUtil(hc);
    this.http = new HttpUtils();
    this.srcUri = downloadUri;
    doiResolver = new DoiResolver(hc);
  }

  @Override
  public void run() {
    try {
      // get latest CSVs
      if (!src.exists() && srcUri != null) {
        LOG.info("Downloading latest data from {}", srcUri);
        download.download(srcUri, src);
      } else if (srcUri == null) {
        LOG.info("Reuse data from {}", src);
      }

      prepare();
      addData();
      if (writer != null) {
        writer.close();
      }
      if (refWriter != null) {
        refWriter.close();
      }
      addMetadata();

      // finish archive and zip it
      LOG.info("Bundling archive at {}", dir.getAbsolutePath());
      File zip = new File(dir.getParentFile(), dir.getName() + ".zip");
      CompressionUtil.zipDir(dir, zip);
      LOG.info("ColDP archive completed at {} !", zip);

    } catch (HttpException e) {

    } catch (Exception e) {
      LOG.error("Error building ColDP archive for {}", cfg.source, e);
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

  protected void prepare() throws IOException {
    //nothing by default, override as needed
  }

  /**
   * Finalizes the current ref record and creates a new ref id if not yet set.
   * @return the ID of the previous record.
   */
  protected String nextRef() throws IOException {
    String id;
    if (refWriter.has(ColdpTerm.ID)) {
      id = refWriter.get(ColdpTerm.ID);
    } else {
      id = "R" + refCounter++;
      refWriter.set(ColdpTerm.ID, id);
    }
    refWriter.next();
    return id;
  }

  public void newWriter(ColdpTerm rowType) throws IOException {
    newWriter(rowType, ColdpTerm.RESOURCES.get(rowType));
  }

  protected void newWriter(Term rowType, List<? extends Term> columns) throws IOException {
    if (writer != null) {
      writer.close();
    }
    writer = additionalWriter(rowType, columns);
  }

  protected void initRefWriter(List<? extends Term> columns) throws IOException {
    refWriter = additionalWriter(ColdpTerm.Reference, columns);
  }

  protected TermWriter additionalWriter(ColdpTerm rowType) throws IOException {
    return new TermWriter.TSV(dir, rowType, ColdpTerm.RESOURCES.get(rowType));
  }

  protected TermWriter additionalWriter(Term rowType, List<? extends Term> columns) throws IOException {
    return new TermWriter.TSV(dir, rowType, columns);
  }

  protected abstract void addData() throws Exception;

  protected void addMetadata() throws Exception {
    if (addMetadata) {
      // do we have sources?
      asYaml(sources).ifPresent(yaml -> {
        metadata.put("sources", "source: \n" + yaml);
      });

      // use metadata to format
      String template = UTF8IoUtils.readString(Resources.stream(cfg.source+"/metadata.yaml"));
      try (var mw = UTF8IoUtils.writerFromFile(new File(dir, "metadata.yaml"))) {
        mw.write(SimpleTemplate.render(template, metadata));
      }
    }
  }

  protected Optional<String> asYaml(List<?> items) throws JsonProcessingException {
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
    return yaml.length()>0 ? Optional.of(yaml.toString()) : Optional.empty();
  }

  /**
   * Adds a new source entry to the metadata map by resolving a DOI.
   * @param doi
   */
  protected void addSource(DOI doi) throws IOException {
    IssueContainer issues = IssueContainer.simple();
    var data = doiResolver.resolve(doi, issues);
    if (data != null) {
      sources.add(data);
    }
    for (var iss : issues.getIssues()) {
      LOG.warn("Resolution of DOI {} caused {}", doi, iss);
    }
  }
}
