package org.catalogueoflife.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.DownloadUtil;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.FileUtils;
import org.gbif.dwc.terms.Term;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public abstract class AbstractColdpGenerator extends AbstractGenerator {
  protected final DownloadUtil download;
  protected final File sources;
  protected final Map<String, URI> downloadURIs = new HashMap<>();
  protected TermWriter writer;
  protected TermWriter refWriter;
  private final List<TermWriter> addWriter = new ArrayList<>();
  private int refCounter = 1;
  protected final static ObjectMapper mapper = new ObjectMapper(new JsonFactory()
          .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET,false)
      )
      .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);

  public AbstractColdpGenerator(GeneratorConfig cfg, boolean addMetadata) throws IOException {
    this(cfg, addMetadata, Collections.EMPTY_MAP);
  }

  public AbstractColdpGenerator(GeneratorConfig cfg, boolean addMetadata, Map<String, URI> downloads) throws IOException {
    super(cfg, addMetadata, "ColDP");
    sources = new File("/tmp/" + name + "-sources");
    download = new DownloadUtil(hc);
    if (downloads != null && !downloads.isEmpty()) {
      downloadURIs.putAll(downloads);
    }
  }

  protected File sourceFile(String fn) {
    return new File(sources, fn);
  }

  protected void prepare() throws Exception {
    // override if needed
  }

  protected abstract void addData() throws Exception;

  @Override
  protected void addDataFiles() throws Exception {
    // get latest data files or reuse existing ones
    if (sources.exists()) {
      LOG.info("Reuse data from {}. To enforce new data downloads please wipe the directory3", sources);
    } else {
      sources.mkdirs();
    }
    for (var e : downloadURIs.entrySet()) {
      var f = new File(sources, e.getKey());
      if (!f.exists()) {
        LOG.info("Downloading latest {} from {} to {}", e.getKey(), e.getValue(), f);
        download.download(e.getValue(), f);
      } else {
        LOG.info("Reuse source file {}", f);
      }
    }
    prepare();
    addData();
    if (writer != null) {
      writer.close();
    }
    if (refWriter != null) {
      refWriter.close();
    }
    for (var w : addWriter) {
      w.close();
    }
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
    return additionalWriter(rowType, ColdpTerm.RESOURCES.get(rowType));
  }

  protected TermWriter additionalWriter(Term rowType, List<? extends Term> columns) throws IOException {
    var w = new TermWriter.TSV(dir, rowType, columns);
    addWriter.add(w);
    return w;
  }

}
