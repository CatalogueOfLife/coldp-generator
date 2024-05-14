package org.catalogueoflife.data;

import com.fasterxml.jackson.annotation.JsonInclude;
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
import java.util.List;

public abstract class AbstractColdpGenerator extends AbstractGenerator {
  protected final DownloadUtil download;
  protected final File src; // optional download
  protected final URI srcUri;
  protected TermWriter writer;
  protected TermWriter refWriter;
  private int refCounter = 1;
  protected final static ObjectMapper mapper = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);

  public AbstractColdpGenerator(GeneratorConfig cfg, boolean addMetadata) throws IOException {
    this(cfg, addMetadata, null);
  }

  public AbstractColdpGenerator(GeneratorConfig cfg, boolean addMetadata, @Nullable URI downloadUri) throws IOException {
    super(cfg, addMetadata, "ColDP");
    src = new File("/tmp/" + name + ".src");
    download = new DownloadUtil(hc);
    srcUri = downloadUri;
  }

  protected void prepare() throws Exception {
    // override if needed
  }

  protected abstract void addData() throws Exception;

  @Override
  protected void addDataFiles() throws Exception {
    try {
      // get latest CSVs
      if (src.exists()) {
        LOG.info("Reuse data from {}", src);
      } else if (srcUri != null) {
        LOG.info("Downloading latest data from {}", srcUri);
        download.download(srcUri, src);
      } else {
        LOG.warn("Missing source file {}", src);
      }
      prepare();
      addData();
      if (writer != null) {
        writer.close();
      }
      if (refWriter != null) {
        refWriter.close();
      }

    } finally {
      if (src.exists()) {
        //FileUtils.deleteQuietly(src);
      }
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
    return new TermWriter.TSV(dir, rowType, ColdpTerm.RESOURCES.get(rowType));
  }

  protected TermWriter additionalWriter(Term rowType, List<? extends Term> columns) throws IOException {
    return new TermWriter.TSV(dir, rowType, columns);
  }

}
