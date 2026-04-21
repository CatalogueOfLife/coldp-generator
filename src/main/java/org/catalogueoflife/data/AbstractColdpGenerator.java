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
import java.time.LocalDate;
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
    sources = cfg.tmpDir();
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
      if (cfg.clearSources) {
        LOG.info("--clear-sources: deleting cached source files at {}", sources);
        FileUtils.cleanDirectory(sources);
      } else {
        LOG.info("Reuse data from {}. Use --clear-sources to force a fresh download", sources);
      }
    } else {
      sources.mkdirs();
    }
    for (var e : downloadURIs.entrySet()) {
      download(e.getKey(), e.getValue());
    }
    prepare();
    addData();
    // Default issued/version to today if not set by the generator
    metadata.computeIfAbsent("issued",  k -> LocalDate.now().toString());
    metadata.computeIfAbsent("version", k -> LocalDate.now().toString());
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
   * Sleeps for {@code ms} milliseconds, restoring interrupt status if interrupted.
   * Use this for polite-crawl delays between page downloads.
   */
  protected static void crawlDelay(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns the cached file for {@code filename} under the sources directory, downloading
   * it from {@code url} if it does not exist. Inserts a {@code delayMs} polite delay after
   * each new download. If {@code --no-download} is set and the file is missing, logs a
   * warning and returns {@code null} — callers should skip processing when null is returned.
   */
  @Nullable
  protected File cachedPage(String filename, URI url, long delayMs) throws IOException {
    File f = sourceFile(filename);
    if (!f.exists()) {
      if (cfg.noDownload) {
        LOG.warn("--no-download set but {} not cached; skipping", filename);
        return null;
      }
      LOG.debug("Downloading {}", url);
      http.download(url, f);
      crawlDelay(delayMs);
    }
    return f;
  }

  protected File download(String filename, URI url) throws IOException {
    return download(new File(sources, filename), url, true);
  }

  protected File download(File f, URI url, boolean log) throws IOException {
    if (!f.exists()) {
      if (cfg.noDownload) {
        throw new IllegalStateException("--no-download set but source file not found: " + f);
      }
      if (log) {
        LOG.info("Downloading latest {} from {} to {}", f.getName(), url, f);
      }
      download.download(url, f);
    } else if (log) {
      LOG.info("Reuse source file {}", f);
    }
    return f;
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
