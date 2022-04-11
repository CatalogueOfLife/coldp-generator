package org.catalogueoflife.data.wsc;

import com.fasterxml.jackson.databind.DatabindException;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import life.catalogue.api.vocab.Gazetteer;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.HttpException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Generator extends AbstractGenerator {
  private static final String API = "https://wsc.nmbe.ch/api/lsid/";
  private final String apiKey;
  private final File tmp;
  private static int MAX = 61000;
  private static String ERROR = "error: ";

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, null);
    apiKey = Preconditions.checkNotNull(cfg.apiKey, "API Key required");
    tmp = new File("/tmp/colp-generator/wsc");
    tmp.mkdirs();
  }

  @Override
  protected void addData() throws Exception {
    for (int id=1; id<=MAX; id++) {
      crawl(id);
    }
    parse();
  }

  private void parse() throws Exception {
    initRefWriter(List.of(
      ColdpTerm.ID,
      ColdpTerm.citation,
      ColdpTerm.doi
    ));
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.status,
      ColdpTerm.rank,
      ColdpTerm.uninomial,
      ColdpTerm.genericName,
      ColdpTerm.specificEpithet,
      ColdpTerm.infraspecificEpithet,
      ColdpTerm.authorship,
      ColdpTerm.nameReferenceID,
      ColdpTerm.publishedInPage
    ));
    var dWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.gazetteer,
        ColdpTerm.area
    ));


    final IntSet refs = new IntOpenHashSet();
    final Set<String> higher = new HashSet<>();
    // simply loop over all files
    for (String fn : tmp.list(new SuffixFileFilter(".json"))) {
      var to = read(fn);
      if (to.isPresent()) {
        var tax = to.get();
        System.out.println(String.format("%s: %s %s %s %s", tax.taxon.lsid, tax.taxon.genus, tax.taxon.species, tax.taxon.subspecies, tax.taxon.author));
        writer.set(ColdpTerm.ID, tax.taxon.lsid);
        writer.set(ColdpTerm.rank, tax.taxon.taxonRank);
        writer.set(ColdpTerm.genericName, tax.taxon.genus);
        writer.set(ColdpTerm.specificEpithet, tax.taxon.species);
        writer.set(ColdpTerm.infraspecificEpithet, tax.taxon.subspecies);
        writer.set(ColdpTerm.authorship, tax.taxon.author);
        writer.set(ColdpTerm.status, tax.taxon.status);

        if (tax.validTaxon != null) {
          writer.set(ColdpTerm.parentID, tax.validTaxon.getLSID());
        } else {
          if (tax.taxon.genusObject != null) {
            writer.set(ColdpTerm.parentID, tax.taxon.genusObject.genLsid);
          } else if (tax.taxon.familyObject != null) {
            writer.set(ColdpTerm.parentID, tax.taxon.familyObject.famLsid);
          }
        }

        if (tax.taxon.referenceObject != null && !StringUtils.isBlank(tax.taxon.referenceObject.reference)) {
          // seen reference before?
          int rid = tax.taxon.referenceObject.reference.hashCode();
          if (!refs.contains(rid)) {
            tax.taxon.referenceObject.write(rid, refWriter);
            refs.add(rid);
          }
          writer.set(ColdpTerm.nameReferenceID, rid);
          writer.set(ColdpTerm.publishedInPage, tax.taxon.referenceObject.pageDescription);
        }
        if (!StringUtils.isBlank(tax.taxon.distribution)) {
          dWriter.set(ColdpTerm.taxonID, tax.taxon.lsid);
          dWriter.set(ColdpTerm.area, tax.taxon.distribution);
          dWriter.set(ColdpTerm.gazetteer, Gazetteer.TEXT);
          dWriter.next();
        }
        writer.next();

        // new higher taxa?
        if (!higher.contains(tax.taxon.genusObject.genLsid)) {
          if (!higher.contains(tax.taxon.familyObject.famLsid)) {
            tax.taxon.familyObject.write(tax.taxon.familyObject.famLsid, null, false, writer, higher);
          }
          tax.taxon.genusObject.write(tax.taxon.genusObject.genLsid, tax.taxon.familyObject.famLsid, true, writer, higher);
        }
      }
    }
  }

  private Optional<NameUsage> read(String fn) throws IOException {
    String content = UTF8IoUtils.readString(new File(tmp, fn));
    if (!content.startsWith(ERROR)) {
      try {
        return Optional.of(mapper.readValue(content, NameUsage.class));
      } catch (DatabindException e) {
        LOG.error("Jackson exception >{}< for content: {}", e.getMessage(), content);
      }
    }
    return Optional.empty();
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", "Version 23.0");
    super.addMetadata();
  }

  private void crawl(int id) throws IOException {
    final String lsid = String.format("urn:lsid:nmbe.ch:spidersp:%06d", id);
    // keep local files so we can reuse them - the API is limits number of requests
    File f = new File(tmp, id+".json");
    URI uri = URI.create(API+lsid+"?apiKey="+apiKey);
    // we will try eternally as there are daily request limits
    while (true) {
      try {
        var x = http.getJSON(uri);
        FileUtils.write(f, x, StandardCharsets.UTF_8);
        System.out.println(String.format("Crawled %06d", id));
        return;

      } catch (HttpException e) {
        // WSC uses 403 to limit number of daily requests
        if (e.status == HttpStatus.SC_FORBIDDEN) {
          try {
            LOG.warn("Max daily limit reached. Go to sleep...", e);
            TimeUnit.HOURS.wait(1);
          } catch (InterruptedException ex) {
            return;
          }
        } else {
          LOG.warn("Failed to fetch {}", id, e);
          FileUtils.write(f, ERROR+String.format("%d - %s - %s", e.status, e.uri, e.getMessage()), StandardCharsets.UTF_8);
          return;
        }
      }
    }
  }

  static class NameUsage {
    public Taxon taxon;
    public Synonym validTaxon;
    public Object error;
    public Object message;
  }
  static class Taxon {
    public String species;
    public String subspecies;
    public String author;
    public String taxonRank;
    public String status;
    public String genus;
    public TaxonObject genusObject;
    public String family;
    public TaxonObject familyObject;
    public String distribution;
    public String lsid;
    public ReferenceObject referenceObject;
  }
  static class Synonym {
    private static Pattern LINK2LSID = Pattern.compile("urn:lsid.+$");
    public String _href;
    String getLSID() {
      if (_href != null) {
        var m = LINK2LSID.matcher(_href);
        if (m.find()) {
          return m.group();
        }
      }
      return null;
    }
  }
  static class TaxonObject {
    public String genus;
    public String genLsid;
    public String family;
    public String famLsid;
    public String author;

    void write(String id, String parentID, boolean isGenus, TermWriter w, Set<String> higher) throws IOException {
      w.set(ColdpTerm.ID, id);
      w.set(ColdpTerm.parentID, parentID);
      if (isGenus) {
        w.set(ColdpTerm.uninomial, genus);
        w.set(ColdpTerm.rank, "genus");
      } else {
        w.set(ColdpTerm.uninomial, family);
        w.set(ColdpTerm.rank, "family");
      }
      w.set(ColdpTerm.authorship, author);
      w.next();
      higher.add(id);
    }
  }
  static class ReferenceObject {
    public String reference;
    public String doi;
    public String pageDescription;

    void write(int rid, TermWriter w) throws IOException {
      w.set(ColdpTerm.citation, reference);
      w.set(ColdpTerm.doi, doi);
      w.set(ColdpTerm.ID, rid);
      w.next();
    }
  }

}
