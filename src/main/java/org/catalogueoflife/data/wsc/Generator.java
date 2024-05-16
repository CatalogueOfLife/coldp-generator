package org.catalogueoflife.data.wsc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import life.catalogue.api.model.SimpleName;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.api.vocab.Gazetteer;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.HttpException;
import org.gbif.nameparser.api.Rank;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;

public class Generator extends AbstractColdpGenerator {
  private static final String API = "https://wsc.nmbe.ch/api/";
  private static final Pattern LSID_PATTERN = Pattern.compile("nmbe.ch:spider(sp|gen|fam):([0-9]+)");
  private static final String ERROR = "error: ";
  private static int MAX_DEFAULT = 65000;
  private static String ORDER = "Araneae";
  private final String apiKey;
  private final File json;
  private final Set<String> higherLSIDs = new HashSet<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, null);
    apiKey = Preconditions.checkNotNull(cfg.apiKey, "API Key required");
    json = cfg.wscDataRepo;
    System.out.println("Keep WSC API responses at " + json);
    if (!json.exists()) {
      System.out.println("  creating missing JSON directory");
      json.mkdirs();
    }
  }

  @Override
  protected void addData() throws Exception {
    if (cfg.date != null) {
      if (cfg.date.equalsIgnoreCase("skip")) {
        System.out.println("Skip WSC updates");
      } else {
        System.out.println("Look for WSC updates since " + cfg.date);
        update();
      }

    } else {
      int max = ObjectUtils.coalesce(cfg.wscMaxKey, MAX_DEFAULT);
      System.out.println("Crawl all of WSC up to " + max);
      for (int id = 1; id <= max; id++) {
        crawl(String.format("urn:lsid:nmbe.ch:spidersp:%06d", id), false);
      }
      System.out.println("\nCrawl " + higherLSIDs.size() + " higher taxa in WSC");
      for (String lsid : higherLSIDs) {
        crawl(lsid, false);
      }
    }
    System.out.println("\nParse JSON files");
    initWriters();
    addRootClassification();
    parse();
  }

  private void addRootClassification() throws IOException {
    SimpleName p = null;
    for (var sn : List.of(
            SimpleName.sn(Rank.KINGDOM, "Animalia"),
            SimpleName.sn(Rank.PHYLUM, "Arthropoda"),
            SimpleName.sn(Rank.CLASS, "Arachnida"),
            SimpleName.sn(Rank.ORDER, ORDER)
    )) {
      writer.set(ColdpTerm.ID, sn.getRank());
      if (p != null) {
        writer.set(ColdpTerm.parentID, p.getName());
      }
      writer.set(ColdpTerm.rank, sn.getRank());
      writer.set(ColdpTerm.scientificName, sn.getName());
      writer.next();
      p = sn;
    }
  }

  private void update() throws Exception {
    URI uri = URI.create(API + "updates?date=" + cfg.date + "&apiKey=" + apiKey);
    var upd = mapper.readValue(http.getStreamJSON(uri), Update.class);
    crawl(upd);
    while (upd.hasNext()) {
      upd = mapper.readValue(http.getStreamJSON(upd.next()), Update.class);
      crawl(upd);
    }
    System.out.println("All updated");
  }

  private void crawl(Update upd) throws IOException {
    System.out.println(String.format("Crawl %s updates", upd.updates.size()));
    for (String lsid : upd.updates) {
      crawl(lsid, true);
    }
  }

  private void initWriters() throws Exception {
    initRefWriter(List.of(
            ColdpTerm.ID,
            ColdpTerm.citation,
            ColdpTerm.doi
    ));
    newWriter(ColdpTerm.NameUsage, List.of(
            ColdpTerm.ID,
            ColdpTerm.parentID,
            ColdpTerm.status,
            ColdpTerm.nameStatus,
            ColdpTerm.rank,
            ColdpTerm.scientificName,
            ColdpTerm.uninomial,
            ColdpTerm.genericName,
            ColdpTerm.specificEpithet,
            ColdpTerm.infraspecificEpithet,
            ColdpTerm.authorship,
            ColdpTerm.nameReferenceID,
            ColdpTerm.publishedInPage
    ));
  }
  private void parse() throws Exception {
    try (var dWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.gazetteer,
        ColdpTerm.area
    ))) {
      final IntSet refs = new IntOpenHashSet();
      // simply loop over all files
      for (String fn : json.list(new SuffixFileFilter(".json"))) {
        try {
          var to = read(new File(json, fn));
          if (to.isPresent()) {
            var tax = to.get();
            writer.set(ColdpTerm.ID, tax.taxon.lsid);
            writer.set(ColdpTerm.rank, tax.taxon.taxonRank);
            writer.set(ColdpTerm.authorship, tax.taxon.author);
            if (tax.taxon.taxonRank.equalsIgnoreCase("family")) {
              System.out.println(String.format("%s: %s %s", tax.taxon.lsid, tax.taxon.family, tax.taxon.author));
              writer.set(ColdpTerm.uninomial, tax.taxon.family);
            } else if (tax.taxon.taxonRank.equalsIgnoreCase("genus")) {
              System.out.println(String.format("%s: %s %s - %s", tax.taxon.lsid, tax.taxon.genus, tax.taxon.author, tax.taxon.family));
              writer.set(ColdpTerm.uninomial, tax.taxon.genus);
            } else {
              String subsp = StringUtils.isBlank(tax.taxon.subspecies) ? "" : " "+tax.taxon.subspecies;
              System.out.println(String.format("%s: %s %s%s %s", tax.taxon.lsid, tax.taxon.genus, tax.taxon.species, subsp, tax.taxon.author));
              writer.set(ColdpTerm.genericName, tax.taxon.genus);
              writer.set(ColdpTerm.specificEpithet, tax.taxon.species);
              writer.set(ColdpTerm.infraspecificEpithet, tax.taxon.subspecies);
            }

            writer.set(ColdpTerm.status, tax.taxon.status);
            writer.set(ColdpTerm.nameStatus, tax.taxon.status);
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
            writer.next();

            if (!StringUtils.isBlank(tax.taxon.distribution)) {
              dWriter.set(ColdpTerm.taxonID, tax.taxon.lsid);
              dWriter.set(ColdpTerm.area, tax.taxon.distribution);
              dWriter.set(ColdpTerm.gazetteer, Gazetteer.TEXT);
              dWriter.next();
            }
          }
        } catch (RuntimeException e) {
          LOG.error("Error parsing file {}", fn);
          throw e;
        }
      }
    }
  }

  private Optional<NameUsage> read(File f) throws IOException {
    String content = readString(f);
    if (!content.startsWith(ERROR)) {
      try {
        return Optional.of(mapper.readValue(content, NameUsage.class));
      } catch (JsonProcessingException e) {
        LOG.error("Jackson exception >{}< for content: {}", e.getMessage(), content);
      }
    }
    return Optional.empty();
  }

  public static String readString(File f) throws IOException {
    try (var stream = new FileInputStream(f)) {
      return IOUtils.toString(stream, StandardCharsets.UTF_8);
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", "Version 23.0");
    super.addMetadata();
  }

  private void crawl(String lsid, boolean forceUpdate) throws IOException {
    // keep local files so we can reuse them - the API limits number of daily requests
    var m = LSID_PATTERN.matcher(lsid);
    if (!m.find()) {
      throw new IllegalArgumentException("Unexpected LSID " + lsid);
    }
    String rank = m.group(1);
    int id = Integer.parseInt(m.group(2));
    File f = new File(json, String.format("%s%06d.json", rank, id));
    // only load from API if it's not yet existing
    boolean failed = false;
    if (f.exists() && !forceUpdate) {
      System.out.println(String.format("Reuse %s", f.getName()));

    } else {
      String uri = API + "lsid/" + lsid;
      try {
        http.downloadJSON(URI.create(uri + "?apiKey=" + apiKey), new HashMap<>(), f);
        System.out.println(String.format("Crawled %s", lsid));

      } catch (HttpException e) {
        failed = true;
        // WSC uses 403 to limit number of daily requests - abort in that case, we cant get any further today
        if (e.status == HttpStatus.SC_FORBIDDEN) {
          FileUtils.deleteQuietly(f);
          LOG.error("Max daily limit reached. Good bye!", e);
          throw new IllegalStateException("Max daily API usage limit reached");

        } else {
          System.out.println(String.format("Crawl error %s: %s", lsid, e.status));
          FileUtils.write(f, ERROR + String.format("%d - %s - %s", e.status, e.uri, e.getMessage()), StandardCharsets.UTF_8);
        }
      }
    }
    // extract higher taxa to make sure we crawl them at the end
    if (!failed) {
      var to = read(f);
      to.ifPresent(nu -> {
        nu.taxon.addHigherTaxa(higherLSIDs);
      });
    }
  }

  static class NameUsage {
    public Taxon taxon;
    public Synonym validTaxon;
  }

  static class Taxon {
    public String lsid;
    public String species;
    public String subspecies;
    public String author;
    public String taxonRank;
    public String status;
    public String genus;
    public String family;
    public TaxonObject genusObject;
    public TaxonObject familyObject;
    public String distribution;
    public ReferenceObject referenceObject;

    void addHigherTaxa(Collection<String> ids) {
      // track higher taxa
      if (genusObject != null) {
        ids.add(genusObject.genLsid);
      }
      if (familyObject != null) {
        ids.add(familyObject.famLsid);
      }
    }
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

  static class Update {
    public List<String> updates;
    public Map<String, String> _links;

    boolean hasNext() {
      return _links != null && _links.containsKey("next");
    }

    URI next() {
      if (_links != null && _links.containsKey("next")) {
        return URI.create(_links.get("next"));
      }
      return null;
    }
  }

}
