package org.catalogueoflife.data.wsc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import life.catalogue.api.model.SimpleName;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.api.vocab.area.Gazetteer;
import life.catalogue.api.vocab.TaxonomicStatus;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Generator extends AbstractColdpGenerator {
  private static final String API = "https://wsc.nmbe.ch/api/";
  private static final String HOMEPAGE = "https://wsc.nmbe.ch/";
  private static final Pattern LSID_PATTERN = Pattern.compile("nmbe.ch:spider(sp|gen|fam):([0-9]+)");
  private static final String ERROR = "error: ";
  static final Pattern yearSuffix = Pattern.compile("(\\d+)[abcdefg]$");
  private final String apiKey;
  private final File json;
  private final Set<String> higherLSIDs = new HashSet<>();
  private String rootId;
  private int synIdGen = 1;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, null);
    apiKey = Preconditions.checkNotNull(cfg.apiKey, "API Key required");
    json = cfg.wscDataRepo;
    LOG.info("Keep WSC API responses at {}", json);
    if (!json.exists()) {
      LOG.info("  creating missing JSON directory");
      json.mkdirs();
    }
  }

  @Override
  protected void addData() throws Exception {
    if (cfg.date != null) {
      if (cfg.date.equalsIgnoreCase("skip")) {
        LOG.info("Skip WSC updates");
      } else {
        LOG.info("Look for WSC updates since {}", cfg.date);
        update();
      }

    } else {
      int max = cfg.wscMaxKey;
      LOG.info("Crawl all of WSC up to {}", max);
      for (int id = 1; id <= max; id++) {
        crawl(String.format("urn:lsid:nmbe.ch:spidersp:%06d", id), false);
      }
      LOG.info("Crawl {} higher taxa in WSC", higherLSIDs.size());
      for (String lsid : higherLSIDs) {
        crawl(lsid, false);
      }
    }
    LOG.info("Parse JSON files");
    initWriters();
    addRootClassification();
    parse();
  }

  private void addRootClassification() throws IOException {
    for (var sn : List.of(
            SimpleName.sn(Rank.KINGDOM, "Animalia"),
            SimpleName.sn(Rank.PHYLUM, "Arthropoda"),
            SimpleName.sn(Rank.CLASS, "Arachnida"),
            SimpleName.sn(Rank.ORDER, "Araneae")
    )) {
      String id = sn.getName().toLowerCase();
      writer.set(ColdpTerm.ID, id);
      if (rootId != null) {
        writer.set(ColdpTerm.parentID, rootId);
      }
      writer.set(ColdpTerm.rank, sn.getRank());
      writer.set(ColdpTerm.scientificName, sn.getName());
      writer.next();
      rootId = id;
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
    LOG.info("All updated");
  }

  private void crawl(Update upd) throws IOException, InterruptedException {
    LOG.info("Crawl {} updates", upd.updates.size());
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
            ColdpTerm.publishedInPage,
            ColdpTerm.link
    ));
  }

  private static String link(String lsid) {
    return "https://wsc.nmbe.ch/lsid/" + lsid;
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
            writer.set(ColdpTerm.link, link(tax.taxon.lsid));
            writer.set(ColdpTerm.rank, tax.taxon.taxonRank);
            writer.set(ColdpTerm.authorship, tax.taxon.author);
            if (tax.taxon.taxonRank.equalsIgnoreCase("family")) {
              LOG.debug("{}: {} {}", tax.taxon.lsid, tax.taxon.family, tax.taxon.author);
              writer.set(ColdpTerm.uninomial, tax.taxon.family);
              writer.set(ColdpTerm.parentID, rootId);

            } else if (tax.taxon.taxonRank.equalsIgnoreCase("genus")) {
              LOG.debug("{}: {} {} - {}", tax.taxon.lsid, tax.taxon.genus, tax.taxon.author, tax.taxon.family);
              writer.set(ColdpTerm.uninomial, tax.taxon.genus);

            } else {
              String subsp = StringUtils.isBlank(tax.taxon.subspecies) ? "" : " "+tax.taxon.subspecies;
              LOG.debug("{}: {} {}{} {}", tax.taxon.lsid, tax.taxon.genus, tax.taxon.species, subsp, tax.taxon.author);
              writer.set(ColdpTerm.genericName, tax.taxon.genus);
              writer.set(ColdpTerm.specificEpithet, tax.taxon.species);
              writer.set(ColdpTerm.infraspecificEpithet, tax.taxon.subspecies);
            }

            writer.set(ColdpTerm.status, mapStatus(tax.taxon.status));
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

            // scrape older synonyms in case of valid names
            if (tax.validTaxon == null) {
              //scrapeSynonyms(tax.taxon.lsid);
            }

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

  private String mapStatus(String status) {
    if (status == null) return null;
    return switch (status.toUpperCase().trim()) {
      case "NOMEN_DUBIUM", "NOMEN_NUDUM" -> "bare name";
      default -> status;
    };
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

  private void scrapeSynonyms(String lsid) {
    try {
      String html = http.get(link(lsid));
      var j = Jsoup.parse(html);
      var bs = j.body().select("main b");
      for (var b : bs) {
        if (b.text().trim().equalsIgnoreCase("Taxonomic references")) {
          for (var div : b.siblingElements()) {
            if (div.normalName().equals("div")) {
              var name = div.firstElementChild();
              if (name != null && name.normalName().equals("i")) {
                scrapeSynonym(lsid, name);
              }
              // find all synonyms, look for BRs
              for (var br : div.select("br")) {
                name = br.firstElementChild();
                if (name != null && name.normalName().equals("i")) {
                  scrapeSynonym(lsid, name);
                }
              }
              break;
            }
          }
          break;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to scrape older synonyms for taxon {}", lsid, e);
    }
  }

  /**
   * Expects the i tag with the name
   */
  private void scrapeSynonym(String validLsid, Element tag) throws IOException {
    String name = tag.text();
    String authorship = null;
    var author = tag.nextElementSibling();
    if (author.normalName().equals("strong")) {
      authorship = yearSuffix.matcher(author.text()).replaceFirst("");
      var ref = author.firstElementChild();
      if (ref != null && ref.hasAttr("href")) {
        String link = ref.attr("href");
        //TODO: scrape reference? IDs seem to be different from the ones we written before
      }
    }
    // does this synonym have an LSID already? then we did write it already as its part of the API data
    boolean hasLsid = false;
    Node nxt = author;
    while (nxt != null) {
      if (nxt instanceof TextNode) {
        if (((TextNode) nxt).text().contains("[urn:lsid")){
          hasLsid = true;
        }
      } else if (nxt.normalName().equals("br") || nxt.normalName().equals("i")) {
        break;
      }
      nxt = nxt.nextSibling();
    }

    if (!hasLsid) {
      writeSynonym(validLsid, name, authorship);
    }
  }

  private void writeSynonym(String validLsid, String name, String authorship) throws IOException {
    writer.set(ColdpTerm.ID, "syn" + synIdGen++);
    writer.set(ColdpTerm.parentID, validLsid);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.status, TaxonomicStatus.SYNONYM);
    writer.set(ColdpTerm.authorship, authorship);
    //writer.set(ColdpTerm.rank, tax.taxon.taxonRank);
    writer.next();
  }

  private String scrapeVersion() {
    try {
      String html = http.get(HOMEPAGE);
      return scrapeVersion(html);
    } catch (IOException e) {
      LOG.error("Failed to retrieve version from homepage", e);
    }
    return null;
  }

  static String scrapeVersion(String html) {
    var j = Jsoup.parse(html);
    // try known selectors in order of preference
    for (String selector : new String[]{"main h1 span", "main h1", "h1 span", "h1"}) {
      var s = j.body().selectFirst(selector);
      if (s != null) {
        String text = s.text().replaceFirst("(?i)version *", "").trim();
        if (!text.isEmpty()) return text;
      }
    }
    LOG.warn("Could not scrape WSC version from homepage");
    return null;
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", scrapeVersion());
    super.addMetadata();
  }

  private void crawl(String lsid, boolean forceUpdate) throws IOException, InterruptedException {
    crawl(lsid, forceUpdate, 0);
  }
  private void crawl(String lsid, boolean forceUpdate, int retry) throws IOException, InterruptedException {
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
      LOG.debug("Reuse {}", f.getName());

    } else {
      String uri = API + "lsid/" + lsid;
      try {
        http.downloadJSON(URI.create(uri + "?apiKey=" + apiKey), new HashMap<>(), f);
        LOG.debug("Crawled {}", lsid);

      } catch (HttpException e) {
        failed = true;
        FileUtils.deleteQuietly(f);
        // WSC uses 403 to limit number of daily requests - abort in that case, we cant get any further today
        if (e.status == HttpStatus.SC_FORBIDDEN) {
          LOG.error("Max daily limit reached. Good bye!", e);
          throw new IllegalStateException("Max daily API usage limit reached");

        } else if (e.status == HttpStatus.SC_TOO_MANY_REQUESTS) {
          LOG.warn("Too many requests. Wait a little bit before we continue", e);
          if (retry++ > 10) {
            throw new IllegalStateException("Too many retries, abort entire crawl!");
          }
          TimeUnit.SECONDS.sleep(10);
          crawl(lsid, forceUpdate, retry);

        } else if (e.status == HttpStatus.SC_NOT_FOUND) {
          LOG.info("Not found. Skip", e);

        } else {
          LOG.warn("Crawl error {}: {}", lsid, e.status);
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
      return _links != null && _links.get("next") != null;
    }

    URI next() {
      if (_links != null && _links.get("next") != null) {
        return URI.create(_links.get("next"));
      }
      return null;
    }
  }

}
