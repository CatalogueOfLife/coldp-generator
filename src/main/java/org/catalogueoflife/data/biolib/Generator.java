package org.catalogueoflife.data.biolib;

import life.catalogue.api.vocab.Language;
import life.catalogue.parser.LanguageParser;
import life.catalogue.parser.RankParser;
import life.catalogue.parser.UnparsableException;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractTextTreeGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;
import org.gbif.txtree.SimpleTreeNode;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;

public class Generator extends AbstractTextTreeGenerator {
  private static Logger LOG = LoggerFactory.getLogger(Generator.class);
  private static final URI TAXON = URI.create("https://www.biolib.cz/en/taxon/id");
  private static final URI NAME = URI.create("https://www.biolib.cz/en/taxonnames/id");
  private static final Pattern idPattern = Pattern.compile("/id(\\d+)/?$", Pattern.CASE_INSENSITIVE);
  private int counter = 0;
  private long synID = -1;
  private final Language SCIENTIFIC = new Language("zz", "Scientific Names");
  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void populateTree() throws Exception {
    // recursively crawl all children of root
    processTaxon(cfg.biolibRootID, null);
    LOG.info("Scraping completed. Added {} taxa and {} synonyms to the tree.", counter, -1 * synID);
  }

  static class XNode {
    final SimpleTreeNode node;
    final List<Integer> children = new ArrayList<>();

    XNode(SimpleTreeNode node) {
      this.node = node;
    }
  }

  void processTaxon(Integer id, SimpleTreeNode parent) throws Exception {

    XNode xn = null;
    try {
      xn = scrape(id);
    } catch (Exception e) {
      LOG.warn("Failed to scrape {}. Try once more!", id, e);
      try {
        xn = scrape(id);
      } catch (Exception e2) {
        LOG.warn("Failed to scrape {} again. Skip!", id, e2);
      }
    }

    if (xn != null) {
      if (parent == null) {
        tree.getRoot().add(xn.node);
      } else {
        parent.children.add(xn.node);
      }
      // recursively process children
      for (Integer cid : xn.children) {
        processTaxon(cid, xn.node);
      }
    }
  }

  XNode scrape(Integer id) throws Exception {
    var html = http.get(TAXON + id.toString());
    final Document doc = Jsoup.parse(html);

    final var screen = doc.select("div#screen");
    final var header = screen.select("h1");
    final var name = header.select("strong:last-of-type");

    var nameTxt = name.text();
    LOG.debug("process {} > {}: {}", ++counter, id, nameTxt);

    Optional<Rank> rank;
    try {
      rank = RankParser.PARSER.parse(NomCode.ZOOLOGICAL, header.prev().text());
    } catch (UnparsableException e) {
      LOG.info("Unknown rank {}", header.prev().text());
      rank = Optional.of(Rank.OTHER);
    }

    final var tn = new SimpleTreeNode(id, nameTxt, rank.orElse(null), false, new HashMap<>(), null);

    // add synonyms & vernaculars - load name details
    scrapeName(tn);

    // process children
    XNode xn = new XNode(tn);
    var treearea = screen.select("div#system");
    for (var ca : treearea.select("div.treediv > a:first-child")) {
      if (ca.hasClass("img")) continue;

      var link = ca.attr("href");
      var m = idPattern.matcher(link);
      if (m.find()) {
        xn.children.add(Integer.parseInt(m.group(1)));
      } else {
        LOG.warn("Cant extract child identifier from link {}", link);
      }
    }
    return xn;
  }

  void scrapeName(SimpleTreeNode tn) throws Exception {
    List<String> vnames = new ArrayList<>();

    var html = http.get(NAME + String.valueOf(tn.id));
    Document ndoc = Jsoup.parse(html);
    var synList = ndoc.select("div#screen div.item-list-box").first();
    var syns = synList.select("div.item-list-item");
    Language lang = null;
    for (Element n : syns) {
      var img = n.select("img");
      if (img.isEmpty()) {
        String n2 = n.text().trim();
        n2 = n2.replaceFirst(" - .+$", "")
               .replaceFirst("\\(type .+$", "")
               .replaceFirst(" ID:.+$", "")
               .trim();
        if (StringUtils.isBlank(n2)) continue;
        if (lang != null && lang.equals(SCIENTIFIC)) {
          // synonym?
          if (!n2.toLowerCase().startsWith(tn.name.toLowerCase())) {
            var s = new SimpleTreeNode(synID--, n2, null, false);
            tn.synonyms.add(s);
          }

        } else if (lang != null){
          // vernacular name
          vnames.add(lang.getCode() + ":" + n2);
        }

      } else {
        // new language
        var langTxt = n.text();
        if (langTxt.equalsIgnoreCase(SCIENTIFIC.getTitle())) {
          lang = SCIENTIFIC;
        } else {
          lang = LanguageParser.PARSER.parseOrNull(langTxt);
        }
      }
    }

    if (!vnames.isEmpty()){
      tn.infos.put("VERN", vnames.toArray(new String[0]));
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now());
    super.addMetadata();
  }
}
