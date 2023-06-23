package org.catalogueoflife.data.biolib;

import life.catalogue.parser.RankParser;
import org.catalogueoflife.data.AbstractTextTreeGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;
import org.gbif.txtree.SimpleTreeNode;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Generator extends AbstractTextTreeGenerator {
  private static Logger LOG = LoggerFactory.getLogger(Generator.class);
  private static final URI BASE = URI.create("https://www.biolib.cz/en/taxon/");
  private static final Integer root = 10713; // Coccinellidae
  //private static final Integer root = 535473; // small test group
  private static final Pattern idPattern = Pattern.compile("/id(\\d+)/?$", Pattern.CASE_INSENSITIVE);

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void populateTree() throws Exception {
    // recursively crawl all children of root
    processTaxon(root, null);
  }

  void processTaxon(Integer id, SimpleTreeNode parent) throws Exception {
    var html = http.get(BASE+"id"+id);
    final Document doc = Jsoup.parse(html);

    String name;
    Map<String, String[]> infos = new HashMap<>();

    var screen = doc.select("div#screen");
    var header = screen.select("h1");
    name = header.select("strong:last-child").text();
    LOG.debug("process {}: {}", id, name);

    var rank = RankParser.PARSER.parse(NomCode.ZOOLOGICAL, header.prev().text());
    final var tn = new SimpleTreeNode(id, name, rank.orElse(null), false, infos);

    // TODO: add synonyms

    // process children - TODO: scrape child ids
    var treearea = screen.select("div#system");
    for (var ca : treearea.select("a")) {
      var link = ca.attr("href");
      var m = idPattern.matcher(link);
      if (m.find()) {
        processTaxon(Integer.parseInt(m.group(1)), tn);
      } else {
        LOG.warn("Cant extract child identifier from link {}", link);
      }
    }

    if (parent == null) {
      tree.getRoot().add(tn);
    } else {
      parent.children.add(tn);
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now());
    super.addMetadata();
  }
}
