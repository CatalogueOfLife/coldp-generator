package org.catalogueoflife.data.biolib;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.catalogueoflife.data.utils.HttpUtils;
import org.gbif.txtree.ParsedTreeNode;
import org.gbif.txtree.SimpleTreeNode;
import org.gbif.txtree.Tree;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TreeDecorator implements AutoCloseable{
  private static Logger LOG = LoggerFactory.getLogger(TreeDecorator.class);
  private final CloseableHttpClient hc;
  private final HttpUtils http;

  public TreeDecorator() {
    HttpClientBuilder htb = HttpClientBuilder.create();
    this.hc = htb.build();
    this.http = new HttpUtils();
  }

  public Tree<SimpleTreeNode> addAuthors(Tree<ParsedTreeNode> tree) {
    Tree<SimpleTreeNode> tree2 = new Tree<>();
    for(var root : tree.getRoot()) {
      tree2.getRoot().add(process(root));
    }
    return tree2;
  }

  private SimpleTreeNode process(ParsedTreeNode n) {
    // convert this node into an updated simple tree node
    SimpleTreeNode nn = convert(n);
    for(var c : n.children) {
      nn.children.add(process(c));
    }
    for(var s : n.synonyms) {
      nn.synonyms.add(convert(s));
    }
    return nn;
  }

  private SimpleTreeNode convert(ParsedTreeNode n) {
    LOG.debug("convert {}", n.name);
    // read genus index
    if (!n.parsedName.hasAuthorship()) {
      try {

        var html = http.get("https://www.biolib.cz/en/formsearch/?searchtype=3&searchrecords=1&selecttaxonid=1&taxonid=&action=execute&string="+n.name);
        final Document doc = Jsoup.parse(html);
        var title = doc.select("head > title").text().toLowerCase();
        if (title.startsWith("search ")) {
          // multiple matches. Pick exact match
          for (var item : doc.select("div#screen .item-list-box div.realm-1")) {
            var name = item.text().trim();
            var exact = item.select("a").text().trim();
            if (n.name.equalsIgnoreCase(exact)) {
              return new SimpleTreeNode(n.id, name, n.rank, n.basionym, n.infos);
            }
          }
        } else if (title.contains(n.name.toLowerCase())) {
          return parseDetails(n, doc);
        }
        LOG.warn("Failed to find BioLib page for {}", n.name);

      } catch (Exception e) {
        LOG.info("Failed to read {}", n.name, e);
      }
    }
    // return as it was already
    return new SimpleTreeNode(n.id, n.name, n.rank, n.basionym, n.infos);
  }

  private SimpleTreeNode parseDetails(ParsedTreeNode n, Document doc) {
    var header = doc.select("div#screen h1");
    var author = header.select("small");
    if (!author.isEmpty()) {
      var name = author.parents().first().text();
      return new SimpleTreeNode(n.id, name, n.rank, n.basionym, n.infos);
    }
    // return as it was already
    return new SimpleTreeNode(n.id, n.name, n.rank, n.basionym, n.infos);
  }

  public static void main(String[] args) throws IOException {
    var dec = new TreeDecorator();
    var f = new File("/Users/markus/code/data/data-coccinellidae/taxonomy.txtree");
    Tree<ParsedTreeNode> tree = org.gbif.txtree.Tree.parsed(new FileInputStream(f));
    Tree<?> t2 = dec.addAuthors(tree);
    t2.print(new File("/Users/markus/code/data/data-coccinellidae/taxonomy2.txtree"));
  }

  @Override
  public void close() throws Exception {
    hc.close();
  }
}
