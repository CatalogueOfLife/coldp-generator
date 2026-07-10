package org.catalogueoflife.data.lpsn;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracted values from a single LPSN website taxon detail page (https://lpsn.dsmz.de).
 * The website is the only source that carries cyanobacteria (and other names the API
 * withholds), so these pages are scraped and their record numbers reused verbatim as
 * ColDP ids — the same id space the API uses.
 *
 * <p>All fields live in the {@code #detail-page} container as {@code <b>Label:</b> value}
 * paragraphs. The one non-trivial extraction is splitting the scientific name from its
 * authorship in the {@code Name} field: the name is rendered in {@code <I>} elements,
 * but so is a trailing {@code et al.} in the authorship, and rank connectors such as
 * {@code subsp.} sit as plain text between the name's italics. The name therefore extends
 * from the first italic through subsequent italics only while the plain text between them
 * is empty or a rank connector; the first other plain text starts the authorship.</p>
 */
public class LpsnPage {
  private static final Pattern RECORD_NO = Pattern.compile("(\\d+)");
  // rank connectors that may sit as plain text inside a scientific name
  private static final Set<String> CONNECTORS = Set.of(
    "subsp.", "var.", "f.", "subvar.", "subf.", "forma", "ssp.");
  private static final Set<String> TAXON_RANKS = Set.of(
    "domain", "kingdom", "phylum", "class", "order", "family", "genus", "species", "subspecies");

  /** The page path this was fetched from (e.g. {@code /genus/aerofilum}); set by the crawler. */
  public String url;
  public Integer recordNo;
  public String name;
  public String author;
  public String rank;
  public String nomStatus;
  public String taxStatus;
  public String correctNameLink;
  public String basionymLink;
  public String typeLink;
  public List<String> childTaxaLinks = new ArrayList<>();
  public List<String> synonymLinks = new ArrayList<>();

  public static LpsnPage parse(String html) {
    Document doc = Jsoup.parse(html);
    Element root = doc.selectFirst("#detail-page");
    if (root == null) {
      root = doc;
    }
    LpsnPage p = new LpsnPage();

    String rec = fieldText(root, "Record number:");
    if (rec != null) {
      Matcher m = RECORD_NO.matcher(rec);
      if (m.find()) {
        p.recordNo = Integer.valueOf(m.group(1));
      }
    }

    Element nameB = labelBold(root, "Name:");
    if (nameB != null) {
      p.parseNameAndAuthor(nameB);
    }

    String cat = fieldText(root, "Category:");
    if (cat != null) {
      p.rank = cat.trim().toLowerCase();
    }
    p.nomStatus = fieldText(root, "Nomenclatural status:");
    p.taxStatus = fieldText(root, "Taxonomic status:");

    p.correctNameLink = fieldLink(root, "Correct name:");
    p.basionymLink = fieldLink(root, "Basionym:");
    // the type is exposed as "Type species:" (genus), "Type genus:" (family/order), etc.
    for (String typeLabel : new String[]{"Type species:", "Type genus:", "Type order:", "Type class:"}) {
      p.typeLink = fieldLink(root, typeLabel);
      if (p.typeLink != null) break;
    }

    p.childTaxaLinks = sectionLinks(root, "Child taxa:");
    p.synonymLinks = sectionLinks(root, "Synonyms:");
    return p;
  }

  /** Split the Name field into scientific name and authorship (see class javadoc). */
  private void parseNameAndAuthor(Element nameB) {
    StringBuilder nameSb = new StringBuilder();
    StringBuilder authorSb = new StringBuilder();
    boolean inName = true;
    String pendingConnector = null;
    for (Node n = nameB.nextSibling(); n != null; n = n.nextSibling()) {
      if (inName) {
        if (n instanceof Element el && el.normalName().equals("i")) {
          if (pendingConnector != null) {
            append(nameSb, pendingConnector);
            pendingConnector = null;
          }
          append(nameSb, el.text());
        } else {
          String txt = nodeText(n).trim();
          if (txt.isEmpty()) {
            continue;
          } else if (CONNECTORS.contains(txt)) {
            pendingConnector = txt; // include only if another name italic follows
          } else {
            inName = false;
            append(authorSb, txt);
          }
        }
      } else {
        append(authorSb, nodeText(n).trim());
      }
    }
    name = nameSb.length() > 0 ? nameSb.toString() : null;
    author = authorSb.length() > 0 ? authorSb.toString() : null;
  }

  private static void append(StringBuilder sb, String s) {
    if (s == null || s.isBlank()) return;
    if (sb.length() > 0) sb.append(' ');
    sb.append(s.trim());
  }

  private static String nodeText(Node n) {
    if (n instanceof TextNode tn) return tn.text();
    if (n instanceof Element el) return el.text();
    return "";
  }

  /** The {@code <b>} element whose own text starts with the given label. */
  private static Element labelBold(Element root, String label) {
    for (Element b : root.select("b")) {
      if (b.ownText().startsWith(label)) {
        return b;
      }
    }
    return null;
  }

  /** Plain text value following a {@code <b>Label:</b>} within its paragraph. */
  private static String fieldText(Element root, String label) {
    Element b = labelBold(root, label);
    if (b == null) return null;
    StringBuilder sb = new StringBuilder();
    for (Node n = b.nextSibling(); n != null; n = n.nextSibling()) {
      String t = nodeText(n);
      if (t != null) sb.append(t);
    }
    String v = sb.toString().trim();
    return v.isEmpty() ? null : v;
  }

  /** First taxon-page href following a {@code <b>Label:</b>}. */
  private static String fieldLink(Element root, String label) {
    Element b = labelBold(root, label);
    if (b == null) return null;
    Element p = b.parent();
    if (p == null) return null;
    for (Element a : p.select("a[href]")) {
      String href = a.attr("href");
      if (isTaxonLink(href)) return href;
    }
    return null;
  }

  /**
   * Taxon-page hrefs inside the {@code <ul>} that follows a section header
   * {@code <span class="open"><b>Label:</b></span>}.
   */
  private static List<String> sectionLinks(Element root, String label) {
    List<String> out = new ArrayList<>();
    Element header = null;
    for (Element span : root.select("span.open")) {
      Element b = span.selectFirst("b");
      if (b != null && b.ownText().startsWith(label)) {
        header = span;
        break;
      }
    }
    if (header == null) return out;
    // a <ul> is not valid inside a <p>, so the parser auto-closes the paragraph and the
    // section list ends up as a sibling of the header's paragraph rather than of the span;
    // search forward in document order for the first following <ul>
    Element ul = followingUl(header);
    if (ul == null) return out;
    for (Element a : ul.select("a[href]")) {
      String href = a.attr("href");
      if (isTaxonLink(href) && !out.contains(href)) {
        out.add(href);
      }
    }
    return out;
  }

  /** First {@code <ul>} appearing after the given element in document order. */
  private static Element followingUl(Element from) {
    for (Element node = from; node != null; node = node.parent()) {
      for (Element sib = node.nextElementSibling(); sib != null; sib = sib.nextElementSibling()) {
        if (sib.normalName().equals("ul")) return sib;
        Element nested = sib.selectFirst("ul");
        if (nested != null) return nested;
      }
    }
    return null;
  }

  private static boolean isTaxonLink(String href) {
    if (href == null || !href.startsWith("/")) return false;
    int slash = href.indexOf('/', 1);
    if (slash < 0) return false;
    return TAXON_RANKS.contains(href.substring(1, slash));
  }
}
