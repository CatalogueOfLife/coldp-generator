package org.catalogueoflife.data.pfnr;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Generator for the International Fossil Plant Names Registry (PFNR).
 * Scrapes https://www.plantfossilnames.org — ~1,200 nomenclatural acts for fossil plant names.
 *
 * Two-phase scrape:
 *   Phase 1: parse sitemap-names-1.xml.gz → name page IDs → download/parse each name page
 *   Phase 2: download/parse /reference/{id}/ pages for DOIs
 */
public class Generator extends AbstractColdpGenerator {

  private static final String BASE_URL     = "https://www.plantfossilnames.org";
  private static final String SITEMAP_URL  = BASE_URL + "/sitemap-names-1.xml.gz";
  private static final Pattern ID_PATTERN  = Pattern.compile("/name/(\\d+)/");
  private static final Pattern DOI_PATTERN = Pattern.compile("\\b(10\\.\\d{4,}/\\S+)");
  private static final Pattern YEAR_PATTERN = Pattern.compile("\\b(1[7-9]\\d{2}|20\\d{2})\\b");

  private TermWriter relWriter;
  private TermWriter tmWriter;
  private TermWriter propWriter;

  // accumulated during name-page parsing; written after all name pages processed
  private final List<int[]> basionymRelations = new ArrayList<>(); // [nameId, basionymId]
  // ref id → [citation text, external URL or null]  (collected from name pages; DOI added in phase 2)
  private final Map<Integer, String[]> refCitations = new LinkedHashMap<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    // ── Writers ───────────────────────────────────────────────────────────────
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.status,
        ColdpTerm.extinct,
        ColdpTerm.nameReferenceID,
        ColdpTerm.publishedInYear,
        ColdpTerm.alternativeID,
        ColdpTerm.link,
        ColdpTerm.remarks,
        ColdpTerm.nameRemarks,
        ColdpTerm.etymology
    ));

    propWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.property,
        ColdpTerm.value
    ));

    relWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));

    tmWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.nameID,
        ColdpTerm.status,
        ColdpTerm.catalogNumber,
        ColdpTerm.institutionCode,
        ColdpTerm.locality,
        ColdpTerm.remarks
    ));

    initRefWriter(List.of(
        ColdpTerm.ID,
        ColdpTerm.citation,
        ColdpTerm.doi,
        ColdpTerm.link
    ));

    // ── Phase 1: enumerate and parse name pages ────────────────────────────
    List<Integer> ids = collectIds();
    LOG.info("PFNR: found {} name IDs", ids.size());

    for (int id : ids) {
      File f = sourceFile("name-" + id + ".html");
      if (!f.exists()) {
        if (cfg.noDownload) {
          LOG.warn("PFNR: --no-download set but {} not cached; skipping", f.getName());
          continue;
        }
        LOG.debug("PFNR: downloading name page {}", id);
        http.download(URI.create(BASE_URL + "/name/" + id + "/"), f);
        try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
      }
      try {
        parseName(Jsoup.parse(f, StandardCharsets.UTF_8.name()), id);
      } catch (Exception e) {
        LOG.warn("PFNR: failed to parse name page {}: {}", id, e.getMessage());
      }
    }

    // ── NameRelation records (basionyms collected during phase 1) ─────────
    for (int[] rel : basionymRelations) {
      relWriter.set(ColdpTerm.nameID, "pfn:" + rel[0]);
      relWriter.set(ColdpTerm.relatedNameID, "pfn:" + rel[1]);
      relWriter.set(ColdpTerm.type, "basionym");
      relWriter.next();
    }

    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());

    // ── Phase 2: reference pages for DOIs ─────────────────────────────────
    for (Map.Entry<Integer, String[]> e : refCitations.entrySet()) {
      int refId = e.getKey();
      String citation = e.getValue()[0];
      String extUrl   = e.getValue()[1]; // external URL from "link" anchor, may be null
      File rf = sourceFile("ref-" + refId + ".html");
      if (!rf.exists()) {
        if (!cfg.noDownload) {
          LOG.debug("PFNR: downloading reference page {}", refId);
          http.download(URI.create(BASE_URL + "/reference/" + refId + "/"), rf);
          try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        }
      }
      String doi = null;
      if (rf.exists()) {
        try {
          doi = parseDoi(Jsoup.parse(rf, StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
          LOG.warn("PFNR: failed to parse reference page {}: {}", refId, ex.getMessage());
        }
      }
      refWriter.set(ColdpTerm.ID, "ref:" + refId);
      refWriter.set(ColdpTerm.citation, citation);
      if (doi != null) refWriter.set(ColdpTerm.doi, doi);
      // prefer PFNR reference page; fall back to external URL from "link" anchor
      refWriter.set(ColdpTerm.link, doi == null && extUrl != null ? extUrl : BASE_URL + "/reference/" + refId + "/");
      refWriter.next();
    }
  }

  // ── ID enumeration via sitemap ──────────────────────────────────────────

  private List<Integer> collectIds() throws IOException {
    List<Integer> ids = new ArrayList<>();
    LOG.info("PFNR: fetching sitemap from {}", SITEMAP_URL);
    try (InputStream gz = new GZIPInputStream(http.getStream(SITEMAP_URL))) {
      String xml = new String(gz.readAllBytes(), StandardCharsets.UTF_8);
      Matcher m = ID_PATTERN.matcher(xml);
      while (m.find()) {
        int id = Integer.parseInt(m.group(1));
        if (!ids.contains(id)) ids.add(id);
      }
    } catch (Exception e) {
      LOG.warn("PFNR: sitemap fetch failed ({}); falling back to index crawl", e.getMessage());
      collectIdsFallback(ids);
    }
    return ids;
  }

  /** Fallback: crawl /name/?p=N index pages. */
  private void collectIdsFallback(List<Integer> ids) throws IOException {
    Pattern linkPat = Pattern.compile("/name/(\\d+)/");
    for (int page = 1; page <= 60; page++) {
      String html = http.get(BASE_URL + "/name/?p=" + page);
      if (html == null || html.isEmpty()) break;
      Matcher m = linkPat.matcher(html);
      boolean found = false;
      while (m.find()) {
        int id = Integer.parseInt(m.group(1));
        if (!ids.contains(id)) { ids.add(id); found = true; }
      }
      if (!found) break;
      try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
  }

  // ── Name page parsing ───────────────────────────────────────────────────

  private void parseName(Document doc, int id) throws IOException {
    // Scientific name: italic text inside <h1>
    Element h1 = doc.selectFirst("h1");
    if (h1 == null) {
      LOG.debug("PFNR: name page {} has no <h1>, skipping", id);
      return;
    }
    String sciName = h1.select("i, em").text();
    if (sciName.isBlank()) sciName = h1.text();

    // Authorship: Authors: label
    String authorship = afterLabel(doc, "Authors:");

    // Rank
    String rank = afterLabel(doc, "Rank:");
    if (rank != null) rank = rank.toLowerCase().trim();

    // PFN number and LSID
    String pfnNum = null;
    String lsid   = null;
    Element pfnStrong = findStrong(doc, "Plant Fossil Names Registry Number:");
    if (pfnStrong != null) {
      Element pfnLink = pfnStrong.parent().selectFirst("a[href*=/act/]");
      if (pfnLink != null) pfnNum = pfnLink.text().trim();
      // LSID is in adjacent text after the PFN link
      for (Node node : pfnStrong.parent().childNodes()) {
        if (node instanceof TextNode tn) {
          String t = tn.text().trim();
          if (t.startsWith("urn:lsid:")) { lsid = t; break; }
        }
      }
    }
    // Also try extracting LSID from any link containing urn:lsid
    if (lsid == null) {
      for (Element a : doc.select("a[href*=urn:lsid]")) {
        lsid = a.text().trim();
        if (!lsid.isEmpty()) break;
      }
    }

    // Reference: link to /reference/N/
    int refId = labelLinkId(doc, "Reference for this name:", "/reference/");
    String[] refParts = refCitationAndUrl(doc, "Reference for this name:");
    String refCitation = refParts[0];
    String refUrl     = refParts[1]; // external URL from "link" anchor, may be null

    // Basionym
    int basionymId = labelLinkId(doc, "Basionym:", "/name/");

    // Parent genus (for species)
    int parentId = labelLinkId(doc, "Genus:", "/name/");

    // Free-text paragraph between Rank and Reference → nameRemarks
    String nameRemarks = textBetweenLabels(doc, "Rank:", "Reference for this name:");

    // Stratigraphy → remarks
    String stratigraphy = sectionText(doc, "Stratigraphy");

    // Etymology
    String etymology = afterLabel(doc, "Etymology:");
    if (etymology == null) etymology = sectionText(doc, "Etymology");

    // Original diagnosis/description → TaxonProperty
    String diagnosis = afterLabel(doc, "Original diagnosis/description:");
    if (diagnosis == null) diagnosis = sectionText(doc, "Original diagnosis/description");

    // Published year: extract from citation text
    String year = null;
    if (refCitation != null) {
      Matcher ym = YEAR_PATTERN.matcher(refCitation);
      if (ym.find()) year = ym.group(1);
    }

    // ── Write NameUsage ──────────────────────────────────────────────────
    writer.set(ColdpTerm.ID, "pfn:" + id);
    if (parentId > 0) writer.set(ColdpTerm.parentID, "pfn:" + parentId);
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, sciName);
    writer.set(ColdpTerm.authorship, authorship);
    writer.set(ColdpTerm.status, "accepted");
    writer.set(ColdpTerm.extinct, "true");
    if (refId > 0) writer.set(ColdpTerm.nameReferenceID, "ref:" + refId);
    writer.set(ColdpTerm.publishedInYear, year);
    // alternativeID: lsid + pfnNum
    List<String> altIds = new ArrayList<>();
    if (lsid != null && !lsid.isBlank())   altIds.add("lsid:" + lsid);
    if (pfnNum != null && !pfnNum.isBlank()) altIds.add("pfn:" + pfnNum);
    if (!altIds.isEmpty()) writer.set(ColdpTerm.alternativeID, String.join(";", altIds));
    writer.set(ColdpTerm.link, BASE_URL + "/name/" + id + "/");
    writer.set(ColdpTerm.remarks, stratigraphy);
    writer.set(ColdpTerm.nameRemarks, nameRemarks);
    writer.set(ColdpTerm.etymology, etymology);
    writer.next();

    // ── Diagnosis TaxonProperty ──────────────────────────────────────────
    if (diagnosis != null) {
      propWriter.set(ColdpTerm.taxonID, "pfn:" + id);
      propWriter.set(ColdpTerm.property, "Diagnosis");
      propWriter.set(ColdpTerm.value, diagnosis);
      propWriter.next();
    }

    // ── Accumulate basionym relation ────────────────────────────────────
    if (basionymId > 0) {
      basionymRelations.add(new int[]{id, basionymId});
    }

    // ── Accumulate reference citation ────────────────────────────────────
    if (refId > 0 && !refCitations.containsKey(refId) && refCitation != null) {
      refCitations.put(refId, new String[]{refCitation.trim(), refUrl});
    }

    // ── Type material ────────────────────────────────────────────────────
    parseTypes(doc, id);
  }

  private void parseTypes(Document doc, int nameId) throws IOException {
    Element h2 = findH2(doc, "Types");
    if (h2 == null) return;

    for (Element sibling : h2.nextElementSiblings()) {
      if ("h2".equalsIgnoreCase(sibling.tagName()) || "h1".equalsIgnoreCase(sibling.tagName())) break;
      String text = sibling.text().trim();
      if (text.isEmpty()) continue;

      String status = resolveTypeStatus(text);
      if (status == null) continue; // skip lines that don't start with a type status keyword

      // Repository from linked /repository/N/ page
      Element repoLink = sibling.selectFirst("a[href*=/repository/]");
      String institution = repoLink != null ? repoLink.text().trim() : null;

      String catalogNum = extractCatalogNumber(text);

      writeTypeMaterial(nameId, status, catalogNum, institution, null, text);
    }
  }

  private void writeTypeMaterial(int nameId, String status, String catalogNumber,
                                  String institutionCode, String locality, String remarks) throws IOException {
    if (status == null) return;
    tmWriter.set(ColdpTerm.nameID, "pfn:" + nameId);
    tmWriter.set(ColdpTerm.status, status);
    tmWriter.set(ColdpTerm.catalogNumber, catalogNumber);
    tmWriter.set(ColdpTerm.institutionCode, institutionCode);
    tmWriter.set(ColdpTerm.locality, locality);
    tmWriter.set(ColdpTerm.remarks, remarks);
    tmWriter.next();
  }

  private static String resolveTypeStatus(String text) {
    if (text == null) return null;
    String lower = text.toLowerCase();
    if (lower.startsWith("holotype") || lower.startsWith("lectotype")) return "holotype";
    if (lower.startsWith("paratype") || lower.startsWith("syntype")) return "paratype";
    return null;
  }

  private static String extractCatalogNumber(String text) {
    if (text == null) return null;
    // Strip leading "Holotype ", "Lectotype ", "Paratype ", etc.
    String stripped = text.replaceFirst("(?i)^(holotype|lectotype|paratype|syntypes?)[:\\s]+", "").trim();
    // Take first whitespace-delimited token
    int space = stripped.indexOf(' ');
    String first = space > 0 ? stripped.substring(0, space) : stripped;
    // Remove trailing punctuation
    first = first.replaceAll("[,;.]+$", "");
    return first.isBlank() ? null : first;
  }

  // ── Reference page parsing ──────────────────────────────────────────────

  private static String parseDoi(Document doc) {
    // Look for an explicit DOI link or text
    for (Element a : doc.select("a[href*=doi.org]")) {
      String href = a.attr("href");
      Matcher m = DOI_PATTERN.matcher(href);
      if (m.find()) return m.group(1);
    }
    // Search full page text
    Matcher m = DOI_PATTERN.matcher(doc.body().text());
    return m.find() ? m.group(1) : null;
  }

  // ── HTML parsing helpers ─────────────────────────────────────────────────

  /**
   * Like {@link #afterLabel} but also strips any trailing anchor whose display text is "link"
   * and returns its href as the second element. Returns a two-element array:
   * [0] = citation text (cleaned), [1] = external URL or null.
   */
  private static String[] refCitationAndUrl(Document doc, String label) {
    Element strong = findStrong(doc, label);
    if (strong == null) return new String[]{null, null};
    Element container = strong.parent();
    // Find "link" anchor — an <a> whose trimmed text equals "link"
    String extUrl = null;
    for (Element a : container.select("a")) {
      if ("link".equalsIgnoreCase(a.text().trim())) {
        extUrl = a.attr("abs:href");
        if (extUrl.isBlank()) extUrl = a.attr("href");
        a.remove(); // remove from DOM so afterLabel won't see its text
        break;
      }
    }
    // Now collect text as usual
    StringBuilder sb = new StringBuilder();
    boolean after = false;
    for (Node node : container.childNodes()) {
      if (!after) {
        if (node == strong) after = true;
        continue;
      }
      if (node instanceof TextNode tn) {
        sb.append(tn.text());
      } else if (node instanceof Element el) {
        if ("strong".equalsIgnoreCase(el.tagName()) || "h2".equalsIgnoreCase(el.tagName())) break;
        sb.append(el.text());
      }
    }
    // Strip trailing comma/whitespace left by the removed "link" anchor
    String citation = sb.toString().replaceAll("[,\\s]+$", "").trim();
    return new String[]{citation.isEmpty() ? null : citation, extUrl.isBlank() ? null : extUrl};
  }

  /**
   * Returns the text content following the given &lt;strong&gt; label in its parent element,
   * with the label text itself removed.
   */
  private static String afterLabel(Document doc, String label) {
    Element strong = findStrong(doc, label);
    if (strong == null) return null;
    // Collect text of all sibling nodes after this strong in the same parent
    StringBuilder sb = new StringBuilder();
    boolean after = false;
    for (Node node : strong.parent().childNodes()) {
      if (!after) {
        if (node == strong) after = true;
        continue;
      }
      if (node instanceof TextNode tn) {
        sb.append(tn.text());
      } else if (node instanceof Element el) {
        // Stop at next strong that looks like a label
        if ("strong".equalsIgnoreCase(el.tagName()) || "h2".equalsIgnoreCase(el.tagName())) break;
        sb.append(el.text());
      }
    }
    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  /**
   * Extracts the numeric ID from a link href matching {@code pathPrefix} in the parent element
   * of the given label's &lt;strong&gt;.
   */
  private static int labelLinkId(Document doc, String label, String pathPrefix) {
    Element strong = findStrong(doc, label);
    if (strong == null) return -1;
    // Search in parent and its immediate next siblings
    Element container = strong.parent();
    Element a = container.selectFirst("a[href*=" + pathPrefix + "]");
    if (a == null) {
      // Try scanning siblings
      for (Element sib : container.nextElementSiblings()) {
        if (sib.select("strong").size() > 0) break; // next label
        a = sib.selectFirst("a[href*=" + pathPrefix + "]");
        if (a != null) break;
      }
    }
    if (a == null) return -1;
    Matcher m = Pattern.compile(Pattern.quote(pathPrefix) + "(\\d+)/").matcher(a.attr("href"));
    return m.find() ? Integer.parseInt(m.group(1)) : -1;
  }

  /** Returns all text within the section introduced by an &lt;h2&gt; with the given heading. */
  private static String sectionText(Document doc, String heading) {
    Element h2 = findH2(doc, heading);
    if (h2 == null) return null;
    StringBuilder sb = new StringBuilder();
    for (Element sib : h2.nextElementSiblings()) {
      if ("h2".equalsIgnoreCase(sib.tagName()) || "h1".equalsIgnoreCase(sib.tagName())) break;
      String t = sib.text().trim();
      if (!t.isEmpty()) { if (sb.length() > 0) sb.append(" "); sb.append(t); }
    }
    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  /**
   * Returns the concatenated text of all sibling elements between the container of
   * {@code afterLabel}'s &lt;strong&gt; and the container of {@code beforeLabel}'s &lt;strong&gt;,
   * skipping any sibling that itself contains a colon-terminated &lt;strong&gt; label.
   * Returns null if the two containers are not siblings or no unlabelled content exists.
   */
  private static String textBetweenLabels(Document doc, String afterLabel, String beforeLabel) {
    Element afterStrong  = findStrong(doc, afterLabel);
    Element beforeStrong = findStrong(doc, beforeLabel);
    if (afterStrong == null || beforeStrong == null) return null;

    Element afterEl  = afterStrong.parent();
    Element beforeEl = beforeStrong.parent();
    if (afterEl == null || beforeEl == null || afterEl == beforeEl) return null;
    if (afterEl.parent() == null || afterEl.parent() != beforeEl.parent()) return null;

    StringBuilder sb = new StringBuilder();
    boolean between = false;
    for (Element sib : afterEl.parent().children()) {
      if (sib == afterEl)  { between = true; continue; }
      if (sib == beforeEl) break;
      if (!between) continue;
      // Skip elements that contain a labelled <strong> (e.g. Genus:, Basionym:)
      boolean hasLabel = sib.select("strong").stream()
          .anyMatch(s -> s.text().trim().endsWith(":"));
      if (hasLabel) continue;
      String t = sib.text().trim();
      if (!t.isEmpty()) { if (sb.length() > 0) sb.append(" "); sb.append(t); }
    }
    return sb.isEmpty() ? null : sb.toString();
  }

  private static Element findStrong(Document doc, String labelPrefix) {
    for (Element s : doc.select("strong")) {
      if (s.text().startsWith(labelPrefix)) return s;
    }
    return null;
  }

  private static Element findH2(Document doc, String text) {
    for (Element h2 : doc.select("h2")) {
      if (h2.text().trim().equalsIgnoreCase(text)) return h2;
    }
    return null;
  }
}
