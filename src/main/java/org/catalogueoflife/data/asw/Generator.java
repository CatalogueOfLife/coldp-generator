package org.catalogueoflife.data.asw;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.FileUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.JsoupUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Generator for Amphibian Species of the World (ASW).
 * Scrapes https://amphibiansoftheworld.amnh.org — the authoritative online reference
 * for ~9,000 amphibian species (Anura, Caudata, Gymnophiona).
 *
 * Recursive crawl starting at /Amphibia, following "Contained taxa" links depth-first.
 * For each taxon page, parses:
 *  - Accepted NameUsage from h1 (name + authorship) and CSS rank class
 *  - Synonyms from div.synonymy (bold names with references)
 *  - Type genus/species → deferred NameRelation records
 *  - Common names → VernacularName records
 *  - Geographic Occurrence → Distribution records (one per country)
 *  - Comment → NameUsage.remarks
 *
 * Bibliography pages are fetched for each unique reference to obtain full citations.
 *
 * Pages are cached as taxon-{path}.html / bib-{key}.html in the source directory;
 * 200 ms delay between new downloads to respect the server.
 *
 * ID scheme: URL path without leading "/" (e.g. "Amphibia/Anura/Arthroleptidae") for taxa,
 *            "ref:{bibliography-path}" for references,
 *            "syn:{n}" for synonym NameUsages.
 */
public class Generator extends AbstractColdpGenerator {

  private static final String BASE_URL   = "https://amphibiansoftheworld.amnh.org";
  private static final String START_PATH = "/Amphibia";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
  private static final int CRAWL_DELAY_MS = 200;

  private static final Pattern YEAR_PATTERN = Pattern.compile("\\b(\\d{4})\\b");
  private static final Pattern RANK_PATTERN = Pattern.compile("rank-(\\w+)");
  private static final Pattern PAGE_PATTERN = Pattern.compile("^[\\s:,]*(\\d+(?:[-\u2013]\\d+)?)");

  // Country names that contain commas — must be recognised before splitting the country list
  private static final List<String> COMMA_COUNTRIES = List.of(
      "Congo, Democratic Republic of the",
      "Congo, Republic of the",
      "Korea, Democratic People's Republic of",
      "Korea, Republic of",
      "Iran, Islamic Republic of",
      "Taiwan, Province of China",
      "Bolivia, Plurinational State of",
      "Venezuela, Bolivarian Republic of",
      "Micronesia, Federated States of",
      "Tanzania, United Republic of",
      "Moldova, Republic of",
      "Macedonia, the former Yugoslav Republic of",
      "Virgin Islands, British",
      "Virgin Islands, U.S."
  );

  // ── Inner data types ──────────────────────────────────────────────────────

  /** Holds data for a bibliography reference, populated during crawl and bib-page fetch. */
  static class RefData {
    final String abbreviation;        // inline link text ("Günther, 1858, Proc. Zool. Soc. London, 1858")
    final String containerTitleShort; // abbreviated journal, stripped of author+year
    // Populated from the bibliography page:
    String citation;    // full citation text (falls back to abbreviation)
    String author;
    String issued;

    RefData(String abbreviation, String containerTitleShort) {
      this.abbreviation = abbreviation;
      this.containerTitleShort = containerTitleShort;
      this.citation = abbreviation; // fallback until bib page is fetched
    }
  }

  /** Parsed data from a single synonymy paragraph. */
  private record SynEntry(
      String name,
      String authorship,
      String nameRefPath,           // first bib link path → nameReferenceID
      String namePublishedInPage,   // page number for the first ref
      List<String> additionalRefPaths, // second+ bib link paths → referenceID
      String remarks
  ) {}

  /** Result of parseSynonymy: the ref + page for the accepted taxon's name publication. */
  private record SynonymyResult(String nameRefId, String namePublishedInPage) {}

  // ── State ─────────────────────────────────────────────────────────────────

  // bibliography-path → RefData (populated during crawl, enriched from bib pages)
  private final Map<String, RefData> refs = new LinkedHashMap<>();
  // Deferred NameRelation: [synonymNameId, typeGenusOrSpeciesName, relType]
  private final List<String[]> typeRelations = new ArrayList<>();
  // scientificName → taxonID (registered while crawling, used to resolve type relations)
  private final Map<String, String> nameToId = new HashMap<>();
  // Buffered synonym NameUsage rows: written after type resolution so unresolved type
  // names can be appended to remarks instead of being silently dropped.
  private record PendingSyn(String id, String parentId, String name, String authorship,
      String nameRefId, String publishedInPage, String referenceIDs, String remarks) {}
  private final List<PendingSyn> pendingSyns = new ArrayList<>();
  // synId → human-readable type text (e.g. "Type genus: Foo") for unresolved type names
  private final Map<String, String> unresolvedTypeText = new HashMap<>();

  private int synCounter = 0;

  private TermWriter relWriter;
  private TermWriter vernacularWriter;
  private TermWriter distributionWriter;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.status,
        ColdpTerm.nameReferenceID,
        ColdpTerm.publishedInPage,
        ColdpTerm.referenceID,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));

    relWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));

    vernacularWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.name,
        ColdpTerm.language
    ));

    distributionWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.area,
        ColdpTerm.gazetteer,
        ColdpTerm.status
    ));

    initRefWriter(List.of(
        ColdpTerm.ID,
        ColdpTerm.citation,
        ColdpTerm.author,
        ColdpTerm.issued,
        ColdpTerm.containerTitleShort,
        ColdpTerm.link
    ));

    // Recursive crawl from the class root
    crawl(START_PATH, null);

    // Resolve and write deferred type-genus / type-species NameRelation records
    for (String[] rel : typeRelations) {
      String typeId = nameToId.get(rel[1]);
      if (typeId != null) {
        relWriter.set(ColdpTerm.nameID, rel[0]);
        relWriter.set(ColdpTerm.relatedNameID, typeId);
        relWriter.set(ColdpTerm.type, rel[2]);
        relWriter.next();
      } else {
        LOG.debug("ASW: could not resolve type '{}' for name '{}'", rel[1], rel[0]);
        // Capitalise first letter: "type genus" → "Type genus"
        String typeText = Character.toUpperCase(rel[2].charAt(0)) + rel[2].substring(1) + ": " + rel[1];
        unresolvedTypeText.put(rel[0], typeText);
      }
    }

    // Write buffered synonym NameUsages, appending unresolved type names to remarks
    for (PendingSyn syn : pendingSyns) {
      writer.set(ColdpTerm.ID, syn.id());
      writer.set(ColdpTerm.parentID, syn.parentId());
      writer.set(ColdpTerm.scientificName, syn.name());
      if (syn.authorship() != null) writer.set(ColdpTerm.authorship, syn.authorship());
      writer.set(ColdpTerm.status, "synonym");
      if (syn.nameRefId() != null) {
        writer.set(ColdpTerm.nameReferenceID, syn.nameRefId());
        if (syn.publishedInPage() != null) writer.set(ColdpTerm.publishedInPage, syn.publishedInPage());
      }
      if (syn.referenceIDs() != null) writer.set(ColdpTerm.referenceID, syn.referenceIDs());
      String remarks = syn.remarks();
      String typeText = unresolvedTypeText.get(syn.id());
      if (typeText != null) remarks = remarks != null ? remarks + ". " + typeText : typeText;
      if (remarks != null) writer.set(ColdpTerm.remarks, remarks);
      writer.next();
    }

    // Fetch bibliography pages to get full citations, then write Reference records
    fetchBibPages();
    for (Map.Entry<String, RefData> e : refs.entrySet()) {
      RefData r = e.getValue();
      refWriter.set(ColdpTerm.ID, "ref:" + e.getKey());
      refWriter.set(ColdpTerm.citation, r.citation);
      if (r.author != null) refWriter.set(ColdpTerm.author, r.author);
      if (r.issued != null) refWriter.set(ColdpTerm.issued, r.issued);
      if (r.containerTitleShort != null) refWriter.set(ColdpTerm.containerTitleShort, r.containerTitleShort);
      refWriter.set(ColdpTerm.link, BASE_URL + "/Bibliography/" + e.getKey());
      refWriter.next();
    }

  }

  // ── Crawling ──────────────────────────────────────────────────────────────

  private void crawl(String path, String parentId) throws IOException {
    String fileKey = path.replaceFirst("^/", "").replace("/", "_");
    File f = sourceFile("taxon-" + fileKey + ".html");

    if (!f.exists()) {
      if (cfg.noDownload) {
        LOG.warn("ASW: --no-download set but {} not cached; skipping", f.getName());
        return;
      }
      LOG.debug("ASW: downloading {}", path);
      try {
        Document doc = Jsoup.connect(BASE_URL + path)
            .userAgent(USER_AGENT)
            .timeout(20_000)
            .get();
        FileUtils.write(f, doc.outerHtml(), StandardCharsets.UTF_8);
      } catch (Exception e) {
        LOG.warn("ASW: failed to download {}: {}", path, e.getMessage());
        return;
      }
      crawlDelay(CRAWL_DELAY_MS);
    }

    try {
      Document doc = Jsoup.parse(f, StandardCharsets.UTF_8.name());
      parseTaxon(doc, path, parentId);
    } catch (Exception e) {
      LOG.warn("ASW: failed to parse {}: {}", path, e.getMessage());
    }
  }

  // ── Page parsing ──────────────────────────────────────────────────────────

  private void parseTaxon(Document doc, String path, String parentId) throws IOException {
    Element content = doc.selectFirst("#aswContent");
    if (content == null) {
      LOG.warn("ASW: no #aswContent at {}", path);
      return;
    }

    String taxonId = path.replaceFirst("^/", "");

    // Rank from CSS class attribute on the content div
    String rank = extractRank(content.attr("class"));

    // Name and authorship from <h1>
    Element h1 = content.selectFirst("h1");
    if (h1 == null) return;
    String[] nameAuth = parseH1(h1);
    String scientificName = nameAuth[0];
    String authorship     = nameAuth[1];

    // Synonymy: collect nameReferenceID, publishedInPage and build synonym NameUsages
    SynonymyResult synResult = parseSynonymy(content, taxonId, scientificName);

    // Write accepted NameUsage
    writer.set(ColdpTerm.ID, taxonId);
    if (parentId != null) writer.set(ColdpTerm.parentID, parentId);
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, scientificName);
    if (authorship != null && !authorship.isBlank()) writer.set(ColdpTerm.authorship, authorship);
    writer.set(ColdpTerm.status, "accepted");
    if (synResult.nameRefId() != null) writer.set(ColdpTerm.nameReferenceID, synResult.nameRefId());
    if (synResult.namePublishedInPage() != null) writer.set(ColdpTerm.publishedInPage, synResult.namePublishedInPage());
    writer.set(ColdpTerm.link, BASE_URL + path);
    String remarks = JsoupUtils.sectionText(content, "Comment");
    if (remarks != null) writer.set(ColdpTerm.remarks, remarks);
    writer.next();

    nameToId.put(scientificName, taxonId);

    parseCommonNames(content, taxonId);
    parseGeographicOccurrence(content, taxonId);

    // Recurse into contained taxa
    for (Element taxaDiv : content.select("div.taxa")) {
      Element link = taxaDiv.selectFirst("a[href]");
      if (link != null) {
        String childPath = link.attr("href");
        if (childPath.startsWith("/Amphibia")) {
          crawl(childPath, taxonId);
        }
      }
    }
  }

  // ── Synonymy ──────────────────────────────────────────────────────────────

  /**
   * Parses div.synonymy, writes synonym NameUsage records, registers references,
   * and defers type-genus/species relations.
   *
   * @return nameRefId and publishedInPage for the accepted taxon (from first entry encountered)
   */
  private SynonymyResult parseSynonymy(Element content, String taxonId, String acceptedName)
      throws IOException {
    Element synonymyDiv = content.selectFirst("div.synonymy");
    if (synonymyDiv == null) return new SynonymyResult(null, null);

    String nameRefId = null;
    String namePublishedInPage = null;
    Set<String> seenSynonymNames = new LinkedHashSet<>();

    for (Element p : synonymyDiv.select("p")) {
      SynEntry entry = parseSynEntry(p);
      if (entry == null) continue;

      // Use first ref as nameReferenceID + publishedInPage for the accepted taxon
      if (nameRefId == null && entry.nameRefPath() != null) {
        nameRefId = entry.nameRefPath();
        namePublishedInPage = entry.namePublishedInPage();
      }

      // Skip entries that repeat the accepted name (subsequent usages, not new names)
      if (entry.name().equalsIgnoreCase(acceptedName)) continue;

      // Create one synonym NameUsage per distinct bold name
      if (!seenSynonymNames.contains(entry.name().toLowerCase())) {
        seenSynonymNames.add(entry.name().toLowerCase());
        String synId = "syn:" + (++synCounter);

        String synNameRefId = entry.nameRefPath() != null ? "ref:" + entry.nameRefPath() : null;
        String synPage = synNameRefId != null ? entry.namePublishedInPage() : null;
        String refIds = entry.additionalRefPaths().isEmpty() ? null :
            entry.additionalRefPaths().stream().map(r -> "ref:" + r).collect(Collectors.joining(","));
        pendingSyns.add(new PendingSyn(synId, taxonId, entry.name(), entry.authorship(),
            synNameRefId, synPage, refIds, entry.remarks()));

        nameToId.put(entry.name(), synId);
        parseTypeRelation(p, synId);
      }
    }

    return new SynonymyResult(
        nameRefId != null ? "ref:" + nameRefId : null,
        namePublishedInPage
    );
  }

  /**
   * Parses one synonymy &lt;p&gt; element into a SynEntry.
   *
   * Expected structure (node sequence within &lt;p&gt;):
   *   &lt;b&gt;Name&lt;/b&gt; [text before first ref: " Author, Year, "]
   *   &lt;a href="/Bibliography/..."&gt;Author, Year, Journal&lt;/a&gt;
   *   [text after ref: ": Page. [; Author, Year,] Remarks."]
   *   [&lt;a&gt;...&lt;/a&gt; additional refs]
   *
   * The bib link text includes the author+year+journal abbreviation; authorship is
   * extracted from it via extractAuthorship().
   */
  private SynEntry parseSynEntry(Element p) {
    Element bold = p.selectFirst("b");
    if (bold == null) return null;
    String name = bold.text().trim();

    // Collect child nodes after <b>, splitting at each bib link
    List<Element> bibLinks = new ArrayList<>();
    List<String> preTexts = new ArrayList<>(); // text segment before bibLinks[i]
    StringBuilder buf = new StringBuilder();
    boolean afterBold = false;

    for (Node node : p.childNodes()) {
      if (!afterBold) {
        if (node == bold) afterBold = true;
        continue;
      }
      if (node instanceof TextNode tn) {
        buf.append(tn.text());
      } else if (node instanceof Element el) {
        if (el.tagName().equals("a") && el.attr("href").startsWith("/Bibliography/")) {
          bibLinks.add(el);
          preTexts.add(buf.toString());
          buf = new StringBuilder();
        }
        // ignore other inline elements (em, i, etc.) — their text is in TextNodes
      }
    }
    String finalText = buf.toString();

    if (bibLinks.isEmpty()) {
      return new SynEntry(name, null, null, null, List.of(), cleanRemarks(null, finalText));
    }

    // Register first ref
    Element firstLink = bibLinks.get(0);
    String firstPath = bibLinkPath(firstLink);
    registerRef(firstPath, firstLink.text());
    String authorship = extractAuthorship(firstLink.text());

    // Page for first ref: from next text segment (before second ref) or from finalText
    String pageText = bibLinks.size() > 1 ? preTexts.get(1) : finalText;
    String namePublishedInPage = extractPageFromText(pageText);

    // Additional refs
    List<String> additionalPaths = new ArrayList<>();
    for (int i = 1; i < bibLinks.size(); i++) {
      String rpath = bibLinkPath(bibLinks.get(i));
      registerRef(rpath, bibLinks.get(i).text());
      additionalPaths.add(rpath);
    }

    // Remarks come from the final text segment (after last ref link), stripped of page prefix
    String remarks = cleanRemarks(namePublishedInPage, finalText);

    return new SynEntry(name, authorship, firstPath, namePublishedInPage, additionalPaths, remarks);
  }

  private static String bibLinkPath(Element a) {
    return a.attr("href").replaceFirst("^/Bibliography/", "");
  }

  /** Register a bibliography reference if not already seen. */
  private void registerRef(String path, String linkText) {
    refs.computeIfAbsent(path, k -> new RefData(linkText, extractContainerTitleShort(linkText)));
  }

  /**
   * Strip the leading page number prefix from text after a ref link, then return remaining
   * text as remarks (excluding type-relation text).
   *
   * @param knownPage if already extracted, skip stripping (may be null)
   * @param text the raw text segment after the last bib link
   */
  static String cleanRemarks(String knownPage, String text) {
    if (text == null || text.isBlank()) return null;
    String s = text.trim();
    // Strip leading ": pageNum." or ": pageNum;" prefix
    s = s.replaceFirst("^[\\s:,]*\\d+(?:[-\u2013]\\d+)?[.,;]?\\s*", "");
    // Strip type-relation text
    int typeIdx = indexOfTypeMarker(s);
    if (typeIdx >= 0) s = s.substring(0, typeIdx);
    s = s.replaceAll("[.;,\\s]+$", "").trim();
    return s.isEmpty() ? null : s;
  }

  /**
   * Extracts abbreviated journal/book name from a bib link text.
   * E.g. "Günther, 1858, Proc. Zool. Soc. London, 1858" → "Proc. Zool. Soc. London"
   */
  static String extractContainerTitleShort(String linkText) {
    if (linkText == null || linkText.isBlank()) return null;
    // Find first year (+ optional quoted year), everything after is the container
    Matcher m = YEAR_PATTERN.matcher(linkText);
    if (!m.find()) return linkText.trim();
    int yearEnd = m.end();
    // Check for quoted year immediately after: 1859 "1858"
    Matcher quoted = Pattern.compile("\\s+\"(\\d{4})\"").matcher(linkText.substring(yearEnd));
    if (quoted.lookingAt()) yearEnd += quoted.end();
    // Skip leading ", "
    String rest = linkText.substring(yearEnd).replaceFirst("^[,\\s]+", "");
    if (rest.isBlank()) return null;
    // Strip trailing ", YYYY"
    rest = rest.replaceAll(",?\\s*\\d{4}\\s*$", "").trim();
    return rest.isEmpty() ? null : rest;
  }

  /**
   * Extracts a page number from the text immediately after a bib link.
   * E.g. ": 347. remarks" or ": 341; next author" → "347" or "341"
   */
  static String extractPageFromText(String text) {
    if (text == null || text.isBlank()) return null;
    Matcher m = PAGE_PATTERN.matcher(text.trim());
    return m.find() ? m.group(1) : null;
  }

  private static int indexOfTypeMarker(String text) {
    int i = text.indexOf("Type genus:");
    if (i >= 0) return i;
    return text.indexOf("Type species:");
  }

  /** Extracts "Author, Year" (or "Author, Year \"Year\"") from a bib link text. */
  static String extractAuthorship(String refText) {
    if (refText == null || refText.isBlank()) return null;
    Matcher m = YEAR_PATTERN.matcher(refText);
    if (m.find()) {
      int end = m.end();
      String candidate = refText.substring(0, end).trim();
      // Check for directly following quoted year: ... 1911 "1910"
      Matcher quoted = Pattern.compile("\\s+\"(\\d{4})\"").matcher(refText.substring(end));
      if (quoted.lookingAt()) {
        candidate = candidate + " \"" + quoted.group(1) + "\"";
      }
      return candidate;
    }
    return null;
  }

  /** Detects "Type genus: X" or "Type species: X" in a synonymy paragraph and adds a deferred relation. */
  private void parseTypeRelation(Element p, String synonymId) {
    String text = p.text();
    Matcher m = Pattern.compile("Type (genus|species):\\s*([A-Z][a-z]+(?:\\s+[a-z]+)?)").matcher(text);
    if (m.find()) {
      String relType  = "type " + m.group(1);  // "type genus" or "type species"
      String typeName = m.group(2).trim();
      typeRelations.add(new String[]{synonymId, typeName, relType});
    }
  }

  // ── Bibliography page fetching ────────────────────────────────────────────

  /**
   * After the full crawl, download each unique bibliography page and parse
   * the full citation text + structured fields (author, issued).
   */
  private void fetchBibPages() throws IOException {
    LOG.info("ASW: fetching {} bibliography pages", refs.size());
    for (Map.Entry<String, RefData> entry : refs.entrySet()) {
      String path = entry.getKey();
      String fileKey = path.replace("/", "_");
      File f = sourceFile("bib-" + fileKey + ".html");

      if (!f.exists()) {
        if (cfg.noDownload) {
          LOG.debug("ASW: --no-download set, bib page {} not cached", fileKey);
          continue;
        }
        try {
          Document doc = Jsoup.connect(BASE_URL + "/Bibliography/" + path)
              .userAgent(USER_AGENT)
              .timeout(20_000)
              .get();
          FileUtils.write(f, doc.outerHtml(), StandardCharsets.UTF_8);
        } catch (Exception e) {
          LOG.warn("ASW: failed to download bib page {}: {}", path, e.getMessage());
          continue;
        }
        crawlDelay(CRAWL_DELAY_MS);
      }

      try {
        Document doc = Jsoup.parse(f, StandardCharsets.UTF_8.name());
        parseBibPage(doc, path, entry.getValue());
      } catch (Exception e) {
        LOG.warn("ASW: failed to parse bib page {}: {}", path, e.getMessage());
      }
    }
  }

  /**
   * Parses the full citation from a bibliography page and updates the RefData.
   * The citation is expected to be the main text content of #aswContent.
   */
  private void parseBibPage(Document doc, String path, RefData ref) {
    Element content = doc.selectFirst("#aswContent");
    if (content == null) {
      LOG.debug("ASW: no #aswContent on bib page {}", path);
      return;
    }

    // Extract the year from the path to help identify the citation element
    Matcher yearM = YEAR_PATTERN.matcher(path);
    String year = yearM.find() ? yearM.group(1) : null;

    // Find the citation: first substantial paragraph (or other element) containing the year
    String citation = null;
    for (Element el : content.select("p, li, h2")) {
      String text = el.text().trim();
      if (text.length() > 40 && (year == null || text.contains(year))) {
        citation = text;
        break;
      }
    }
    // Fallback: take the longest text element overall
    if (citation == null) {
      for (Element el : content.select("p, li, h2, div")) {
        String text = el.text().trim();
        if (citation == null || text.length() > citation.length()) {
          if (text.length() > 20) citation = text;
        }
      }
    }

    if (citation == null || citation.isBlank()) return;

    ref.citation = citation;

    // Parse author (before first year) and issued (year, possibly with quoted year)
    Matcher m = YEAR_PATTERN.matcher(citation);
    if (m.find()) {
      String authorPart = citation.substring(0, m.start()).replaceAll("[,\\s]+$", "").trim();
      if (!authorPart.isEmpty()) ref.author = authorPart;
      String issued = m.group(1);
      Matcher q = Pattern.compile("\\s+\"(\\d{4})\"").matcher(citation.substring(m.end()));
      if (q.lookingAt()) issued = issued + " \"" + q.group(1) + "\"";
      ref.issued = issued;
    }
  }

  // ── Common Names ──────────────────────────────────────────────────────────

  private void parseCommonNames(Element content, String taxonId) throws IOException {
    Element h2 = JsoupUtils.findH2(content, "Common Names");
    if (h2 == null) return;

    for (Element sib : h2.nextElementSiblings()) {
      if (JsoupUtils.isHeading(sib)) break;
      if (!sib.tagName().equals("p")) continue;

      String text = sib.text().trim();
      // Common name = text before the opening parenthesis (source citation)
      int paren = text.indexOf('(');
      String name = (paren > 0 ? text.substring(0, paren) : text).trim();
      // Strip trailing comma or period
      name = name.replaceAll("[,.]$", "").trim();
      if (name.isEmpty()) continue;

      vernacularWriter.set(ColdpTerm.taxonID, taxonId);
      vernacularWriter.set(ColdpTerm.name, name);
      vernacularWriter.set(ColdpTerm.language, "eng");
      vernacularWriter.next();
    }
  }

  // ── Distribution ─────────────────────────────────────────────────────────

  private void parseGeographicOccurrence(Element content, String taxonId) throws IOException {
    Element h2 = JsoupUtils.findH2(content, "Geographic Occurrence");
    if (h2 == null) return;

    for (Element sib : h2.nextElementSiblings()) {
      if (JsoupUtils.isHeading(sib)) break;
      if (!sib.tagName().equals("p")) continue;

      Element bold = sib.selectFirst("b");
      String statusLabel = bold != null ? bold.text().trim() : "";
      String aswStatus   = parseOccurrenceStatus(statusLabel);

      // Get country list text (after the bold label and colon)
      String fullText = sib.text().trim();
      String countries = fullText;
      if (bold != null) {
        int colonIdx = fullText.indexOf(':');
        countries = colonIdx >= 0 ? fullText.substring(colonIdx + 1).trim() : fullText;
      }
      if (countries.isEmpty()) continue;

      for (String country : splitCountries(countries)) {
        distributionWriter.set(ColdpTerm.taxonID, taxonId);
        distributionWriter.set(ColdpTerm.area, country);
        distributionWriter.set(ColdpTerm.gazetteer, "text");
        if (aswStatus != null) distributionWriter.set(ColdpTerm.status, aswStatus);
        distributionWriter.next();
      }
    }
  }

  /** Splits a comma-separated country list, keeping known multi-word names intact. */
  static List<String> splitCountries(String text) {
    // Replace commas inside known country names with a placeholder
    String processed = text;
    for (String cc : COMMA_COUNTRIES) {
      processed = processed.replace(cc, cc.replace(", ", "\u0000"));
    }
    List<String> result = new ArrayList<>();
    for (String part : processed.split(",")) {
      String country = part.replace("\u0000", ", ").trim();
      if (!country.isEmpty()) result.add(country);
    }
    return result;
  }

  private static String parseOccurrenceStatus(String label) {
    if (label.contains("Natural Resident"))   return "native";
    if (label.contains("Introduced"))         return "introduced";
    return null; // "Likely/Controversially Present" etc. have no clear ColDP equivalent
  }

  // ── HTML helpers ──────────────────────────────────────────────────────────

  private static String extractRank(String cssClass) {
    Matcher m = RANK_PATTERN.matcher(cssClass);
    return m.find() ? m.group(1).toLowerCase() : null;
  }

  /**
   * Parses name and authorship from an &lt;h1&gt; element.
   * <ul>
   *   <li>If the h1 contains an &lt;i&gt; tag: italic text = scientificName, trailing text = authorship.</li>
   *   <li>Otherwise: first whitespace-delimited token = scientificName, rest = authorship.</li>
   * </ul>
   * Returns [scientificName, authorship] (authorship may be null).
   */
  static String[] parseH1(Element h1) {
    Element italic = h1.selectFirst("i, em");
    if (italic != null) {
      String name  = italic.text().trim();
      String after = h1.text().trim().substring(name.length()).trim();
      return new String[]{name, after.isEmpty() ? null : after};
    }
    String text = h1.text().trim();
    // Split at the first uppercase letter that begins a recognisable "Author, Year" block
    int space = text.indexOf(' ');
    if (space > 0) {
      return new String[]{text.substring(0, space), text.substring(space + 1).trim()};
    }
    return new String[]{text, null};
  }
}
