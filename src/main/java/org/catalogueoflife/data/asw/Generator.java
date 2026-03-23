package org.catalogueoflife.data.asw;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.FileUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Pages are cached as taxon-{path}.html in the source directory; 200 ms delay between
 * new downloads to respect the server.
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

  // reference bibliography-path → citation text
  private final Map<String, String> references = new LinkedHashMap<>();
  // Deferred NameRelation: [synonymNameId, typeGenusOrSpeciesName, relType]
  private final List<String[]> typeRelations = new ArrayList<>();
  // scientificName → taxonID (registered while crawling, used to resolve type relations)
  private final Map<String, String> nameToId = new HashMap<>();

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
      }
    }

    // Write accumulated references
    for (Map.Entry<String, String> e : references.entrySet()) {
      refWriter.set(ColdpTerm.ID, "ref:" + e.getKey());
      refWriter.set(ColdpTerm.citation, e.getValue());
      refWriter.set(ColdpTerm.link, BASE_URL + "/Bibliography/" + e.getKey());
      refWriter.next();
    }

    metadata.put("issued",  LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
  }

  // ── Crawling ──────────────────────────────────────────────────────────────

  private void crawl(String path, String parentId) throws IOException, InterruptedException {
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
      Thread.sleep(CRAWL_DELAY_MS);
    }

    try {
      Document doc = Jsoup.parse(f, StandardCharsets.UTF_8.name());
      parseTaxon(doc, path, parentId);
    } catch (Exception e) {
      LOG.warn("ASW: failed to parse {}: {}", path, e.getMessage());
    }
  }

  // ── Page parsing ──────────────────────────────────────────────────────────

  private void parseTaxon(Document doc, String path, String parentId) throws IOException, InterruptedException {
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

    // Synonymy: collect nameReferenceID and build synonym NameUsages
    String nameRefId = parseSynonymy(content, taxonId, scientificName);

    // Write accepted NameUsage
    writer.set(ColdpTerm.ID, taxonId);
    if (parentId != null) writer.set(ColdpTerm.parentID, parentId);
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, scientificName);
    if (authorship != null && !authorship.isBlank()) writer.set(ColdpTerm.authorship, authorship);
    writer.set(ColdpTerm.status, "accepted");
    if (nameRefId != null) writer.set(ColdpTerm.nameReferenceID, nameRefId);
    writer.set(ColdpTerm.link, BASE_URL + path);
    String remarks = sectionText(content, "Comment");
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
   * Parses div.synonymy, writes synonym NameUsage records, accumulates references
   * and deferred type-genus/species relations.
   *
   * @return the reference ID to use as nameReferenceID for the accepted taxon
   */
  private String parseSynonymy(Element content, String taxonId, String acceptedName)
      throws IOException {
    Element synonymyDiv = content.selectFirst("div.synonymy");
    if (synonymyDiv == null) return null;

    String nameRefId = null;
    Set<String> seenSynonymNames = new LinkedHashSet<>();

    for (Element p : synonymyDiv.select("p")) {
      Element bold = p.selectFirst("b");
      if (bold == null) continue;

      String boldName = bold.text().trim();

      // Collect first bibliography reference link
      Element refLink = firstBibLink(p);
      String refId = null;
      if (refLink != null) {
        refId = refLink.attr("href").replaceFirst("^/Bibliography/", "");
        if (!references.containsKey(refId)) {
          references.put(refId, buildCitation(p, refLink));
        }
      }

      // Use the first reference we encounter as nameReferenceID for the accepted name
      if (nameRefId == null && refId != null) {
        nameRefId = refId;
      }

      // Skip entries that repeat the accepted name (subsequent usages, not new names)
      if (boldName.equalsIgnoreCase(acceptedName)) continue;

      // Create one synonym NameUsage per distinct bold name
      if (!seenSynonymNames.contains(boldName.toLowerCase())) {
        seenSynonymNames.add(boldName.toLowerCase());
        String synId = "syn:" + (++synCounter);

        writer.set(ColdpTerm.ID, synId);
        writer.set(ColdpTerm.parentID, taxonId);
        writer.set(ColdpTerm.scientificName, boldName);
        // Extract authorship from the first reference link text (Author, Year)
        if (refLink != null) {
          String auth = extractAuthorship(refLink.text());
          if (auth != null) writer.set(ColdpTerm.authorship, auth);
        }
        writer.set(ColdpTerm.status, "synonym");
        if (refId != null) writer.set(ColdpTerm.nameReferenceID, "ref:" + refId);
        writer.set(ColdpTerm.link, BASE_URL + "/Amphibia"); // no dedicated page for synonyms
        writer.next();

        nameToId.put(boldName, synId);

        // Type genus / type species → deferred NameRelation
        parseTypeRelation(p, synId);
      }

      if (refId != null && nameRefId == null) nameRefId = refId;
    }

    return nameRefId != null ? "ref:" + nameRefId : null;
  }

  /** Returns the first &lt;a&gt; whose href starts with /Bibliography/. */
  private static Element firstBibLink(Element p) {
    return p.selectFirst("a[href^=/Bibliography/]");
  }

  /**
   * Builds a citation string from a reference link and its following page-number text.
   * Stops accumulating text at "Type genus:" / "Type species:" to exclude type info.
   */
  private static String buildCitation(Element p, Element refLink) {
    StringBuilder sb = new StringBuilder(refLink.text());
    boolean after = false;
    for (Node node : p.childNodes()) {
      if (!after) {
        if (node == refLink) after = true;
        continue;
      }
      if (node instanceof TextNode tn) {
        String t = tn.text();
        // Stop before "Type genus:" / "Type species:" text
        int typeIdx = indexOfTypeMarker(t);
        if (typeIdx >= 0) {
          sb.append(t, 0, typeIdx);
          break;
        }
        sb.append(t);
      } else if (node instanceof Element el) {
        // Stop at next bibliography link or block-level break
        if (el.tagName().equals("a") && el.attr("href").startsWith("/Bibliography/")) break;
        if (el.tagName().equals("b")) break;
      }
    }
    return sb.toString().replaceAll("[;,\\s]+$", "").trim();
  }

  private static int indexOfTypeMarker(String text) {
    int i = text.indexOf("Type genus:");
    if (i >= 0) return i;
    return text.indexOf("Type species:");
  }

  /** Extracts "Author, Year" from a reference link text like "Mivart, 1869, Proc. Zool...". */
  static String extractAuthorship(String refText) {
    if (refText == null || refText.isBlank()) return null;
    Matcher m = YEAR_PATTERN.matcher(refText);
    if (m.find()) {
      // Include optional quoted year immediately following (e.g. "1911 \"1910\"")
      int end = m.end();
      String candidate = refText.substring(0, end).trim();
      // Check for a directly following quoted year: ... 1911 "1910"
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
      // Use the first word only for genus-group names (strip infraspecific epithet if any)
      typeRelations.add(new String[]{synonymId, typeName, relType});
    }
  }

  // ── Common Names ──────────────────────────────────────────────────────────

  private void parseCommonNames(Element content, String taxonId) throws IOException {
    Element h2 = findH2(content, "Common Names");
    if (h2 == null) return;

    for (Element sib : h2.nextElementSiblings()) {
      if (isHeading(sib)) break;
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
    Element h2 = findH2(content, "Geographic Occurrence");
    if (h2 == null) return;

    for (Element sib : h2.nextElementSiblings()) {
      if (isHeading(sib)) break;
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

  /** Returns concatenated text of all siblings after an &lt;h2&gt; matching {@code heading}. */
  private static String sectionText(Element parent, String heading) {
    Element h2 = findH2(parent, heading);
    if (h2 == null) return null;
    StringBuilder sb = new StringBuilder();
    for (Element sib : h2.nextElementSiblings()) {
      if (isHeading(sib)) break;
      String t = sib.text().trim();
      if (!t.isEmpty()) {
        if (sb.length() > 0) sb.append(" ");
        sb.append(t);
      }
    }
    return sb.isEmpty() ? null : sb.toString();
  }

  private static Element findH2(Element parent, String headingPrefix) {
    for (Element h2 : parent.select("h2")) {
      if (h2.text().trim().startsWith(headingPrefix)) return h2;
    }
    return null;
  }

  private static boolean isHeading(Element el) {
    String tag = el.tagName();
    return tag.equals("h1") || tag.equals("h2") || tag.equals("h3");
  }

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
