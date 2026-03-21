package org.catalogueoflife.data.wikispecies;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.sweble.wikitext.parser.ParserConfig;
import org.sweble.wikitext.parser.WikitextParser;
import org.sweble.wikitext.parser.nodes.*;
import org.sweble.wikitext.parser.utils.SimpleParserConfig;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.catalogueoflife.data.wikispecies.WtUtils.*;

public class Generator extends AbstractColdpGenerator {
  static final URI DOWNLOAD = URI.create("https://dumps.wikimedia.org/specieswiki/latest/specieswiki-latest-pages-articles.xml.bz2");
  static final String srcFN = "data.xml.bz2";

  private static final Set<String> SKIP_TITLES = Set.of("Main Page", "MediaWiki:Sidebar");
  private static final Pattern TAXONBAR_PAT = Pattern.compile(
      "\\{\\{[Tt]axonbar[^}]*\\bfrom=\\s*(Q[0-9]+)");

  /** Maps Wikispecies rank label text (lower-case, letters only) → ColDP rank string. */
  private static final Map<String, String> RANK_MAP = buildRankMap();

  private TermWriter vernWriter;
  private final WikitextParser parser;

  // Counters
  private int taxonCount = 0;
  private int synCount = 0;
  private int redirectCount = 0;
  private int synSeq = 0;

  record NavInfo(String parentTemplate, String rank) {}
  record SynonymData(String name, String authorship, String status) {}

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, Map.of(srcFN, DOWNLOAD));
    ParserConfig pcfg = new SimpleParserConfig();
    parser = new WikitextParser(pcfg);
  }

  @Override
  protected void prepare() throws Exception {
    initRefWriter(List.of(ColdpTerm.ID, ColdpTerm.citation));
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.alternativeID,
        ColdpTerm.parentID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.link
    ));
    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.language,
        ColdpTerm.name
    ));
  }

  @Override
  protected void addData() throws Exception {
    File src = sourceFile(srcFN);

    // Pass 1: collect navigation templates from Template: pages
    LOG.info("Pass 1: collecting navigation templates...");
    Map<String, NavInfo> navTemplates = new HashMap<>();
    streamXml(src, page -> {
      if (page.title != null && page.title.startsWith("Template:")) {
        String templateName = page.title.substring(9);
        NavInfo info = parseNavTemplate(page.text);
        if (info != null) {
          navTemplates.put(templateName, info);
        }
      }
    });
    LOG.info("Loaded {} navigation templates", navTemplates.size());

    // Pass 2: process taxon and redirect pages
    LOG.info("Pass 2: processing taxon pages...");
    Set<String> taxonIds = new HashSet<>();
    List<WikiPage> redirects = new ArrayList<>();
    Set<String> writtenRefIds = new HashSet<>();

    streamXml(src, page -> {
      if (page.title == null || page.title.contains(":") || SKIP_TITLES.contains(page.title)) return;
      if (page.redirect != null) {
        redirects.add(page);
      } else if (page.text != null && "wikitext".equalsIgnoreCase(page.model)) {
        try {
          processTaxonPage(page, navTemplates, taxonIds, writtenRefIds);
        } catch (Exception e) {
          LOG.warn("Failed to process page '{}': {}", page.title, e.getMessage());
        }
      }
    });
    LOG.info("Processed {} taxon pages", taxonCount);

    // Write redirects as synonyms where target is a known taxon
    for (WikiPage redirect : redirects) {
      if (redirect.redirect == null) continue;
      String targetId = WikiPage.id(redirect.redirect);
      if (taxonIds.contains(targetId)) {
        try {
          writeRedirectAsSynonym(redirect, targetId);
          redirectCount++;
        } catch (Exception e) {
          LOG.warn("Failed to write redirect '{}': {}", redirect.title, e.getMessage());
        }
      }
    }
    LOG.info("Written {} synonyms ({} from redirects)", synCount + redirectCount, redirectCount);
  }

  private void processTaxonPage(WikiPage page, Map<String, NavInfo> navTemplates,
                                Set<String> taxonIds, Set<String> writtenRefIds) throws Exception {
    WtNode article = parser.parseArticle(page.text, page.title);

    String parentId = null;
    String rank = null;
    String sciName = null;
    String authorship = null;
    String wikidataQid = null;
    List<VernacularExtractor.VernacularName> vernaculars = new ArrayList<>();
    List<SynonymData> synonyms = new ArrayList<>();
    Set<String> refIds = new LinkedHashSet<>();

    // Scan for {{Taxonbar|from=Q...}} via regex (it often appears in the last section's body)
    Matcher tbMatcher = TAXONBAR_PAT.matcher(page.text);
    if (tbMatcher.find()) wikidataQid = tbMatcher.group(1);

    for (WtNode node : article) {
      if (!(node instanceof WtSection)) continue;
      WtSection sect = (WtSection) node;
      if (!sect.hasBody()) continue;
      String key = sectionKey(sect.getHeading());

      switch (key) {
        case "taxonavigation" -> {
          String[] parentRank = parseTaxonavSection(sect.getBody(), page.title, navTemplates);
          if (parentRank != null) {
            parentId = parentRank[0];
            rank = parentRank[1];
          }
        }
        case "name" -> {
          NameExtractor.ParsedName pn = NameExtractor.extract(sect.getBody());
          sciName = pn.scientificName();
          String auth = pn.authorship();
          String yr = pn.year();
          if (auth != null && yr != null) {
            authorship = auth + ", " + yr;
          } else if (auth != null) {
            authorship = auth;
          } else if (yr != null) {
            authorship = yr;
          }
          collectRefs(sect.getBody(), refIds);
          // Also look in sub-sections (Primary references, Additional references)
          for (WtNode sub : sect.getBody()) {
            if (sub instanceof WtSection && ((WtSection) sub).hasBody()) {
              collectRefs(((WtSection) sub).getBody(), refIds);
            }
          }
        }
        case "synonymy", "synonyms" ->
            synonyms.addAll(parseSynonymSection(sect.getBody()));
        case "vernacular names" ->
            vernaculars.addAll(VernacularExtractor.extract(sect.getBody()));
        case "references" -> {
          collectRefs(sect.getBody(), refIds);
          for (WtNode sub : sect.getBody()) {
            if (sub instanceof WtSection && ((WtSection) sub).hasBody()) {
              collectRefs(((WtSection) sub).getBody(), refIds);
            }
          }
        }
      }
    }

    // Fall back: use page title as scientific name if not extracted
    if (sciName == null || sciName.isEmpty()) {
      sciName = page.title.replaceAll("\\s*\\([^)]+\\)\\s*$", "").trim();
    }

    String id = WikiPage.id(page.title);

    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentId);
    writer.set(ColdpTerm.status, "accepted");
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, sciName);
    writer.set(ColdpTerm.authorship, authorship);
    if (wikidataQid != null) writer.set(ColdpTerm.alternativeID, "wd:" + wikidataQid);
    writer.set(ColdpTerm.link, "https://species.wikimedia.org/wiki/" + id);
    writer.next();
    taxonCount++;
    taxonIds.add(id);

    // Vernacular names
    for (var vn : vernaculars) {
      vernWriter.set(ColdpTerm.taxonID, id);
      vernWriter.set(ColdpTerm.language, vn.language());
      vernWriter.set(ColdpTerm.name, vn.name());
      vernWriter.next();
    }

    // Synonyms from synonymy section
    for (var syn : synonyms) {
      if (syn.name() == null || syn.name().isEmpty()) continue;
      writer.set(ColdpTerm.ID, id + "_syn_" + (++synSeq));
      writer.set(ColdpTerm.parentID, id);
      writer.set(ColdpTerm.status, syn.status());
      writer.set(ColdpTerm.scientificName, syn.name());
      writer.set(ColdpTerm.authorship, syn.authorship());
      writer.next();
      synCount++;
    }

    // References
    for (String refId : refIds) {
      if (writtenRefIds.add(refId)) {
        refWriter.set(ColdpTerm.ID, refId);
        refWriter.set(ColdpTerm.citation, refId.replace("ref:", "").replace("_", " "));
        refWriter.next();
      }
    }
  }

  private void writeRedirectAsSynonym(WikiPage redirect, String targetId) throws Exception {
    writer.set(ColdpTerm.ID, WikiPage.id(redirect.title));
    writer.set(ColdpTerm.parentID, targetId);
    writer.set(ColdpTerm.status, "synonym");
    writer.set(ColdpTerm.scientificName, redirect.title.replaceAll("\\s*\\([^)]+\\)\\s*$", "").trim());
    writer.set(ColdpTerm.link, "https://species.wikimedia.org/wiki/" + WikiPage.id(redirect.title));
    writer.next();
  }

  // ─── Navigation template parsing ─────────────────────────────────────────

  /**
   * Parse a Template: page wikitext to extract NavInfo (parent template name + rank).
   * Template structure:
   *   {{ParentTemplate}}        OR   {{Taxonav|ParentName}}
   *   Rank: [[Current taxon]]
   */
  private NavInfo parseNavTemplate(String wikitext) {
    if (StringUtils.isBlank(wikitext)) return null;
    try {
      // Strip noinclude / includeonly blocks before parsing
      String cleaned = wikitext
          .replaceAll("(?s)<noinclude>.*?</noinclude>", "")
          .replaceAll("(?s)<includeonly>.*?</includeonly>", "")
          .trim();
      if (cleaned.isEmpty()) return null;

      WtNode article = parser.parseArticle(cleaned, "template");
      String parentTemplate = null;
      String rank = null;

      for (WtNode node : article) {
        if (node instanceof WtTemplate && parentTemplate == null) {
          WtTemplate t = (WtTemplate) node;
          String tname = templateName(t);
          if (tname.equalsIgnoreCase("Taxonav")) {
            parentTemplate = templateArg(t, 0);
          } else if (!tname.isEmpty()) {
            parentTemplate = tname;
          }
        }
        if (rank == null) {
          String text = nodeText(node).trim();
          rank = parseRankLabel(text);
        }
      }
      if (parentTemplate != null && rank != null) {
        return new NavInfo(parentTemplate, rank);
      }
    } catch (Exception e) {
      // ignore parse errors for templates
    }
    return null;
  }

  // ─── Taxonavigation section parsing ──────────────────────────────────────

  /**
   * Returns [parentId, rank] by examining the Taxonavigation section body.
   *
   * Strategy:
   * 1. Find first template in the body → this is the nav template T.
   * 2. Look for a rank label in the body text that references the current page.
   *    If found: rank from the label, parent = T.
   * 3. If not found: look up T in navTemplates to get rank and parent.
   */
  private String[] parseTaxonavSection(WtBody body, String pageTitle,
                                       Map<String, NavInfo> navTemplates) {
    String cleanTitle = pageTitle.replaceAll("\\s*\\([^)]+\\)\\s*$", "").trim();

    // Step 1: find first template
    String navTemplate = firstTemplateInBody(body);
    if (navTemplate == null) return null;

    // Step 2: look for inline rank label matching the current page
    String bodyText = nodeText(body);
    for (String line : bodyText.split("\\n")) {
      line = line.trim();
      int colonIdx = line.indexOf(':');
      if (colonIdx < 0) continue;
      String rankKey = line.substring(0, colonIdx).trim().toLowerCase().replaceAll("[^a-z]", "");
      String ref = line.substring(colonIdx + 1).trim();
      String colRank = RANK_MAP.get(rankKey);
      if (colRank != null && containsTitle(ref, cleanTitle)) {
        return new String[]{WikiPage.id(navTemplate), colRank};
      }
    }

    // Step 3: look up nav template definition
    NavInfo info = navTemplates.get(navTemplate);
    if (info != null) {
      return new String[]{WikiPage.id(info.parentTemplate()), info.rank()};
    }

    return null;
  }

  private String firstTemplateInBody(WtBody body) {
    for (WtNode node : body) {
      if (node instanceof WtTemplate) {
        String name = templateName((WtTemplate) node);
        if (!name.isEmpty()) return name;
      }
    }
    return null;
  }

  private boolean containsTitle(String text, String title) {
    // Strip disambiguation from any title references in the text
    String cleanText = text.replaceAll("\\s*\\([^)]+\\)", "");
    return cleanText.contains(title);
  }

  // ─── Synonym section parsing ──────────────────────────────────────────────

  private List<SynonymData> parseSynonymSection(WtBody body) {
    List<SynonymData> result = new ArrayList<>();
    String currentStatus = "synonym";

    for (WtNode node : body) {
      if (node instanceof WtTemplate) {
        String tname = templateName((WtTemplate) node).toUpperCase();
        currentStatus = switch (tname) {
          case "HOT" -> "homotypic synonym";
          case "HET" -> "heterotypic synonym";
          case "REP" -> "replacement name";
          default -> currentStatus;
        };
      } else if (node instanceof WtUnorderedList) {
        collectSynonymItems((WtUnorderedList) node, currentStatus, result);
      }
    }
    return result;
  }

  private void collectSynonymItems(WtUnorderedList list, String synType,
                                   List<SynonymData> result) {
    for (WtNode item : list) {
      if (item instanceof WtListItem) {
        String synName = NameExtractor.extractItalics(item);
        if (synName != null && !synName.isEmpty()) {
          String auth = NameExtractor.extractAuthor(item);
          String yr = extractYear(nodeText(item));
          String authorship = null;
          if (auth != null && yr != null) authorship = auth + ", " + yr;
          else if (auth != null) authorship = auth;
          result.add(new SynonymData(synName, authorship, synType));
        }
        // Recurse into nested lists (*** level)
        for (WtNode child : (WtListItem) item) {
          if (child instanceof WtUnorderedList) {
            collectSynonymItems((WtUnorderedList) child, synType, result);
          }
        }
      }
    }
  }

  // ─── Reference collection ─────────────────────────────────────────────────

  /**
   * Collect reference IDs from template citations like {{Miller, 1768}} or
   * {{Fleming, 1822a}} found in a section body.
   * Template names containing a comma or matching "Author, year" pattern are
   * treated as reference templates.
   */
  private void collectRefs(WtBody body, Set<String> refIds) {
    for (WtNode node : body) {
      collectRefsFromNode(node, refIds);
    }
  }

  private void collectRefsFromNode(WtNode node, Set<String> refIds) {
    if (node instanceof WtTemplate) {
      WtTemplate t = (WtTemplate) node;
      String tname = templateName(t);
      if (isRefTemplate(tname)) {
        refIds.add("ref:" + WikiPage.id(tname));
      }
      // Don't recurse into template args for ref collection
      return;
    }
    for (WtNode child : node) {
      collectRefsFromNode(child, refIds);
    }
  }

  private boolean isRefTemplate(String name) {
    // Reference templates match pattern like "Lastname, 1234" or "Lastname et al., 1234x"
    return name.contains(",") && name.matches(".*,\\s*\\d{4}.*");
  }

  // ─── Rank label parsing ───────────────────────────────────────────────────

  /** Try to extract a ColDP rank from a line of text containing a rank label. */
  private static String parseRankLabel(String text) {
    int colonIdx = text.indexOf(':');
    if (colonIdx < 0) return null;
    String key = text.substring(0, colonIdx).trim().toLowerCase().replaceAll("[^a-z]", "");
    return RANK_MAP.get(key);
  }

  // ─── XML streaming ────────────────────────────────────────────────────────

  private void streamXml(File src, Consumer<WikiPage> handler) throws Exception {
    var factory = XMLInputFactory.newInstance();
    try (InputStream in = new BZip2CompressorInputStream(new FileInputStream(src), true)) {
      XMLStreamReader xmlReader = factory.createXMLStreamReader(
          new InputStreamReader(in, StandardCharsets.UTF_8));
      WikiPage page = null;
      boolean revision = false;
      boolean contributor = false;
      StringBuilder text = null;

      int event;
      while ((event = xmlReader.next()) != XMLStreamConstants.END_DOCUMENT) {
        switch (event) {
          case XMLStreamConstants.START_ELEMENT -> {
            text = new StringBuilder();
            switch (xmlReader.getLocalName()) {
              case "page" -> page = new WikiPage();
              case "revision" -> revision = true;
              case "contributor" -> contributor = true;
              case "redirect" -> { if (page != null) page.redirect = xmlReader.getAttributeValue(null, "title"); }
            }
          }
          case XMLStreamConstants.END_ELEMENT -> {
            switch (xmlReader.getLocalName()) {
              case "page" -> {
                if (page != null) { handler.accept(page); page = null; }
              }
              case "revision" -> revision = false;
              case "contributor" -> contributor = false;
              case "title" -> { if (page != null) page.title = text.toString(); }
              case "id" -> {
                if (contributor) { if (page != null) page.contributorID = text.toString(); }
                else if (!revision && page != null) page.id = text.toString();
              }
              case "timestamp" -> { if (page != null) page.timestamp = text.toString(); }
              case "model" -> { if (page != null) page.model = text.toString(); }
              case "text" -> { if (page != null) page.text = text.toString(); }
            }
          }
          case XMLStreamConstants.CHARACTERS -> {
            if (page != null && text != null) {
              String x = xmlReader.getText();
              if (!StringUtils.isBlank(x)) text.append(x);
            }
          }
        }
      }
    }
  }

  // ─── Metadata ─────────────────────────────────────────────────────────────

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

  // ─── Rank map ─────────────────────────────────────────────────────────────

  private static Map<String, String> buildRankMap() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("regnum", "kingdom");
    m.put("kingdom", "kingdom");
    m.put("subregnum", "subkingdom");
    m.put("superphylum", "superphylum");
    m.put("superdivisio", "superphylum");
    m.put("phylum", "phylum");
    m.put("divisio", "phylum");
    m.put("division", "phylum");
    m.put("subphylum", "subphylum");
    m.put("subdivisio", "subphylum");
    m.put("infraphylum", "infraphylum");
    m.put("superclassis", "superclass");
    m.put("superclass", "superclass");
    m.put("classis", "class");
    m.put("class", "class");
    m.put("subclassis", "subclass");
    m.put("subclass", "subclass");
    m.put("infraclassis", "infraclass");
    m.put("infraclass", "infraclass");
    m.put("superordo", "superorder");
    m.put("superorder", "superorder");
    m.put("ordo", "order");
    m.put("order", "order");
    m.put("subordo", "suborder");
    m.put("suborder", "suborder");
    m.put("infraordo", "infraorder");
    m.put("infraorder", "infraorder");
    m.put("superfamilia", "superfamily");
    m.put("superfamily", "superfamily");
    m.put("familia", "family");
    m.put("family", "family");
    m.put("subfamilia", "subfamily");
    m.put("subfamily", "subfamily");
    m.put("infrafamilia", "infrafamily");
    m.put("tribus", "tribe");
    m.put("tribe", "tribe");
    m.put("subtribus", "subtribe");
    m.put("subtribe", "subtribe");
    m.put("genus", "genus");
    m.put("subgenus", "subgenus");
    m.put("sectio", "section");
    m.put("section", "section");
    m.put("series", "series");
    m.put("species", "species");
    m.put("subspecies", "subspecies");
    m.put("varietas", "variety");
    m.put("variety", "variety");
    m.put("forma", "form");
    m.put("form", "form");
    return Collections.unmodifiableMap(m);
  }
}
