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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.catalogueoflife.data.wikispecies.WtUtils.*;

public class Generator extends AbstractColdpGenerator {
  static final String srcFN = "data.xml.bz2";
  private static final String MIRROR_BASE   = "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/specieswiki/";
  private static final String FALLBACK_URL  = "https://dumps.wikimedia.org/specieswiki/latest/specieswiki-latest-pages-articles.xml.bz2";

  private static final Set<String> SKIP_TITLES = Set.of("Main Page", "MediaWiki:Sidebar");
  private static final Pattern TAXONBAR_PAT = Pattern.compile(
      "\\{\\{[Tt]axonbar[^}]*\\bfrom=\\s*(Q[0-9]+)");

  /** Maps Wikispecies rank label text (lower-case, letters only) → ColDP rank string. */

  private TermWriter vernWriter;
  private TermWriter nameRelWriter;
  private TermWriter distWriter;
  private final WikitextParser parser;

  private static final Pattern DIST_SKIP =
      Pattern.compile("(?i)continental|regional|\u02D0");

  // Counters
  private int taxonCount = 0;
  private int synCount = 0;
  private int redirectCount = 0;
  private int synSeq = 0;

  record NavInfo(String parentTemplate, String rank) {}
  record TaxonavResult(String parentId, String rank, String remarks) {}
  record SynonymData(String name, String authorship, String status) {}

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
    ParserConfig pcfg = new SimpleParserConfig();
    parser = new WikitextParser(pcfg);
  }

  @Override
  protected void prepare() throws Exception {
    // Download dump — prefer Swedish mirror, fall back to main Wikimedia site.
    File dumpFile = sourceFile(srcFN);
    if (dumpFile.exists()) {
      if (!cfg.noDownload && isRemoteNewer(dumpFile)) {
        LOG.info("Remote WikiSpecies dump is newer, re-downloading...");
        dumpFile.delete();
        downloadDump(dumpFile);
      } else {
        LOG.info("Reusing cached WikiSpecies dump: {}", dumpFile);
      }
    } else if (cfg.noDownload) {
      throw new IllegalStateException("--no-download set but WikiSpecies dump not found: " + dumpFile);
    } else {
      downloadDump(dumpFile);
    }

    initRefWriter(List.of(ColdpTerm.ID, ColdpTerm.citation));
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.alternativeID,
        ColdpTerm.parentID,
        ColdpTerm.basionymID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.nameReferenceID,
        ColdpTerm.publishedInYear,
        ColdpTerm.referenceID,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.language,
        ColdpTerm.name
    ));
    nameRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));
    distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.area
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
        // Fast pre-filter: all taxon pages have a Name or Taxonavigation section;
        // person/author pages (e.g. botanists, zoologists) have neither.
        if (!page.text.contains("{{int:Name}}") && !page.text.contains("{{int:Taxonavigation}}")) {
          LOG.debug("Skipping non-taxon page: {}", page.title);
        } else try {
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
    String nameYear = null;
    String primaryRefId = null;
    List<String> additionalRefIds = new ArrayList<>();
    String wikidataQid = null;
    String remarks = null;
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
          TaxonavResult nav = parseTaxonavSection(sect.getBody(), page.title, navTemplates);
          if (nav != null) {
            parentId = nav.parentId();
            rank = nav.rank();
            remarks = nav.remarks();
          }
        }
        case "name" -> {
          NameExtractor.ParsedName pn = NameExtractor.extract(sect.getBody());
          sciName = pn.scientificName();
          nameYear = pn.year();
          String auth = pn.authorship();
          if (auth != null && nameYear != null) {
            authorship = auth + ", " + nameYear;
          } else if (auth != null) {
            authorship = auth;
          } else if (nameYear != null) {
            authorship = nameYear;
          }
          // Scan sub-sections for synonyms, primary and additional references.
          // On many pages ==Name== contains ===Synonyms=== as a level-3 child, so
          // the top-level section switch never sees it — we must handle it here.
          Set<String> primRefs = new LinkedHashSet<>();
          Set<String> addlRefs = new LinkedHashSet<>();
          for (WtNode sub : sect.getBody()) {
            if (!(sub instanceof WtSection) || !((WtSection) sub).hasBody()) continue;
            WtSection subSect = (WtSection) sub;
            String subKey = sectionKey(subSect.getHeading());
            if (subKey.contains("primary")) {
              collectRefsAny(subSect.getBody(), primRefs);
            } else if (subKey.contains("additional")) {
              collectRefs(subSect.getBody(), addlRefs);
            } else if (subKey.equals("synonymy") || subKey.equals("synonyms")) {
              synonyms.addAll(parseSynonymSection(subSect.getBody()));
            }
          }
          if (!primRefs.isEmpty()) {
            primaryRefId = primRefs.iterator().next();
            refIds.addAll(primRefs);
          }
          additionalRefIds.addAll(addlRefs);
          refIds.addAll(addlRefs);
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

    // Pre-scan synonyms to locate the basionym's future ID (assigned sequentially)
    String basionymSynId = null;
    {
      int tempSeq = synSeq;
      for (var syn : synonyms) {
        if (syn.name() == null || syn.name().isEmpty()) continue;
        tempSeq++;
        if ("basionym".equals(syn.status())) {
          basionymSynId = id + "_syn_" + tempSeq;
          break;
        }
      }
    }

    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentId);
    writer.set(ColdpTerm.basionymID, basionymSynId);
    writer.set(ColdpTerm.status, "accepted");
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, sciName);
    writer.set(ColdpTerm.authorship, authorship);
    writer.set(ColdpTerm.nameReferenceID, primaryRefId);
    writer.set(ColdpTerm.publishedInYear, nameYear);
    if (!additionalRefIds.isEmpty()) {
      writer.set(ColdpTerm.referenceID, String.join(",", additionalRefIds));
    }
    if (wikidataQid != null) writer.set(ColdpTerm.alternativeID, "wd:" + wikidataQid);
    writer.set(ColdpTerm.link, "https://species.wikimedia.org/wiki/" + id);
    writer.set(ColdpTerm.remarks, remarks);
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
      String synId = id + "_syn_" + (++synSeq);
      boolean isBasionymEntry = "basionym".equals(syn.status());
      // Both the basionym entry and HOT synonyms are written as homotypic synonyms
      String colStatus = isBasionymEntry ? "homotypic synonym" : syn.status();
      boolean isHomotypic = "homotypic synonym".equals(colStatus);

      writer.set(ColdpTerm.ID, synId);
      writer.set(ColdpTerm.parentID, id);
      // HOT synonyms point to the basionym; the basionym entry itself does not
      if (!isBasionymEntry && isHomotypic && basionymSynId != null) {
        writer.set(ColdpTerm.basionymID, basionymSynId);
      }
      writer.set(ColdpTerm.status, colStatus);
      writer.set(ColdpTerm.scientificName, syn.name());
      writer.set(ColdpTerm.authorship, syn.authorship());
      writer.next();
      synCount++;

      // NameRelation for all homotypic synonyms (including basionym entry)
      if (isHomotypic) {
        nameRelWriter.set(ColdpTerm.nameID, synId);
        nameRelWriter.set(ColdpTerm.relatedNameID, id);
        nameRelWriter.set(ColdpTerm.type, "homotypic synonym");
        nameRelWriter.next();
      }
    }

    // Distribution from {{nadi|...}} templates anywhere in the article
    List<WtTemplate> nadiTemplates = new ArrayList<>();
    findTemplates(article, "nadi", nadiTemplates);
    for (WtTemplate nadi : nadiTemplates) {
      for (String area : extractDistAreas(nadi)) {
        distWriter.set(ColdpTerm.taxonID, id);
        distWriter.set(ColdpTerm.area, area);
        distWriter.next();
      }
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

  // ─── Template invocation patterns (regex-based, since Sweble returns WtText for {{...}}) ──
  // Sweble's parseArticle() does NOT parse {{template}} invocations as WtTemplate nodes;
  // they are returned as raw WtText containing the literal {{...}} markup. We therefore
  // extract template names and arguments with regular expressions.
  private static final Pattern FIRST_TEMPLATE_PAT =
      Pattern.compile("\\{\\{([^|{}\\n]+?)(?:\\|[^}]*)?\\}\\}");
  private static final Pattern TAXONAV_ARG_PAT =
      Pattern.compile("\\{\\{[Tt]axonav\\|([^}|\\n]+)");
  // Short (1–5 char) formatting templates that wrap incertae-sedis taxon names: {{g|X}}, {{glast|X}}, etc.
  private static final Pattern INCERTAE_TEMPLATE_PAT =
      Pattern.compile("\\{\\{([a-z]{1,5})\\|([^}|{\\n]+)");

  // ─── Navigation template parsing ─────────────────────────────────────────

  /**
   * Parse a Template: page wikitext to extract NavInfo (parent template name + rank).
   * Template structure:
   *   {{ParentTemplate}}        OR   {{Taxonav|ParentName}}
   *   Rank: [[Current taxon]]
   * Uses regex because Sweble returns {{...}} invocations as raw WtText, not WtTemplate nodes.
   */
  private NavInfo parseNavTemplate(String wikitext) {
    if (StringUtils.isBlank(wikitext)) return null;
    String cleaned = wikitext
        .replaceAll("(?s)<noinclude>.*?</noinclude>", "")
        .replaceAll("(?s)<includeonly>.*?</includeonly>", "")
        .trim();
    if (cleaned.isEmpty()) return null;

    // Find parent template: first {{Name}} or {{Taxonav|Name}}, skipping parser functions
    String parentTemplate = null;
    Matcher taxonavM = TAXONAV_ARG_PAT.matcher(cleaned);
    if (taxonavM.find()) {
      parentTemplate = taxonavM.group(1).trim();
    } else {
      Matcher m = FIRST_TEMPLATE_PAT.matcher(cleaned);
      while (m.find()) {
        String name = m.group(1).trim();
        if (!name.isEmpty() && !isParserFunction(name)) {
          parentTemplate = name;
          break;
        }
      }
    }

    // Find rank label: scan each line for "RankLabel: ..." pattern
    String rank = null;
    for (String line : cleaned.split("\\n")) {
      // Strip any remaining template markup from the line before rank detection
      String stripped = line.replaceAll("\\{\\{[^}]*\\}\\}", "").replaceAll("\\[\\[([^\\]|]*)(?:\\|[^\\]]*)?\\]\\]", "$1").trim();
      rank = parseRankLabel(stripped);
      if (rank != null) break;
    }

    if (parentTemplate != null && rank != null) {
      return new NavInfo(parentTemplate, rank);
    }
    return null;
  }

  // ─── Taxonavigation section parsing ──────────────────────────────────────

  /**
   * Returns TaxonavResult (parentId, rank, remarks) by examining the Taxonavigation section body.
   *
   * Strategy:
   * 1. Find first non-magic-word template in the body → this is the nav template T.
   * 2. Look for an inline rank label in the body text that references the current page.
   *    If found: rank from the label, parent = T.
   * 3. If not found: look up T in navTemplates to get rank and parent.
   *
   * Additionally, any children referenced via formatting templates (not wiki links) in the body
   * are collected as remarks (incertae sedis taxa without their own article pages).
   */
  private TaxonavResult parseTaxonavSection(WtBody body, String pageTitle,
                                            Map<String, NavInfo> navTemplates) {
    String cleanTitle = pageTitle.replaceAll("\\s*\\([^)]+\\)\\s*$", "").trim();

    // Step 1: find first non-magic-word template
    String navTemplate = firstTemplateInBody(body);
    if (navTemplate == null) return null;

    // Collect template-formatted children (not wiki-linked) for remarks
    String remarks = collectTaxonavRemarks(body, navTemplate);

    // Step 2: look for inline rank label matching the current page
    String bodyText = nodeText(body);
    for (String line : bodyText.split("\\n")) {
      line = line.trim();
      String colRank = parseRankLabel(line);
      if (colRank != null) {
        int colonIdx = line.indexOf(':');
        String ref = line.substring(colonIdx + 1).trim();
        if (containsTitle(ref, cleanTitle)) {
          return new TaxonavResult(WikiPage.id(navTemplate), colRank, remarks);
        }
      }
    }

    // Step 3: look up nav template definition
    NavInfo info = navTemplates.get(navTemplate);
    if (info != null) {
      return new TaxonavResult(WikiPage.id(info.parentTemplate()), info.rank(), remarks);
    }

    return null;
  }

  /**
   * Extracts the name of the first non-magic-word template from the body text.
   * Uses regex because Sweble returns {{...}} as WtText (not WtTemplate nodes).
   */
  private String firstTemplateInBody(WtBody body) {
    String text = nodeText(body); // WtText nodes include raw {{...}} markup
    Matcher m = FIRST_TEMPLATE_PAT.matcher(text);
    while (m.find()) {
      String name = m.group(1).trim();
      if (!name.isEmpty() && !isParserFunction(name)) return name;
    }
    return null;
  }

  /**
   * Collects names of taxa referenced via short formatting templates (e.g. {{g|Lycoptera}})
   * in the Taxonavigation body after the nav template. These are incertae sedis or extinct
   * taxa that don't have their own article pages.
   */
  private String collectTaxonavRemarks(WtBody body, String navTemplateName) {
    String text = nodeText(body); // WtText includes raw {{...}}
    // Skip everything up to and including the nav template invocation
    int navEnd = text.indexOf("{{" + navTemplateName + "}}");
    if (navEnd >= 0) navEnd += navTemplateName.length() + 4;
    else navEnd = 0;
    String afterNav = text.substring(navEnd);

    List<String> names = new ArrayList<>();
    Matcher m = INCERTAE_TEMPLATE_PAT.matcher(afterNav);
    while (m.find()) {
      String arg = m.group(2).trim();
      if (arg.length() > 1) names.add(arg);
    }
    return names.isEmpty() ? null : String.join(", ", names);
  }

  /** Returns true for MediaWiki magic words and parser functions (DISPLAYTITLE, int:X, etc.). */
  private static boolean isParserFunction(String name) {
    return name.equals(name.toUpperCase()) || name.contains(":");
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
      if (node instanceof WtText) {
        // Sweble returns {{BA}}, {{HOT}}, {{HET}}, {{REP}} as raw WtText nodes
        String raw = ((WtText) node).getContent().strip().toUpperCase();
        currentStatus = switch (raw) {
          case "{{BA}}"  -> "basionym";
          case "{{HOT}}" -> "homotypic synonym";
          case "{{HET}}" -> "heterotypic synonym";
          case "{{REP}}" -> "replacement name";
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
          // Inline protonym markers (zoological style) override the section-level status.
          // Two forms: [[protonym]] internal link, or {{PR}} / {{PR|2}} template.
          String effectiveType = isProtonymItem((WtListItem) item) ? "basionym" : synType;
          String auth = NameExtractor.extractAuthor(item);
          String yr = extractYear(nodeText(item));
          String authorship = null;
          if (auth != null && yr != null) authorship = auth + ", " + yr;
          else if (auth != null) authorship = auth;
          result.add(new SynonymData(synName, authorship, effectiveType));
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

  /**
   * Returns true if a list item carries an inline protonym/basionym marker.
   * Handles two zoological conventions:
   *   [[protonym]]       — internal wikilink to the "protonym" page
   *   {{PR}} / {{PR|2}}  — the PR template (renders as "[protonym]" or "Protonym")
   */
  private boolean isProtonymItem(WtListItem item) {
    return hasProtonymMarker(item);
  }

  private boolean hasProtonymMarker(WtNode node) {
    if (node instanceof WtText) {
      // Sweble returns {{PR}} / {{PR|2}} as raw WtText
      return ((WtText) node).getContent().strip().matches("(?i)\\{\\{PR(?:\\|[^}]*)?\\}\\}");
    }
    if (node instanceof WtInternalLink) {
      String target = ((WtInternalLink) node).getTarget().getAsString();
      return "protonym".equalsIgnoreCase(target.trim());
    }
    for (WtNode child : node) {
      if (hasProtonymMarker(child)) return true;
    }
    return false;
  }

  // ─── Reference collection ─────────────────────────────────────────────────

  /**
   * Collect any non-empty template invocation in a body as a reference.
   * Uses regex on nodeText because Sweble returns {{...}} as WtText, not WtTemplate.
   */
  private void collectRefsAny(WtBody body, Set<String> refIds) {
    String text = nodeText(body); // includes raw {{...}} from WtText nodes
    Matcher m = FIRST_TEMPLATE_PAT.matcher(text);
    while (m.find()) {
      String tname = m.group(1).trim();
      if (!tname.isEmpty() && !isParserFunction(tname)) {
        refIds.add("ref:" + WikiPage.id(tname));
      }
    }
  }

  /**
   * Collect reference IDs from template citations like {{Miller, 1768}} found in a section body.
   * Uses regex on nodeText because Sweble returns {{...}} as WtText, not WtTemplate.
   */
  private void collectRefs(WtBody body, Set<String> refIds) {
    String text = nodeText(body);
    Matcher m = FIRST_TEMPLATE_PAT.matcher(text);
    while (m.find()) {
      String tname = m.group(1).trim();
      if (isRefTemplate(tname)) {
        refIds.add("ref:" + WikiPage.id(tname));
      }
    }
  }

  private boolean isRefTemplate(String name) {
    // Reference templates match pattern like "Lastname, 1234" or "Lastname et al., 1234x"
    return name.contains(",") && name.matches(".*,\\s*\\d{4}.*");
  }

  // ─── Distribution extraction ──────────────────────────────────────────────

  /** Recursively find all templates with a given name in an AST node. Does not recurse into template args. */
  private void findTemplates(WtNode node, String name, List<WtTemplate> result) {
    if (node instanceof WtTemplate) {
      if (name.equalsIgnoreCase(templateName((WtTemplate) node))) {
        result.add((WtTemplate) node);
      }
      return; // don't recurse into template args
    }
    for (WtNode child : node) {
      findTemplates(child, name, result);
    }
  }

  /**
   * Extract individual area names from a {{nadi|...}} distribution template.
   * The template argument contains nested wikitext lists:
   *   *'''Continental: ...'''
   *   **'''Regional: ...'''
   *   ***  Country1, Country2, ...
   * We collect the leaf-level text and split by comma.
   */
  private List<String> extractDistAreas(WtTemplate nadiTemplate) {
    String content = templateArg(nadiTemplate, 0);
    if (content == null || content.isBlank()) return List.of();
    List<String> areas = new ArrayList<>();
    for (String line : content.split("\n")) {
      line = line.strip();
      if (line.isEmpty() || DIST_SKIP.matcher(line).find()) continue;
      for (String part : line.split(",")) {
        String area = part.strip();
        if (area.length() > 1) areas.add(area);
      }
    }
    return areas;
  }

  // ─── Rank label parsing ───────────────────────────────────────────────────

  /**
   * Extract a rank label from a line of text.
   * Returns the raw label key (e.g. "genus", "familia", "cohort") if recognised,
   * or null. ChecklistBank normalises the raw label to its internal rank enum on import.
   * Handles slash/comma alternatives like "Cohort/Superordo" by trying each part.
   */
  private static String parseRankLabel(String text) {
    int colonIdx = text.indexOf(':');
    if (colonIdx < 0) return null;
    String rankPart = text.substring(0, colonIdx).trim().toLowerCase();
    for (String part : rankPart.split("[/,]")) {
      String key = part.trim().replaceAll("[^a-z]", "");
      if (RANK_LABELS.contains(key)) return key;
    }
    return null;
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

  // ─── Mirror / download helpers ────────────────────────────────────────────

  /**
   * Resolves the WikiSpecies dump URL: discovers the latest dated directory on the Swedish mirror
   * (same layout as commonswiki) and verifies the file is present.
   * Falls back to the main Wikimedia "latest/" URL if the mirror is unreachable.
   */
  private String resolveDumpUrl() {
    try {
      String listing = http.get(URI.create(MIRROR_BASE));
      Pattern p = Pattern.compile("href=\"(\\d{8})/\"");
      Matcher m = p.matcher(listing);
      String latest = null;
      while (m.find()) {
        String date = m.group(1);
        if (latest == null || date.compareTo(latest) > 0) latest = date;
      }
      if (latest != null) {
        String url = MIRROR_BASE + latest + "/specieswiki-" + latest + "-pages-articles.xml.bz2";
        if (http.exists(url)) {
          LOG.info("WikiSpecies dump: using Swedish mirror, date={}", latest);
          return url;
        }
        LOG.warn("WikiSpecies dump: mirror date {} found but file unavailable, falling back", latest);
      } else {
        LOG.warn("WikiSpecies dump: no dated directories found on mirror");
      }
    } catch (Exception e) {
      LOG.warn("WikiSpecies dump: mirror lookup failed ({}), falling back to main site", e.getMessage());
    }
    LOG.info("WikiSpecies dump: using main Wikimedia site");
    return FALLBACK_URL;
  }

  private boolean isRemoteNewer(File localFile) {
    String url = resolveDumpUrl();
    try {
      var resp = http.head(url);
      var lastModHeader = resp.headers().firstValue("Last-Modified").orElse(null);
      if (lastModHeader != null) {
        var remoteDate = ZonedDateTime.parse(lastModHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
        return remoteDate.toInstant().toEpochMilli() > localFile.lastModified();
      }
    } catch (Exception e) {
      LOG.warn("Failed to check WikiSpecies dump freshness, will reuse local file: {}", e.getMessage());
    }
    return false;
  }

  private void downloadDump(File target) throws IOException {
    String url = resolveDumpUrl();
    LOG.info("Downloading WikiSpecies dump from {} ...", url);
    http.download(url, target);
    LOG.info("WikiSpecies dump download complete: {}", target);
  }

  // ─── Metadata ─────────────────────────────────────────────────────────────

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

  // ─── Rank label detection ─────────────────────────────────────────────────
  // Raw rank labels are passed through to ColDP; the checklistbank rank parser
  // normalises them (e.g. "familia" → family, "classis" → class) on import.

  private static final Set<String> RANK_LABELS = Set.of(
      "regnum", "kingdom", "subregnum", "superphylum", "superdivisio", "superdivision",
      "phylum", "divisio", "division", "subphylum", "subdivisio", "infraphylum",
      "superclassis", "superclass", "classis", "class", "subclassis", "subclass",
      "infraclassis", "infraclass", "gigaclass", "megaclass", "parvclass",
      "supercohort", "supercohors", "cohort", "cohors", "subcohort", "subcohors",
      "superordo", "superorder", "magnorder", "grandorder", "mirorder",
      "ordo", "order", "subordo", "suborder", "infraordo", "infraorder",
      "parvorder", "nanorder",
      "superfamilia", "superfamily", "familia", "family",
      "subfamilia", "subfamily", "infrafamilia",
      "tribus", "tribe", "subtribus", "subtribe",
      "genus", "subgenus", "sectio", "section", "series",
      "species", "subspecies", "varietas", "variety", "forma", "form"
  );
}
