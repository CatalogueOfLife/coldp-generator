package org.catalogueoflife.data.bats;

import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import org.catalogueoflife.data.utils.RefCache;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ColDP generator for batnames.org — Bats of the World by Simmons & Cirranello (AMNH).
 *
 * The explore.html accordion is flat (not nested): all button+panel pairs are direct children
 * of div#homeresults. Rank is on each panel's id attribute. Genera are not accordion entries;
 * they are linked via div.generatbl inside each panel.
 *
 * Phase 1: explore.html → write NameUsage for all taxa Order → Subtribe; collect genus names
 *          and their parents from div.generatbl links.
 * Phase 2: for each genus, fetch /genera/{name} → write genus NameUsage + all species with
 *          synonyms, vernacular names, type localities, distribution, and references.
 */
public class Generator extends AbstractColdpGenerator {

  private static final String BASE = "https://batnames.org";
  private static final URI EXPLORE_URL = URI.create(BASE + "/explore.html");
  private static final Pattern AUTH_YEAR = Pattern.compile("^(.*?),\\s*(\\d{4})\\s*(\\)?)\\s*[.,]?(.*)$", Pattern.DOTALL);
  private static final Pattern GBIF_KEY = Pattern.compile("[?&]key=(\\d+)");
  // Splits journal line into citation (group 1) and page (group 2).
  // Handles ": 533", ": p. 533", ": pp. 87-88", " p. 107.", " pp. 87-88."
  private static final Pattern PAGE_SEP = Pattern.compile(
      "^(.+?)(?::\\s*(?:pp?\\.\\s*)?|\\s+pp?\\.\\s+)(\\S.*?)\\s*\\.?$");
  private static final String EXPLORE_FILE = "explore.html";

  // Rank ordering for parent-tracking in the flat accordion
  private static final Map<String, Integer> RANK_LEVEL = Map.of(
      "order", 0, "suborder", 1, "superfamily", 2, "family", 3,
      "subfamily", 4, "tribe", 5, "subtribe", 6
  );

  private final List<String> genusNames = new ArrayList<>();
  private final Map<String, String> genusParents = new HashMap<>();  // genusName → parentTaxonId
  private final Map<String, String> refIdToCitation = new HashMap<>(); // Reference ID → citation (for bib matching)
  private RefCache refCache;
  private TermWriter vnWriter;
  private TermWriter distWriter;
  private TermWriter typeWriter;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void prepare() throws Exception {
    download(EXPLORE_FILE, EXPLORE_URL);
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID, ColdpTerm.parentID, ColdpTerm.rank,
        ColdpTerm.scientificName, ColdpTerm.authorship, ColdpTerm.publishedInYear,
        ColdpTerm.status, ColdpTerm.nameReferenceID, ColdpTerm.publishedInPage,
        ColdpTerm.referenceID, ColdpTerm.extinct, ColdpTerm.link, ColdpTerm.remarks
    ));
    initRefWriter(List.of(
        ColdpTerm.ID, ColdpTerm.citation, ColdpTerm.link
    ));
    refCache = new RefCache(refWriter);
    vnWriter   = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID, ColdpTerm.name, ColdpTerm.language
    ));
    distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID, ColdpTerm.area, ColdpTerm.gazetteer
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.nameID, ColdpTerm.locality
    ));

    // Phase 1: explore.html → higher-rank taxa + genus list
    File exploreFile = sourceFile(EXPLORE_FILE);
    Document doc = Jsoup.parse(Files.readString(exploreFile.toPath()));
    Element root = doc.selectFirst("div#homeresults");
    if (root == null) {
      throw new IllegalStateException("Could not find div#homeresults in explore.html");
    }
    parseAccordionBlock(root);
    LOG.info("Higher-rank phase complete: {} genera to crawl", genusNames.size());

    // Phase 2: per-genus pages → genus NameUsage + species
    int n = 0;
    for (String genus : genusNames) {
      File gFile = sourceFile("genera-" + genus + ".html");
      if (!gFile.exists()) {
        if (cfg.noDownload) {
          throw new IllegalStateException("--no-download set but genus page not found: " + gFile);
        }
        String gHtml = http.get(URI.create(BASE + "/genera/" + genus));
        Files.writeString(gFile.toPath(), gHtml);
        crawlDelay(100);
      }
      Document gDoc = Jsoup.parse(Files.readString(gFile.toPath()));
      parseGenusPage(genus, gDoc);
      if (++n % 50 == 0) {
        LOG.info("Genus pages processed: {}/{}", n, genusNames.size());
      }
    }

    vnWriter.close();
    distWriter.close();
    typeWriter.close();
  }

  // ── Phase 1: explore.html flat accordion parsing ─────────────────────────

  /**
   * explore.html accordion is flat: all button+panel pairs are direct children of the root div.
   * Hierarchy is derived from the rank id on each panel using RANK_LEVEL ordering.
   * Genera are not accordion entries; they appear as links inside div.generatbl.
   */
  private void parseAccordionBlock(Element root) throws IOException {
    // currentAtLevel[i] = taxon ID currently active at rank level i (0=order…6=subtribe)
    String[] currentAtLevel = new String[RANK_LEVEL.size()];

    for (Element button : root.select("> button.accordion")) {
      Element panel = button.nextElementSibling();
      if (panel == null || !"panel".equals(panel.className())) continue;

      String name = button.id();
      if (StringUtils.isBlank(name)) continue;
      String rank = panel.id();
      Integer level = RANK_LEVEL.get(rank);
      if (level == null) continue;

      // Parent = closest ancestor level that has a taxon
      String parentId = null;
      for (int i = level - 1; i >= 0; i--) {
        if (currentAtLevel[i] != null) { parentId = currentAtLevel[i]; break; }
      }
      currentAtLevel[level] = name;
      for (int i = level + 1; i < currentAtLevel.length; i++) currentAtLevel[i] = null;

      // Common name from <p> inside button
      Element cnEl = button.selectFirst("p");
      String commonName = cnEl != null ? clean(cnEl.text()) : null;

      Element uppertbl = panel.selectFirst("div.uppertbl, div.uppertbl2");
      String authorRaw = null;
      String year = null;
      String remarks = null;
      String dist = null;
      List<String> refIds = new ArrayList<>();

      // Parse name + authorship from button text: first word = uninomial, rest = complete authorship.
      // The div.author field is often corrupt (e.g. ", . ,"), so we ignore it.
      String buttonText = clean(button.ownText()); // ownText() excludes the <p> child
      if (!StringUtils.isBlank(buttonText)) {
        int sp = buttonText.indexOf(' ');
        if (sp > 0) {
          String remainder = buttonText.substring(sp + 1).replaceAll("[.,]+$", "").trim();
          authorRaw = StringUtils.trimToNull(remainder);
          year = parseAuthorYear(remainder)[1]; // extract year for publishedInYear only
        }
      }

      if (uppertbl != null) {
        Element synsEl = uppertbl.selectFirst("div.syns");
        if (synsEl != null) writeSynonyms(name, clean(synsEl.text()));
        Element distEl = uppertbl.selectFirst("div.dist");
        if (distEl != null) dist = clean(distEl.text());
        Element commentsEl = uppertbl.selectFirst("div.comments");
        if (commentsEl != null) remarks = clean(commentsEl.text());
        for (Element bib : uppertbl.select("div.bibentry")) {
          String refId = writeBibEntry(bib);
          if (refId != null) refIds.add(refId);
        }
      }

      writer.set(ColdpTerm.ID, name);
      writer.set(ColdpTerm.parentID, parentId);
      writer.set(ColdpTerm.rank, rank);
      writer.set(ColdpTerm.scientificName, name);
      writer.set(ColdpTerm.authorship, authorRaw);
      writer.set(ColdpTerm.publishedInYear, year);
      writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
      writer.set(ColdpTerm.extinct, "false");
      if (!refIds.isEmpty()) {
        writer.set(ColdpTerm.nameReferenceID, refIds.get(0));
        if (refIds.size() > 1) {
          writer.set(ColdpTerm.referenceID, String.join(",", refIds.subList(1, refIds.size())));
        }
      }
      writer.set(ColdpTerm.link, BASE + "/family/" + name);
      writer.set(ColdpTerm.remarks, remarks);
      writer.next();

      if (!StringUtils.isBlank(dist)) {
        distWriter.set(ColdpTerm.taxonID, name);
        distWriter.set(ColdpTerm.area, dist);
        distWriter.set(ColdpTerm.gazetteer, "text");
        distWriter.next();
      }
      if (!StringUtils.isBlank(commonName)) {
        vnWriter.set(ColdpTerm.taxonID, name);
        vnWriter.set(ColdpTerm.name, commonName);
        vnWriter.set(ColdpTerm.language, "eng");
        vnWriter.next();
      }

      // Collect genera from div.generatbl — links like <a href="/genera/Acerodon">
      Element generatbl = panel.selectFirst("div.generatbl");
      if (generatbl != null) {
        for (Element a : generatbl.select("a[href]")) {
          String href = a.attr("href");
          String genusName = href.contains("/") ? href.substring(href.lastIndexOf('/') + 1) : a.text().trim();
          genusName = genusName.trim();
          if (!StringUtils.isBlank(genusName) && !genusParents.containsKey(genusName)) {
            genusNames.add(genusName);
            genusParents.put(genusName, name);
          }
        }
      }
    }
  }

  // ── Phase 2: genus page parsing ──────────────────────────────────────────

  private void parseGenusPage(String genusName, Document doc) throws IOException {
    Element resultsArea = doc.selectFirst("div#homeresults");
    if (resultsArea == null) {
      LOG.warn("No homeresults div in genus page: {}", genusName);
      return;
    }

    // Write genus NameUsage from div.taxon[id=genus]
    String parentId = genusParents.get(genusName);
    Element genusTaxon = resultsArea.selectFirst("div.taxon[id=genus]");
    if (genusTaxon != null) {
      String fullText = clean(genusTaxon.text());
      String afterName = fullText.startsWith(genusName)
          ? fullText.substring(genusName.length()).trim() : fullText;
      String[] ayp = parseAuthorYear(afterName);
      writer.set(ColdpTerm.ID, genusName);
      writer.set(ColdpTerm.parentID, parentId);
      writer.set(ColdpTerm.rank, "genus");
      writer.set(ColdpTerm.scientificName, genusName);
      writer.set(ColdpTerm.authorship, StringUtils.trimToNull(ayp[0]));
      writer.set(ColdpTerm.publishedInYear, ayp[1]);
      writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
      writer.set(ColdpTerm.extinct, "false");
      writer.set(ColdpTerm.link, BASE + "/genera/" + genusName);
      writer.next();
    }

    // Subgenus tracking; process each speciestest block
    String currentParent = genusName;
    for (Element el : resultsArea.children()) {
      String cls = el.className();
      if ("taxon".equals(cls) && "subgenus".equals(el.id())) {
        currentParent = parseSubgenus(el, genusName);
      } else if ("speciestest".equals(cls)) {
        processSpeciesBlock(el, currentParent, genusName);
      }
    }
  }

  /**
   * Processes one div.speciestest block (one species and all its associated data divs).
   */
  private void processSpeciesBlock(Element speciestest, String parentId, String genusName) throws IOException {
    // Pre-scan for comments (appears after the taxon div, but needed before writing NameUsage)
    Element commentsEl = speciestest.selectFirst("div.comments");
    String remarks = commentsEl != null ? StringUtils.trimToNull(clean(commentsEl.text())) : null;

    // Pre-collect all references from the "References:" section so we can link them to the
    // species NameUsage and match the nomenclatural reference by year+author.
    List<String> bibRefIds = new ArrayList<>();
    for (Element bib : speciestest.select("div.bibentry")) {
      String refId = writeBibEntry(bib);
      if (refId != null) bibRefIds.add(refId);
    }

    String currentSpeciesId = null;

    for (Element el : speciestest.children()) {
      String cls = el.className();

      if ("taxon".equals(cls) && "species".equals(el.id())) {
        currentSpeciesId = parseSpecies(el, parentId, genusName, remarks, bibRefIds);

      } else if (currentSpeciesId != null) {
        switch (cls) {
          case "syn_group" -> parseSynGroup(el, currentSpeciesId, genusName);
          case "type_local" -> {
            String locality = clean(el.text());
            if (!StringUtils.isBlank(locality)) {
              typeWriter.set(ColdpTerm.nameID, currentSpeciesId);
              typeWriter.set(ColdpTerm.locality, locality);
              typeWriter.next();
            }
          }
          case "dist" -> {
            String area = clean(el.text());
            if (!StringUtils.isBlank(area)) {
              distWriter.set(ColdpTerm.taxonID, currentSpeciesId);
              distWriter.set(ColdpTerm.area, area);
              distWriter.set(ColdpTerm.gazetteer, "text");
              distWriter.next();
            }
          }
          case "map" -> {
            Element iframe = el.selectFirst("iframe");
            if (iframe != null) {
              Matcher m = GBIF_KEY.matcher(iframe.attr("src"));
              if (m.find()) LOG.debug("GBIF key {} for {}", m.group(1), currentSpeciesId);
            }
          }
          // div.bibentry: already pre-collected above; skip here
        }
      }
    }
  }

  /** Parses a subgenus div and writes it as a NameUsage. Returns the subgenus ID. */
  private String parseSubgenus(Element div, String genusName) throws IOException {
    Element nameEl = div.select("b i, b, i").first();
    if (nameEl == null) return genusName;
    String subgenusName = clean(nameEl.text()).replace("SUBGENUS", "").trim();
    if (StringUtils.isBlank(subgenusName)) return genusName;

    String fullText = clean(div.text());
    int nameEnd = fullText.indexOf(subgenusName);
    String afterName = nameEnd >= 0 ? fullText.substring(nameEnd + subgenusName.length()).trim() : "";
    String[] ayp = parseAuthorYear(afterName);

    writer.set(ColdpTerm.ID, subgenusName);
    writer.set(ColdpTerm.parentID, genusName);
    writer.set(ColdpTerm.rank, "subgenus");
    writer.set(ColdpTerm.scientificName, subgenusName);
    writer.set(ColdpTerm.authorship, StringUtils.trimToNull(ayp[0]));
    writer.set(ColdpTerm.publishedInYear, ayp[1]);
    writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
    writer.set(ColdpTerm.extinct, "false");
    writer.next();
    return subgenusName;
  }

  /** Parses a species div and writes the NameUsage. Returns the species ID. */
  private String parseSpecies(Element div, String parentId, String genusName, String remarks,
                               List<String> bibRefIds) throws IOException {
    Element nameEl = div.selectFirst("b i, b");
    if (nameEl == null) {
      LOG.warn("No name element in species div under {}", genusName);
      return null;
    }
    String sciName = clean(nameEl.text());
    String epithet = sciName.contains(" ") ? sciName.substring(sciName.lastIndexOf(' ') + 1) : sciName;
    String speciesId = genusName + "_" + epithet;

    String[] lines = extractBrLines(div);
    String authYear = "";
    if (lines.length > 0) {
      int nameEndIdx = lines[0].indexOf(sciName);
      authYear = nameEndIdx >= 0 ? lines[0].substring(nameEndIdx + sciName.length()).trim() : lines[0];
    }
    String[] ayp = parseAuthorYear(authYear.endsWith(".") ? authYear : authYear + ".");
    String journal = lines.length > 1 ? clean(lines[1]) : null;
    String commonName = lines.length > 2 ? clean(lines[2]) : null;

    // Extract page from the journal line on the taxon div (e.g. "Bull. Soc. Zool. France 5: 71.")
    String page = null;
    if (!StringUtils.isBlank(journal)) {
      Matcher pm = PAGE_SEP.matcher(journal);
      if (pm.matches()) page = pm.group(2).trim();
    }

    // Match nomenclatural reference from the pre-collected bibentries by year (+ author family name).
    // Remaining bibentries become referenceID links on the taxon.
    String nameRefId = null;
    List<String> otherRefIds = new ArrayList<>(bibRefIds);
    if (ayp[1] != null && !bibRefIds.isEmpty()) {
      // Family name: first token of authorship after stripping parentheses
      String auth = ayp[0] != null ? ayp[0].replaceAll("[()]", "").trim() : "";
      String family = auth.contains(",") ? auth.substring(0, auth.indexOf(',')).trim() : auth;
      String year = ayp[1];
      String bestId = null;
      for (String refId : bibRefIds) {
        String cit = refIdToCitation.get(refId);
        if (cit == null || !cit.contains(year)) continue;
        if (!family.isEmpty() && cit.toLowerCase().contains(family.toLowerCase())) {
          bestId = refId; // strong match: year + author family name
          break;
        }
        if (bestId == null) bestId = refId; // year-only match, keep searching
      }
      nameRefId = bestId;
      otherRefIds.remove(nameRefId);
    }

    writer.set(ColdpTerm.ID, speciesId);
    writer.set(ColdpTerm.parentID, parentId);
    writer.set(ColdpTerm.rank, "species");
    writer.set(ColdpTerm.scientificName, sciName);
    writer.set(ColdpTerm.authorship, StringUtils.trimToNull(ayp[0]));
    writer.set(ColdpTerm.publishedInYear, ayp[1]);
    writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
    writer.set(ColdpTerm.extinct, "false");
    writer.set(ColdpTerm.nameReferenceID, nameRefId);
    writer.set(ColdpTerm.publishedInPage, page);
    if (!otherRefIds.isEmpty()) writer.set(ColdpTerm.referenceID, String.join(",", otherRefIds));
    writer.set(ColdpTerm.link, BASE + "/genera/" + genusName);
    writer.set(ColdpTerm.remarks, remarks);
    writer.next();

    if (!StringUtils.isBlank(commonName)) {
      vnWriter.set(ColdpTerm.taxonID, speciesId);
      vnWriter.set(ColdpTerm.name, commonName);
      vnWriter.set(ColdpTerm.language, "eng");
      vnWriter.next();
    }
    return speciesId;
  }

  /** Parses a syn_group div and writes synonym NameUsage records. */
  private void parseSynGroup(Element synGroup, String acceptedId, String genusName) throws IOException {
    int idx = 0;
    for (Element child : synGroup.children()) {
      if (!"synonyms".equals(child.className())) continue;
      String synText = clean(child.text()).replaceAll("[;.]$", "").trim();
      if (StringUtils.isBlank(synText)) continue;
      Element italicEl = child.selectFirst("i");
      String synEpithet = italicEl != null ? clean(italicEl.text()) : synText.split("\\s")[0];
      String synName = genusName + " " + synEpithet;
      String afterEpithet = synText.length() > synEpithet.length()
          ? synText.substring(synEpithet.length()).trim() : "";
      String[] ayp = parseAuthorYear(afterEpithet.isEmpty() ? afterEpithet : afterEpithet + ".");

      writer.set(ColdpTerm.ID, acceptedId + "_syn_" + idx++);
      writer.set(ColdpTerm.parentID, acceptedId);
      writer.set(ColdpTerm.rank, "species");
      writer.set(ColdpTerm.scientificName, synName);
      writer.set(ColdpTerm.authorship, StringUtils.trimToNull(ayp[0]));
      writer.set(ColdpTerm.publishedInYear, ayp[1]);
      writer.set(ColdpTerm.status, TaxonomicStatus.SYNONYM);
      writer.set(ColdpTerm.extinct, "false");
      writer.next();
    }
  }

  private void writeSynonyms(String acceptedId, String synsText) throws IOException {
    if (StringUtils.isBlank(synsText)) return;
    int idx = 0;
    for (String syn : synsText.split(";")) {
      syn = syn.trim().replaceAll("\\.$", "");
      if (StringUtils.isBlank(syn)) continue;
      String synName;
      String synAuth = null;
      String synYear = null;
      int sp = syn.indexOf(' ');
      if (sp > 0) {
        synName = syn.substring(0, sp);
        String[] ayp = parseAuthorYear(syn.substring(sp + 1));
        synAuth = StringUtils.trimToNull(ayp[0]);
        synYear = ayp[1];
      } else {
        synName = syn;
      }
      if (StringUtils.isBlank(synName)) continue;
      writer.set(ColdpTerm.ID, acceptedId + "_syn_" + idx++);
      writer.set(ColdpTerm.parentID, acceptedId);
      writer.set(ColdpTerm.scientificName, synName);
      writer.set(ColdpTerm.authorship, synAuth);
      writer.set(ColdpTerm.publishedInYear, synYear);
      writer.set(ColdpTerm.status, TaxonomicStatus.SYNONYM);
      writer.set(ColdpTerm.extinct, "false");
      writer.next();
    }
  }

  // ── Shared helpers ────────────────────────────────────────────────────────

  /** Writes a Reference record, deduplicating by citation text. Returns the Reference ID. */
  private String writeRef(String citation, String link) throws IOException {
    if (StringUtils.isBlank(citation)) return null;
    String id = refCache.getOrCreate(citation, w -> w.set(ColdpTerm.link, link));
    refIdToCitation.putIfAbsent(id, citation.strip());
    return id;
  }

  private String writeBibEntry(Element bib) throws IOException {
    Element anchor = bib.selectFirst("a[href]");
    String link = anchor != null ? anchor.attr("href") : null;
    // Remove the anchor element before extracting text so link labels
    // ("Read abstract.", "Read article.", etc.) don't end up in the citation.
    Element bibClone = bib.clone();
    bibClone.select("a").remove();
    String citation = clean(bibClone.text());
    return writeRef(citation, link);
  }

  /**
   * Extracts text lines split by br elements within a div, recursing into p elements.
   */
  static String[] extractBrLines(Element div) {
    List<String> lines = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (Node node : div.childNodes()) {
      if (node instanceof TextNode tn) {
        current.append(tn.text());
      } else if (node instanceof Element el) {
        if (el.tagName().equals("br")) {
          lines.add(clean(current.toString()));
          current = new StringBuilder();
        } else if (el.tagName().equals("p")) {
          for (Node pChild : el.childNodes()) {
            if (pChild instanceof TextNode tn2) {
              current.append(tn2.text());
            } else if (pChild instanceof Element pEl) {
              if (pEl.tagName().equals("br")) {
                lines.add(clean(current.toString()));
                current = new StringBuilder();
              } else {
                current.append(pEl.text());
              }
            }
          }
        } else {
          current.append(el.text());
        }
      }
    }
    String last = clean(current.toString());
    if (!last.isEmpty()) lines.add(last);
    return lines.toArray(new String[0]);
  }

  /**
   * Parses "Author, Year. Publication" or "Author, Year" strings.
   * Returns {author, year, publication}; any component may be null.
   */
  static String[] parseAuthorYear(String raw) {
    if (StringUtils.isBlank(raw)) return new String[]{null, null, null};
    raw = raw.trim();
    Matcher m = AUTH_YEAR.matcher(raw);
    if (m.matches()) {
      // group(1)=author, group(2)=year, group(3)=optional closing ")", group(4)=publication
      String author = m.group(1);
      String year = m.group(2);
      String closingParen = m.group(3);
      // Full authorship = "Author, Year" (+ closing paren if parenthetical)
      String authorship = author + ", " + year + closingParen;
      return new String[]{
          StringUtils.trimToNull(authorship),
          year,
          StringUtils.trimToNull(m.group(4))
      };
    }
    return new String[]{StringUtils.trimToNull(raw), null, null};
  }

  /** Normalises nbsp (both well-formed &amp;nbsp; and malformed &amp;nbsp without semicolon) and excess whitespace. */
  static String clean(String s) {
    if (s == null) return null;
    return s.replace("&nbsp;", " ")
            .replace("&nbsp", " ")
            .replace('\u00a0', ' ')
            .replaceAll("\\s+", " ")
            .trim();
  }

}
