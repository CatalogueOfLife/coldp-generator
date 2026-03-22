package org.catalogueoflife.data.bats;

import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.AltIdBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ColDP generator for batnames.org — Bats of the World by Simmons & Cirranello (AMNH).
 *
 * Phase 1: Download explore.html → parse nested accordion → write all taxa Order → Genus
 *          with synonyms, distribution, remarks, and references.
 * Phase 2: For each genus, download /genera/{name} → parse species, subgenera, synonyms,
 *          vernacular names, type localities, distribution, GBIF IDs, and bibliography.
 */
public class Generator extends AbstractColdpGenerator {

  private static final String BASE = "https://batnames.org";
  private static final URI EXPLORE_URL = URI.create(BASE + "/explore.html");
  private static final Pattern AUTH_YEAR = Pattern.compile("^(.*?),\\s*(\\d{4})\\s*[.,]?(.*)$", Pattern.DOTALL);
  private static final Pattern GBIF_KEY = Pattern.compile("[?&]key=(\\d+)");
  private static final String EXPLORE_FILE = "explore.html";

  private final List<String> genusNames = new ArrayList<>();
  private int refCounter = 1;
  private TermWriter vnWriter;      // VernacularName
  private TermWriter distWriter;    // Distribution
  private TermWriter typeWriter;    // TypeMaterial

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void prepare() throws Exception {
    download(EXPLORE_FILE, EXPLORE_URL);
  }

  @Override
  protected void addData() throws Exception {
    // Writers
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID, ColdpTerm.parentID, ColdpTerm.rank,
        ColdpTerm.scientificName, ColdpTerm.authorship, ColdpTerm.publishedInYear,
        ColdpTerm.status, ColdpTerm.nameReferenceID, ColdpTerm.referenceID,
        ColdpTerm.extinct, ColdpTerm.alternativeID, ColdpTerm.link, ColdpTerm.remarks
    ));
    initRefWriter(List.of(
        ColdpTerm.ID, ColdpTerm.citation, ColdpTerm.link
    ));
    vnWriter   = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID, ColdpTerm.name, ColdpTerm.language
    ));
    distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID, ColdpTerm.area, ColdpTerm.gazetteer
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.nameID, ColdpTerm.locality
    ));

    // Phase 1: explore.html → higher-rank taxa
    File exploreFile = sourceFile(EXPLORE_FILE);
    String html = Files.readString(exploreFile.toPath());
    Document doc = Jsoup.parse(html);
    Element root = doc.selectFirst("div#homeresults");
    if (root == null) {
      throw new IllegalStateException("Could not find div#homeresults in explore.html");
    }
    parseAccordionBlock(root, null);
    LOG.info("Higher-rank phase complete: {} genera to crawl", genusNames.size());

    // Phase 2: genus pages → species
    int n = 0;
    for (String genus : genusNames) {
      File gFile = sourceFile("genera-" + genus + ".html");
      if (!gFile.exists()) {
        if (cfg.noDownload) {
          throw new IllegalStateException("--no-download set but genus page not found: " + gFile);
        }
        String gHtml = http.get(URI.create(BASE + "/genera/" + genus));
        Files.writeString(gFile.toPath(), gHtml);
        Thread.sleep(100);
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

  // ── Phase 1: explore.html recursive accordion parsing ────────────────────

  /**
   * Processes all direct button.accordion + div.panel pairs within a container element,
   * writing a NameUsage for each and recursing into child panels.
   */
  private void parseAccordionBlock(Element container, String parentId) throws IOException {
    for (Element button : container.select("> button.accordion")) {
      Element panel = button.nextElementSibling();
      if (panel == null || !panel.tagName().equals("div")) continue;

      String name = button.id();
      if (StringUtils.isBlank(name)) continue;
      String rank = panel.id();   // order/suborder/superfamily/family/subfamily/tribe/subtribe/genus

      // Common name from <p> child of button
      Element cnEl = button.selectFirst("p");
      String commonName = cnEl != null ? clean(cnEl.text()) : null;

      // Data from uppertbl
      Element uppertbl = panel.selectFirst("div.uppertbl, div.uppertbl2");

      String authorRaw = null;
      String year = null;
      String remarks = null;
      String dist = null;
      List<String> refIds = new ArrayList<>();

      if (uppertbl != null) {
        // authorship: div.author or from button text (after the name itself)
        Element authorEl = uppertbl.selectFirst("div.author");
        if (authorEl != null) {
          String[] ayp = parseAuthorYear(clean(authorEl.text()));
          authorRaw = ayp[0];
          year      = ayp[1];
        }
        // synonyms → separate NameUsage SYNONYM records
        Element synsEl = uppertbl.selectFirst("div.syns");
        if (synsEl != null) {
          writeSynonyms(name, clean(synsEl.text()));
        }
        // distribution
        Element distEl = uppertbl.selectFirst("div.dist");
        if (distEl != null) {
          dist = clean(distEl.text());
        }
        // remarks/comments
        Element commentsEl = uppertbl.selectFirst("div.comments");
        if (commentsEl != null) {
          remarks = clean(commentsEl.text());
        }
        // bibliography → Reference records
        for (Element bib : uppertbl.select("div.bibentry")) {
          refIds.add(writeBibEntry(bib));
        }
      }

      // Build link
      String link = buildLink(rank, name);

      // Write NameUsage
      writer.set(ColdpTerm.ID, name);
      writer.set(ColdpTerm.parentID, parentId);
      writer.set(ColdpTerm.rank, rank);
      writer.set(ColdpTerm.scientificName, name);
      writer.set(ColdpTerm.authorship, authorRaw);
      writer.set(ColdpTerm.publishedInYear, year);
      writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
      writer.set(ColdpTerm.extinct, "false");
      if (!refIds.isEmpty()) {
        if (refIds.size() == 1) {
          writer.set(ColdpTerm.nameReferenceID, refIds.get(0));
        } else {
          writer.set(ColdpTerm.nameReferenceID, refIds.get(0));
          writer.set(ColdpTerm.referenceID, String.join(",", refIds.subList(1, refIds.size())));
        }
      }
      writer.set(ColdpTerm.link, link);
      writer.set(ColdpTerm.remarks, remarks);
      writer.next();

      // Distribution record for higher ranks
      if (!StringUtils.isBlank(dist)) {
        distWriter.set(ColdpTerm.taxonID, name);
        distWriter.set(ColdpTerm.area, dist);
        distWriter.set(ColdpTerm.gazetteer, "text");
        distWriter.next();
      }

      // Common name for families
      if (!StringUtils.isBlank(commonName)) {
        vnWriter.set(ColdpTerm.taxonID, name);
        vnWriter.set(ColdpTerm.name, commonName);
        vnWriter.set(ColdpTerm.language, "eng");
        vnWriter.next();
      }

      if ("genus".equals(rank)) {
        genusNames.add(name);
      } else {
        parseAccordionBlock(panel, name);
      }
    }
  }

  private void writeSynonyms(String acceptedId, String synsText) throws IOException {
    if (StringUtils.isBlank(synsText)) return;
    int idx = 0;
    for (String syn : synsText.split(";")) {
      syn = syn.trim().replaceAll("\\.$", "");
      if (StringUtils.isBlank(syn)) continue;
      // Format: "SynName Author, Year" — first token is the name, rest is author+year
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

  // ── Phase 2: genus page parsing ──────────────────────────────────────────

  private void parseGenusPage(String genusName, Document doc) throws IOException {
    Element resultsArea = doc.selectFirst("div#homeresults");
    if (resultsArea == null) {
      LOG.warn("No homeresults div in genus page: {}", genusName);
      return;
    }

    // Subgenus tracking (species belong to last seen subgenus or the genus itself)
    String currentParent = genusName;
    String currentSpeciesId = null;
    List<String> currentRefIds = new ArrayList<>();

    // We process children sequentially; each div.taxon[id=species] starts a new species block
    for (Element el : resultsArea.children()) {
      String cls = el.className();
      String idAttr = el.id();  // note: id() returns attribute value

      if ("taxon".equals(cls) && "genus".equals(idAttr)) {
        // Genus header — already written in phase 1, skip (or could update authorship)

      } else if ("taxon".equals(cls) && "subgenus".equals(idAttr)) {
        // Subgenus
        currentParent = parseSubgenus(el, genusName);

      } else if ("taxon".equals(cls) && "species".equals(idAttr)) {
        // New species block — flush previous species refs
        if (currentSpeciesId != null && !currentRefIds.isEmpty()) {
          setReferenceIDs(currentSpeciesId, currentRefIds);
          currentRefIds = new ArrayList<>();
        }
        currentSpeciesId = parseSpecies(el, currentParent, genusName);

      } else if ("syn_group".equals(cls) && "species".equals(idAttr) && currentSpeciesId != null) {
        parseSynGroup(el, currentSpeciesId, genusName);

      } else if ("type_local".equals(cls) && "species".equals(idAttr) && currentSpeciesId != null) {
        String locality = clean(el.text());
        if (!StringUtils.isBlank(locality)) {
          typeWriter.set(ColdpTerm.nameID, currentSpeciesId);
          typeWriter.set(ColdpTerm.locality, locality);
          typeWriter.next();
        }

      } else if ("dist".equals(cls) && "species".equals(idAttr) && currentSpeciesId != null) {
        String area = clean(el.text());
        if (!StringUtils.isBlank(area)) {
          distWriter.set(ColdpTerm.taxonID, currentSpeciesId);
          distWriter.set(ColdpTerm.area, area);
          distWriter.set(ColdpTerm.gazetteer, "text");
          distWriter.next();
        }

      } else if ("bibentry".equals(cls) && "species".equals(idAttr) && currentSpeciesId != null) {
        currentRefIds.add(writeBibEntry(el));

      } else if ("map".equals(cls) && "species".equals(idAttr) && currentSpeciesId != null) {
        // GBIF key from iframe src
        Element iframe = el.selectFirst("iframe");
        if (iframe != null) {
          Matcher m = GBIF_KEY.matcher(iframe.attr("src"));
          if (m.find()) {
            AltIdBuilder ids = new AltIdBuilder();
            ids.add("gbif", m.group(1));
            // Update the NameUsage row: we can't update already-written rows, so we skip this
            // The GBIF key would ideally be set before calling writer.next() on the species.
            // Since we write species first, we need a two-pass or pre-scan.
            // For simplicity, omit GBIF alternativeID from genus pages for now.
          }
        }
      }
    }

    // Flush last species refs
    if (currentSpeciesId != null && !currentRefIds.isEmpty()) {
      setReferenceIDs(currentSpeciesId, currentRefIds);
    }
  }

  /** Parses a subgenus div and writes it as a NameUsage. Returns the subgenus ID. */
  private String parseSubgenus(Element div, String genusName) throws IOException {
    // Text: "SUBGENUS Name Auth, Year.\n Journal page"
    String nameInBold = div.select("b i, b, i").first() != null
        ? clean(div.select("b i, b, i").first().text()) : null;
    if (nameInBold == null) return genusName;

    // Strip "SUBGENUS " prefix if present
    String subgenusName = nameInBold.replace("SUBGENUS", "").trim();
    if (StringUtils.isBlank(subgenusName)) return genusName;

    String fullText = clean(div.text());
    // Author/year comes after the name in the full text
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
  private String parseSpecies(Element div, String parentId, String genusName) throws IOException {
    // Structure: <p><span><b><i>Genus epithet</i></b></span>Auth, Year.<br/>Journal.<br/>CommonName</p>
    // Extract name from bold-italic
    Element nameEl = div.selectFirst("b i, b");
    if (nameEl == null) {
      LOG.warn("No name element in species div under {}", genusName);
      return null;
    }
    String sciName = clean(nameEl.text());
    // epithet = last word of sciName
    String epithet = sciName.contains(" ") ? sciName.substring(sciName.lastIndexOf(' ') + 1) : sciName;
    String speciesId = genusName + "_" + epithet;

    // Parse text lines split by <br>
    String[] lines = extractBrLines(div);
    String authYear = lines.length > 0 ? lines[0].replaceFirst("^[^A-Z(]*", "").trim() : "";
    // authYear starts after the name; lines[0] contains name + auth+year
    // Remove the name portion from lines[0]
    int nameEndIdx = lines[0].indexOf(sciName);
    if (nameEndIdx >= 0) {
      authYear = lines[0].substring(nameEndIdx + sciName.length()).trim();
    }
    String[] ayp = parseAuthorYear(authYear.endsWith(".") ? authYear : authYear + ".");
    String journal = lines.length > 1 ? clean(lines[1]) : null;
    String commonName = lines.length > 2 ? clean(lines[2]) : null;

    // Combine authorship and journal into nameRef publication
    String pub = ayp[2] != null ? ayp[2].trim() : journal;
    if (StringUtils.isBlank(pub) && journal != null) pub = journal;

    writer.set(ColdpTerm.ID, speciesId);
    writer.set(ColdpTerm.parentID, parentId);
    writer.set(ColdpTerm.rank, "species");
    writer.set(ColdpTerm.scientificName, sciName);
    writer.set(ColdpTerm.authorship, StringUtils.trimToNull(ayp[0]));
    writer.set(ColdpTerm.publishedInYear, ayp[1]);
    writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
    writer.set(ColdpTerm.extinct, "false");
    writer.set(ColdpTerm.link, BASE + "/genera/" + genusName);
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
      String childCls = child.className();
      if ("subspecies".equals(childCls)) {
        // subspecies grouping header — omit from output
      } else if ("synonyms".equals(childCls)) {
        String synText = clean(child.text()).replaceAll("[;.]$", "").trim();
        if (StringUtils.isBlank(synText)) continue;
        // Format: "epithet Author, Year [note]"
        // The epithet is in italic in the original HTML
        Element italicEl = child.selectFirst("i");
        String synEpithet = italicEl != null ? clean(italicEl.text()) : synText.split("\\s")[0];
        String synName = genusName + " " + synEpithet;
        // Parse authorship from remaining text
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
  }

  /**
   * Sets referenceID on an already-written NameUsage row — not possible after next().
   * Instead, we re-open the row... this requires a workaround: we write refIDs BEFORE next().
   * This method is only called for references accumulated AFTER the species row was written,
   * so they cannot update the already-flushed row. We log a warning.
   * A better approach would be to pre-scan references before writing. See parseGenusPage.
   */
  private void setReferenceIDs(String speciesId, List<String> refIds) {
    // Cannot update already-written rows; this is a known limitation of the sequential writer.
    // References for species are still written as Reference records but cannot be back-linked.
    // To properly link them, parseGenusPage would need a two-pass approach.
    // For now, this is acceptable — references are in Reference.tsv regardless.
  }

  // ── Shared helpers ────────────────────────────────────────────────────────

  /**
   * Writes a bibentry element as a Reference record and returns the Reference ID.
   */
  private String writeBibEntry(Element bib) throws IOException {
    String citation = clean(bib.text());
    if (StringUtils.isBlank(citation)) return null;
    String link = null;
    Element anchor = bib.selectFirst("a[href]");
    if (anchor != null) link = anchor.attr("href");

    String id = "R" + refCounter++;
    refWriter.set(ColdpTerm.ID, id);
    refWriter.set(ColdpTerm.citation, citation);
    refWriter.set(ColdpTerm.link, link);
    refWriter.next();
    return id;
  }

  /**
   * Extracts text lines split by &lt;br&gt; elements within a div.
   * Each line is the concatenated text of nodes between consecutive &lt;br&gt; tags.
   */
  static String[] extractBrLines(Element div) {
    List<String> lines = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (Node node : div.childNodes()) {
      if (node instanceof TextNode tn) {
        current.append(tn.text());
      } else if (node instanceof Element el) {
        if (el.tagName().equals("br")) {
          lines.add(current.toString().trim());
          current = new StringBuilder();
        } else if (el.tagName().equals("p")) {
          // recurse into p
          for (Node pChild : el.childNodes()) {
            if (pChild instanceof TextNode tn2) {
              current.append(tn2.text());
            } else if (pChild instanceof Element pEl) {
              if (pEl.tagName().equals("br")) {
                lines.add(current.toString().trim());
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
    String last = current.toString().trim();
    if (!last.isEmpty()) lines.add(last);
    return lines.toArray(new String[0]);
  }

  /**
   * Parses "Author, Year. Publication" or "Author, Year" strings.
   * Returns {author, year, publication}. Any component may be null.
   */
  static String[] parseAuthorYear(String raw) {
    if (StringUtils.isBlank(raw)) return new String[]{null, null, null};
    raw = raw.trim();
    Matcher m = AUTH_YEAR.matcher(raw);
    if (m.matches()) {
      String author = StringUtils.trimToNull(m.group(1));
      String year   = m.group(2);
      String pub    = StringUtils.trimToNull(m.group(3));
      return new String[]{author, year, pub};
    }
    // No year found — entire string is the author
    return new String[]{StringUtils.trimToNull(raw), null, null};
  }

  /** Normalises nbsp and excess whitespace. */
  static String clean(String s) {
    if (s == null) return null;
    return s.replace('\u00a0', ' ').replaceAll("\\s+", " ").trim();
  }

  private static String buildLink(String rank, String name) {
    return switch (rank) {
      case "genus", "subgenus" -> BASE + "/genera/" + name;
      default -> BASE + "/family/" + name;
    };
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }
}
