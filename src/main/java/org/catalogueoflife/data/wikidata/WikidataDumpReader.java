package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.catalogueoflife.data.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

/**
 * Streams through a Wikidata JSON dump file, providing helper methods
 * for extracting claim values and building lookup maps.
 */
public class WikidataDumpReader {
  private static final Logger LOG = LoggerFactory.getLogger(WikidataDumpReader.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * ChecklistBank identifier-scope vocabulary endpoint. Each scope carries an optional
   * {@code wikidataProperty} field linking it to a Wikidata external-identifier property,
   * which we use to assign a stable, CLB-recognised CURIE prefix to that property.
   */
  static final String ID_SCOPE_VOCAB_URL = "https://api.checklistbank.org/vocab/identifier-scope";

  /**
   * Wikidata property ID (e.g. {@code "P846"}) → ColDP identifier scope (e.g. {@code "gbif"}),
   * fetched from {@link #ID_SCOPE_VOCAB_URL} at construction time. Overrides the auto-derived
   * prefix from the formatter URL. Properties without a mapping (or if the fetch fails) fall
   * back to an auto-derived prefix (see {@link #deriveExtIdPrefix}).
   */
  private final Map<String, String> idScopeByProperty;

  public WikidataDumpReader() {
    this.idScopeByProperty = loadIdentifierScopes();
  }

  /** Fetch the Wikidata property → identifier scope mapping from the ChecklistBank API. */
  static Map<String, String> loadIdentifierScopes() {
    Map<String, String> map = new HashMap<>();
    try {
      String json = new HttpUtils().getJSON(URI.create(ID_SCOPE_VOCAB_URL));
      for (JsonNode node : MAPPER.readTree(json)) {
        JsonNode pid = node.get("wikidataProperty");
        JsonNode scope = node.get("scope");
        if (pid != null && scope != null && !pid.asText().isBlank() && !scope.asText().isBlank()) {
          map.put(pid.asText().trim(), scope.asText().trim()); // PID → scope
        }
      }
    } catch (Exception e) {
      LOG.warn("Could not load identifier scopes from {}: {}. Falling back to auto-derived prefixes.",
          ID_SCOPE_VOCAB_URL, e.getMessage());
    }
    LOG.info("Loaded {} Wikidata property → identifier scope mappings from {}", map.size(), ID_SCOPE_VOCAB_URL);
    return Collections.unmodifiableMap(map);
  }

  // Wikidata property IDs
  static final String P225 = "P225";   // taxon name
  static final String P105 = "P105";   // taxon rank
  static final String P171 = "P171";   // parent taxon
  static final String P835 = "P835";   // author citation
  static final String P566 = "P566";   // basionym
  static final String P1843 = "P1843"; // taxon common name (vernacular)
  static final String P5588 = "P5588"; // invasive to
  static final String P9714 = "P9714"; // taxon range
  static final String P141 = "P141";   // IUCN conservation status
  static final String P297 = "P297";   // ISO 3166-1 alpha-2 code
  static final String P1476 = "P1476"; // title (of publication)
  static final String P577 = "P577";   // publication date
  static final String P356 = "P356";   // DOI
  static final String P2093 = "P2093"; // author name string
  static final String P1433 = "P1433"; // published in (journal)
  static final String P478 = "P478";   // volume
  static final String P433 = "P433";   // issue
  static final String P304 = "P304";   // page(s)
  static final String P248 = "P248";   // stated in (reference)
  static final String P6184 = "P6184"; // nomenclatural status (reference qualifier)
  static final String P687 = "P687";   // BHL page ID
  static final String P31 = "P31";     // instance of
  static final String P1420 = "P1420"; // taxon synonym of (accepted name)
  static final String P694  = "P694";  // replaced synonym (this name replaced P694 value)
  static final String P1135 = "P1135"; // nomenclatural status (qualifier on P225)
  static final String P2433 = "P2433"; // gender of scientific name of a genus
  static final String P1353 = "P1353"; // original combination (original spelling)
  static final String P574  = "P574";  // year of taxon name publication (qualifier on P225)
  static final String P18   = "P18";   // image (representative taxon image on Wikimedia Commons)
  // External taxon identifier properties
  static final String P935  = "P935";  // Commons gallery
  static final String P685  = "P685";  // NCBI taxonomy ID
  static final String P846  = "P846";  // GBIF species ID
  static final String P830  = "P830";  // Encyclopedia of Life ID
  static final String P961  = "P961";  // ITIS taxonomic serial number (TSN)
  static final String P405  = "P405";  // taxon author (item) — qualifier on P225
  static final String P734  = "P734";  // family name (item)
  static final String P735  = "P735";  // given name (item)
  static final String P496  = "P496";  // ORCID iD
  static final String P569  = "P569";  // date of birth
  static final String P570  = "P570";  // date of death
  static final String P27   = "P27";   // country of citizenship
  static final String P21   = "P21";   // sex or gender
  static final String P108  = "P108";  // employer (affiliation)
  static final String P3831 = "P3831"; // object has role (qualifier) — recombination flag
  static final String P1403 = "P1403"; // original combination (basionym item)
  static final String Q14594740 = "Q14594740"; // recombination

  static final String Q1361864 = "Q1361864";   // first valid description
  static final String Q427626 = "Q427626";     // taxonomic rank
  static final String Q13442814 = "Q13442814"; // scholarly article
  static final String Q17362920 = "Q17362920"; // Wikimedia duplicated page
  static final String Q18609040 = "Q18609040"; // "Wikidata property related to taxa"

  // Taxon-property PIDs (used by Generator.loadTaxonProperties and pass-2 emission)
  static final String P523  = "P523";  // temporal range start (geological period item)
  static final String P524  = "P524";  // temporal range end
  static final String P1034 = "P1034"; // main food source (interaction: eats)
  static final String P2975 = "P2975"; // has host (interaction)
  static final String P1605 = "P1605"; // has natural reservoir (interaction)
  static final String P1606 = "P1606"; // natural reservoir of (interaction, inverse)
  static final String P2067 = "P2067"; // mass (curated extra)
  static final String P462  = "P462";  // color (curated extra)
  static final String P3485 = "P3485"; // bite force quotient (curated extra)
  static final String P788  = "P788";  // mushroom ecological type (curated extra)

  // Data records for lookup maps
  record AreaInfo(String label, String isoCode) {}
  record PubInfo(String title, String doi, String date, String author,
                 String journalQid, String volume, String issue, String pages) {}
  /** Info about a Wikidata external-identifier property (those that have a formatter URL P1630). */
  record ExtIdInfo(String prefix, String label, String formatterUrl, String formatRegex) {}
  /** A discovered taxon-describing property: its English label and Wikidata datatype. */
  record TaxonPropInfo(String label, String datatype) {}

  // Lookup maps populated during pass 1
  final Map<String, String> rankLabels = new HashMap<>();
  final Map<String, AreaInfo> areaInfo = new HashMap<>();
  final Map<String, String> iucnLabels = new HashMap<>();
  final Map<String, PubInfo> pubInfo = new HashMap<>();
  final Map<String, String> journalLabels = new HashMap<>();
  /** Nomenclatural status / gender QID → resolved English label */
  final Map<String, String> nomStatusLabels = new HashMap<>();
  /** External identifier properties discovered during pass 1: PID → info */
  final Map<String, ExtIdInfo> extIdProperties = new LinkedHashMap<>();
  /** QID → Commons gallery name (P935), populated during pass 1 */
  final Map<String, String> galleryNames = new HashMap<>();
  /**
   * synonym QID → accepted QID, populated during pass 1 from P1420 ("taxon synonym").
   * P1420 is declared on the accepted name and points to its synonyms, so the synonymy
   * direction must be reconstructed here rather than read off the synonym's own record.
   */
  final Map<String, String> synonymToAccepted = new HashMap<>();

  // Sets of QIDs needed by taxa, collected during pass 1
  final Set<String> neededPubQids = new HashSet<>();
  final Set<String> neededAreaQids = new HashSet<>();
  final Set<String> neededRankQids = new HashSet<>();
  final Set<String> neededIucnQids = new HashSet<>();
  final Set<String> neededJournalQids = new HashSet<>();
  final Set<String> neededNomStatusQids = new HashSet<>();
  /** Author QIDs referenced by P405, collected in pass 1, resolved via SPARQL. */
  final Set<String> neededAuthorQids = new HashSet<>();
  /** Author QID → resolved info, populated by SPARQL between passes. */
  final Map<String, AuthorInfo> authors = new HashMap<>();

  // Nomenclatural reference info collected during pass 1
  record NomRef(String pubQid, String page, String bhlPageLink) {}
  /** Resolved author item → ColDP Author columns. */
  record AuthorInfo(String given, String family, String abbreviationBotany,
                    String birth, String death, String country, String sex,
                    String affiliation, String orcid) {}
  /** Authorship extracted from a taxon's first P225 statement qualifiers. */
  record NameAuthorship(java.util.List<String> authorQids, String year, boolean recombination) {}
  final Map<String, NomRef> nomRefs = new HashMap<>();
  /** Property PID → info, fetched at startup via SPARQL (Generator.loadTaxonProperties). */
  final Map<String, TaxonPropInfo> taxonProps = new HashMap<>();
  /** QID → resolved English label (item-values, units, geological periods). */
  final Map<String, String> labels = new HashMap<>();
  /** QIDs whose labels are needed, collected in pass 1, resolved between passes. */
  final Set<String> neededLabels = new HashSet<>();

  /**
   * Stream through the gzipped JSON dump, applying a line-level pre-filter
   * and then parsing matching lines as JSON.
   */
  void streamDump(File gz, Predicate<String> lineFilter, Consumer<JsonNode> handler) throws IOException {
    long lineCount = 0;
    long matchCount = 0;
    try (var br = new BufferedReader(new InputStreamReader(
        new GZIPInputStream(new FileInputStream(gz), 65536), "UTF-8"), 1 << 20)) {
      String line;
      while ((line = br.readLine()) != null) {
        lineCount++;
        if (lineCount % 1_000_000 == 0) {
          LOG.info("Streamed {} million lines, {} matched", lineCount / 1_000_000, matchCount);
        }
        // Strip array delimiters and trailing commas
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.equals("[") || trimmed.equals("]")) continue;
        if (trimmed.endsWith(",")) {
          trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (!lineFilter.test(trimmed)) continue;
        try {
          JsonNode node = MAPPER.readTree(trimmed);
          if (node != null && node.has("id")) {
            matchCount++;
            handler.accept(node);
          }
        } catch (Exception e) {
          LOG.debug("Failed to parse line {}: {}", lineCount, e.getMessage());
        }
      }
    }
    LOG.info("Streaming complete: {} total lines, {} matched", lineCount, matchCount);
  }

  /**
   * Pass 1: Collect lookup maps from the dump.
   * Scans all entities to:
   * 1) Identify taxa (P225) and collect their referenced QIDs
   * 2) Collect area info (P297), rank labels, IUCN labels, pub info, journal labels
   */
  void collectLookups(File gz, Map<String, String> seedRankMap) throws IOException {
    LOG.info("Pass 1: collecting lookup maps...");
    // Seed with known rank mappings
    rankLabels.putAll(seedRankMap);

    // Pre-filter: lines containing any of these property strings
    Predicate<String> filter = line ->
        line.contains("\"P225\"") || line.contains("\"P297\"") ||
        line.contains("\"P1476\"") || line.contains("\"P31\"") ||
        line.contains("\"P1630\"") || line.contains("\"P935\""); // P935 = Commons gallery

    // Track used prefixes to ensure uniqueness; pre-seed with the CLB scopes to avoid collisions
    final Set<String> usedPrefixes = new HashSet<>(idScopeByProperty.values());
    final Set<String> ignoredPrefixes = Set.of(
        P18,     // wikicommons media
        "P4839"  // wolframalpha
    );

    streamDump(gz, filter, entity -> {
      String qid = entity.path("id").asText(null);
      if (qid == null) return;

      // Collect external identifier property metadata (P entities with formatter URL P1630)
      if (qid.startsWith("P") && !ignoredPrefixes.contains(qid) && hasClaim(entity, "P1630")) {
        String formatterUrl = getStringClaimValue(entity, "P1630");
        // Skip Wikimedia Commons properties (e.g. P373 Commons category, P935 Commons gallery):
        // these are links into Commons, not external taxon identifiers, so they make no sense as alternativeIDs.
        if (formatterUrl != null && formatterUrl.contains("commons.wikimedia.org")) {
          return;
        }
        String formatRegex = getStringClaimValue(entity, "P1793");
        String label = getEnglishLabel(entity);
        String prefix = idScopeByProperty.containsKey(qid)
            ? idScopeByProperty.get(qid)
            : deriveExtIdPrefix(formatterUrl, qid, usedPrefixes);
        if (prefix != null && formatterUrl != null) {
          extIdProperties.put(qid, new ExtIdInfo(prefix, label, formatterUrl, formatRegex));
          usedPrefixes.add(prefix);
        }
        return; // property entities don't have taxon data
      }

      // If entity has P225, it's a taxon - collect referenced QIDs
      if (hasClaim(entity, P225)) {
        collectTaxonReferences(entity, qid);
      }

      // Collect area info from entities with ISO country code (P297)
      JsonNode isoVal = getClaimValue(entity, P297);
      if (isoVal != null) {
        String iso = isoVal.asText(null);
        String label = getEnglishLabel(entity);
        if (iso != null) {
          areaInfo.put(qid, new AreaInfo(label, iso));
          neededAreaQids.remove(qid);
        }
      }

      // Check if this entity is a taxonomic rank (P31=Q427626)
      if (isInstanceOf(entity, Q427626)) {
        String label = getEnglishLabel(entity);
        if (label != null) {
          rankLabels.put(qid, label.toLowerCase());
          neededRankQids.remove(qid);
        }
      }

      // Check if this entity is an IUCN status
      String label = getEnglishLabel(entity);
      if (label != null && neededIucnQids.contains(qid)) {
        iucnLabels.put(qid, label);
        neededIucnQids.remove(qid);
      }

      // Check if this entity is a needed nom status / gender item
      if (label != null && neededNomStatusQids.contains(qid)) {
        nomStatusLabels.put(qid, label);
        neededNomStatusQids.remove(qid);
      }

      // Check if this entity is a needed publication (scholarly article with P1476)
      if (neededPubQids.contains(qid) && hasClaim(entity, P1476)) {
        collectPubInfo(entity, qid);
        neededPubQids.remove(qid);
      }

      // Check if this entity is a needed journal
      if (label != null && neededJournalQids.contains(qid)) {
        journalLabels.put(qid, label);
        neededJournalQids.remove(qid);
      }
    });

    LOG.info("Pass 1 complete. Ranks: {}, Areas: {}, IUCN: {}, Pubs: {}, Journals: {}, ExtIdProps: {}",
        rankLabels.size(), areaInfo.size(), iucnLabels.size(), pubInfo.size(), journalLabels.size(), extIdProperties.size());
    LOG.info("Unresolved: Ranks: {}, Areas: {}, IUCN: {}, Pubs: {}, Journals: {}, NomStatus: {}",
        neededRankQids.size(), neededAreaQids.size(), neededIucnQids.size(),
        neededPubQids.size(), neededJournalQids.size(), neededNomStatusQids.size());
  }

  private void collectTaxonReferences(JsonNode entity, String taxonQid) {
    // Collect rank QID
    JsonNode rankVal = getClaimValue(entity, P105);
    if (rankVal != null) {
      String rankQid = getItemId(rankVal);
      if (rankQid != null && !rankLabels.containsKey(rankQid)) {
        neededRankQids.add(rankQid);
      }
    }

    // Collect parent QID (not needed for lookup, just tracking)
    // Collect basionym QID (not needed for lookup)

    // Record synonym → accepted links from P1420 (declared on the accepted name)
    collectSynonymLinks(entity, taxonQid, synonymToAccepted);
    // Collect P405 author QIDs for SPARQL resolution between passes
    collectAuthorRefs(entity, neededAuthorQids);
    collectPropertyLabelRefs(entity, taxonProps, neededLabels);

    // Collect IUCN status QID
    JsonNode iucnVal = getClaimValue(entity, P141);
    if (iucnVal != null) {
      String iucnQid = getItemId(iucnVal);
      if (iucnQid != null && !iucnLabels.containsKey(iucnQid)) {
        neededIucnQids.add(iucnQid);
      }
    }

    // Collect area QIDs from distributions
    for (JsonNode val : getClaimValues(entity, P5588)) {
      String areaQid = getItemId(val);
      if (areaQid != null && !areaInfo.containsKey(areaQid)) {
        neededAreaQids.add(areaQid);
      }
    }
    for (JsonNode val : getClaimValues(entity, P9714)) {
      String areaQid = getItemId(val);
      if (areaQid != null && !areaInfo.containsKey(areaQid)) {
        neededAreaQids.add(areaQid);
      }
    }

    // Collect nom status and gender QIDs from P225 statement qualifiers and direct P2433 property
    JsonNode claims = entity.path("claims").path(P225);
    if (claims.isArray()) {
      for (JsonNode stmt : claims) {
        JsonNode qualifiers = stmt.path("qualifiers");
        // Nomenclatural status (P1135) qualifier → needs label resolution
        String nomStatQid = getSnakItemId(qualifiers, P1135);
        if (nomStatQid != null && !nomStatusLabels.containsKey(nomStatQid)) {
          neededNomStatusQids.add(nomStatQid);
        }
        // Gender (P2433) qualifier → needs label resolution
        String genderQid = getSnakItemId(qualifiers, P2433);
        if (genderQid != null && !nomStatusLabels.containsKey(genderQid)) {
          neededNomStatusQids.add(genderQid);
        }
      }
    }
    // Also collect P2433 as a direct property (some taxa store gender outside P225 qualifiers)
    JsonNode genderVal = getClaimValue(entity, P2433);
    if (genderVal != null) {
      String genderQid = getItemId(genderVal);
      if (genderQid != null && !nomStatusLabels.containsKey(genderQid)) {
        neededNomStatusQids.add(genderQid);
      }
    }

    // Collect nomenclatural reference info from P225 statement references
    if (claims.isArray()) {
      for (JsonNode stmt : claims) {
        JsonNode refs = stmt.path("references");
        if (!refs.isArray()) continue;
        for (JsonNode ref : refs) {
          JsonNode snaks = ref.path("snaks");
          // Check for P6184 = Q1361864 (first valid description)
          JsonNode roleSnaks = snaks.path(P6184);
          if (!roleSnaks.isArray()) continue;
          boolean isFirstDesc = false;
          for (JsonNode roleSnak : roleSnaks) {
            String roleQid = getItemId(roleSnak.path("datavalue").path("value"));
            if (Q1361864.equals(roleQid)) {
              isFirstDesc = true;
              break;
            }
          }
          if (!isFirstDesc) continue;

          // Get publication QID
          JsonNode pubSnaks = snaks.path(P248);
          if (!pubSnaks.isArray() || pubSnaks.isEmpty()) continue;
          String pubQid = getItemId(pubSnaks.get(0).path("datavalue").path("value"));
          if (pubQid == null) continue;

          neededPubQids.add(pubQid);

          if (!nomRefs.containsKey(taxonQid)) {
            String page = getSnakStringValue(snaks, P304);
            String bhlPageId = getSnakStringValue(snaks, P687);
            String bhlLink = bhlPageId != null ? "https://www.biodiversitylibrary.org/page/" + bhlPageId : null;
            nomRefs.put(taxonQid, new NomRef(pubQid, page, bhlLink));
          }
          break; // only take first matching reference
        }
      }
    }

    // Collect Commons gallery name (P935)
    JsonNode galleryVal = getClaimValue(entity, P935);
    if (galleryVal != null) {
      String gallery = galleryVal.asText(null);
      if (gallery != null) {
        galleryNames.put(taxonQid, gallery);
      }
    }

    // Collect distribution reference pub QIDs from P9714 statement references
    JsonNode rangeStmts = entity.path("claims").path(P9714);
    if (rangeStmts.isArray()) {
      for (JsonNode stmt : rangeStmts) {
        JsonNode refs = stmt.path("references");
        if (!refs.isArray()) continue;
        for (JsonNode ref : refs) {
          JsonNode pubSnaks = ref.path("snaks").path(P248);
          if (pubSnaks.isArray()) {
            for (JsonNode pubSnak : pubSnaks) {
              String pubQid = getItemId(pubSnak.path("datavalue").path("value"));
              if (pubQid != null) {
                neededPubQids.add(pubQid);
              }
            }
          }
        }
      }
    }
  }

  private void collectPubInfo(JsonNode entity, String qid) {
    String title = getMonolingualTextOrString(getClaimValue(entity, P1476));
    String doi = getStringClaimValue(entity, P356);
    String date = getStringClaimValue(entity, P577);
    String author = getStringClaimValue(entity, P2093);
    String volume = getStringClaimValue(entity, P478);
    String issue = getStringClaimValue(entity, P433);
    String pages = getStringClaimValue(entity, P304);

    String journalQid = null;
    JsonNode journalVal = getClaimValue(entity, P1433);
    if (journalVal != null) {
      journalQid = getItemId(journalVal);
      if (journalQid != null) {
        neededJournalQids.add(journalQid);
      }
    }

    pubInfo.put(qid, new PubInfo(title, doi, date, author, journalQid, volume, issue, pages));
  }

  // --- JSON extraction helpers ---

  static boolean hasClaim(JsonNode entity, String prop) {
    JsonNode claims = entity.path("claims").path(prop);
    return claims.isArray() && !claims.isEmpty();
  }

  /**
   * Get the first claim's mainsnak datavalue.value for a property.
   */
  static JsonNode getClaimValue(JsonNode entity, String prop) {
    JsonNode claims = entity.path("claims").path(prop);
    if (!claims.isArray() || claims.isEmpty()) return null;
    return claims.get(0).path("mainsnak").path("datavalue").path("value");
  }

  /**
   * Get all claims' mainsnak datavalue.value for a property.
   */
  static List<JsonNode> getClaimValues(JsonNode entity, String prop) {
    List<JsonNode> values = new ArrayList<>();
    JsonNode claims = entity.path("claims").path(prop);
    if (claims.isArray()) {
      for (JsonNode claim : claims) {
        JsonNode val = claim.path("mainsnak").path("datavalue").path("value");
        if (!val.isMissingNode()) {
          values.add(val);
        }
      }
    }
    return values;
  }

  /**
   * Record the synonym→accepted links declared by an accepted taxon via P1420 ("taxon synonym").
   * P1420 points from the accepted name to each of its synonyms, so for an entity {@code acceptedQid}
   * every P1420 value QID is a synonym whose accepted name is {@code acceptedQid}.
   */
  static void collectSynonymLinks(JsonNode entity, String acceptedQid, Map<String, String> out) {
    if (acceptedQid == null) return;
    for (JsonNode val : getClaimValues(entity, P1420)) {
      String synonymQid = getItemId(val);
      if (synonymQid != null && !synonymQid.equals(acceptedQid)) {
        out.put(synonymQid, acceptedQid);
      }
    }
  }

  /** Collect P405 author QIDs of a taxon into {@code out} (for later SPARQL resolution). */
  static void collectAuthorRefs(JsonNode entity, Set<String> out) {
    NameAuthorship na = extractAuthorship(entity);
    if (na != null) out.addAll(na.authorQids());
  }

  /**
   * Extract QID from a wikibase-entityid value: {"entity-type":"item","id":"Q123"}
   */
  static String getItemId(JsonNode datavalue) {
    if (datavalue == null || datavalue.isMissingNode()) return null;
    JsonNode id = datavalue.path("id");
    return id.isMissingNode() ? null : id.asText(null);
  }

  /**
   * Extract text and language from a monolingualtext value.
   */
  static String[] getMonolingualText(JsonNode datavalue) {
    if (datavalue == null || datavalue.isMissingNode()) return null;
    String text = datavalue.path("text").asText(null);
    String lang = datavalue.path("language").asText(null);
    if (text == null) return null;
    return new String[]{text, lang};
  }

  /**
   * Get text from a monolingualtext or plain string value.
   */
  static String getMonolingualTextOrString(JsonNode datavalue) {
    if (datavalue == null || datavalue.isMissingNode()) return null;
    // monolingualtext has a "text" field
    if (datavalue.has("text")) {
      return datavalue.path("text").asText(null);
    }
    // plain string
    return datavalue.asText(null);
  }

  /**
   * Get the English label from an entity's labels.
   */
  static String getEnglishLabel(JsonNode entity) {
    return entity.path("labels").path("en").path("value").asText(null);
  }

  /**
   * Check if entity is an instance of a given type QID (via P31).
   */
  static boolean isInstanceOf(JsonNode entity, String typeQid) {
    for (JsonNode val : getClaimValues(entity, P31)) {
      String qid = getItemId(val);
      if (typeQid.equals(qid)) return true;
    }
    return false;
  }

  /**
   * Get a string value from the first claim of a property.
   */
  static String getStringClaimValue(JsonNode entity, String prop) {
    JsonNode val = getClaimValue(entity, prop);
    if (val == null || val.isMissingNode()) return null;
    // time values have a nested structure
    if (val.has("time")) {
      return val.path("time").asText(null);
    }
    // plain string or quantity
    if (val.isTextual()) {
      return val.asText(null);
    }
    // monolingualtext
    if (val.has("text")) {
      return val.path("text").asText(null);
    }
    return val.asText(null);
  }

  /**
   * Get a string value from qualifier/reference snaks (time, string, monolingualtext).
   */
  static String getSnakStringValue(JsonNode snaks, String prop) {
    JsonNode propSnaks = snaks.path(prop);
    if (!propSnaks.isArray() || propSnaks.isEmpty()) return null;
    JsonNode val = propSnaks.get(0).path("datavalue").path("value");
    if (val.isMissingNode()) return null;
    if (val.isTextual()) return val.asText(null);
    if (val.has("time")) return val.path("time").asText(null);
    if (val.has("text")) return val.path("text").asText(null);
    return val.asText(null);
  }

  /**
   * Get a Wikidata item ID (QID) from qualifier/reference snaks.
   */
  static String getSnakItemId(JsonNode snaks, String prop) {
    JsonNode propSnaks = snaks.path(prop);
    if (!propSnaks.isArray() || propSnaks.isEmpty()) return null;
    return getItemId(propSnaks.get(0).path("datavalue").path("value"));
  }

  /**
   * Extract authorship from the first P225 statement's qualifiers:
   * P405 author item(s), P574 year, P3831=Q14594740 recombination flag.
   * Returns null when the entity has no P225 statement.
   */
  static NameAuthorship extractAuthorship(JsonNode entity) {
    JsonNode claims = entity.path("claims").path(P225);
    if (!claims.isArray() || claims.isEmpty()) return null;
    JsonNode q = claims.get(0).path("qualifiers");
    java.util.List<String> authorQids = new java.util.ArrayList<>();
    JsonNode p405 = q.path(P405);
    if (p405.isArray()) {
      for (JsonNode snak : p405) {
        String id = getItemId(snak.path("datavalue").path("value"));
        if (id != null) authorQids.add(id);
      }
    }
    String year = WikidataMappings.extractYear(getSnakStringValue(q, P574));
    boolean recomb = Q14594740.equals(getSnakItemId(q, P3831));
    return new NameAuthorship(authorQids, year, recomb);
  }

  /** Amount string of a Wikidata quantity value (e.g. "+250"), or null. */
  static String quantityAmount(JsonNode value) {
    if (value == null) return null;
    return value.path("amount").asText(null);
  }

  /**
   * Collect QIDs whose labels are needed for taxon-property and temporal-range emission:
   * item-values of WikibaseItem taxon properties, units of Quantity taxon properties,
   * and the geological-period values of P523/P524.
   */
  static void collectPropertyLabelRefs(JsonNode entity, Map<String, TaxonPropInfo> taxonProps, Set<String> out) {
    JsonNode claims = entity.path("claims");
    claims.fieldNames().forEachRemaining(pid -> {
      TaxonPropInfo info = taxonProps.get(pid);
      if (info == null) return;
      for (JsonNode val : getClaimValues(entity, pid)) {
        if ("WikibaseItem".equals(info.datatype())) {
          String q = getItemId(val);
          if (q != null) out.add(q);
        } else if ("Quantity".equals(info.datatype())) {
          String u = quantityUnitQid(val);
          if (u != null) out.add(u);
        }
      }
    });
    for (String tp : new String[]{P523, P524}) {
      for (JsonNode val : getClaimValues(entity, tp)) {
        String q = getItemId(val);
        if (q != null) out.add(q);
      }
    }
  }

  /** Unit QID of a quantity value, or null when dimensionless ("1"/absent). */
  static String quantityUnitQid(JsonNode value) {
    if (value == null) return null;
    String unit = value.path("unit").asText(null);
    if (unit == null || unit.isBlank() || unit.equals("1")) return null;
    int slash = unit.lastIndexOf('/');
    return slash >= 0 ? unit.substring(slash + 1) : unit;
  }

  /** Build [property, value] rows for all taxon-property claims on an entity. */
  static java.util.List<String[]> taxonPropertyRows(JsonNode entity,
      Map<String, TaxonPropInfo> taxonProps, Map<String, String> labels) {
    java.util.List<String[]> rows = new java.util.ArrayList<>();
    JsonNode claims = entity.path("claims");
    java.util.Iterator<String> pids = claims.fieldNames();
    while (pids.hasNext()) {
      String pid = pids.next();
      TaxonPropInfo info = taxonProps.get(pid);
      if (info == null) continue;
      for (JsonNode val : getClaimValues(entity, pid)) {
        String value = switch (info.datatype()) {
          case "WikibaseItem" -> {
            String q = getItemId(val);
            yield q == null ? null : labels.getOrDefault(q, q);
          }
          case "Quantity" -> {
            String unitQid = quantityUnitQid(val);
            String unitLabel = unitQid == null ? null : labels.get(unitQid);
            yield WikidataMappings.formatQuantity(quantityAmount(val), unitLabel);
          }
          case "String" -> val.isTextual() ? val.asText(null) : null;
          case "Monolingualtext" -> val.path("text").asText(null);
          default -> null;
        };
        if (value != null && !value.isBlank() && info.label() != null) {
          rows.add(new String[]{info.label(), value});
        }
      }
    }
    return rows;
  }

  /** Build [type, relatedTaxonID] rows for the interaction properties on an entity. */
  static java.util.List<String[]> speciesInteractionRows(JsonNode entity) {
    java.util.List<String[]> rows = new java.util.ArrayList<>();
    for (String pid : new String[]{P1034, P2975, P1605, P1606}) {
      String type = WikidataMappings.interactionType(pid);
      for (JsonNode val : getClaimValues(entity, pid)) {
        String related = getItemId(val);
        if (related != null) rows.add(new String[]{type, related});
      }
    }
    return rows;
  }

  /**
   * Derive a short unique CURIE prefix for an external identifier property.
   * Tries to extract a meaningful slug from the formatter URL host, falling back to the PID.
   */
  static String deriveExtIdPrefix(String formatterUrl, String pid, Set<String> usedPrefixes) {
    String prefix = null;
    if (formatterUrl != null) {
      try {
        String url = formatterUrl.replace("$1", "placeholder");
        URI uri = URI.create(url);
        String host = uri.getHost();
        if (host != null) {
          // Strip common non-distinctive sub-domains
          host = host.replaceFirst("^(www|data|species|api|taxa|taxonomy|search|portal|itis|eol)\\.", "");
          // Take the first segment before first dot
          String[] parts = host.split("\\.");
          prefix = parts[0].toLowerCase().replaceAll("[^a-z0-9]", "");
          if (prefix.isBlank()) prefix = null;
        }
      } catch (Exception ignored) {
        // fall through to PID fallback
      }
    }
    if (prefix == null) prefix = pid.toLowerCase();
    // Ensure uniqueness by appending PID suffix if needed
    if (usedPrefixes.contains(prefix)) {
      prefix = prefix + "_" + pid.toLowerCase();
    }
    return prefix;
  }
}
