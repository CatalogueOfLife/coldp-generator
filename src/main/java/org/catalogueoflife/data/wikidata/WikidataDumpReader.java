package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

  static final String Q1361864 = "Q1361864"; // first valid description
  static final String Q427626 = "Q427626";   // taxonomic rank
  static final String Q13442814 = "Q13442814"; // scholarly article

  // Data records for lookup maps
  record AreaInfo(String label, String isoCode) {}
  record PubInfo(String title, String doi, String date, String author,
                 String journalQid, String volume, String issue, String pages) {}

  // Lookup maps populated during pass 1
  final Map<String, String> rankLabels = new HashMap<>();
  final Map<String, AreaInfo> areaInfo = new HashMap<>();
  final Map<String, String> iucnLabels = new HashMap<>();
  final Map<String, PubInfo> pubInfo = new HashMap<>();
  final Map<String, String> journalLabels = new HashMap<>();

  // Sets of QIDs needed by taxa, collected during pass 1
  final Set<String> neededPubQids = new HashSet<>();
  final Set<String> neededAreaQids = new HashSet<>();
  final Set<String> neededRankQids = new HashSet<>();
  final Set<String> neededIucnQids = new HashSet<>();
  final Set<String> neededJournalQids = new HashSet<>();

  // Nomenclatural reference info collected during pass 1
  record NomRef(String pubQid, String page, String bhlPageLink) {}
  final Map<String, NomRef> nomRefs = new HashMap<>();

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
        line.contains("\"P1476\"") || line.contains("\"P31\"");

    streamDump(gz, filter, entity -> {
      String qid = entity.path("id").asText(null);
      if (qid == null) return;

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

    LOG.info("Pass 1 complete. Ranks: {}, Areas: {}, IUCN: {}, Pubs: {}, Journals: {}",
        rankLabels.size(), areaInfo.size(), iucnLabels.size(), pubInfo.size(), journalLabels.size());
    LOG.info("Unresolved: Ranks: {}, Areas: {}, IUCN: {}, Pubs: {}, Journals: {}",
        neededRankQids.size(), neededAreaQids.size(), neededIucnQids.size(),
        neededPubQids.size(), neededJournalQids.size());
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

    // Collect nomenclatural reference info from P225 statement references
    JsonNode claims = entity.path("claims").path(P225);
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
   * Get a string value from reference snaks.
   */
  private static String getSnakStringValue(JsonNode snaks, String prop) {
    JsonNode propSnaks = snaks.path(prop);
    if (!propSnaks.isArray() || propSnaks.isEmpty()) return null;
    JsonNode val = propSnaks.get(0).path("datavalue").path("value");
    if (val.isMissingNode()) return null;
    if (val.isTextual()) return val.asText(null);
    if (val.has("text")) return val.path("text").asText(null);
    return val.asText(null);
  }
}
