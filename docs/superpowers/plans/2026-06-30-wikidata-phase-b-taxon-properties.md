# Wikidata Phase B — Taxon Properties, Temporal Range, Species Interactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit ColDP `TaxonProperty` (measurements/traits), `temporalRangeStart/End` (geological range), and `SpeciesInteraction` (eats/host/symbiont) records from the Wikidata dump.

**Architecture:** The set of taxon-describing properties is fetched once at startup via SPARQL (`P31`=Q18609040 ∪ curated extras, minus special-cased PIDs and unsuitable datatypes), so pass 1 already knows which claims are taxon properties. Pass 1 collects the QIDs whose English labels are needed (item-values, quantity units, geological periods); they are resolved via SPARQL between passes (reusing `resolveLabels`). Pass 2 emits the records. Pure formatting/mapping lives in `WikidataMappings`; row-building helpers are pure and unit-tested.

**Tech Stack:** Java 21, Jackson `JsonNode`, JUnit 4, ColDP `TermWriter`/`ColdpTerm`, Wikidata SPARQL.

## Global Constraints

- Java 21; pure helpers live in `WikidataMappings` (no IO). Reuse the existing `propWriter` (`ColdpTerm.TaxonProperty`, columns `taxonID/property/value`) for taxon properties — it currently writes IUCN.
- **Property discovery** = SPARQL at startup: all `?p wdt:P31 wd:Q18609040` UNION curated extras `{P2067 mass, P462 color, P3485 bite force quotient, P788 mushroom ecological type}`. **Exclude** the special-cased PIDs `P523, P524, P1034, P2975, P1605, P1606`, and **keep only** datatypes `WikibaseItem, Quantity, String, Monolingualtext` (drop `ExternalId, CommonsMedia, Url, GeoShape, Time`).
- **Temporal range:** `P523`→`temporalRangeStart`, `P524`→`temporalRangeEnd` on `NameUsage` (new columns), value = resolved period label (fallback: the QID).
- **Species interactions** (`ColdpTerm.SpeciesInteraction`, new writer): `P1034`→`eats`, `P2975`→`has_host`, `P1605`→`symbiont_of`, `P1606`→`host_of`; `relatedTaxonID` = the value QID. Type strings are lowercase enum names (CLB parses them like `establishmentMeans`).
- **TaxonProperty value formatting:** item-valued → resolved label (fallback QID); quantity-valued → `amount [unit-label]` via `formatQuantity`; string/monolingualtext → verbatim. Multiple values → multiple rows. `property` column = the property's English label.
- Build/run offline-first: `mvn -q -o test -Dtest=...`; fall back to `mvn -q test -Dtest=...` if offline fails on missing artifacts. Run the full `mvn -o test` before committing integration tasks.
- This builds on Phase A (merged, `31c3023`). Some constants (`P1843`, `P141`, `getItemId`, `getClaimValues`, `resolveLabels`, `propWriter`) already exist — reuse, don't duplicate.

---

### Task 1: `WikidataMappings` — formatQuantity + interactionType

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataMappings.java`
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataMappingsTest.java`

**Interfaces:**
- Produces:
  - `static String formatQuantity(String amount, String unitLabel)` — strips a leading `+`; appends `" " + unitLabel` unless unitLabel is null/blank; returns null if amount null.
  - `static String interactionType(String pid)` — `P1034`→`"eats"`, `P2975`→`"has_host"`, `P1605`→`"symbiont_of"`, `P1606`→`"host_of"`, else null.

- [ ] **Step 1: Write the failing test** (append to `WikidataMappingsTest`)

```java
  @Test public void formatQuantity() {
    assertEquals("250 kilogram", WikidataMappings.formatQuantity("+250", "kilogram"));
    assertEquals("3.5", WikidataMappings.formatQuantity("+3.5", null));
    assertEquals("3.5", WikidataMappings.formatQuantity("3.5", ""));
    assertNull(WikidataMappings.formatQuantity(null, "kilogram"));
  }

  @Test public void interactionType() {
    assertEquals("eats", WikidataMappings.interactionType("P1034"));
    assertEquals("has_host", WikidataMappings.interactionType("P2975"));
    assertEquals("symbiont_of", WikidataMappings.interactionType("P1605"));
    assertEquals("host_of", WikidataMappings.interactionType("P1606"));
    assertNull(WikidataMappings.interactionType("P999"));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -o test -Dtest=WikidataMappingsTest`
Expected: FAIL — `formatQuantity`/`interactionType` undefined.

- [ ] **Step 3: Write minimal implementation** (add to `WikidataMappings`)

```java
  static String formatQuantity(String amount, String unitLabel) {
    if (amount == null) return null;
    String a = amount.startsWith("+") ? amount.substring(1) : amount;
    return (unitLabel != null && !unitLabel.isBlank()) ? a + " " + unitLabel : a;
  }

  static String interactionType(String pid) {
    if (pid == null) return null;
    return switch (pid) {
      case "P1034" -> "eats";
      case "P2975" -> "has_host";
      case "P1605" -> "symbiont_of";
      case "P1606" -> "host_of";
      default -> null;
    };
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -o test -Dtest=WikidataMappingsTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataMappings.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataMappingsTest.java
git commit -m "wikidata: add formatQuantity and interactionType mapping helpers"
```

---

### Task 2: `WikidataDumpReader` — constants, TaxonPropInfo, quantity extraction

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java`
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java`

**Interfaces:**
- Consumes: existing `getItemId`.
- Produces:
  - constants `P523, P524, P1034, P2975, P1605, P1606, Q18609040, P2067, P462, P3485, P788`.
  - `record TaxonPropInfo(String label, String datatype)`.
  - fields `final Map<String, TaxonPropInfo> taxonProps = new HashMap<>();`, `final Map<String, String> labels = new HashMap<>();`, `final Set<String> neededLabels = new HashSet<>();`.
  - `static String quantityAmount(JsonNode value)` — returns the `amount` string of a quantity value, or null.
  - `static String quantityUnitQid(JsonNode value)` — returns the unit QID extracted from the quantity value's `unit` URL, or null when dimensionless (`unit` is `"1"` or absent).

- [ ] **Step 1: Write the failing test** (append to `WikidataDumpReaderTest`)

```java
  @Test
  public void quantityExtraction() throws Exception {
    String json = "{\"amount\":\"+250\",\"unit\":\"http://www.wikidata.org/entity/Q11570\"}";
    assertEquals("+250", WikidataDumpReader.quantityAmount(parse(json)));
    assertEquals("Q11570", WikidataDumpReader.quantityUnitQid(parse(json)));

    String dimensionless = "{\"amount\":\"+3.5\",\"unit\":\"1\"}";
    assertEquals("+3.5", WikidataDumpReader.quantityAmount(parse(dimensionless)));
    assertNull(WikidataDumpReader.quantityUnitQid(parse(dimensionless)));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -o test -Dtest=WikidataDumpReaderTest#quantityExtraction`
Expected: FAIL — methods/constants undefined.

- [ ] **Step 3: Write minimal implementation** (in `WikidataDumpReader`)

Add constants next to the other property constants:

```java
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
  static final String Q18609040 = "Q18609040"; // "Wikidata property related to taxa"
```

Add the record next to the other records:

```java
  /** A discovered taxon-describing property: its English label and Wikidata datatype. */
  record TaxonPropInfo(String label, String datatype) {}
```

Add the fields next to the other lookup maps:

```java
  /** Property PID → info, fetched at startup via SPARQL (Generator.loadTaxonProperties). */
  final Map<String, TaxonPropInfo> taxonProps = new HashMap<>();
  /** QID → resolved English label (item-values, units, geological periods). */
  final Map<String, String> labels = new HashMap<>();
  /** QIDs whose labels are needed, collected in pass 1, resolved between passes. */
  final Set<String> neededLabels = new HashSet<>();
```

Add the extraction helpers next to the other static helpers:

```java
  /** Amount string of a Wikidata quantity value (e.g. "+250"), or null. */
  static String quantityAmount(JsonNode value) {
    if (value == null) return null;
    String a = value.path("amount").asText(null);
    return a;
  }

  /** Unit QID of a quantity value, or null when dimensionless ("1"/absent). */
  static String quantityUnitQid(JsonNode value) {
    if (value == null) return null;
    String unit = value.path("unit").asText(null);
    if (unit == null || unit.isBlank() || unit.equals("1")) return null;
    int slash = unit.lastIndexOf('/');
    return slash >= 0 ? unit.substring(slash + 1) : unit;
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -o test -Dtest=WikidataDumpReaderTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java
git commit -m "wikidata: add taxon-property constants, TaxonPropInfo, quantity extraction"
```

---

### Task 3: `Generator.loadTaxonProperties` — fetch the property set at startup

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/Generator.java` (new `loadTaxonProperties`/`parseSparqlTaxonProps`; call in `addData`)
- Test: `src/test/java/org/catalogueoflife/data/wikidata/GeneratorParseTaxonPropsTest.java` (new)

**Interfaces:**
- Consumes: existing `querySparql`, the inherited static `mapper`/`LOG`; `WikidataDumpReader.taxonProps`, `TaxonPropInfo`, the special-cased/curated constants (Task 2).
- Produces:
  - `static void parseSparqlTaxonProps(String json, Map<String, WikidataDumpReader.TaxonPropInfo> target)` — static, unit-testable; parses SPARQL bindings `p`/`pType`/`pLabel`; **skips** the special-cased PIDs (`P523, P524, P1034, P2975, P1605, P1606`) and datatypes not in `{WikibaseItem, Quantity, String, Monolingualtext}`.
  - `private void loadTaxonProperties(WikidataDumpReader reader)` — builds and runs the SPARQL query, calls `parseSparqlTaxonProps`.

- [ ] **Step 1: Write the failing test**

```java
package org.catalogueoflife.data.wikidata;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GeneratorParseTaxonPropsTest {

  @Test public void parseAndFilter() {
    // habitat (kept, WikibaseItem), gestation (kept, Quantity),
    // P523 temporal (skipped — special-cased), an ExternalId (skipped — datatype)
    String json = "{\"results\":{\"bindings\":[" +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P2974\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#WikibaseItem\"},\"pLabel\":{\"value\":\"habitat\"}}," +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P3063\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#Quantity\"},\"pLabel\":{\"value\":\"gestation period\"}}," +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P523\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#WikibaseItem\"},\"pLabel\":{\"value\":\"temporal range start\"}}," +
        "{\"p\":{\"value\":\"http://www.wikidata.org/entity/P3151\"},\"pType\":{\"value\":\"http://wikiba.se/ontology#ExternalId\"},\"pLabel\":{\"value\":\"iNaturalist taxon ID\"}}" +
        "]}}";
    Map<String, WikidataDumpReader.TaxonPropInfo> target = new HashMap<>();
    Generator.parseSparqlTaxonProps(json, target);

    assertEquals(2, target.size());
    assertEquals("habitat", target.get("P2974").label());
    assertEquals("WikibaseItem", target.get("P2974").datatype());
    assertEquals("Quantity", target.get("P3063").datatype());
    assertNull(target.get("P523"));   // special-cased
    assertNull(target.get("P3151"));  // ExternalId datatype dropped
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -o test -Dtest=GeneratorParseTaxonPropsTest`
Expected: FAIL — `parseSparqlTaxonProps` undefined.

- [ ] **Step 3: Write minimal implementation** (add to `Generator`)

```java
  // Datatypes we emit as TaxonProperty values; others (ExternalId, CommonsMedia, Url, GeoShape, Time) are skipped.
  private static final Set<String> TAXON_PROP_DATATYPES =
      Set.of("WikibaseItem", "Quantity", "String", "Monolingualtext");
  // PIDs handled specially (temporal range / interactions) — never generic TaxonProperty.
  private static final Set<String> NON_TAXON_PROP_PIDS =
      Set.of(P523, P524, P1034, P2975, P1605, P1606);

  static void parseSparqlTaxonProps(String json, Map<String, WikidataDumpReader.TaxonPropInfo> target) {
    try {
      JsonNode bindings = mapper.readTree(json).path("results").path("bindings");
      for (JsonNode b : bindings) {
        String uri = b.path("p").path("value").asText(null);
        if (uri == null) continue;
        String pid = uri.substring(uri.lastIndexOf('/') + 1);
        if (NON_TAXON_PROP_PIDS.contains(pid)) continue;
        String typeUri = b.path("pType").path("value").asText("");
        String datatype = typeUri.substring(typeUri.lastIndexOf('#') + 1);
        if (!TAXON_PROP_DATATYPES.contains(datatype)) continue;
        String label = b.path("pLabel").path("value").asText(null);
        target.put(pid, new WikidataDumpReader.TaxonPropInfo(label, datatype));
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse SPARQL taxon-property results: {}", e.getMessage());
    }
  }

  private void loadTaxonProperties(WikidataDumpReader reader) {
    String sparql = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        SELECT ?p ?pType ?pLabel WHERE {
          { ?p wdt:P31 wd:Q18609040 } UNION { VALUES ?p { wd:P2067 wd:P462 wd:P3485 wd:P788 } }
          ?p wikibase:propertyType ?pType .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        """;
    try {
      String result = querySparql(sparql);
      if (result != null) parseSparqlTaxonProps(result, reader.taxonProps);
    } catch (Exception e) {
      LOG.warn("Could not load taxon-property set via SPARQL: {}", e.getMessage());
    }
    LOG.info("Loaded {} taxon-describing properties", reader.taxonProps.size());
  }
```

Wire it in `addData`, immediately after `WikidataDumpReader reader = new WikidataDumpReader();` and before `reader.collectLookups(...)`:

```java
    loadTaxonProperties(reader);
```

Add imports if missing: `java.util.Set`, `java.util.Map` (likely already present).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -o test -Dtest=GeneratorParseTaxonPropsTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/Generator.java \
        src/test/java/org/catalogueoflife/data/wikidata/GeneratorParseTaxonPropsTest.java
git commit -m "wikidata: load taxon-describing property set via SPARQL at startup"
```

---

### Task 4: Pass-1 label-QID collection + between-pass resolution

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java` (new `collectPropertyLabelRefs`; call in `collectTaxonReferences`)
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/Generator.java` (`resolveUnresolved`)
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java`

**Interfaces:**
- Consumes: `taxonProps`, `neededLabels`, `quantityUnitQid`, `getItemId`, `getClaimValues` (Tasks 2/existing); existing `resolveLabels` in `Generator`.
- Produces: `static void collectPropertyLabelRefs(JsonNode entity, Map<String, TaxonPropInfo> taxonProps, Set<String> out)` — for each taxon-property claim, adds item-value QIDs (WikibaseItem) or unit QIDs (Quantity) to `out`; also adds `P523`/`P524` value QIDs.

- [ ] **Step 1: Write the failing test** (append to `WikidataDumpReaderTest`)

```java
  @Test
  public void collectPropertyLabelRefs() throws Exception {
    Map<String, WikidataDumpReader.TaxonPropInfo> tp = new java.util.HashMap<>();
    tp.put("P2974", new WikidataDumpReader.TaxonPropInfo("habitat", "WikibaseItem"));
    tp.put("P3063", new WikidataDumpReader.TaxonPropInfo("gestation period", "Quantity"));
    String json = "{\"id\":\"Q1\",\"claims\":{"
        + "\"P2974\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q123\"}}}}],"
        + "\"P3063\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"amount\":\"+100\",\"unit\":\"http://www.wikidata.org/entity/Q573\"}}}}],"
        + "\"P523\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q789\"}}}}]"
        + "}}";
    java.util.Set<String> out = new java.util.HashSet<>();
    WikidataDumpReader.collectPropertyLabelRefs(parse(json), tp, out);
    assertTrue(out.contains("Q123")); // habitat item value
    assertTrue(out.contains("Q573")); // gestation unit
    assertTrue(out.contains("Q789")); // temporal period
    assertEquals(3, out.size());
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -o test -Dtest=WikidataDumpReaderTest#collectPropertyLabelRefs`
Expected: FAIL — `collectPropertyLabelRefs` undefined.

- [ ] **Step 3: Write minimal implementation**

Add to `WikidataDumpReader`:

```java
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
```

Call it from `collectTaxonReferences`, right after the `collectAuthorRefs(entity, neededAuthorQids);` line added in Phase A:

```java
    collectPropertyLabelRefs(entity, taxonProps, neededLabels);
```

Wire resolution in `Generator.resolveUnresolved`, after the author block:

```java
    if (!reader.neededLabels.isEmpty()) {
      LOG.info("Resolving {} taxon-property/temporal label QIDs via SPARQL...", reader.neededLabels.size());
      resolveLabels(reader.neededLabels, reader.labels, false);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -o test -Dtest=WikidataDumpReaderTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java \
        src/main/java/org/catalogueoflife/data/wikidata/Generator.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java
git commit -m "wikidata: collect taxon-property/temporal label QIDs in pass 1"
```

---

### Task 5: Pass-2 emission — TaxonProperty, temporalRange, SpeciesInteraction

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java` (pure row-builders)
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/Generator.java` (writers + emission + wiring)
- Test: `src/test/java/org/catalogueoflife/data/wikidata/GeneratorPhaseBFixtureTest.java` (new)

**Interfaces:**
- Consumes: `taxonProps`, `labels` (Tasks 2–4); `WikidataMappings.formatQuantity`, `interactionType` (Task 1); existing `propWriter`, `writer`, `getClaimValues`, `getItemId`.
- Produces:
  - `static List<String[]> taxonPropertyRows(JsonNode entity, Map<String, TaxonPropInfo> taxonProps, Map<String, String> labels)` — returns `[property, value]` pairs (item → label/QID; quantity → `formatQuantity(amount, unitLabel)`; string/monolingualtext → text), one per value.
  - `static List<String[]> speciesInteractionRows(JsonNode entity)` — returns `[type, relatedTaxonID]` pairs for `P1034/P2975/P1605/P1606`.
  - `Generator`: `interactionWriter`; `writeTaxonProperties`, `writeSpeciesInteractions`, temporalRange in `writeNameUsage`; calls in `emitColdpRecords`.

- [ ] **Step 1: Write the failing test**

```java
package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class GeneratorPhaseBFixtureTest {
  private static final ObjectMapper M = new ObjectMapper();

  @Test public void taxonPropertyRows() throws Exception {
    Map<String, WikidataDumpReader.TaxonPropInfo> tp = new HashMap<>();
    tp.put("P2974", new WikidataDumpReader.TaxonPropInfo("habitat", "WikibaseItem"));
    tp.put("P2067", new WikidataDumpReader.TaxonPropInfo("mass", "Quantity"));
    Map<String, String> labels = new HashMap<>();
    labels.put("Q123", "forest");
    labels.put("Q11570", "kilogram");
    String json = "{\"id\":\"Q1\",\"claims\":{"
        + "\"P2974\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q123\"}}}}],"
        + "\"P2067\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"amount\":\"+250\",\"unit\":\"http://www.wikidata.org/entity/Q11570\"}}}}]"
        + "}}";
    List<String[]> rows = WikidataDumpReader.taxonPropertyRows(M.readTree(json), tp, labels);
    Set<String> got = new HashSet<>();
    for (String[] r : rows) got.add(r[0] + "=" + r[1]);
    assertTrue(got.contains("habitat=forest"));
    assertTrue(got.contains("mass=250 kilogram"));
    assertEquals(2, rows.size());
  }

  @Test public void speciesInteractionRows() throws Exception {
    String json = "{\"id\":\"Q1\",\"claims\":{"
        + "\"P1034\":[{\"mainsnak\":{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q1231177\"}}}}]"
        + "}}";
    List<String[]> rows = WikidataDumpReader.speciesInteractionRows(M.readTree(json));
    assertEquals(1, rows.size());
    assertEquals("eats", rows.get(0)[0]);
    assertEquals("Q1231177", rows.get(0)[1]);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -o test -Dtest=GeneratorPhaseBFixtureTest`
Expected: FAIL — row-builders undefined.

- [ ] **Step 3: Write minimal implementation**

Add the pure row-builders to `WikidataDumpReader` (add `import java.util.List;`/`ArrayList` if missing):

```java
  /** Build [property, value] rows for all taxon-property claims on an entity. */
  static java.util.List<String[]> taxonPropertyRows(JsonNode entity,
      Map<String, TaxonPropInfo> taxonProps, Map<String, String> labels) {
    java.util.List<String[]> rows = new java.util.ArrayList<>();
    for (var e : taxonProps.entrySet()) {
      String pid = e.getKey();
      TaxonPropInfo info = e.getValue();
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
```

In `Generator`, add the interaction writer field near the other writers:

```java
  private TermWriter interactionWriter;
```

In `initWriters`, add the two temporal columns to the `NameUsage` column list (after `ColdpTerm.basionymAuthorshipYear`, near the other name fields — place beside `ColdpTerm.link`/`remarks`):

```java
        ColdpTerm.temporalRangeStart,
        ColdpTerm.temporalRangeEnd,
```

and add the interaction writer beside the other `additionalWriter` calls:

```java
    interactionWriter = additionalWriter(ColdpTerm.SpeciesInteraction, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.relatedTaxonID,
        ColdpTerm.type
    ));
```

Add the emission methods to `Generator`:

```java
  private int writeTaxonProperties(JsonNode entity, String qid, WikidataDumpReader reader) throws IOException {
    int count = 0;
    for (String[] row : WikidataDumpReader.taxonPropertyRows(entity, reader.taxonProps, reader.labels)) {
      propWriter.set(ColdpTerm.taxonID, qid);
      propWriter.set(ColdpTerm.property, row[0]);
      propWriter.set(ColdpTerm.value, row[1]);
      propWriter.next();
      count++;
    }
    return count;
  }

  private int writeSpeciesInteractions(JsonNode entity, String qid) throws IOException {
    int count = 0;
    for (String[] row : WikidataDumpReader.speciesInteractionRows(entity)) {
      interactionWriter.set(ColdpTerm.taxonID, qid);
      interactionWriter.set(ColdpTerm.type, row[0]);
      interactionWriter.set(ColdpTerm.relatedTaxonID, row[1]);
      interactionWriter.next();
      count++;
    }
    return count;
  }
```

In `writeNameUsage`, set temporal range before `writer.next();` (after the existing remarks/link block):

```java
    // Temporal range (geological period items) → resolved labels
    JsonNode tStart = getClaimValue(entity, P523);
    if (tStart != null) {
      String q = getItemId(tStart);
      if (q != null) writer.set(ColdpTerm.temporalRangeStart, reader.labels.getOrDefault(q, q));
    }
    JsonNode tEnd = getClaimValue(entity, P524);
    if (tEnd != null) {
      String q = getItemId(tEnd);
      if (q != null) writer.set(ColdpTerm.temporalRangeEnd, reader.labels.getOrDefault(q, q));
    }
```

In `emitColdpRecords`, declare an interaction counter beside the other `int[] …Count` locals:

```java
    int[] interCount = {0};
```

Then, inside the pass-2 per-entity block (next to the existing `propCount[0] += writeIucnStatus(...)` call), add the two calls:

```java
        propCount[0]  += writeTaxonProperties(entity, qid, reader);
        interCount[0] += writeSpeciesInteractions(entity, qid);
```

Add `interCount[0]` to the final summary `LOG.info(...)` at the end of `emitColdpRecords` (extend the existing message with `", {} interactions"` and the argument), keeping the existing logging shape.

- [ ] **Step 4: Run focused test, then the full suite**

Run: `mvn -q -o test -Dtest=GeneratorPhaseBFixtureTest`
Expected: PASS
Run: `mvn -o test`
Expected: BUILD SUCCESS, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java \
        src/main/java/org/catalogueoflife/data/wikidata/Generator.java \
        src/test/java/org/catalogueoflife/data/wikidata/GeneratorPhaseBFixtureTest.java
git commit -m "wikidata: emit TaxonProperty, temporalRange and SpeciesInteraction records"
```

---

## Self-Review

**Spec coverage (Phase B):**
- TaxonProperty dynamic discovery (Q18609040) + curated extras, minus special-cased/unsuitable → Task 3 (`loadTaxonProperties`/`parseSparqlTaxonProps`). ✓
- TaxonProperty value formatting (item label / quantity+unit / string) → Task 5 `taxonPropertyRows` + Task 1 `formatQuantity`. ✓
- P788 mushroom ecological type → TaxonProperty (curated extra, WikibaseItem) → Tasks 3+5. ✓
- temporalRange P523/P524 → NameUsage columns → Task 5. ✓
- SpeciesInteraction P1034/P2975/P1605/P1606 → Tasks 1+5. ✓
- Label resolution of item-values/units/periods → Tasks 2+4 (`neededLabels` + `resolveLabels`). ✓

**Placeholder scan:** none — every step has runnable code/commands.

**Type consistency:** `TaxonPropInfo(label, datatype)` defined in Task 2, used identically in Tasks 3/4/5. Row-builders return `String[]` `[property,value]` / `[type,relatedTaxonID]`, consumed in Task 5's writers. `taxonProps`/`labels`/`neededLabels` names consistent across tasks. Datatype string literals (`WikibaseItem`/`Quantity`/`String`/`Monolingualtext`) consistent between Task 3 filter, Task 4 collection, and Task 5 formatting. ✓

## Notes / scope decisions to surface

- **`relatedTaxonID` is set to the value QID unconditionally.** The spec's "non-taxon values → `relatedTaxonScientificName`" refinement is deferred: determining whether a value QID is itself a taxon during streaming is awkward, and most food/host values are taxa (verified: lion → *ungulate* is a taxon). Non-taxon related values will produce a `relatedTaxonID` that does not resolve in ChecklistBank; acceptable for a first cut, revisit if it proves noisy.
- **Property set fetched live via SPARQL at startup** (mirrors `loadIdentifierScopes`). If the SPARQL endpoint is unreachable, `taxonProps` is empty and no TaxonProperty/temporal records are emitted (interactions still work — they use fixed PIDs); this is logged.
- `propWriter` (TaxonProperty) is shared with the existing IUCN output — no new TaxonProperty writer.
