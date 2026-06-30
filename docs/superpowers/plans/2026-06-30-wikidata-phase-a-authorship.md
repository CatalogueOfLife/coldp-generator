# Wikidata Phase A — Structured Authorship Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit a name's authorship from the `P225` qualifiers (`P405` author items, `P574` year, `P3831` recombination, `P1403` basionym), as ColDP atomized authorship fields plus a shared `col:Author` table, so ChecklistBank renders the code-correct display.

**Architecture:** Pure mapping/extraction helpers (no IO) do the work and are unit-tested; `WikidataDumpReader` collects author QIDs in pass 1 and resolves them via SPARQL between passes; `Generator` wires the atomized fields onto the `NameUsage` writer and writes the `Author` table after pass 2. Atomized fields are authoritative; the flat `authorship` string is a fallback.

**Tech Stack:** Java 21, Jackson `JsonNode`, JUnit 4, ColDP `TermWriter` / `ColdpTerm`, Wikidata SPARQL (`query.wikidata.org`).

## Global Constraints

- Java 21; depends on `coldp`/`api` `1.1.2-SNAPSHOT` (has `col:Author`, `abbreviationBotany`, atomized `*Authorship*` terms).
- Pure helpers live in `WikidataMappings` (mirrors `ColacMappings`), kept free of IO/JDBC so they unit-test.
- Author records are written **only** for authors referenced by an emitted name (track in `usedAuthorQids`, like `usedExtIdPids`).
- The atomized fields are authoritative; the flat `authorship` is a human-readable fallback (surname + year, parenthesised for recombinations).
- Author resolution is **SPARQL-only** in Phase A (collect needed QIDs in pass 1, resolve between passes like `resolvePubs`). The spec's dump-opportunistic caching is deferred as an optimization.
- Build with `mvn -q -o test` (offline) when the local `.m2` is warm; otherwise `mvn -q test`.

---

### Task 1: `WikidataMappings` string helpers

**Files:**
- Create: `src/main/java/org/catalogueoflife/data/wikidata/WikidataMappings.java`
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataMappingsTest.java`

**Interfaces:**
- Produces:
  - `static String extractYear(String date)` — 4-digit year from a Wikidata time string (`"+1758-01-01T..."` → `"1758"`); null-safe.
  - `static String joinFamilies(List<String> families)` — `["A"]`→`"A"`, `["A","B"]`→`"A & B"`, `["A","B","C"]`→`"A, B & C"`; blanks dropped; empty→null.
  - `static String flatAuthorship(List<String> families, String year, boolean recombination)` — `"Gertsch & Mulaik, 1940"`, `"(Linnaeus, 1758)"`, `"Linnaeus"` (null year); empty families→null.
  - `static String lastToken(String label)` — last whitespace-separated token, or null.
  - `static String givenFromLabel(String label, String family)` — label with a trailing `family` removed and trimmed, or null/blank→null.
  - `static String mapSex(String qid)` — `Q6581097`→`"male"`, `Q6581072`→`"female"`, else null.

- [ ] **Step 1: Write the failing test**

```java
package org.catalogueoflife.data.wikidata;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class WikidataMappingsTest {

  @Test public void extractYear() {
    assertEquals("1758", WikidataMappings.extractYear("+1758-01-01T00:00:00Z"));
    assertEquals("1753", WikidataMappings.extractYear("1753"));
    assertNull(WikidataMappings.extractYear(null));
  }

  @Test public void joinFamilies() {
    assertEquals("Linnaeus", WikidataMappings.joinFamilies(List.of("Linnaeus")));
    assertEquals("Gertsch & Mulaik", WikidataMappings.joinFamilies(List.of("Gertsch", "Mulaik")));
    assertEquals("A, B & C", WikidataMappings.joinFamilies(List.of("A", "B", "C")));
    assertNull(WikidataMappings.joinFamilies(List.of()));
  }

  @Test public void flatAuthorship() {
    assertEquals("Gertsch & Mulaik, 1940",
        WikidataMappings.flatAuthorship(List.of("Gertsch", "Mulaik"), "1940", false));
    assertEquals("(Linnaeus, 1758)",
        WikidataMappings.flatAuthorship(List.of("Linnaeus"), "1758", true));
    assertEquals("Linnaeus",
        WikidataMappings.flatAuthorship(List.of("Linnaeus"), null, false));
    assertNull(WikidataMappings.flatAuthorship(List.of(), "1940", false));
  }

  @Test public void labelSplit() {
    assertEquals("Linnaeus", WikidataMappings.lastToken("Carl Linnaeus"));
    assertEquals("Carl", WikidataMappings.givenFromLabel("Carl Linnaeus", "Linnaeus"));
    assertNull(WikidataMappings.givenFromLabel("Linnaeus", "Linnaeus"));
  }

  @Test public void sex() {
    assertEquals("male", WikidataMappings.mapSex("Q6581097"));
    assertEquals("female", WikidataMappings.mapSex("Q6581072"));
    assertNull(WikidataMappings.mapSex("Q1"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=WikidataMappingsTest`
Expected: FAIL — `WikidataMappings` does not exist (compilation error).

- [ ] **Step 3: Write minimal implementation**

```java
package org.catalogueoflife.data.wikidata;

import java.util.List;

/** Pure mapping helpers for the Wikidata generator. No IO so they unit-test. */
public class WikidataMappings {

  private WikidataMappings() {}

  static String extractYear(String date) {
    if (date == null) return null;
    String c = date.startsWith("+") ? date.substring(1) : date;
    return c.length() >= 4 ? c.substring(0, 4) : c;
  }

  static String joinFamilies(List<String> families) {
    List<String> fs = families.stream().filter(f -> f != null && !f.isBlank()).toList();
    if (fs.isEmpty()) return null;
    if (fs.size() == 1) return fs.get(0);
    if (fs.size() == 2) return fs.get(0) + " & " + fs.get(1);
    return String.join(", ", fs.subList(0, fs.size() - 1)) + " & " + fs.get(fs.size() - 1);
  }

  static String flatAuthorship(List<String> families, String year, boolean recombination) {
    String base = joinFamilies(families);
    if (base == null) return null;
    String s = (year != null && !year.isBlank()) ? base + ", " + year : base;
    return recombination ? "(" + s + ")" : s;
  }

  static String lastToken(String label) {
    if (label == null || label.isBlank()) return null;
    String[] parts = label.trim().split("\\s+");
    return parts[parts.length - 1];
  }

  static String givenFromLabel(String label, String family) {
    if (label == null) return null;
    String g = label.trim();
    if (family != null && g.endsWith(family)) {
      g = g.substring(0, g.length() - family.length()).trim();
    }
    return g.isBlank() ? null : g;
  }

  static String mapSex(String qid) {
    if (qid == null) return null;
    return switch (qid) {
      case "Q6581097" -> "male";
      case "Q6581072" -> "female";
      default -> null;
    };
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=WikidataMappingsTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataMappings.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataMappingsTest.java
git commit -m "wikidata: add WikidataMappings authorship string helpers"
```

---

### Task 2: Author/name records + `extractAuthorship` in `WikidataDumpReader`

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java`
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java`

**Interfaces:**
- Consumes: existing static helpers `getItemId`, `getSnakStringValue`, `getSnakItemId`; `WikidataMappings.extractYear` (Task 1).
- Produces:
  - `record AuthorInfo(String given, String family, String abbreviationBotany, String birth, String death, String country, String sex, String affiliation, String orcid)`
  - `record NameAuthorship(java.util.List<String> authorQids, String year, boolean recombination)`
  - `static NameAuthorship extractAuthorship(JsonNode entity)` — reads the first `P225` statement's qualifiers; returns null if no `P225`.
  - new constants: `P405`, `P734`, `P735`, `P496`, `P569`, `P570`, `P27`, `P21`, `P108`, `P3831`, `P1403`, `Q14594740`.
  - new fields: `final Map<String, AuthorInfo> authors`, `final Set<String> neededAuthorQids`.

- [ ] **Step 1: Write the failing test** (append to `WikidataDumpReaderTest`)

```java
  @Test
  public void extractAuthorshipRecombination() throws Exception {
    // Panthera tigris: P405=Q1043, P574=1758, P3831=Q14594740 (recombination)
    String json = "{"
        + "\"id\":\"Q19939\",\"claims\":{\"P225\":[{"
        + "  \"mainsnak\":{\"datavalue\":{\"value\":\"Panthera tigris\"}},"
        + "  \"qualifiers\":{"
        + "    \"P405\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q1043\"}}}],"
        + "    \"P574\":[{\"datavalue\":{\"value\":{\"time\":\"+1758-01-01T00:00:00Z\"}}}],"
        + "    \"P3831\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q14594740\"}}}]"
        + "  }}]}}";
    WikidataDumpReader.NameAuthorship na = WikidataDumpReader.extractAuthorship(parse(json));
    assertEquals(java.util.List.of("Q1043"), na.authorQids());
    assertEquals("1758", na.year());
    assertTrue(na.recombination());
  }

  @Test
  public void extractAuthorshipMultiAuthorOriginal() throws Exception {
    // Loxosceles reclusa: two authors, no recombination
    String json = "{"
        + "\"id\":\"Q284352\",\"claims\":{\"P225\":[{"
        + "  \"mainsnak\":{\"datavalue\":{\"value\":\"Loxosceles reclusa\"}},"
        + "  \"qualifiers\":{"
        + "    \"P405\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q955086\"}}},"
        + "             {\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q22114001\"}}}],"
        + "    \"P574\":[{\"datavalue\":{\"value\":{\"time\":\"+1940-01-01T00:00:00Z\"}}}]"
        + "  }}]}}";
    WikidataDumpReader.NameAuthorship na = WikidataDumpReader.extractAuthorship(parse(json));
    assertEquals(java.util.List.of("Q955086", "Q22114001"), na.authorQids());
    assertEquals("1940", na.year());
    assertFalse(na.recombination());
  }
```

Add the needed imports/static imports at the top of the test file:
`import static org.junit.Assert.assertTrue;` and `import static org.junit.Assert.assertFalse;`

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=WikidataDumpReaderTest`
Expected: FAIL — `extractAuthorship` / `NameAuthorship` undefined.

- [ ] **Step 3: Write minimal implementation** (in `WikidataDumpReader`)

Add constants alongside the existing property constants (near line 91):

```java
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
```

Add records next to the other `record` declarations (near line 140):

```java
  /** Resolved author item → ColDP Author columns. */
  record AuthorInfo(String given, String family, String abbreviationBotany,
                    String birth, String death, String country, String sex,
                    String affiliation, String orcid) {}
  /** Authorship extracted from a taxon's first P225 statement qualifiers. */
  record NameAuthorship(java.util.List<String> authorQids, String year, boolean recombination) {}
```

Add fields next to the other lookup maps (near line 129):

```java
  /** Author QID → resolved info, populated by SPARQL between passes. */
  final Map<String, AuthorInfo> authors = new HashMap<>();
  /** Author QIDs referenced by P405, collected in pass 1, resolved via SPARQL. */
  final Set<String> neededAuthorQids = new HashSet<>();
```

Add the extraction helper (near the other static extraction helpers):

```java
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=WikidataDumpReaderTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java
git commit -m "wikidata: extract author/year/recombination from P225 qualifiers"
```

---

### Task 3: `assembleAuthorship` — atomized authorship from authors map

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataMappings.java`
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataMappingsTest.java`

**Interfaces:**
- Consumes: `WikidataDumpReader.NameAuthorship`, `WikidataDumpReader.AuthorInfo` (Task 2); `joinFamilies`, `flatAuthorship` (Task 1).
- Produces:
  - `record Authorship(String combinationAuthorship, String combinationAuthorshipID, String combinationAuthorshipYear, String basionymAuthorship, String basionymAuthorshipID, String basionymAuthorshipYear, String flat)`
  - `static Authorship assembleAuthorship(WikidataDumpReader.NameAuthorship na, Map<String, WikidataDumpReader.AuthorInfo> authors)` — recombination → basionym* fields, else combination* fields; `flat` always set; falls back to the QID as the family name when an author is unresolved.

- [ ] **Step 1: Write the failing test** (append to `WikidataMappingsTest`)

```java
  private static java.util.Map<String, WikidataDumpReader.AuthorInfo> authorMap() {
    var m = new java.util.HashMap<String, WikidataDumpReader.AuthorInfo>();
    m.put("Q1043", new WikidataDumpReader.AuthorInfo("Carl", "Linnaeus", "L.", "1707", "1778", "SE", "male", null, null));
    m.put("Q955086", new WikidataDumpReader.AuthorInfo(null, "Gertsch", null, null, null, null, null, null, null));
    m.put("Q22114001", new WikidataDumpReader.AuthorInfo(null, "Mulaik", null, null, null, null, null, null, null));
    return m;
  }

  @Test public void assembleOriginalSingle() {
    var na = new WikidataDumpReader.NameAuthorship(java.util.List.of("Q1043"), "1753", false);
    var a = WikidataMappings.assembleAuthorship(na, authorMap());
    assertEquals("Linnaeus", a.combinationAuthorship());
    assertEquals("Q1043", a.combinationAuthorshipID());
    assertEquals("1753", a.combinationAuthorshipYear());
    assertNull(a.basionymAuthorship());
    assertEquals("Linnaeus, 1753", a.flat());
  }

  @Test public void assembleOriginalMulti() {
    var na = new WikidataDumpReader.NameAuthorship(java.util.List.of("Q955086", "Q22114001"), "1940", false);
    var a = WikidataMappings.assembleAuthorship(na, authorMap());
    assertEquals("Gertsch & Mulaik", a.combinationAuthorship());
    assertEquals("Q955086,Q22114001", a.combinationAuthorshipID());
    assertEquals("Gertsch & Mulaik, 1940", a.flat());
  }

  @Test public void assembleRecombination() {
    var na = new WikidataDumpReader.NameAuthorship(java.util.List.of("Q1043"), "1758", true);
    var a = WikidataMappings.assembleAuthorship(na, authorMap());
    assertNull(a.combinationAuthorship());
    assertEquals("Linnaeus", a.basionymAuthorship());
    assertEquals("Q1043", a.basionymAuthorshipID());
    assertEquals("1758", a.basionymAuthorshipYear());
    assertEquals("(Linnaeus, 1758)", a.flat());
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=WikidataMappingsTest`
Expected: FAIL — `assembleAuthorship` / `Authorship` undefined.

- [ ] **Step 3: Write minimal implementation** (add to `WikidataMappings`; add `import java.util.Map;` and `import java.util.ArrayList;`)

```java
  record Authorship(String combinationAuthorship, String combinationAuthorshipID,
                    String combinationAuthorshipYear, String basionymAuthorship,
                    String basionymAuthorshipID, String basionymAuthorshipYear, String flat) {}

  static Authorship assembleAuthorship(WikidataDumpReader.NameAuthorship na,
                                       Map<String, WikidataDumpReader.AuthorInfo> authors) {
    if (na == null || na.authorQids().isEmpty()) return null;
    List<String> families = new ArrayList<>();
    for (String qid : na.authorQids()) {
      WikidataDumpReader.AuthorInfo ai = authors.get(qid);
      families.add(ai != null && ai.family() != null ? ai.family() : qid);
    }
    String fam = joinFamilies(families);
    String ids = String.join(",", na.authorQids());
    String flat = flatAuthorship(families, na.year(), na.recombination());
    if (na.recombination()) {
      return new Authorship(null, null, null, fam, ids, na.year(), flat);
    }
    return new Authorship(fam, ids, na.year(), null, null, null, flat);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=WikidataMappingsTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataMappings.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataMappingsTest.java
git commit -m "wikidata: assemble atomized combination/basionym authorship"
```

---

### Task 4: Collect author QIDs in pass 1

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java:285-299` (`collectTaxonReferences`)
- Test: `src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java`

**Interfaces:**
- Consumes: `extractAuthorship` (Task 2), `neededAuthorQids` field (Task 2).
- Produces: a static, testable collector `static void collectAuthorRefs(JsonNode entity, Set<String> out)` called from `collectTaxonReferences`.

- [ ] **Step 1: Write the failing test** (append to `WikidataDumpReaderTest`)

```java
  @Test
  public void collectAuthorRefs() throws Exception {
    String json = "{"
        + "\"id\":\"Q284352\",\"claims\":{\"P225\":[{"
        + "  \"mainsnak\":{\"datavalue\":{\"value\":\"Loxosceles reclusa\"}},"
        + "  \"qualifiers\":{\"P405\":["
        + "    {\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q955086\"}}},"
        + "    {\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q22114001\"}}}]}}]}}";
    java.util.Set<String> out = new java.util.HashSet<>();
    WikidataDumpReader.collectAuthorRefs(parse(json), out);
    assertEquals(2, out.size());
    assertTrue(out.contains("Q955086"));
    assertTrue(out.contains("Q22114001"));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=WikidataDumpReaderTest#collectAuthorRefs`
Expected: FAIL — `collectAuthorRefs` undefined.

- [ ] **Step 3: Write minimal implementation**

Add the static helper to `WikidataDumpReader`:

```java
  /** Collect P405 author QIDs of a taxon into {@code out} (for later SPARQL resolution). */
  static void collectAuthorRefs(JsonNode entity, Set<String> out) {
    NameAuthorship na = extractAuthorship(entity);
    if (na != null) out.addAll(na.authorQids());
  }
```

Then call it from `collectTaxonReferences`, right after the existing `collectSynonymLinks(...)` call (around line 299):

```java
    // Collect P405 author QIDs for SPARQL resolution between passes
    collectAuthorRefs(entity, neededAuthorQids);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=WikidataDumpReaderTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/WikidataDumpReader.java \
        src/test/java/org/catalogueoflife/data/wikidata/WikidataDumpReaderTest.java
git commit -m "wikidata: collect P405 author QIDs during pass 1"
```

---

### Task 5: SPARQL author resolution in `Generator`

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/Generator.java` (`resolveUnresolved`, new `resolveAuthors`/`parseSparqlAuthors`)
- Test: `src/test/java/org/catalogueoflife/data/wikidata/GeneratorParseAuthorsTest.java` (new)

**Interfaces:**
- Consumes: `WikidataDumpReader.authors`, `WikidataDumpReader.neededAuthorQids`, `AuthorInfo` (Task 2); `WikidataMappings.extractYear/lastToken/givenFromLabel` (Task 1); existing `querySparql`, `sleepBriefly`, and the inherited `static` `mapper`/`LOG`.
- Produces:
  - `static void parseSparqlAuthors(String json, Map<String, WikidataDumpReader.AuthorInfo> target)` — static (uses the inherited static `mapper`/`LOG`) so it unit-tests without constructing a `Generator`.
  - `private void resolveAuthors(Set<String> qids, Map<String, WikidataDumpReader.AuthorInfo> target)`.

The SPARQL binding keys are: `a` (author URI), `lab` (label), `g` (given), `f` (family), `ab` (abbreviationBotany), `c` (P835 citation), `b` (birth), `d` (death), `o` (orcid), `s` (sex label), `cc` (country ISO), `aff` (affiliation label).

- [ ] **Step 1: Write the failing test**

```java
package org.catalogueoflife.data.wikidata;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GeneratorParseAuthorsTest {

  @Test public void parseAuthors() throws Exception {
    // Linnaeus: P734/P735 absent → family falls back to P835 citation, given from label
    String json = "{\"results\":{\"bindings\":[{"
        + "\"a\":{\"value\":\"http://www.wikidata.org/entity/Q1043\"},"
        + "\"lab\":{\"value\":\"Carl Linnaeus\"},"
        + "\"ab\":{\"value\":\"L.\"},"
        + "\"c\":{\"value\":\"Linnaeus\"},"
        + "\"b\":{\"value\":\"1707-05-23T00:00:00Z\"},"
        + "\"d\":{\"value\":\"1778-01-10T00:00:00Z\"},"
        + "\"s\":{\"value\":\"male\"},"
        + "\"cc\":{\"value\":\"SE\"}"
        + "}]}}";
    Map<String, WikidataDumpReader.AuthorInfo> target = new HashMap<>();
    Generator.parseSparqlAuthors(json, target);

    WikidataDumpReader.AuthorInfo a = target.get("Q1043");
    assertNotNull(a);
    assertEquals("Linnaeus", a.family());
    assertEquals("Carl", a.given());
    assertEquals("L.", a.abbreviationBotany());
    assertEquals("1707", a.birth());
    assertEquals("1778", a.death());
    assertEquals("male", a.sex());
    assertEquals("SE", a.country());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=GeneratorParseAuthorsTest`
Expected: FAIL — `parseSparqlAuthors` undefined.

- [ ] **Step 3: Write minimal implementation** (add to `Generator`)

```java
  /** Resolve author items via SPARQL in batches of 200; same shape as resolvePubs. */
  private void resolveAuthors(Set<String> qids, Map<String, WikidataDumpReader.AuthorInfo> target) {
    List<String> qidList = new ArrayList<>(qids);
    for (int i = 0; i < qidList.size(); i += 200) {
      List<String> batch = qidList.subList(i, Math.min(i + 200, qidList.size()));
      StringBuilder values = new StringBuilder();
      for (String qid : batch) values.append(" wd:").append(qid);
      String sparql = String.format("""
          PREFIX wd: <http://www.wikidata.org/entity/>
          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
          PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          SELECT ?a (SAMPLE(?label) AS ?lab) (SAMPLE(?given) AS ?g) (SAMPLE(?family) AS ?f)
                 (SAMPLE(?abbr) AS ?ab) (SAMPLE(?cit) AS ?c) (SAMPLE(?birth) AS ?b)
                 (SAMPLE(?death) AS ?d) (SAMPLE(?orcid) AS ?o) (SAMPLE(?sexL) AS ?s)
                 (SAMPLE(?cc0) AS ?cc) (SAMPLE(?afflL) AS ?aff)
          WHERE {
            VALUES ?a {%s}
            OPTIONAL { ?a rdfs:label ?label . FILTER(LANG(?label) = "en") }
            OPTIONAL { ?a wdt:P735 ?gi . ?gi rdfs:label ?given . FILTER(LANG(?given) = "en") }
            OPTIONAL { ?a wdt:P734 ?fi . ?fi rdfs:label ?family . FILTER(LANG(?family) = "en") }
            OPTIONAL { ?a wdt:P428 ?abbr }
            OPTIONAL { ?a wdt:P835 ?cit }
            OPTIONAL { ?a wdt:P569 ?birth }
            OPTIONAL { ?a wdt:P570 ?death }
            OPTIONAL { ?a wdt:P496 ?orcid }
            OPTIONAL { ?a wdt:P21 ?sx . ?sx rdfs:label ?sexL . FILTER(LANG(?sexL) = "en") }
            OPTIONAL { ?a wdt:P27 ?ctry . ?ctry wdt:P297 ?cc0 }
            OPTIONAL { ?a wdt:P108 ?af . ?af rdfs:label ?afflL . FILTER(LANG(?afflL) = "en") }
          }
          GROUP BY ?a
          """, values);
      try {
        String result = querySparql(sparql);
        if (result != null) parseSparqlAuthors(result, target);
      } catch (Exception e) {
        LOG.warn("SPARQL author resolution failed for batch starting at {}: {}", i, e.getMessage());
      }
      sleepBriefly();
    }
    qids.clear();
  }

  static void parseSparqlAuthors(String json, Map<String, WikidataDumpReader.AuthorInfo> target) {
    try {
      JsonNode bindings = mapper.readTree(json).path("results").path("bindings");
      for (JsonNode b : bindings) {
        String uri = b.path("a").path("value").asText(null);
        if (uri == null) continue;
        String qid = uri.substring(uri.lastIndexOf('/') + 1);
        String label = v(b, "lab");
        String family = firstNonNull(v(b, "f"), v(b, "c"), WikidataMappings.lastToken(label));
        String given = firstNonNull(v(b, "g"), WikidataMappings.givenFromLabel(label, family));
        target.put(qid, new WikidataDumpReader.AuthorInfo(
            given, family, v(b, "ab"),
            WikidataMappings.extractYear(v(b, "b")), WikidataMappings.extractYear(v(b, "d")),
            v(b, "cc"), v(b, "s"), v(b, "aff"), v(b, "o")));
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse SPARQL author results: {}", e.getMessage());
    }
  }

  private static String v(JsonNode binding, String key) {
    String s = binding.path(key).path("value").asText(null);
    return (s == null || s.isBlank()) ? null : s;
  }

  private static String firstNonNull(String... vals) {
    for (String s : vals) if (s != null && !s.isBlank()) return s;
    return null;
  }
```

Wire it into `resolveUnresolved`, after the `neededPubQids` block (around line 309):

```java
    if (!reader.neededAuthorQids.isEmpty()) {
      LOG.info("Resolving {} unresolved author QIDs via SPARQL...", reader.neededAuthorQids.size());
      resolveAuthors(reader.neededAuthorQids, reader.authors);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=GeneratorParseAuthorsTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/Generator.java \
        src/test/java/org/catalogueoflife/data/wikidata/GeneratorParseAuthorsTest.java
git commit -m "wikidata: resolve Author records via SPARQL between passes"
```

---

### Task 6: Wire atomized authorship + Author table into output

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/wikidata/Generator.java` (`initWriters`, `writeNameUsage`, `emitColdpRecords`, new `authorWriter`/`usedAuthorQids`/`writeAuthors`)
- Test: `src/test/java/org/catalogueoflife/data/wikidata/GeneratorAuthorshipFixtureTest.java` (new)

**Interfaces:**
- Consumes: `WikidataDumpReader.extractAuthorship` (Task 2), `WikidataMappings.assembleAuthorship` (Task 3), `WikidataDumpReader.authors` (Task 2).
- Produces: atomized authorship + `basionymID(P1403)` on `NameUsage`; a `col:Author` writer emitting only `usedAuthorQids`.

- [ ] **Step 1: Write the failing test** — assert the end-to-end mapping the writer relies on (pure-function level, no IO)

```java
package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GeneratorAuthorshipFixtureTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static Map<String, WikidataDumpReader.AuthorInfo> authors() {
    var m = new HashMap<String, WikidataDumpReader.AuthorInfo>();
    m.put("Q1043", new WikidataDumpReader.AuthorInfo("Carl", "Linnaeus", "L.", "1707", "1778", "SE", "male", null, null));
    return m;
  }

  @Test public void tigerRecombinationEndToEnd() throws Exception {
    String json = "{\"id\":\"Q19939\",\"claims\":{\"P225\":[{"
        + "\"mainsnak\":{\"datavalue\":{\"value\":\"Panthera tigris\"}},"
        + "\"qualifiers\":{"
        + "  \"P405\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q1043\"}}}],"
        + "  \"P574\":[{\"datavalue\":{\"value\":{\"time\":\"+1758-01-01T00:00:00Z\"}}}],"
        + "  \"P3831\":[{\"datavalue\":{\"value\":{\"entity-type\":\"item\",\"id\":\"Q14594740\"}}}]"
        + "}}]}}";
    var na = WikidataDumpReader.extractAuthorship(M.readTree(json));
    var a = WikidataMappings.assembleAuthorship(na, authors());
    assertEquals("(Linnaeus, 1758)", a.flat());
    assertEquals("Linnaeus", a.basionymAuthorship());
    assertEquals("1758", a.basionymAuthorshipYear());
    assertNull(a.combinationAuthorship());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=GeneratorAuthorshipFixtureTest`
Expected: FAIL — class is new (compile error) until created; if helpers from Tasks 2–3 are present it then PASSES, confirming the contract. (If it already passes after creation, that is acceptable — proceed.)

- [ ] **Step 3: Write minimal implementation** (the writer wiring)

In `Generator`, add fields near the other writer fields (line ~38):

```java
  private TermWriter authorWriter;
  private final Set<String> usedAuthorQids = new HashSet<>();
```

In `initWriters`, append the atomized columns to the existing `NameUsage` column list, immediately after `ColdpTerm.authorship`:

```java
        ColdpTerm.authorship,
        ColdpTerm.combinationAuthorship,
        ColdpTerm.combinationAuthorshipID,
        ColdpTerm.combinationAuthorshipYear,
        ColdpTerm.basionymAuthorship,
        ColdpTerm.basionymAuthorshipID,
        ColdpTerm.basionymAuthorshipYear,
```

And add the Author writer (next to the other `additionalWriter` calls):

```java
    authorWriter = additionalWriter(ColdpTerm.Author, List.of(
        ColdpTerm.ID,
        ColdpTerm.given,
        ColdpTerm.family,
        ColdpTerm.abbreviationBotany,
        ColdpTerm.alternativeID,
        ColdpTerm.birth,
        ColdpTerm.death,
        ColdpTerm.country,
        ColdpTerm.sex,
        ColdpTerm.affiliation,
        ColdpTerm.link
    ));
```

In `writeNameUsage`, **replace** the current authorship block (the `// Authorship (author citation string)` lines reading `P835`, ~680-684) with:

```java
    // Authorship — atomized from P405 author items (authoritative); flat string is a fallback.
    WikidataDumpReader.NameAuthorship na = WikidataDumpReader.extractAuthorship(entity);
    WikidataMappings.Authorship auth = WikidataMappings.assembleAuthorship(na, reader.authors);
    if (auth != null) {
      writer.set(ColdpTerm.authorship, auth.flat());
      writer.set(ColdpTerm.combinationAuthorship, auth.combinationAuthorship());
      writer.set(ColdpTerm.combinationAuthorshipID, auth.combinationAuthorshipID());
      writer.set(ColdpTerm.combinationAuthorshipYear, auth.combinationAuthorshipYear());
      writer.set(ColdpTerm.basionymAuthorship, auth.basionymAuthorship());
      writer.set(ColdpTerm.basionymAuthorshipID, auth.basionymAuthorshipID());
      writer.set(ColdpTerm.basionymAuthorshipYear, auth.basionymAuthorshipYear());
      if (na != null) usedAuthorQids.addAll(na.authorQids());
    } else {
      // No P405 → fall back to the P835 author-citation string on the taxon.
      String fallback = getStringClaimValue(entity, P835);
      if (fallback != null) writer.set(ColdpTerm.authorship, fallback);
    }
```

In `writeNameUsage`, **replace** the current basionym block (the `// Basionym` lines, ~687-693) with P1403 fallback:

```java
    // Basionym — P566, else P1403 (original combination item)
    JsonNode basVal = getClaimValue(entity, P566);
    String basQid = basVal != null ? getItemId(basVal) : null;
    if (basQid == null) {
      JsonNode origComb = getClaimValue(entity, P1403);
      if (origComb != null) basQid = getItemId(origComb);
    }
    if (basQid != null) {
      writer.set(ColdpTerm.basionymID, basQid);
    }
```

Add `writeAuthors`:

```java
  private void writeAuthors(WikidataDumpReader reader) throws IOException {
    int count = 0;
    for (var e : reader.authors.entrySet()) {
      if (!usedAuthorQids.contains(e.getKey())) continue;
      WikidataDumpReader.AuthorInfo a = e.getValue();
      authorWriter.set(ColdpTerm.ID, e.getKey());
      authorWriter.set(ColdpTerm.given, a.given());
      authorWriter.set(ColdpTerm.family, a.family());
      authorWriter.set(ColdpTerm.abbreviationBotany, a.abbreviationBotany());
      if (a.orcid() != null) authorWriter.set(ColdpTerm.alternativeID, "orcid:" + a.orcid());
      authorWriter.set(ColdpTerm.birth, a.birth());
      authorWriter.set(ColdpTerm.death, a.death());
      authorWriter.set(ColdpTerm.country, a.country());
      authorWriter.set(ColdpTerm.sex, a.sex());
      authorWriter.set(ColdpTerm.affiliation, a.affiliation());
      authorWriter.set(ColdpTerm.link, "https://www.wikidata.org/wiki/" + e.getKey());
      authorWriter.next();
      count++;
    }
    LOG.info("Wrote {} author records ({} referenced)", count, usedAuthorQids.size());
  }
```

In `emitColdpRecords`, call it right after `writeReferences(reader);`:

```java
    writeReferences(reader);
    writeAuthors(reader);
```

- [ ] **Step 4: Run the full build + tests**

Run: `mvn -q test`
Expected: PASS (all wikidata tests, including the new fixture test). Confirm it compiles end-to-end.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/wikidata/Generator.java \
        src/test/java/org/catalogueoflife/data/wikidata/GeneratorAuthorshipFixtureTest.java
git commit -m "wikidata: write atomized authorship and the col:Author table"
```

---

## Self-Review

**Spec coverage (Phase A sections):**
- `col:Author` table with rich fields → Tasks 5 (resolve) + 6 (write). All spec columns present: given, family, abbreviationBotany, birth, death, country, sex, affiliation, alternativeID(orcid), link. ✓
- Author resolution (collect pass 1 + SPARQL) → Tasks 4 + 5. ✓ (dump-opportunistic deferred — noted in Global Constraints.)
- Atomized authorship, recombination → basionym, multi-author join, flat fallback → Tasks 1, 3, 6. ✓
- No-P405 fallback to P835 string → Task 6. ✓
- basionymID from P566 else P1403, originalSpelling from P1353 → Task 6 covers basionymID; **P1353→originalSpelling already exists** in `writeNameUsage` (unchanged), so no new task needed. ✓
- namePublishedInYear unchanged (nomRef path) → not modified, correct by omission. ✓
- Only-used authors emitted → Task 6 `usedAuthorQids`. ✓
- Worked examples (Poa annua / Loxosceles / Panthera tigris) → Tasks 3 + 6 tests. ✓

**Placeholder scan:** none — every step has runnable code/commands.

**Type consistency:** `AuthorInfo`(9 fields) and `NameAuthorship`(3 fields) defined in Task 2 and used identically in Tasks 3, 5, 6. `Authorship`(7 fields) defined in Task 3 and consumed in Task 6. SPARQL binding keys in Task 5 (`a/lab/g/f/ab/c/b/d/o/s/cc/aff`) match `parseSparqlAuthors`. `extractAuthorship`/`assembleAuthorship`/`collectAuthorRefs` names consistent across tasks. ✓

## Notes / deviations to surface

- **Author resolution is SPARQL-only** in this plan (the spec's dump-opportunistic caching is deferred). For very large author counts this adds a noticeable between-pass SPARQL phase, consistent with the existing publication resolution. Revisit if runtime is a problem.
- `Generator` already has a private `extractYear`; new code uses `WikidataMappings.extractYear`. Leaving both is harmless; a later cleanup can converge them.
