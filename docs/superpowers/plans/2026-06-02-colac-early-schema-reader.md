# colac Early Species 2000 CD-ROM Reader (2000, 2002–2004) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third era reader (`EarlySchemaReader`) so `colac` converts the 2000/2002/2003/2004 Annual Checklists (Species 2000 CD-ROM schema) to ColDP at the same quality bar as 2005–2019 (0 dangling refs, 0 orphan synonyms).

**Architecture:** Extract the schema-agnostic synonym/name-resolution helpers (`normCode`, `resolveAcceptedTaxon`) from `OldSchemaReader` into `ColacMappings` (Approach A), then build a new `EarlySchemaReader extends SchemaReader` that reconstructs the classification from the flat `HIERARCHY` columns + atomized `SCINAMES` (no `parent_id` tree), synthesizing higher-taxon and genus nodes, and reuses the shared resolution for synonyms/bare names.

**Tech Stack:** Java 21, JDBC (MariaDB Connector/J), JUnit 4, `life.catalogue.coldp` ColDP terms.

**Reference:** `OldSchemaReader.java` is the closest existing pattern — its `emitSynonyms`, `emitVernaculars`, `emitDistributions`, `writeReferences`, `loadSources`/`addSourceCitation` methods are near-mirrors of what the early reader needs. Read it before Tasks 4–6.

**Spec:** `docs/superpowers/specs/2026-06-02-colac-early-schema-reader-design.md`

---

## File Structure

- **Modify** `src/main/java/org/catalogueoflife/data/colac/ColacMappings.java` — add `normCode`, `resolveAcceptedTaxon` (moved from `OldSchemaReader`); extend `status` for plural synonyms.
- **Modify** `src/main/java/org/catalogueoflife/data/colac/OldSchemaReader.java` — delete the two moved methods (calls keep working via the existing `import static ColacMappings.*`).
- **Create** `src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java` — the new reader.
- **Modify** `src/main/java/org/catalogueoflife/data/colac/Generator.java` — dispatch `year ≤ 2004 → EarlySchemaReader`; add per-year `EDITORS` for 2000/2002/2003/2004; `issued` from the `CD-Date` table; `ORCID` unchanged.
- **Modify** `src/main/java/org/catalogueoflife/data/GeneratorConfig.java` — widen `--year` validation to 2000–2019, reject 2001 (validation actually lives in `colac/Generator` constructor — see Task 3).
- **Modify** `src/test/java/org/catalogueoflife/data/colac/ColacMappingsTest.java` — relocate the `normCode`/`resolveAcceptedTaxon` tests; add plural-status test.
- **Modify** `src/test/java/org/catalogueoflife/data/colac/OldSchemaReaderTest.java` — drop the two relocated tests (keep `acceptedAncestor`/`repairParent`/`nameParentKey`).
- **Create** `src/test/java/org/catalogueoflife/data/colac/EarlySchemaReaderTest.java` — path-id unit tests.
- **Modify** `src/test/java/org/catalogueoflife/data/colac/GeneratorTest.java` — add an `@Ignore`d integration test for an early year.
- **Modify** `CLAUDE.md` — document the early reader.

---

## Task 1: Extract shared resolution helpers into ColacMappings

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/colac/ColacMappings.java`
- Modify: `src/main/java/org/catalogueoflife/data/colac/OldSchemaReader.java`
- Modify: `src/test/java/org/catalogueoflife/data/colac/ColacMappingsTest.java`
- Modify: `src/test/java/org/catalogueoflife/data/colac/OldSchemaReaderTest.java`

- [ ] **Step 1: Add `import java.util.Map;` to ColacMappings** (after `import java.util.Locale;`).

- [ ] **Step 2: Add the two helpers + extend `status` in ColacMappings.** Add the plural synonym cases to `status`:

```java
      case "synonym", "unambiguous synonym", "unambiguous synonyms" -> "synonym";
      case "ambiguous synonym", "ambiguous synonyms"                -> "ambiguous synonym";
```
(replace the existing `"synonym"` and `"ambiguous synonym"` cases). Then append these methods (verbatim from `OldSchemaReader`, with the same javadoc):

```java
  /** Canonical (upper-cased, trimmed) form of a name_code for case-insensitive matching;
   *  the early/old CoL data mixes e.g. "Mos-" and "MOS-". Returns null for null/blank. */
  static String normCode(String code) {
    if (code == null) return null;
    String s = code.trim();
    return s.isEmpty() ? null : s.toUpperCase(Locale.ENGLISH);
  }

  /** Resolves a name_code to the ColDP id of its accepted taxon, following synonym chains.
   *  acceptedNameCodeToId keys and synAcceptedCode keys/values are pre-normalised with normCode. */
  static String resolveAcceptedTaxon(String nameCode, Map<String, String> acceptedNameCodeToId,
                                     Map<String, String> synAcceptedCode) {
    String cur = nameCode;
    for (int guard = 0; guard < 25 && cur != null; guard++) {
      String tid = acceptedNameCodeToId.get(cur);
      if (tid != null) return tid;
      String next = synAcceptedCode.get(cur);
      if (next == null || next.equals(cur)) return null;
      cur = next;
    }
    return null;
  }
```

- [ ] **Step 3: Delete `normCode` and `resolveAcceptedTaxon` from `OldSchemaReader`.** Leave `acceptedAncestor`, `repairParent`, `buildRepairedParents`, `nameParentKey` in place. The unqualified calls in `OldSchemaReader` (`normCode(...)`, `resolveAcceptedTaxon(...)`) keep resolving via the existing `import static org.catalogueoflife.data.colac.ColacMappings.*;`.

- [ ] **Step 4: Move the two tests to `ColacMappingsTest`.** Cut `testResolveAcceptedTaxon`, `testResolveAcceptedTaxonUnresolvable`, `testNormCodeIsCaseInsensitive` and the `ACC`/`SYN` fixtures from `OldSchemaReaderTest` into `ColacMappingsTest`, changing `OldSchemaReader.resolveAcceptedTaxon`/`OldSchemaReader.normCode` to the static-imported `resolveAcceptedTaxon`/`normCode`. Add a plural-status test:

```java
  @Test
  public void testStatusEarlyVocabulary() {
    assertEquals("synonym", status("unambiguous synonym"));
    assertEquals("synonym", status("unambiguous synonyms"));
    assertEquals("ambiguous synonym", status("ambiguous synonyms"));
  }
```
Keep `testAcceptedAncestor*`, `testRepairParent`, `testNameParentKey` in `OldSchemaReaderTest` (add `import static org.catalogueoflife.data.colac.ColacMappings.normCode;` there only if still referenced — it is not, so no import needed).

- [ ] **Step 5: Run tests.** `mvn test -Dtest=OldSchemaReaderTest,ColacMappingsTest` → Expected: BUILD SUCCESS, all pass (old reader behaviour unchanged).

- [ ] **Step 6: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/ColacMappings.java \
        src/main/java/org/catalogueoflife/data/colac/OldSchemaReader.java \
        src/test/java/org/catalogueoflife/data/colac/ColacMappingsTest.java \
        src/test/java/org/catalogueoflife/data/colac/OldSchemaReaderTest.java
git commit -m "colac: move normCode/resolveAcceptedTaxon to ColacMappings; map unambiguous synonym"
```

---

## Task 2: Pipe-path id helpers (pure, TDD)

Synthetic ids for higher taxa and genera. Path segments are the ancestor names down to the node.

**Files:**
- Create: `src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java`
- Create: `src/test/java/org/catalogueoflife/data/colac/EarlySchemaReaderTest.java`

- [ ] **Step 1: Write the failing test** (`EarlySchemaReaderTest.java`):

```java
package org.catalogueoflife.data.colac;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class EarlySchemaReaderTest {
  @Test
  public void testPathId() {
    assertEquals("h|Plantae", EarlySchemaReader.pathId(List.of("Plantae")));
    assertEquals("h|Plantae|Rhodophyta|Palmariaceae|Palmaria",
        EarlySchemaReader.pathId(List.of("Plantae", "Rhodophyta", "Palmariaceae", "Palmaria")));
    assertNull(EarlySchemaReader.pathId(List.of()));
  }
  @Test
  public void testParentPathId() {
    assertEquals("h|Plantae|Rhodophyta",
        EarlySchemaReader.parentPathId("h|Plantae|Rhodophyta|Palmariaceae"));
    assertNull(EarlySchemaReader.parentPathId("h|Plantae")); // kingdom is a root
    assertNull(EarlySchemaReader.parentPathId(null));
  }
}
```

- [ ] **Step 2: Run it — expect FAIL** (`EarlySchemaReader` does not yet exist / methods undefined).
Run: `mvn test -Dtest=EarlySchemaReaderTest` → Expected: compile failure "cannot find symbol pathId".

- [ ] **Step 3: Create `EarlySchemaReader` with the two helpers** (class extends `SchemaReader`; constructor mirrors the other readers):

```java
package org.catalogueoflife.data.colac;

import life.catalogue.coldp.ColdpTerm;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import static org.catalogueoflife.data.colac.ColacMappings.*;

/** Reader for the 2000/2002–2004 Species 2000 CD-ROM schema (flat HIERARCHY + atomized SCINAMES). */
class EarlySchemaReader extends SchemaReader {
  static final String PREFIX = "h|";   // synthetic higher-taxon / genus id prefix

  EarlySchemaReader(Generator g, Connection conn) { super(g, conn); }

  /** Deterministic id for a synthetic node from its ancestor-name path (kingdom..this node). */
  static String pathId(List<String> segments) {
    if (segments == null || segments.isEmpty()) return null;
    return PREFIX + String.join("|", segments);
  }

  /** Parent id of a synthetic path id, or null when it is a top-level (kingdom) node. */
  static String parentPathId(String id) {
    if (id == null) return null;
    int cut = id.lastIndexOf('|');
    String parent = id.substring(0, cut);          // drop last segment
    return parent.equals(PREFIX.substring(0, PREFIX.length() - 1)) ? null // "h" only -> root
         : (parent.endsWith("|") ? null : parent);
  }

  @Override void read() throws Exception { /* filled in Tasks 3-6 */ }
}
```
Note: `parentPathId("h|Plantae")` → `lastIndexOf('|')` at index 1 → `parent = "h"` → returns null (root). `parentPathId("h|Plantae|Rhodophyta")` → `parent = "h|Plantae"` → returned.

- [ ] **Step 4: Run tests — expect PASS.** `mvn test -Dtest=EarlySchemaReaderTest` → Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java \
        src/test/java/org/catalogueoflife/data/colac/EarlySchemaReaderTest.java
git commit -m "colac: EarlySchemaReader skeleton + synthetic pipe-path id helpers"
```

---

## Task 3: Dispatch + year validation

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/colac/Generator.java` (constructor validation ~lines 86–94; `addData` dispatch ~line 144)

- [ ] **Step 1: Widen + guard the year validation** in the `Generator(GeneratorConfig)` constructor. Replace the existing 2005–2019 check with:

```java
    if (cfg.year == null) {
      throw new IllegalArgumentException("--year is required for the colac source (2000-2019, no 2001)");
    }
    if (cfg.year < 2000 || cfg.year > 2019 || cfg.year == 2001) {
      throw new IllegalArgumentException("colac supports annual checklist years 2000, 2002-2019 (2001 was never released), got " + cfg.year);
    }
```

- [ ] **Step 2: Add the dispatch** in `addData()`, replacing the existing `SchemaReader reader = year <= 2011 ? ... : ...;` line:

```java
      SchemaReader reader = year <= 2004 ? new EarlySchemaReader(this, conn)
                          : year <= 2011 ? new OldSchemaReader(this, conn)
                          : new NewSchemaReader(this, conn);
```

- [ ] **Step 3: Build.** `mvn -q package -DskipTests` → Expected: exit 0 (compiles; `read()` is still a stub so a run produces an empty archive — acceptable for now).

- [ ] **Step 4: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/Generator.java
git commit -m "colac: dispatch 2000-2004 to EarlySchemaReader; allow 2000-2019 (no 2001)"
```

---

## Task 4: Classification + accepted names

Implements the backbone of `read()`: build higher-taxon nodes from `HIERARCHY`, synthesize genus nodes, emit accepted species/infraspecies. Writers are created exactly as in `Generator.addData` already (the `NameUsage`/`Reference`/`VernacularName`/`Distribution` writers exist for all readers).

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java`

- [ ] **Step 1: Implement a minimal `read()`** — accepted-only for now; Tasks 5 and 6 extend it. (Keeping `read()` compilable at every task; it grows incrementally.)

```java
  @Override
  void read() throws Exception {
    // hierarchyCode(normCode) -> [kingdom,phylum,class,order,family]
    Map<String, String[]> hier = loadHierarchy();
    // accepted NameCode(normCode) -> emitted id (the verbatim NameCode)
    Map<String, String> acceptedNameCodeToId = new HashMap<>();
    Set<String> emittedNodes = new LinkedHashSet<>(); // synthetic path ids already written

    int nAcc = emitAccepted(hier, emittedNodes, acceptedNameCodeToId);
    LOG.info("Early schema done: {} accepted ({} synthetic nodes)", nAcc, emittedNodes.size());
  }
```

- [ ] **Step 2: `loadHierarchy()`** — read `HIERARCHY` into a map keyed by `normCode(HierarchyCode)`:

```java
  private Map<String, String[]> loadHierarchy() throws Exception {
    Map<String, String[]> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT HierarchyCode, Kingdom, Phylum, Class, `Order`, Family FROM HIERARCHY")) {
      while (rs.next()) {
        String hc = normCode(rs.getString("HierarchyCode"));
        if (hc == null) continue;
        m.put(hc, new String[]{
            rs.getString("Kingdom"), rs.getString("Phylum"), rs.getString("Class"),
            rs.getString("Order"), rs.getString("Family")});
      }
    }
    LOG.info("Loaded {} hierarchy rows", m.size());
    return m;
  }
```

- [ ] **Step 3: `emitAccepted(...)`** — for each accepted `SCINAMES` row: ensure its higher-taxon + genus nodes exist (emit once), then emit the species/infraspecies under the genus. Classification = `hier[normCode(HierarchyCode)]` if present, else fall back to `[Kingdom?,…,SCINAMES.Family]` using only `SCINAMES.Family` (genus under a root family). Helper `ensureLineage` emits any not-yet-emitted ancestor nodes and returns the genus path id:

```java
  private int emitAccepted(Map<String, String[]> hier, Set<String> emitted,
                           Map<String, String> acceptedNameCodeToId) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, HierarchyCode, Family, Genus, Species, InfraSpecies, InfraSpMarker, " +
             "ScientificNameAuthor, Sp2kStatus, AcceptedNameCode, AuthorRefNumber, DatabaseName, Comment " +
             "FROM SCINAMES WHERE Sp2kStatus LIKE '%accepted%'")) {
      while (rs.next()) {
        String code = rs.getString("NameCode");
        String nc = normCode(code);
        if (nc == null) continue;
        String[] h = hier.get(normCode(rs.getString("HierarchyCode")));
        String kingdom = h != null ? h[0] : null;
        String genusId = ensureLineage(h, rs.getString("Family"), rs.getString("Genus"), emitted);

        Generator.set(nameW, ColdpTerm.ID, code);
        if (genusId != null) Generator.set(nameW, ColdpTerm.parentID, genusId);
        Generator.set(nameW, ColdpTerm.status, ColacMappings.status(rs.getString("Sp2kStatus")));
        Generator.set(nameW, ColdpTerm.rank, infraRank(rs.getString("InfraSpMarker"), rs.getString("InfraSpecies")));
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("Genus"), rs.getString("Species"),
                        rs.getString("InfraSpMarker"), rs.getString("InfraSpecies")));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("ScientificNameAuthor"));
        Generator.set(nameW, ColdpTerm.code, nomCode(kingdom));
        Generator.set(nameW, ColdpTerm.sourceID, sourceId(rs.getString("DatabaseName")));
        Generator.set(nameW, ColdpTerm.nameReferenceID, refId(rs.getString("AuthorRefNumber")));
        Generator.set(nameW, ColdpTerm.remarks, rs.getString("Comment"));
        nameW.next();
        acceptedNameCodeToId.put(nc, code);
        n++;
      }
    }
    LOG.info("Wrote {} accepted taxa ({} synthetic higher/genus nodes)", n, emitted.size());
    return n;
  }
```

- [ ] **Step 4: `ensureLineage`, `emitNode`, and small helpers.** `ensureLineage` walks Kingdom→Family (from `hier`, else just `Family`), then Genus, emitting each node once, and returns the genus path id (or null if no Genus):

```java
  /** Emits Kingdom..Family (from hier, else a root Family) and the Genus node once each;
   *  returns the genus node id (parent for the species), or null when Genus is blank. */
  private String ensureLineage(String[] h, String scinamesFamily, String genus, Set<String> emitted) throws Exception {
    List<String> path = new ArrayList<>();
    String[] ranks = {"kingdom","phylum","class","order","family"};
    if (h != null) {
      for (int i = 0; i < 5; i++) {
        String v = h[i];
        if (v != null && !v.isBlank() && !"none".equalsIgnoreCase(v.trim())) {
          path.add(v.trim());
          emitNode(path, ranks[i], emitted);
        }
      }
    } else if (scinamesFamily != null && !scinamesFamily.isBlank()) {
      path.add(scinamesFamily.trim());
      emitNode(path, "family", emitted);
    }
    if (genus == null || genus.isBlank() || path.isEmpty()) return path.isEmpty() ? null : pathId(path);
    path.add(genus.trim());
    emitNode(path, "genus", emitted);
    return pathId(path);
  }

  private void emitNode(List<String> path, String rank, Set<String> emitted) throws Exception {
    String id = pathId(path);
    if (!emitted.add(id)) return;             // already written
    Generator.set(nameW, ColdpTerm.ID, id);
    String parent = parentPathId(id);
    if (parent != null) Generator.set(nameW, ColdpTerm.parentID, parent);
    Generator.set(nameW, ColdpTerm.status, "accepted");
    Generator.set(nameW, ColdpTerm.rank, rank);
    Generator.set(nameW, ColdpTerm.scientificName, path.get(path.size() - 1));
    Generator.set(nameW, ColdpTerm.code, nomCode(path.get(0)));
    nameW.next();
  }

  private static String infraRank(String marker, String infra) {
    if (infra != null && !infra.isBlank() && !"none".equalsIgnoreCase(infra.trim())) {
      return (marker != null && !marker.isBlank() && !"none".equalsIgnoreCase(marker.trim())) ? marker.trim() : "infraspecific name";
    }
    return "species";
  }
  private static String sourceId(String db) { return db == null || db.isBlank() ? null : "d" + db.trim(); }
  private static String refId(String ref)   { return ref == null || ref.isBlank() ? null : "r" + ref.trim(); }
```
Note: the early data uses the literal `none` for empty InfraSpecies/InfraSpMarker (seen in the sample) — `synonymName`/`infraRank` must treat `none` as blank. Confirm and, if needed, add a small `blankNone()` wrapper used in `synonymName` arguments here (do **not** change shared `synonymName`).

- [ ] **Step 5: Build + smoke-run 2004** (writers for higher nodes share the `NameUsage` writer; no schema change needed):
```bash
mvn -q package -DskipTests
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year 2004 -r /tmp/early2004 2>&1 | grep -iE 'Early schema done|ERROR'
```
Expected: a "Early schema done: N accepted …" line, no ERROR. (Synonyms/vernaculars are still 0 until Tasks 5–6.)

- [ ] **Step 6: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java
git commit -m "colac(early): classification synthesis + accepted name emission"
```

---

## Task 5: Synonyms + bare names

Mirrors `OldSchemaReader.emitSynonyms` (the bare-name version). No missing-accepted pass — every accepted name was already emitted in Task 4.

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java`

- [ ] **Step 1: `loadSynonymChain()`** — `SCINAMES` synonym rows → `normCode(NameCode)` → `normCode(AcceptedNameCode)`:

```java
  private Map<String, String> loadSynonymChain() throws Exception {
    Map<String, String> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, AcceptedNameCode FROM SCINAMES WHERE Sp2kStatus NOT LIKE '%accepted%'")) {
      while (rs.next()) {
        String nc = normCode(rs.getString("NameCode"));
        String acc = normCode(rs.getString("AcceptedNameCode"));
        if (nc != null && acc != null) m.putIfAbsent(nc, acc);
      }
    }
    LOG.info("Loaded {} synonym chain links", m.size());
    return m;
  }
```

- [ ] **Step 2: `emitSynonyms(...)`** — resolve `AcceptedNameCode`; real synonym when it resolves, else bare name:

```java
  private int[] emitSynonyms(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode) throws Exception {
    int nSyn = 0, nBare = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, Genus, Species, InfraSpecies, InfraSpMarker, ScientificNameAuthor, " +
             "Sp2kStatus, AcceptedNameCode, AuthorRefNumber, DatabaseName, Family, HierarchyCode, Comment " +
             "FROM SCINAMES WHERE Sp2kStatus NOT LIKE '%accepted%'")) {
      while (rs.next()) {
        String code = rs.getString("NameCode");
        String accCode = normCode(rs.getString("AcceptedNameCode"));
        String parentId = accCode == null ? null : resolveAcceptedTaxon(accCode, acceptedNameCodeToId, synAcceptedCode);
        String statusLabel = ColacMappings.status(rs.getString("Sp2kStatus"));
        String comment = rs.getString("Comment");

        Generator.set(nameW, ColdpTerm.ID, code);
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("Genus"), rs.getString("Species"),
                        rs.getString("InfraSpMarker"), rs.getString("InfraSpecies")));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("ScientificNameAuthor"));
        Generator.set(nameW, ColdpTerm.rank, infraRank(rs.getString("InfraSpMarker"), rs.getString("InfraSpecies")));
        Generator.set(nameW, ColdpTerm.sourceID, sourceId(rs.getString("DatabaseName")));
        Generator.set(nameW, ColdpTerm.nameReferenceID, refId(rs.getString("AuthorRefNumber")));
        if (parentId != null) {
          Generator.set(nameW, ColdpTerm.parentID, parentId);
          Generator.set(nameW, ColdpTerm.status, statusLabel);
          Generator.set(nameW, ColdpTerm.remarks, comment);
          nSyn++;
        } else {
          Generator.set(nameW, ColdpTerm.status, "bare name");
          String note = (statusLabel != null ? statusLabel : "synonym") + " without accepted name";
          Generator.set(nameW, ColdpTerm.remarks, comment == null || comment.isBlank() ? note : note + "; " + comment);
          nBare++;
        }
        nameW.next();
      }
    }
    LOG.info("Wrote {} synonyms, {} bare names", nSyn, nBare);
    return new int[]{nSyn, nBare};
  }
```
Note: the early `code` field for `nomCode` — the kingdom for a synonym isn't directly available; leave `code` unset for synonyms (the old reader derives it from `family_id`, which the early schema lacks). ChecklistBank infers the code on import.

- [ ] **Step 2b: Extend `read()`** to load the chain and emit synonyms. After the `acceptedNameCodeToId` line add `Map<String, String> synAcceptedCode = loadSynonymChain();`; after the `emitAccepted(...)` line add `int[] syn = emitSynonyms(acceptedNameCodeToId, synAcceptedCode);`; change the LOG line to:
```java
    LOG.info("Early schema done: {} accepted, {} synonyms, {} bare names ({} synthetic nodes)",
        nAcc, syn[0], syn[1], emittedNodes.size());
```

- [ ] **Step 3: Build + run 2004, check 0 orphan synonyms.**
```bash
mvn -q package -DskipTests && java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year 2004 -r /tmp/early2004 2>&1 | grep -iE 'Early schema done|ERROR'
awk -F'\t' 'NR==1{next} ($4=="synonym"||$4=="ambiguous synonym"||$4=="misapplied") && $3==""{c++} END{print "orphan synonyms = " c+0}' /tmp/early2004/colac/NameUsage.tsv
```
Expected: "orphan synonyms = 0".

- [ ] **Step 4: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java
git commit -m "colac(early): synonyms + bare names via shared resolveAcceptedTaxon"
```

---

## Task 6: Vernaculars, distributions, references, sources

All mirror `OldSchemaReader`. Vernaculars/distributions attach to the accepted taxon via `resolveAcceptedTaxon(normCode(NameCode), …)`.

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java`

- [ ] **Step 1: `writeReferences()`** — `REFERENCES` → `Reference` rows (`refW` from `SchemaReader`):

```java
  private void writeReferences() throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT RefNumber, ScientificNameAuthor, Year, Title, Source FROM `REFERENCES`")) {
      while (rs.next()) {
        String ref = rs.getString("RefNumber");
        if (ref == null || ref.isBlank()) continue;
        Generator.set(refW, ColdpTerm.ID, "r" + ref.trim());
        Generator.set(refW, ColdpTerm.author, rs.getString("ScientificNameAuthor"));
        Generator.set(refW, ColdpTerm.issued, rs.getString("Year"));
        Generator.set(refW, ColdpTerm.title, rs.getString("Title"));
        Generator.set(refW, ColdpTerm.containerTitle, rs.getString("Source"));
        refW.next();
        n++;
      }
    }
    LOG.info("Wrote {} references", n);
  }
```
Note: if `RefNumber` is not unique per row, de-duplicate with a `Set<String>` guard (confirm during the run — a "duplicate id" import warning would flag it).

- [ ] **Step 2: `emitVernaculars(...)`** — `COMNAMES`:

```java
  private int emitVernaculars(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT NameCode, CommonName, Language, Country, RefNumber FROM COMNAMES")) {
      while (rs.next()) {
        String name = rs.getString("CommonName");
        String taxonId = resolveAcceptedTaxon(normCode(rs.getString("NameCode")), acceptedNameCodeToId, synAcceptedCode);
        if (taxonId == null || name == null || name.isBlank()) continue;
        Generator.set(vernW, ColdpTerm.taxonID, taxonId);
        Generator.set(vernW, ColdpTerm.name, name);
        Generator.set(vernW, ColdpTerm.language, rs.getString("Language"));
        Generator.set(vernW, ColdpTerm.country, rs.getString("Country"));
        Generator.set(vernW, ColdpTerm.referenceID, refId(rs.getString("RefNumber")));
        vernW.next();
        n++;
      }
    }
    LOG.info("Wrote {} vernacular names", n);
    return n;
  }
```

- [ ] **Step 3: `emitDistributions(...)`** — `DISTRIBUTION` (free text, `gazetteer=text`):

```java
  private int emitDistributions(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT NameCode, Distribution FROM DISTRIBUTION")) {
      while (rs.next()) {
        String area = rs.getString("Distribution");
        String taxonId = resolveAcceptedTaxon(normCode(rs.getString("NameCode")), acceptedNameCodeToId, synAcceptedCode);
        if (taxonId == null || area == null || area.isBlank()) continue;
        Generator.set(distW, ColdpTerm.taxonID, taxonId);
        Generator.set(distW, ColdpTerm.area, area);
        Generator.set(distW, ColdpTerm.gazetteer, "text");
        distW.next();
        n++;
      }
    }
    LOG.info("Wrote {} distributions", n);
    return n;
  }
```

- [ ] **Step 4: `loadSources()`** — `GSDATABASES` → source citations via the inherited `addSourceCitation(id, title, alias, agents, publisher, version, releaseDate)`:

```java
  private void loadSources() throws Exception {
    int n = 0;
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT DatabaseName, DbFullName, Abbr, Institute, Contact, Version, ReleaseDate FROM GSDATABASES")) {
      while (rs.next()) {
        String title = rs.getString("DbFullName");
        if (title == null || title.isBlank()) title = rs.getString("DatabaseName");
        addSourceCitation("d" + rs.getString("DatabaseName"), title, rs.getString("Abbr"),
            rs.getString("Contact"), rs.getString("Institute"),
            rs.getString("Version"), parseDate(rs.getString("ReleaseDate")));
        n++;
      }
    }
    LOG.info("Loaded {} source databases (GSDs)", n);
  }
```
Note: `addSourceCitation`'s `agents` param is parsed by `ColacMappings.parseAgents`; `GSDATABASES.Contact` is a single contact name and is a reasonable author. If `Contact` proves low-quality, switch to joining `SPECIALISTS` — decide from the generated `source:` block.

- [ ] **Step 4b: Extend `read()`** to wire sources/references/vernaculars/distributions. At the top of `read()` (after `loadHierarchy`) add:
```java
    boolean hasDistribution = !tableColumns("DISTRIBUTION").isEmpty(); // false for 2000
```
After the `synAcceptedCode` line add `loadSources();` then `writeReferences();`. After the `emitSynonyms(...)` line add:
```java
    int nVern = emitVernaculars(acceptedNameCodeToId, synAcceptedCode);
    int nDist = hasDistribution ? emitDistributions(acceptedNameCodeToId, synAcceptedCode) : 0;
```
and change the LOG line to:
```java
    LOG.info("Early schema done: {} accepted, {} synonyms, {} bare names, {} vernaculars, {} distributions ({} synthetic nodes)",
        nAcc, syn[0], syn[1], nVern, nDist, emittedNodes.size());
```

- [ ] **Step 5: Build + run 2004 and 2000; full integrity check** (reuse `/tmp/colac_integrity.sh` from the prior work, or inline awk):
```bash
mvn -q package -DskipTests
for y in 2004 2000; do java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year $y -r /tmp/early$y 2>&1 | grep -iE 'Early schema done|ERROR'; done
```
Then verify 0 dangling parentID/taxonID (NameUsage parentID, Vernacular/Distribution taxonID all resolve to an emitted ID) for both years. Expected: 0 dangling, distributions=0 for 2000.

- [ ] **Step 6: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/EarlySchemaReader.java
git commit -m "colac(early): vernaculars, distributions, references, GSD sources"
```

---

## Task 7: Metadata (editors, issued, publisher)

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/colac/Generator.java` (`EDITORS` map ~lines 39–55; `addData` to read `CD-Date`)

- [ ] **Step 1: Add `EDITORS` entries** for the four early years (fallback = organisation + Bisby-led core, same style as 2006–2009):
```java
      Map.entry(2000, "Bisby F.A., Roskov Y.R."),
      Map.entry(2002, "Bisby F.A., Roskov Y.R."),
      Map.entry(2003, "Bisby F.A., Roskov Y.R."),
      Map.entry(2004, "Bisby F.A., Roskov Y.R."),
```
(insert before the `2005` entry).

- [ ] **Step 2: Set `issued` from the `CD-Date` table** for early years. The existing `addMetadata` uses `releaseDate` (the latest GSD release date) for `issued`; for early years prefer the `CD-Date` value. In `addData()`, after connecting, for `year <= 2004` read it and call `noteReleaseDate`:
```java
      if (year <= 2004) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT `Date` FROM `CD-Date` LIMIT 1")) {
          if (rs.next()) noteReleaseDate(SchemaReader.parseDate(rs.getString(1)));
        }
      }
```
(GSD `ReleaseDate`s also feed `noteReleaseDate` via `loadSources`; `noteReleaseDate` keeps the latest, which is fine — the CD-Date is the canonical release. If the latest GSD date should not override, gate `loadSources`' date-noting; decide from the emitted `issued`.)

- [ ] **Step 3: Build + check metadata** for 2004 and 2000:
```bash
mvn -q package -DskipTests
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year 2000 -r /tmp/early2000
grep -E 'alias|issued|version|publisher|Reading' /tmp/early2000/colac/metadata.yaml
```
Expected: `alias: COL00`, an `issued` near 2001-02, publisher Species 2000 & ITIS / Reading, GB.

- [ ] **Step 4: Commit.**
```bash
git add src/main/java/org/catalogueoflife/data/colac/Generator.java
git commit -m "colac(early): per-year editors + CD-Date issued for 2000-2004"
```

---

## Task 8: Integration test + docs

**Files:**
- Modify: `src/test/java/org/catalogueoflife/data/colac/GeneratorTest.java`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add an `@Ignore`d integration test** mirroring the existing ones:
```java
  @Test
  public void testEarlySchema2004() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "colac";
    cfg.year = 2004;
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }
```

- [ ] **Step 2: Generate all four years + verify 0 dangling / 0 orphan synonyms.**
```bash
mvn -q package -DskipTests
for y in 2000 2002 2003 2004; do
  java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year $y -r /tmp/early$y 2>&1 | grep -iE 'Early schema done|ERROR'
  zsh /tmp/colac_integrity.sh /tmp/early$y
  awk -F'\t' 'NR==1{next} ($4=="synonym"||$4=="ambiguous synonym"||$4=="misapplied") && $3==""{c++} END{print "  orphan synonyms="c+0}' /tmp/early$y/colac/NameUsage.tsv
done
```
Expected: every year 0 dangling parentID/taxonID, 0 orphan synonyms.

- [ ] **Step 3: Update `CLAUDE.md`** — add an "Early schema (2000, 2002–2004) → EarlySchemaReader" bullet next to the Old/New ones describing: flat `HIERARCHY` + `SCINAMES`, synthesized higher-taxon/genus nodes (`h|`-prefixed pipe-path ids), `NameCode` ids for names, shared synonym/bare-name resolution, sources from `GSDATABASES`, issued from `CD-Date`, 2001 rejected. Update the "Supported Sources"/colac year range to 2000–2019.

- [ ] **Step 4: Run the full colac unit suite.** `mvn test -Dtest=ColacMappingsTest,OldSchemaReaderTest,EarlySchemaReaderTest` → Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit.**
```bash
git add src/test/java/org/catalogueoflife/data/colac/GeneratorTest.java CLAUDE.md
git commit -m "colac(early): integration test + CLAUDE.md for 2000-2004"
```

---

## Notes for the implementer

- The early data uses the literal string `none` for empty `InfraSpecies`/`InfraSpMarker` and possibly other fields — treat `none` (case-insensitive) as blank wherever it would otherwise leak into a name or rank. Verify the exact set of sentinel values from `SELECT DISTINCT InfraSpMarker FROM SCINAMES` before finalizing `infraRank`/name building.
- Confirm per-year column stability across `col2000ac`/`col2002ac`/`col2003ac`/`col2004ac` (2000 lacks `DISTRIBUTION`; the code already guards that via `tableColumns`). If a column name differs in an earlier year, guard it the way `OldSchemaReader.loadSources` guards `authors_editors`/`organization`.
- MariaDB reserved word `Order` must stay backtick-quoted in SQL.
- All name_code matching goes through `normCode`; never compare raw codes.
