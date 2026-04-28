# SILVA SSU & LSU Generators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement two new ColDP generators (`silva-ssu`, `silva-lsu`) that convert the SILVA rRNA higher-rank taxonomy TSV files into ColDP archives.

**Architecture:** A shared `silva.BaseGenerator` holds all parsing and writing logic, parameterised on `"ssu"` or `"lsu"`. Two thin subclasses in `silvassu` and `silvalsu` packages satisfy the CLI convention. A one-line change to `GeneratorConfig` strips hyphens from source names so `-s silva-ssu` resolves to `silvassu.Generator`.

**Tech Stack:** Java 21, Apache HttpClient 5 (redirect following), `life.catalogue.common.io.TermWriter` (TSV writing), JUnit 4, Maven.

---

## File Map

| Action | Path |
|--------|------|
| Modify | `src/main/java/org/catalogueoflife/data/GeneratorConfig.java` |
| Create | `src/main/java/org/catalogueoflife/data/silva/BaseGenerator.java` |
| Create | `src/main/java/org/catalogueoflife/data/silvassu/Generator.java` |
| Create | `src/main/java/org/catalogueoflife/data/silvalsu/Generator.java` |
| Create | `src/main/resources/silva-ssu/metadata.yaml` |
| Create | `src/main/resources/silva-lsu/metadata.yaml` |
| Create | `src/test/java/org/catalogueoflife/data/silva/BaseGeneratorTest.java` |
| Create | `src/test/java/org/catalogueoflife/data/silvassu/GeneratorTest.java` |
| Create | `src/test/java/org/catalogueoflife/data/silvalsu/GeneratorTest.java` |
| Modify | `src/test/java/org/catalogueoflife/data/ManualCli.java` |

---

## Task 1: Fix CLI to accept hyphenated source names

**Files:**
- Modify: `src/main/java/org/catalogueoflife/data/GeneratorConfig.java:66-68`

- [ ] **Step 1: Make the change**

In `GeneratorConfig.builderClass()`, change the single line that builds `classname`:

```java
// Before:
String classname = GeneratorConfig.class.getPackage().getName() + "." + source.toLowerCase() + ".Generator";

// After:
String classname = GeneratorConfig.class.getPackage().getName() + "." + source.toLowerCase().replace("-", "") + ".Generator";
```

The full method becomes:

```java
public Class<? extends AbstractColdpGenerator> builderClass() {
  try {
    String classname = GeneratorConfig.class.getPackage().getName() + "." + source.toLowerCase().replace("-", "") + ".Generator";
    return (Class<? extends AbstractColdpGenerator>) GeneratorConfig.class.getClassLoader().loadClass(classname);
  } catch (ClassNotFoundException e) {
    List<String> sources = Lists.newArrayList();
    Joiner joiner = Joiner.on(",").skipNulls();
    throw new IllegalArgumentException(source + " not a valid source. Please use one of: " + joiner.join(sources), e);
  } catch (Exception e) {
    throw new RuntimeException(e);
  }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
mvn compile -q
```

Expected: `BUILD SUCCESS` with no errors.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/GeneratorConfig.java
git commit -m "feat: strip hyphens from source name in CLI class lookup"
```

---

## Task 2: Write failing unit tests for BaseGenerator static helpers

**Files:**
- Create: `src/test/java/org/catalogueoflife/data/silva/BaseGeneratorTest.java`

The static helpers `splitLine`, `extractName`, `parentPath`, and `mapRemark` will be package-private methods on `silva.BaseGenerator`. The test lives in the same package so it can access them.

- [ ] **Step 1: Create the test file**

```java
package org.catalogueoflife.data.silva;

import org.junit.Test;
import static org.junit.Assert.*;

public class BaseGeneratorTest {

  // ── splitLine ─────────────────────────────────────────────────────────────

  @Test
  public void splitLineTypical() {
    String[] f = BaseGenerator.splitLine("Archaea;Crenarchaeota;\t12\tphylum\t\t119");
    assertNotNull(f);
    assertEquals(5, f.length);
    assertEquals("Archaea;Crenarchaeota;", f[0]);
    assertEquals("12",    f[1]);
    assertEquals("phylum", f[2]);
    assertEquals("",       f[3]);
    assertEquals("119",    f[4]);
  }

  @Test
  public void splitLineRootDomain() {
    // Root entry: no release version, empty remark
    String[] f = BaseGenerator.splitLine("Archaea;\t2\tdomain\t\t");
    assertNotNull(f);
    assertEquals("Archaea;", f[0]);
    assertEquals("2",        f[1]);
    assertEquals("domain",   f[2]);
    assertEquals("",         f[3]);
    assertEquals("",         f[4]);
  }

  @Test
  public void splitLineRemarkPresent() {
    String[] f = BaseGenerator.splitLine("Bacteria;uncultured;\t999\tphylum\ta\t138");
    assertEquals("a", f[3]);
  }

  @Test
  public void splitLineNullOrBlank() {
    assertNull(BaseGenerator.splitLine(null));
    assertNull(BaseGenerator.splitLine(""));
    assertNull(BaseGenerator.splitLine("   "));
  }

  // ── extractName ───────────────────────────────────────────────────────────

  @Test
  public void extractNameDeepPath() {
    assertEquals("Nitrososphaeria",
        BaseGenerator.extractName("Archaea;Crenarchaeota;Nitrososphaeria;"));
  }

  @Test
  public void extractNameRootPath() {
    assertEquals("Archaea", BaseGenerator.extractName("Archaea;"));
  }

  @Test
  public void extractNameCandidatus() {
    assertEquals("Candidatus Nitrosocaldus",
        BaseGenerator.extractName("Archaea;Crenarchaeota;Nitrososphaeria;Nitrosocaldales;Nitrosocaldaceae;Candidatus Nitrosocaldus;"));
  }

  // ── parentPath ────────────────────────────────────────────────────────────

  @Test
  public void parentPathDeep() {
    assertEquals("Archaea;Crenarchaeota;",
        BaseGenerator.parentPath("Archaea;Crenarchaeota;Nitrososphaeria;"));
  }

  @Test
  public void parentPathOneLevel() {
    assertEquals("Archaea;",
        BaseGenerator.parentPath("Archaea;Crenarchaeota;"));
  }

  @Test
  public void parentPathRoot() {
    // Single-segment paths have no parent
    assertNull(BaseGenerator.parentPath("Archaea;"));
  }

  // ── mapRemark ─────────────────────────────────────────────────────────────

  @Test
  public void mapRemarkA() {
    assertEquals("environmental origin", BaseGenerator.mapRemark("a"));
  }

  @Test
  public void mapRemarkW() {
    assertEquals("scheduled for revision", BaseGenerator.mapRemark("w"));
  }

  @Test
  public void mapRemarkEmpty() {
    assertNull(BaseGenerator.mapRemark(""));
    assertNull(BaseGenerator.mapRemark(null));
  }

  @Test
  public void mapRemarkUnknown() {
    assertNull(BaseGenerator.mapRemark("z"));
  }
}
```

- [ ] **Step 2: Run tests to verify they fail with class-not-found**

```bash
mvn test -pl . -Dtest=BaseGeneratorTest -q 2>&1 | tail -5
```

Expected: compilation error (`silva.BaseGenerator` does not exist yet) or test failure.

- [ ] **Step 3: Commit the failing tests**

```bash
git add src/test/java/org/catalogueoflife/data/silva/BaseGeneratorTest.java
git commit -m "test: add failing unit tests for silva.BaseGenerator static helpers"
```

---

## Task 3: Implement BaseGenerator

**Files:**
- Create: `src/main/java/org/catalogueoflife/data/silva/BaseGenerator.java`

- [ ] **Step 1: Create the file**

```java
package org.catalogueoflife.data.silva;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.UTF8IoUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Shared base for the SILVA SSU and LSU taxonomy generators.
 *
 * Source file: tax_slv_{unit}_{version}.txt.gz
 * Format: tab-separated, 5 columns — path, taxid, rank, remark, release.
 * All taxa are accepted higher-rank entries; no synonyms, authors, or references.
 */
public abstract class BaseGenerator extends AbstractColdpGenerator {

  private static final String BASE_URL =
      "https://www.arb-silva.de/fileadmin/silva_databases/current/Exports/taxonomy/";
  private static final String REDIRECT_URL =
      "https://www.arb-silva.de/current-release/";
  private static final java.util.regex.Pattern VERSION_PAT =
      java.util.regex.Pattern.compile("release[_-](\\d+\\.\\d+)", java.util.regex.Pattern.CASE_INSENSITIVE);

  /** "ssu" or "lsu" */
  private final String unit;
  private String version;

  protected BaseGenerator(GeneratorConfig cfg, String unit) throws IOException {
    super(cfg, true);
    this.unit = unit;
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  @Override
  protected void prepare() throws Exception {
    version = detectVersion();
    if (version == null) {
      version = LocalDate.now().toString();
      LOG.warn("Could not detect SILVA version from redirect; using {}", version);
    } else {
      LOG.info("Detected SILVA version: {}", version);
    }
  }

  @Override
  protected void addData() throws Exception {
    String filename = "tax_slv_" + unit + "_" + version + ".txt.gz";
    URI downloadUri = URI.create(BASE_URL + filename);
    File gz = download(filename, downloadUri);

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.status,
        ColdpTerm.remarks
    ));

    // Pass 1: build path → taxid map
    Map<String, Integer> pathMap = new HashMap<>();
    try (BufferedReader br = gzipReader(gz)) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] f = splitLine(line);
        if (f == null) continue;
        String path  = f[0].trim();
        int    taxid = parseId(f[1]);
        if (taxid > 0) pathMap.put(path, taxid);
      }
    }
    LOG.info("Pass 1: {} paths indexed", pathMap.size());

    // Pass 2: write NameUsage rows
    int n = 0;
    try (BufferedReader br = gzipReader(gz)) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] f = splitLine(line);
        if (f == null) continue;
        String path   = f[0].trim();
        int    taxid  = parseId(f[1]);
        String rank   = f[2].trim();
        String remark = f.length > 3 ? f[3].trim() : "";
        if (taxid <= 0) continue;

        String name     = extractName(path);
        String parent   = parentPath(path);
        Integer parentId = parent != null ? pathMap.get(parent) : null;
        String remarks  = mapRemark(remark);

        writer.set(ColdpTerm.ID,             taxid);
        if (parentId != null) writer.set(ColdpTerm.parentID, parentId);
        if (!rank.isEmpty())  writer.set(ColdpTerm.rank,     rank);
        writer.set(ColdpTerm.scientificName,  name);
        writer.set(ColdpTerm.status,          "accepted");
        if (remarks != null)  writer.set(ColdpTerm.remarks,  remarks);
        writer.next();
        n++;
      }
    }
    LOG.info("Pass 2: {} NameUsage records written", n);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued",  LocalDate.now().toString());
    // version with dot removed for the release-page URL, e.g. 138.2 → 1382
    metadata.put("versionNoDot", version.replace(".", ""));
    super.addMetadata();
  }

  // ── Static helpers (package-private for testing) ──────────────────────────

  /** Splits a SILVA taxonomy line on TAB; returns null for blank lines. */
  static String[] splitLine(String line) {
    if (line == null || line.isBlank()) return null;
    return line.split("\t", -1);
  }

  /** Returns the taxon name: last non-empty semicolon-delimited segment of {@code path}. */
  static String extractName(String path) {
    // strip trailing semicolon, split, take last element
    String stripped = path.endsWith(";") ? path.substring(0, path.length() - 1) : path;
    int lastSemi = stripped.lastIndexOf(';');
    return lastSemi < 0 ? stripped : stripped.substring(lastSemi + 1);
  }

  /**
   * Returns the parent path (with trailing semicolon) for the given path,
   * or null if {@code path} is a root (single-segment) path.
   */
  static String parentPath(String path) {
    String stripped = path.endsWith(";") ? path.substring(0, path.length() - 1) : path;
    int lastSemi = stripped.lastIndexOf(';');
    if (lastSemi < 0) return null;
    return stripped.substring(0, lastSemi + 1);
  }

  /** Maps SILVA remark codes to human-readable ColDP remarks, or null if none. */
  static String mapRemark(String remark) {
    if (remark == null || remark.isBlank()) return null;
    return switch (remark.trim()) {
      case "a" -> "environmental origin";
      case "w" -> "scheduled for revision";
      default  -> null;
    };
  }

  // ── Private helpers ────────────────────────────────────────────────────────

  private static int parseId(String raw) {
    try { return Integer.parseInt(raw.trim()); }
    catch (NumberFormatException e) { return 0; }
  }

  private static BufferedReader gzipReader(File gz) throws IOException {
    return new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(gz)), StandardCharsets.UTF_8));
  }

  /**
   * Follows the SILVA /current-release/ redirect and extracts the version
   * string (e.g. "138.2") from the final URL using the pattern
   * {@code release[_-](\d+\.\d+)}.  Returns null if detection fails.
   */
  private String detectVersion() {
    try {
      HttpClient client = HttpClient.newBuilder()
          .followRedirects(HttpClient.Redirect.NORMAL)
          .build();
      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(REDIRECT_URL))
          .method("HEAD", HttpRequest.BodyPublishers.noBody())
          .build();
      HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
      String finalUrl = resp.uri().toString();
      LOG.info("SILVA current-release redirected to: {}", finalUrl);
      var m = VERSION_PAT.matcher(finalUrl);
      if (m.find()) return m.group(1);
    } catch (Exception e) {
      LOG.warn("Version detection failed: {}", e.getMessage());
    }
    return null;
  }
}
```

- [ ] **Step 2: Run the unit tests**

```bash
mvn test -pl . -Dtest=BaseGeneratorTest -q
```

Expected: `BUILD SUCCESS`, all tests green.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/silva/BaseGenerator.java
git commit -m "feat: add silva.BaseGenerator with TSV parsing and NameUsage writing"
```

---

## Task 4: Create the two thin subclass generators

**Files:**
- Create: `src/main/java/org/catalogueoflife/data/silvassu/Generator.java`
- Create: `src/main/java/org/catalogueoflife/data/silvalsu/Generator.java`

- [ ] **Step 1: Create `silvassu/Generator.java`**

```java
package org.catalogueoflife.data.silvassu;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.silva.BaseGenerator;

import java.io.IOException;

public class Generator extends BaseGenerator {
  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, "ssu");
  }
}
```

- [ ] **Step 2: Create `silvalsu/Generator.java`**

```java
package org.catalogueoflife.data.silvalsu;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.silva.BaseGenerator;

import java.io.IOException;

public class Generator extends BaseGenerator {
  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, "lsu");
  }
}
```

- [ ] **Step 3: Verify compilation**

```bash
mvn compile -q
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/org/catalogueoflife/data/silvassu/Generator.java \
        src/main/java/org/catalogueoflife/data/silvalsu/Generator.java
git commit -m "feat: add silvassu and silvalsu generator subclasses"
```

---

## Task 5: Create metadata.yaml files

**Files:**
- Create: `src/main/resources/silva-ssu/metadata.yaml`
- Create: `src/main/resources/silva-lsu/metadata.yaml`

The `{version}`, `{issued}`, and `{versionNoDot}` placeholders are filled in by `BaseGenerator.addMetadata()` via `SimpleTemplate.render()`.

- [ ] **Step 1: Create `src/main/resources/silva-ssu/metadata.yaml`**

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/CatalogueOfLife/coldp/master/metadata.json
---
alias: SILVA-SSU
title: SILVA SSU Taxonomy
description: |-
  SILVA provides a comprehensive, quality-checked ribosomal RNA sequence database for the three domains of life:
  Bacteria, Archaea, and Eukaryota. This dataset contains the SILVA SSU (Small Subunit, 16S/18S rRNA) taxonomy —
  a curated higher-rank classification covering all sequences in the SILVA SSU databases.

  The taxonomy is produced by a semi-automatic curation pipeline that integrates authoritative external resources
  including Bergey's taxonomic outline (Bacteria and Archaea backbone since release 102), LPSN, NCBI Taxonomy,
  the Genome Taxonomy Database (GTDB, integrated since release 138), and UniEuk for eukaryotic lineages
  (also since release 138). Since release 111, substantial effort has been invested in improving coverage and
  accuracy of eukaryotic higher-rank classification.

  All taxa in this file are accepted higher-rank entries (domain through genus); no species-level or synonym
  records are included. Numeric taxon IDs are mostly stable across SILVA releases but do not match LSU IDs.

  See the full release notes at https://www.arb-silva.de/documentation/release-{versionNoDot}

issued: {issued}
version: {version}
contact:
  email: contact@arb-silva.de
  organisation: Leibniz Institute DSMZ – German Collection of Microorganisms and Cell Cultures GmbH
creator:
  - organisation: Leibniz Institute DSMZ – German Collection of Microorganisms and Cell Cultures GmbH
    url: https://www.arb-silva.de
contributor:
  - family: Döring
    given: Markus
    orcid: 0000-0001-7757-1889
    country: Germany
    city: Berlin
    email: mdoering@gbif.org
    note: Developer of ColDP generator code
license: CC BY 4.0
url: https://www.arb-silva.de/documentation/silva-taxonomy
taxonomicScope: Bacteria, Archaea, Eukaryota (higher-rank only, SSU rRNA)
geographicScope: global
confidence: 4
completeness: 95
notes: Generated by coldp-generator from tax_slv_ssu_{version}.txt.gz
```

- [ ] **Step 2: Create `src/main/resources/silva-lsu/metadata.yaml`**

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/CatalogueOfLife/coldp/master/metadata.json
---
alias: SILVA-LSU
title: SILVA LSU Taxonomy
description: |-
  SILVA provides a comprehensive, quality-checked ribosomal RNA sequence database for the three domains of life:
  Bacteria, Archaea, and Eukaryota. This dataset contains the SILVA LSU (Large Subunit, 23S/28S rRNA) taxonomy —
  a curated higher-rank classification covering all sequences in the SILVA LSU databases.

  The taxonomy is produced by a semi-automatic curation pipeline that integrates authoritative external resources
  including Bergey's taxonomic outline (Bacteria and Archaea backbone since release 102), LPSN, NCBI Taxonomy,
  the Genome Taxonomy Database (GTDB, integrated since release 138), and UniEuk for eukaryotic lineages
  (also since release 138). Since release 111, substantial effort has been invested in improving coverage and
  accuracy of eukaryotic higher-rank classification.

  All taxa in this file are accepted higher-rank entries (domain through genus); no species-level or synonym
  records are included. Numeric taxon IDs are mostly stable across SILVA releases but do not match SSU IDs.

  See the full release notes at https://www.arb-silva.de/documentation/release-{versionNoDot}

issued: {issued}
version: {version}
contact:
  email: contact@arb-silva.de
  organisation: Leibniz Institute DSMZ – German Collection of Microorganisms and Cell Cultures GmbH
creator:
  - organisation: Leibniz Institute DSMZ – German Collection of Microorganisms and Cell Cultures GmbH
    url: https://www.arb-silva.de
contributor:
  - family: Döring
    given: Markus
    orcid: 0000-0001-7757-1889
    country: Germany
    city: Berlin
    email: mdoering@gbif.org
    note: Developer of ColDP generator code
license: CC BY 4.0
url: https://www.arb-silva.de/documentation/silva-taxonomy
taxonomicScope: Bacteria, Archaea, Eukaryota (higher-rank only, LSU rRNA)
geographicScope: global
confidence: 4
completeness: 95
notes: Generated by coldp-generator from tax_slv_lsu_{version}.txt.gz
```

- [ ] **Step 3: Compile to verify resources load**

```bash
mvn compile -q
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/silva-ssu/metadata.yaml \
        src/main/resources/silva-lsu/metadata.yaml
git commit -m "feat: add silva-ssu and silva-lsu metadata.yaml"
```

---

## Task 6: Write integration-style generator tests and update ManualCli

**Files:**
- Create: `src/test/java/org/catalogueoflife/data/silvassu/GeneratorTest.java`
- Create: `src/test/java/org/catalogueoflife/data/silvalsu/GeneratorTest.java`
- Modify: `src/test/java/org/catalogueoflife/data/ManualCli.java`

These tests are `@Ignore`-annotated manual integration tests (same pattern as other generators — they hit the network and write to disk, so they're not run in CI).

- [ ] **Step 1: Create `silvassu/GeneratorTest.java`**

```java
package org.catalogueoflife.data.silvassu;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.Utils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore("manual / integration test — requires network access")
public class GeneratorTest {

  @Test
  public void testGenerate() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "silva-ssu";
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }
}
```

- [ ] **Step 2: Create `silvalsu/GeneratorTest.java`**

```java
package org.catalogueoflife.data.silvalsu;

import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.Utils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore("manual / integration test — requires network access")
public class GeneratorTest {

  @Test
  public void testGenerate() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "silva-lsu";
    new Generator(cfg).run();
    Utils.verifyMetadata(new File(cfg.archiveDir(), "metadata.yaml"));
  }
}
```

- [ ] **Step 3: Add ManualCli entries**

In `src/test/java/org/catalogueoflife/data/ManualCli.java`, add two commented-out lines inside `main()`:

```java
//GeneratorCLI.main( new String[]{"-s", "silva-ssu", "-r", "/tmp/coldp/archives"} );
//GeneratorCLI.main( new String[]{"-s", "silva-lsu", "-r", "/tmp/coldp/archives"} );
```

- [ ] **Step 4: Run all non-ignored tests to confirm nothing is broken**

```bash
mvn test -q
```

Expected: `BUILD SUCCESS`. The two new `@Ignore` tests are skipped; `BaseGeneratorTest` passes.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/org/catalogueoflife/data/silvassu/GeneratorTest.java \
        src/test/java/org/catalogueoflife/data/silvalsu/GeneratorTest.java \
        src/test/java/org/catalogueoflife/data/ManualCli.java
git commit -m "test: add silva-ssu and silva-lsu generator tests and ManualCli entries"
```
