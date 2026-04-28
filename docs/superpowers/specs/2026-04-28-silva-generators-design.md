# SILVA SSU & LSU Generator Design

**Date:** 2026-04-28  
**Status:** Approved

## Overview

Two new ColDP generators for the SILVA rRNA taxonomy:
- **`silva-ssu`** — Small Subunit (16S/18S) rRNA taxonomy
- **`silva-lsu`** — Large Subunit (23S/28S) rRNA taxonomy

Both process the same TSV format from the same SILVA release. A shared abstract base class eliminates duplication; two thin subclasses satisfy the project's one-package-per-source convention.

## Source Data

**Primary file:** `tax_slv_{ssu|lsu}_{version}.txt.gz`  
**Download base:** `https://www.arb-silva.de/fileadmin/silva_databases/current/Exports/taxonomy/`  
**Current release:** 138.2 (2024-07-11)  
**License:** CC BY 4.0

File format: tab-separated, no header, 5 columns:

| # | Field   | Description |
|---|---------|-------------|
| 0 | `path`  | Semicolon-separated full path including the taxon itself, trailing semicolon. E.g. `Archaea;Crenarchaeota;Nitrososphaeria;` |
| 1 | `taxid` | Unique numeric identifier (stable across releases within SSU or LSU; IDs do NOT match between SSU and LSU) |
| 2 | `rank`  | Rank label (e.g. `domain`, `phylum`, `class`, `order`, `family`, `genus`). Passed through as-is; ChecklistBank normalises. |
| 3 | `remark` | Empty, `a` (environmental origin — no cultured sequences), or `w` (scheduled for revision in next release) |
| 4 | `release` | SILVA release version when the taxon was added, or empty for the oldest entries |

~8,800 rows for LSU; SSU is similar in scale. All taxa are accepted higher-rank entries (no species, no synonyms, no authors).

## Architecture

```
src/main/java/…/data/
├── silva/
│   └── BaseGenerator.java       # all parsing + writing logic
├── silvassu/
│   └── Generator.java           # extends silva.BaseGenerator, passes "ssu"
└── silvalsu/
    └── Generator.java           # extends silva.BaseGenerator, passes "lsu"

src/main/resources/
├── silva-ssu/
│   └── metadata.yaml
└── silva-lsu/
    └── metadata.yaml
```

`BaseGenerator` extends `AbstractColdpGenerator` but is not a registered source. `silvassu.Generator` and `silvalsu.Generator` each extend it with a two-line constructor.

## CLI Change

`GeneratorConfig.builderClass()` — strip hyphens before the package lookup:

```java
String classname = … + source.toLowerCase().replace("-", "") + ".Generator";
```

This allows `-s silva-ssu` and `-s silva-lsu` to resolve to `silvassu.Generator` and `silvalsu.Generator`.

## Data Processing

### Version Detection (`prepare()`)

1. HTTP GET `https://www.arb-silva.de/current-release/` (follow redirects).
2. Apply regex `release[_-](\d+\.\d+)` to the final URL.
3. If matched: set `version` and construct the download URL.
4. If no match: log a warning and fall back to today's date as version.

### Parsing (`addData()`)

**Two-pass approach** over the uncompressed lines:

**Pass 1 — build path→ID map:**
- Read every line, split on `\t`.
- Strip trailing `;` from path, store `path → taxid` in a `HashMap<String, Integer>`.

**Pass 2 — write NameUsage:**
- For each line, extract fields.
- `scientificName` = last segment of path (after splitting on `;` and dropping the empty trailing element).
- `parentID` = look up `path minus last segment` in the map built in pass 1. Root taxa (single-segment paths like `Archaea;`) have no parent.
- `rank` = column 2, passed through as-is.
- `remarks`:
  - `a` → `"environmental origin"`
  - `w` → `"scheduled for revision"`
  - empty → omit

### ColDP Output

**NameUsage.tsv columns:**

| ColDP field | Value |
|---|---|
| `ID` | `taxid` |
| `parentID` | resolved from path map |
| `rank` | rank column |
| `scientificName` | last path segment |
| `status` | `"accepted"` (all) |
| `remarks` | mapped from remark flag, or omitted |

No other ColDP files (no synonyms, references, vernacular names, distributions, type material).

## Metadata

Both `metadata.yaml` files share the same description body and differ only in `title`, `alias`, and `url`.

- `title`: `SILVA SSU Taxonomy` / `SILVA LSU Taxonomy`
- `alias`: `SILVA-SSU` / `SILVA-LSU`
- `version`: `{version}` (detected from redirect URL, e.g. `138.2`)
- `issued`: `{issued}` (date of generation)
- `license`: CC BY 4.0
- `creator`: Leibniz Institute DSMZ
- `url`: SSU — `https://www.arb-silva.de/documentation/silva-taxonomy`; LSU — same
- `taxonomicScope`: Bacteria, Archaea, Eukaryota
- `confidence`: 4
- `completeness`: 95

Description synthesises content from:
- `https://www.arb-silva.de/documentation/silva-taxonomy`
- `https://www.arb-silva.de/documentation/release-1382`

Closes with a link to the release page: `https://www.arb-silva.de/documentation/release-{version_nodot}` where `version_nodot` = version string with the dot removed (e.g. `138.2` → `1382`).

## Tests

`src/test/java/silvassu/GeneratorTest.java` and `src/test/java/silvalsu/GeneratorTest.java` — same minimal pattern as other generators.

## Out of Scope

- Diff files (`tax_slv_*.diff`) — operational change tracking, not needed for ColDP
- Sequence files (`*.fasta.gz`) — out of scope
- Alternative taxonomies (GTDB, RDP, LTP mappings)
