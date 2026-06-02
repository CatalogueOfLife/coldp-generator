# colac: Early Species 2000 CD-ROM Schema Reader (2000, 2002–2004)

**Source key:** `colac` (third era reader)
**Date:** 2026-06-02

## 1. Goal

Extend the `colac` generator to convert the four earliest Catalogue of Life *Annual
Checklist* releases — **2000, 2002, 2003, 2004** — into ColDP archives, matching the
quality bar of the existing 2005–2019 output (every synonym links to its own accepted
name; 0 dangling `parentID`/`taxonID`; 0 orphan synonyms).

2001 was never released; 2000 was the first. The databases are loaded locally as
`col2000ac`, `col2002ac`, `col2003ac`, `col2004ac`.

These years use a **third schema** — the original Species 2000 CD-ROM ("ACSII"/CD)
layout — distinct from both existing readers:

| Era | Years | Reader | Hierarchy source |
|-----|-------|--------|------------------|
| Early | 2000, 2002–2004 | **`EarlySchemaReader`** (new) | flat `HIERARCHY` columns + atomized `SCINAMES` |
| Old | 2005–2011 | `OldSchemaReader` | denormalized `taxa` parent_id tree |
| New | 2012–2019 | `NewSchemaReader` | normalized + `_taxon_tree`/`_search_scientific` |

## 2. Source schema (early)

Uppercase tables; the `all*` tables are CD-ROM UI helpers (no `NameCode`/author/status)
and are **not** used.

- **`SCINAMES`** — species/infraspecies-level names. Key columns: `NameCode` (id),
  `HierarchyCode` (→ `HIERARCHY`), `Family`, `Genus`, `Species`, `InfraSpecies`,
  `InfraSpMarker`, `ScientificNameAuthor`, `Sp2kStatus`, `AcceptedNameCode`,
  `AuthorRefNumber`, `DatabaseName`, `Comment`. ~583k rows (2004).
- **`HIERARCHY`** — one row per family, keyed by `HierarchyCode`, giving the flat
  Linnean columns `Kingdom`, `Phylum`, `Class`, `Order`, `Family`. ~6,024 rows.
- **`GSDATABASES`** — per-GSD source metadata (`DatabaseName`, `DbFullName`, `Abbr`,
  `Institute`, `Country`, `Contact`, `Version`, `ReleaseDate`, `Description`).
- **`SPECIALISTS`** — per-GSD contributors.
- **`COMNAMES`** — vernaculars (`NameCode`, `CommonName`, `Language`, `Country`,
  `RefNumber`, `DatabaseName`).
- **`DISTRIBUTION`** — free-text distributions (`NameCode`, `Distribution`,
  `DatabaseName`). **Absent in 2000.**
- **`REFERENCES`** — keyed by `RefNumber` (`ScientificNameAuthor`, `Year`, `Title`,
  `Source`, `DatabaseName`).
- **`CD-Date`** — single row, the CD release date → dataset `issued`.

`Sp2kStatus` text values: `accepted name`, `provisionally accepted name`,
`unambiguous synonym`(+ plural), `ambiguous synonym`(+ plural), `misapplied name`.

There is **no** checklist-level editor/credits/title metadata in the DB.

## 3. Approach A — shared resolution logic

The early and old schemas resolve synonyms identically (code-keyed names,
`AcceptedNameCode`/`accepted_name_code` linking, synonym chains, bare names,
case-insensitive codes). Extract the schema-agnostic pure helpers so the ColDP synonym
rules live in one place:

- Move `normCode` and `resolveAcceptedTaxon` from `OldSchemaReader` into
  **`ColacMappings`**; move their unit tests into `ColacMappingsTest`.
- Extend `ColacMappings.status` to map `unambiguous synonym`(s) → `synonym`
  (case/plural-tolerant; existing mappings unchanged).
- `acceptedAncestor` / `repairParent` / `buildRepairedParents` / `nameParentKey` stay in
  `OldSchemaReader` (they operate on the `taxa` parent_id tree, which the early schema
  lacks).
- `OldSchemaReader` output must be unchanged after the move (guarded by its tests + the
  prior 2005–2011 integration verification).

## 4. EarlySchemaReader

### 4.1 Classification synthesis (the new core)

No parent tree exists; reconstruct it from flat columns.

- **Higher taxa** Kingdom→Phylum→Class→Order→Family: from the `HIERARCHY` row for each
  used `HierarchyCode`. Each non-blank rank becomes a node; parent = next-higher
  non-blank rank; nodes deduplicated by their full path.
- **Genus** nodes: from `SCINAMES.Genus`, parent = the family node.
- **Accepted species/infraspecies**: from `SCINAMES` accepted statuses, parent = the
  genus node; `rank` species/infraspecies; `code` from Kingdom; authorship from
  `ScientificNameAuthor`.
- **Missing `HierarchyCode`** (~5,377/2004): fall back to `SCINAMES.Family` as a root
  family (no higher classification), genus under it. `SCINAMES.Family` is 100% populated
  for accepted names.

### 4.2 ID scheme

- Names (accepted species/infraspecies, synonyms, bare names): the `NameCode` verbatim
  (the stable CD-ROM id, e.g. `Alg-1`). Note: the *emitted* id is the verbatim `NameCode`,
  but all code↔code matching (`acceptedNameCodeToId`, `AcceptedNameCode` resolution) keys
  on `normCode(NameCode)` (upper-cased) for the same case-insensitivity reason as the old
  reader; the resolver returns the verbatim id so `parentID`s line up.
- Synthesized higher taxa + genera: a deterministic pipe-path id prefixed `h|`, e.g.
  `h|Plantae|Rhodophyta|Rhodophyceae|Palmariales|Palmariaceae|Palmaria`; parent = the
  path minus its last segment. `h|` cannot collide with a `NameCode`.
- References: `r<RefNumber>`; sources: `d<DatabaseName>`.

### 4.3 Synonyms & bare names (reuse §3)

- Synonyms (`unambiguous/ambiguous synonym`, `misapplied`): `parentID` resolved from
  `AcceptedNameCode` via `resolveAcceptedTaxon` (chains + case-insensitive).
- Unresolvable accepted name → **bare name** (`status = bare name`, no parent, no
  distribution/vernacular; original status in `remarks`).
- **No "missing-accepted" pass**: every accepted name is emitted directly from
  `SCINAMES`, so there is no tree/name mismatch (unlike 2005). A synonym either resolves
  to an emitted accepted `NameCode` or becomes a bare name.

### 4.4 Vernaculars / distributions / references / sources

- `COMNAMES` → `VernacularName` (`NameCode` → accepted taxon; single `referenceID` via
  `firstRef`).
- `DISTRIBUTION` → `Distribution` (`gazetteer = text`); skipped for 2000.
- `REFERENCES` → `Reference`; `SCINAMES.AuthorRefNumber` → `nameReferenceID`,
  `COMNAMES.RefNumber` → vernacular `referenceID`.
- `GSDATABASES` → `source:` registry (`DbFullName` title, `Abbr` alias, `Institute`
  publisher, `Version`, `ReleaseDate` issued); each NameUsage `sourceID = d<DatabaseName>`.
  GSD author/editor sourcing (`Contact` vs `SPECIALISTS`) finalized during the plan.

## 5. Dispatch, year range, metadata

- `Generator.addData`: `year ≤ 2004 → EarlySchemaReader; year ≤ 2011 → OldSchemaReader;
  else NewSchemaReader`.
- `--year` validation widened to 2000–2019, with **2001 explicitly rejected**.
- Metadata: `alias` `COL00`/`COL02`/`COL03`/`COL04`; `version` `Annual Checklist {year}`;
  `issued` from the `CD-Date` table; `url`
  `https://www.catalogueoflife.org/annual-checklist/{year}/`; publisher **Species 2000 &
  ITIS** (ITIS present from the 2000 release), city/country **Reading, GB** (all four
  years pre-2014). `creator` via the same fallback as 2006–2009 — the organisation plus
  the Bisby-led core editors of the era (no "How to cite" pages exist for these years).
  ISSN consistent with the pre-2016 years (confirm the early-CD value during the plan).

## 6. Testing

- **Unit** (`ColacMappingsTest` / a new `EarlySchemaReaderTest`): the relocated
  `normCode` / `resolveAcceptedTaxon` tests; the extended `status` mapping
  (`unambiguous synonym` → `synonym`); the pipe-path id builder + parent derivation.
- **Integration** (against the local MariaDB, `@Ignore`d like the others): generate all
  four years and assert **0 dangling `parentID`/`taxonID`** and **0 orphan synonyms**,
  plus a spot-check of a known taxon and its synonyms.

## 7. To confirm during planning

- Per-year column drift across 2000/2002/2003/2004 (2000 lacks `DISTRIBUTION`; verify
  `SCINAMES`/`HIERARCHY`/`GSDATABASES` columns are otherwise stable).
- `RefNumber` uniqueness and whether references are GSD-scoped.
- GSD author sourcing for the `source:` Citation (`GSDATABASES.Contact` and/or
  `SPECIALISTS`).
- Early-CD ISSN value.
- Whether bare-name / synthesized-node volumes per year are sane (sanity log lines like
  the old reader's).
