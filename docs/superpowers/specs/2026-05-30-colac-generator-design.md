# ColDP Generator for Historical CoL Annual Checklists (2005–2019)

**Source key:** `colac`
**Date:** 2026-05-30

## 1. Goal

Convert the historical Catalogue of Life *Annual Checklist* releases (2005–2019) into
ColDP archives. Each year exists only as a MySQL dump, restored locally in a MariaDB
docker container with one database per year: `col2005ac` … `col2019ac`.

Two distinct database schemas are used across the years:

| Era | Years | Schema style |
|-----|-------|--------------|
| Old | 2005–2011 | denormalized `taxa` tree + atomized `scientific_names` |
| New | 2012–2019 | normalized Species 2000 format, **plus** fully-populated denormalized helper tables (`_taxon_tree`, `_search_scientific`) |

The MySQL dumps are publicly downloadable from
<https://www.catalogueoflife.org/data/download>. This must be documented in the
generator's README/CLAUDE.md section.

## 2. Invocation & framework changes

- A single source `colac`, with the year selected at runtime:
  ```
  java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year 2015
  ```
- New `GeneratorConfig` CLI parameters:
  - `--year` (int) — required for this source; selects database `col{year}ac`.
  - `--db-host` (default `localhost`)
  - `--db-port` (default `3306`)
  - `--db-user` (default `root`)
  - `--db-pass` (default `root`)
- Connection string: `jdbc:mariadb://{host}:{port}/col{year}ac`.
- Add **MariaDB Connector/J** (`org.mariadb.jdbc:mariadb-java-client`) to `pom.xml`.
  (No JDBC MySQL/MariaDB driver is present today; only `sqlite-jdbc` for ITIS.)
- Output is written to `repository/colac` (and `colac.zip`), cleaned on each run, as per
  the existing `AbstractGenerator` behaviour. For batch production of all years, the
  caller varies `-r/--repository` per year. (This is the agreed output model.)

The ITIS generator (`itis/Generator.java`) is the JDBC reference implementation — same
`AbstractColdpGenerator` + `java.sql` pattern, just SQLite instead of MariaDB.

## 3. Code structure (package `org.catalogueoflife.data.colac`)

- **`Generator.java`** — extends `AbstractColdpGenerator`. Opens the JDBC connection,
  creates the shared `TermWriter`s, dispatches to the era reader by year
  (`year <= 2011` → `OldSchemaReader`, else `NewSchemaReader`), then writes the GSD
  source registry into metadata.
- **`OldSchemaReader.java`** — 2005–2011 conversion.
- **`NewSchemaReader.java`** — 2012–2019 conversion.

Each reader receives the open `Connection` and the writer set, and is independently
testable. Both write through the same writers, producing identical ColDP output files.

## 4. ColDP output files

`NameUsage.tsv`, `VernacularName.tsv`, `Distribution.tsv`, `Reference.tsv`,
`metadata.yaml`.

Single denormalized **`NameUsage`** row type holding accepted taxa **and** synonyms
(like the ITIS generator), with columns:

```
ID, sourceID, parentID, status, rank,
scientificName, authorship, code,
referenceID, nameReferenceID, remarks
```

(No `nameStatus`/`basionymID` — the dumps carry no nomenclatural status or basionym
links; see §12.)

## 5. ID scheme (both eras)

| Kind | ID |
|------|----|
| Accepted taxon | `t{taxonNodeId}` |
| Synonym | `s{synonymId}` |
| Reference | `r{referenceId}` |
| Source database (sourceID) | `d{sourceDbId}` |

All reference IDs are `r` + a numeric DB id, so they **never contain a comma** — this is
what makes the comma-joined multi-valued `referenceID` (§7) safe. A guard/test asserts
no emitted reference ID contains the ColDP multi-value separator.

## 6. Field mapping

### 6.1 Accepted/synonym split (robust discriminators)

- **Old:** accepted taxa = `taxa` rows with `is_accepted_name = 1`, hierarchy via
  `taxa.parent_id`. Synonyms = `scientific_names` rows whose `sp2000_status` is a synonym
  type, linked to the accepted name via `accepted_name_code`.
- **New:** accepted taxa = `_taxon_tree` rows, hierarchy via `_taxon_tree.parent_id`.
  Synonyms = `_search_scientific` rows with `accepted_species_id > 0`, linked via
  `accepted_species_id`. (`accepted_species_id` is the reliable split — the
  `_search_scientific.status` column, values 0–5, is only used to refine the synonym
  subtype / provisional flag, never to decide accepted-vs-synonym.)

### 6.2 NameUsage

| ColDP field | Old (2005–2011) | New (2012–2019) |
|---|---|---|
| ID | `t{taxa.record_id}` / `s{scientific_names.record_id}` | `t{_taxon_tree.taxon_id}` / `s{synonym/_search id}` |
| parentID | `t{taxa.parent_id}` (0 ⇒ root, omit) / accepted via `accepted_name_code` → `t{…}` | `t{_taxon_tree.parent_id}` / `t{accepted_species_id}` |
| scientificName | `taxa.name` (full name; strip none — `name_with_italics` is the HTML variant, ignored) for accepted; reconstruct `genus + [infraspecies_marker] + species + infraspecies` for synonyms | `_taxon_tree.name` (no author) for accepted; reconstruct from `_search_scientific` genus/species/infraspecies for synonyms |
| authorship | `scientific_names.author` looked up by `name_code` (species/infraspecies only) | `_search_scientific.author` looked up by `taxon_id` |
| rank | `taxa.taxon` lower-cased | `_taxon_tree.rank` |
| status | mapped from `sp2000_status` (§6.3) | accepted: from `_search_scientific.status` (1→accepted, 4→provisionally accepted); synonym: from status (§6.3) |
| code | nomenclatural code inferred from the lineage's kingdom (§6.5) | same |
| referenceID / nameReferenceID | §7 | §7 |
| sourceID | `d{scientific_names.database_id}` / for higher taxa, none unless derivable | `d{_search_scientific.source_database_id}` / `taxon.source_database_id` |

### 6.3 Status mapping

ColDP `TaxonomicStatus` values match the source labels directly.

Old `sp2000_status` / New `scientific_name_status`:

| Source label | id (new) | ColDP status |
|---|---|---|
| accepted name | 1 | `accepted` |
| ambiguous synonym | 2 | `ambiguous synonym` |
| misapplied name | 3 | `misapplied` |
| provisionally accepted name | 4 | `provisionally accepted` |
| synonym | 5 | `synonym` |

New-schema `_search_scientific.status` numeric fallback: `2`→ambiguous synonym,
`3`→misapplied, `5`→synonym, any other value on a synonym row → `synonym`
(verify `status = 0` semantics during implementation; default to `synonym`).

### 6.4 VernacularName

| ColDP | Old | New |
|---|---|---|
| taxonID | `name_code` → resolved NameUsage ID | `common_name.taxon_id` → `t{…}` |
| name | `common_names.common_name` | `common_name_element.name` (via `common_name_element_id`) |
| language | `common_names.language` is the **English language name** (e.g. "English", "Spanish") → map to ISO 639-3 via a lookup; blank ⇒ omit | `common_name.language_iso` is already ISO 639-3 (e.g. `eng`, `cmn`) → use directly; `NULL` ⇒ omit |
| country | `common_names.country` (free text, e.g. "USA") | `common_name.country_iso` (e.g. `US`) |
| referenceID | ComNameRef links (§7) | `reference_to_common_name` (§7) |

The old→ISO 639-3 language mapping uses a static lookup of the English language names
present in the data (top values: English, Spanish, French, Portuguese, Russian, Danish,
Finnish, Czech, Italian, German, Japanese, Malay, Tagalog, Afrikaans, …). Unmapped values
are passed through verbatim (ChecklistBank will attempt to normalise on import).

### 6.5 Nomenclatural code

Reuse ITIS's kingdom→code logic: Animalia/Protozoa → ICZN, Plantae/Fungi/Chromista →
botanical, Bacteria/Archaea → bacterial, Viruses → virus. The kingdom per record is
resolved from the denormalized kingdom column where available (`_search_scientific.kingdom`
/ `families.kingdom`) else by walking the parent chain to the kingdom-rank node.

### 6.6 Distribution

| ColDP | Old | New |
|---|---|---|
| taxonID | `distribution.name_code` → resolved NameUsage ID | `distribution.taxon_detail_id` → `taxon_detail.taxon_id` → `t{…}` |
| area | `distribution.distribution` (free text) | `region.name` (via `region_id`) |
| gazetteer | `text` | mapped from `region.region_standard_id` → `region_standard` (iso/tdwg/text…); default `text` |
| establishmentMeans | — | `distribution_status` → `native`/`native-domesticated` ⇒ native; `alien`/`alien-domesticated`/`domesticated` ⇒ introduced; `uncertain` ⇒ omit |

## 7. References (link categories → ColDP target)

Both eras agree on three link categories:

| Category | Old (`scientific_name_references.reference_type`) | New (`reference_type`) | ColDP target |
|---|---|---|---|
| Nomenclatural | `AuthorRef` | id 1 "Nomenclatural Reference" | **`nameReferenceID`** on the name's NameUsage row |
| Taxonomic acceptance | `StatusRef`, `StatRef` | id 2 "Taxonomic Acceptance Reference" | **`referenceID`** on the taxon's NameUsage row |
| Common name | `ComNameRef` | — | VernacularName `referenceID` |

New-schema link tables: `reference_to_taxon`, `reference_to_synonym`
(both carry `reference_type_id`; `NULL` ⇒ treat as taxonomic-acceptance / `referenceID`),
`reference_to_common_name`.

**Multiplicity:** `nameReferenceID` is single-valued (first nomenclatural ref if several).
Taxon-level `referenceID` is commonly multi-valued (e.g. old `StatusRef` ≈ 1.9M rows for
~560k accepted names). All taxon-level reference IDs for a name are **joined into the
single `referenceID` column** using the ColDP multi-value separator; ChecklistBank parses
`referenceID` as a list. Because every reference ID is `r{numeric}`, no value can contain
the separator — covered by a guard/test.

`Reference.tsv` columns: `ID, author, year, title, source` (old `references`:
author/year/title/source) and `ID, author, year, title, source` mapped from new
`reference`: `authors`/`year`/`title`/`text`.

## 8. Source registry / provenance

For each contributing GSD:
- **Old:** `databases` table (`database_name`, `database_full_name`, `custodian`,
  `version`, `release_date`).
- **New:** `source_database` table (`name`, `abbreviated_name`, `authors_and_editors`,
  `organisation`, `version`, `release_date`).

Build a `life.catalogue.api.model.Citation` per GSD (id = `d{id}`, title = full name,
author = custodian/authors, issued = release date, version), add to the inherited
`sourceCitations` list. The existing `AbstractGenerator.addMetadata()` renders these into
`metadata.yaml` via the `{sources}` placeholder. Every NameUsage carries the matching
`sourceID = d{id}`.

## 9. Metadata

Shared template `src/main/resources/colac/metadata.yaml` with placeholders:
- `alias: CoL{year}` (provide `year` in the metadata map)
- `title: Catalogue of Life — {year} Annual Checklist`
- `version` / `issued`: the AC release date (from the GSD release dates / overall release;
  fall back to `{year}`)
- CoL logo (`src/main/resources/colac/logo.png`)
- `{sources}` placeholder for the GSD registry
- `taxonomicScope: all life`, `geographicScope: global`

## 10. Testing

`src/test/java/colac/GeneratorTest.java`:
- Skips gracefully (assumption-based) if `localhost:3306` / the year DB is unreachable, so
  CI without the dumps still passes.
- For an available year, runs the generator and asserts:
  - non-empty `NameUsage.tsv`; every `parentID` resolves to an existing `ID`;
  - both an old-schema year (e.g. 2008) and a new-schema year (e.g. 2015) convert;
  - the source registry (`metadata.yaml` sources) is non-empty and each `sourceID` used in
    NameUsage exists in the registry;
  - **no emitted reference ID contains the ColDP multi-value separator.**

## 11. Documentation

- Add a `### CoL Annual Checklist (colac) Generator` section to `CLAUDE.md` and the README
  describing: the two schemas, the year/db parameters, the helper-table strategy, the
  reference link-category mapping, and that the **MySQL dumps are available from
  <https://www.catalogueoflife.org/data/download>**.
- Bump the supported-sources count/list.

## 12. Out of scope (YAGNI)

- No basionym/NameRelation extraction (the dumps don't carry reliable basionym links).
- No TypeMaterial, Media, or TaxonProperty.
- No automatic download/restore of the dumps — they are assumed already loaded in MariaDB.
- No cross-year merging — each year is its own archive.
