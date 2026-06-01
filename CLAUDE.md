# CLAUDE.md

## Project Overview

ColDP Archive Generator — a Java CLI tool that converts taxonomic data from various online sources into [ColDP (Catalogue of Life Data Package)](https://github.com/CatalogueOfLife/coldp/) archives. Conversions are fully automated and schedulable.

## Build & Run

```bash
# Build fat JAR
mvn package

# Run a specific generator
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s <source>

# Run tests
mvn test
```

**CLI options** (`GeneratorConfig.java`):
- `-s, --source` (required) — source name, e.g. `wsc`, `ipni`, `lpsn`
- `-r, --repository` — working directory (default: `/tmp/coldp-generator`)
- `--api-key` — API key for authenticated sources
- `--lpsn-user / --lpsn-pass` — LPSN credentials
- `--date` — date filter for incremental updates
- `--no-download` — skip downloading source files; reuse existing local copies (useful for development re-runs)
- `--year` — (colac only) annual checklist year 2005–2019, selects MariaDB database `col{year}ac`
- `--db-host / --db-port / --db-user / --db-pass` — (colac only) MariaDB connection (defaults `localhost` / `3306` / `root` / `root`)

## Project Structure

```
src/main/java/org/catalogueoflife/data/
├── GeneratorCLI.java           # CLI entry point
├── GeneratorConfig.java        # CLI argument config (JCommander)
├── AbstractGenerator.java      # Base generator
├── AbstractColdpGenerator.java # ColDP-specific base
├── AbstractXlsSrcGenerator.java # Excel-based source base
├── AbstractTextTreeGenerator.java
├── utils/                      # HTTP, HTML, Markdown, bibjson utilities
├── antcat/                     # AntCat generator
├── asw/                        # Amphibian Species of the World generator
├── birdlife/                   # Birdlife HBW generator
├── biolib/                     # BioLib generator
├── cites/                      # CITES generator
├── clements/                   # Clements checklist generator
├── cycads/                     # Cycads generator
├── ictv/                       # ICTV MSL generator
├── ipni/                       # IPNI generator (API-based)
├── lpsn/                       # LPSN prokaryotes generator
├── mdd/                        # Mammal Diversity Database generator
├── mites/                      # Mites generator
├── otl/                        # Open Tree of Life synthesis generator
├── ott/                        # Open Tree of Life taxonomy generator
├── pbdb/                       # Paleobiology Database generator
├── pfnr/                       # Plant Fossil Names Registry generator
├── wikidata/                   # Wikidata generator
├── wikispecies/                # WikiSpecies generator
└── wsc/                        # World Spider Catalog generator

src/main/resources/
└── <source>/
    ├── metadata.yaml           # Source metadata for ColDP package
    └── logo.*                  # Source logo
```

## Adding a New Generator

1. Create a package under `src/main/java/org/catalogueoflife/data/<source>/`
2. Implement `Generator.java` extending one of:
   - `AbstractColdpGenerator` — general ColDP output
   - `AbstractXlsSrcGenerator` — Excel input sources
   - `AbstractTextTreeGenerator` — text tree input
3. Add `metadata.yaml` and logo under `src/main/resources/<source>/`
4. Register the source in `GeneratorCLI.java`
5. Add a test in `src/test/java/<source>/GeneratorTest.java`

## Key Dependencies

| Library | Purpose |
|---------|---------|
| JCommander | CLI argument parsing |
| Apache HttpClient 5 | HTTP requests |
| Jsoup | HTML/XML parsing |
| Jackson | JSON processing |
| Apache POI | Excel file reading |
| Apache Jena (ARQ) | RDF/SPARQL for WikiData |
| univocity-parsers | CSV parsing |
| Newick-IO / text-tree | Tree format parsing |
| name-parser | Scientific name parsing |
| citeproc-java | Citation/reference formatting |
| Sweble wikitext parser | Wikitext parsing (sections, links, lists, formatting only — see note) |
| coldp / dwc-api | ColDP and Darwin Core types |

## Notes on Dependencies

### Sweble Wikitext Parser

**Sweble's `WikitextParser.parseArticle()` does NOT parse `{{template}}` invocations as `WtTemplate` AST nodes.** Template calls are returned as raw `WtText` nodes containing the literal `{{...}}` markup. Only structural elements are properly parsed: sections (`WtSection`), headings (`WtHeading`), wiki links (`WtInternalLink`), formatting (`WtItalics`, `WtBold`), and lists (`WtUnorderedList`, `WtListItem`).

As a consequence, all template detection in the wikispecies generator (and any future generator using Sweble) must use **regex on the `nodeText()` output** rather than `instanceof WtTemplate` checks. The `nodeText()` method returns `WtText` content verbatim, so raw `{{TemplateName}}` strings are present in the output and can be matched with `Pattern.compile("\\{\\{([^|{}\\n]+)")`.

### ColDP Rank Vocabulary

Generators output raw rank labels (e.g. `"familia"`, `"classis"`, `"cohort"`) rather than normalised ColDP rank names. The ChecklistBank rank parser normalises these on import, so generators do not need a rank-mapping table.

## Supported Sources (22)

AntCat, ASW, Birdlife (HBW), BioLib, CITES, Clements, CoL Annual Checklists (colac), Cycads, GRIN, ICTV, IPNI, LPSN, MDD, Mites, OTL, OTT, PBDB, PFNR, USDA, WikiData, WikiSpecies, WSC

### USDA PLANTS Generator

The USDA generator (`usda/`) downloads `plantlst.txt` from https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt — a CSV with columns `Symbol | Synonym Symbol | Scientific Name with Author | Common Name | Family` (~93K rows: ~49K accepted, ~44K synonyms). Families and genera are not present as their own entries in the file.

**Two-pass CSV parsing:**
- **Pass 1**: builds `genusToSymbol` (genus → real symbol), `genusToFamily` (genus → family), and `acceptedSymbols` list for enrichment.
- Before pass 2: emits synthetic family nodes (`fam:{FamilyName}`) and synthetic genus nodes (`gen:{GenusName}`) for the 278 genera absent from the file.
- **Pass 2**: writes all file rows — accepted rows get `parentID` pointing to the genus symbol (real or synthetic); synonym rows get `parentID` = the accepted symbol from column 1.

**Rank inference**: `NameParserGBIF` parses the full name-with-author string. Uninomials are detected by checking whether the second token starts with an uppercase letter (authorship) vs. lowercase (specific epithet).

**ID scheme:** real symbols from the file for accepted/synonym taxa; `fam:{FamilyName}` for synthetic families; `gen:{GenusName}` for synthetic missing genera.

**Optional enrichment (`--enrich` flag):** calls `https://plantsservices.sc.egov.usda.gov/api/PlantProfile?symbol={symbol}` for each accepted species/infraspecific (not genera). Uses 10-thread `ExecutorService` for parallel downloads; each response cached as `profile-{symbol}.json`. Emits:
- `Distribution.tsv` — from `NativeStatuses[]`; `N`/`N?` → `native`, `I`/`I?` → `introduced`; `gazetteer=text`; region codes (AK, L48, HI, CAN, …) used as-is.
- `TaxonProperty.tsv` — `Durations[]` → `duration`, `GrowthHabits[]` → `growth habit`, `Group` → `group`.
- `Media.tsv` — `ProfileImageFilename` → `https://plants.sc.egov.usda.gov/ImageLibrary/standard/{filename}`.

### CoL Annual Checklists Generator (colac)

The `colac` generator (`colac/`) converts the historical Catalogue of Life Annual Checklists **2005–2019** into one ColDP archive per year. Each year exists only as a MySQL/MariaDB dump (no API). **The dumps are publicly available from https://www.catalogueoflife.org/data/download** and must be restored locally into MariaDB with one database per year named `col{year}ac` (e.g. `col2015ac`). Connect over TCP (`jdbc:mariadb://localhost:3306/col{year}ac`, defaults `root`/`root`).

Run: `-s colac --year 2015` (one year per run; vary `-r` for batch). Uses MariaDB Connector/J (added to `pom.xml`) and explicitly registers the driver (`Class.forName("org.mariadb.jdbc.Driver")`) because the shaded fat JAR does not merge JDBC service files. Large result sets are streamed via `Statement.setFetchSize(1000)` (MariaDB Connector/J streams for any positive fetch size; `Integer.MIN_VALUE` is rejected).

**Two schemas, dispatched by year** in `Generator.addData()`:
- **2005–2011** → `OldSchemaReader`. Accepted classification = the `taxa` tree (Kingdom→Infraspecies via `parent_id`, `is_accepted_name=1`); names/authors/synonymy = `scientific_names` keyed by `name_code`; synonyms linked via `accepted_name_code`. **Schema drift within the era**: the `sp2000_status_id`→label assignment differs (e.g. id 4 = ambiguous synonym in 2005, but provisionally accepted in 2008+), the `sp2000_status` text column exists only in 2005, `is_accepted_name` on `scientific_names` is absent in 2005, and the `scientific_name_references.reference_type` vocabulary changes (2005–06 `AuthorRef`/`StatusRef`/`StatRef`; 2007–11 `NomRef`/`TaxAccRef` + stray types + NULL). So accepted-vs-synonym status ids are derived per-year from the `sp2000_statuses` **labels** (`statusIdClause`), and reference categories are matched by label with all non-nomenclatural/non-common-name types defaulting to taxon `referenceID`. **Dangling-reference repair** (the `taxa` tree and `accepted_name_code` links are internally inconsistent in 2005–2010 ITIS data, so naive ID mapping produces parent/taxon references to never-emitted nodes): (1) accepted infraspecies are commonly parented to a species `taxa` node that is itself a synonym (`is_accepted_name=0`) and thus not emitted — typically because the binomial is a **homonym** (e.g. the varieties of *Monarda fistulosa* sit under the synonym *M. fistulosa* Sims, not the accepted *M. fistulosa* L.). `emitAccepted` repairs such `parentID`s via `buildRepairedParents`/`repairParent` in priority order: **accepted homonym** of the same binomial (indexed by `acceptedByNameParent` keyed on `nameParentKey(name, parent_id)`), else the synonym's **accepted species** reached by following `accepted_name_code` (often a same-epithet recombination, e.g. *Stipa nelsonii* → *Achnatherum nelsonii*), else as a last resort the nearest accepted ancestor (`acceptedAncestor`, the genus — only 94 of 4094 rows in 2005, 0 in 2007–2011). ~4–9k infraspecies/year 2005–2010, 1 in 2011; all kingdom roots are accepted so resolution never fails. (2) `acceptedNameCodeToId` maps **only accepted** taxa nodes (a non-accepted node is never emitted), and synonym parentIDs plus vernacular/distribution taxonIDs are resolved with `resolveAcceptedTaxon`, which follows `accepted_name_code` through any synonym→synonym **chain** to the accepted terminus (null ⇒ reference dropped, never left dangling). Verified: 0 dangling `parentID`/`taxonID` across all of 2005–2019.
- **2012–2019** → `NewSchemaReader`. Normalized Species 2000 format, read via its fully-populated denormalized helper tables `_taxon_tree` (accepted hierarchy) and `_search_scientific` (atomized names/author/status/source). Synonyms = `_search_scientific` rows with `accepted_species_id > 0`. Provisionally-accepted taxa come from `taxon_detail.scientific_name_status_id`. Reference categories from the explicit `reference_type` table (1 Nomenclatural → `nameReferenceID`, 2 Taxonomic Acceptance → `referenceID`) via `reference_to_taxon`/`reference_to_synonym`/`reference_to_common_name`.

**Output:** `NameUsage` (accepted + synonyms in one file), `VernacularName`, `Distribution`, `Reference`, and `metadata.yaml` (shared template `resources/colac/metadata.yaml` with `{year}`/`{issued}`/`{version}`/`{sources}` placeholders). Each NameUsage carries `sourceID = d<id>` linking to its contributing GSD; `issued` is the latest GSD release date.

**Rich source registry**: each GSD becomes a full ColDP source `Citation` built from the `databases` (old) / `source_database` (new) tables — title (long name), **author** and **editor** (parsed from the `authors_editors`/`authors_and_editors` string by `ColacMappings.parseAgents`, which splits "(eds)" editors from authors and drops scope tags like "(Pulmonata)"), **publisher** (`organization`/`organisation`, or `custodian` for 2005), **version**, and **issued** (release date). All 2005–2019 GSDs are fully populated. The GSD short name (`database_name`/`abbreviated_name`) is set as the source **`alias`** (via `Citation.setAlias`, available since coldp/api 1.1.2-SNAPSHOT — the version this project depends on). The source YAML is rendered by `Generator.addMetadata()` itself (not the inherited `sourceCitations` path) and wrapped with `Matcher.quoteReplacement`, because the shared `SimpleTemplate` substitutes `{sources}` via `Matcher.appendReplacement`, which would otherwise corrupt the backslashes snakeyaml emits for escaped quotes and wrapped lines (e.g. a publisher containing `"Luiz de Queiroz"`).

**Reference multiplicity:** taxon-level (`referenceID`) references are commonly multi-valued and are comma-joined (`ColacMappings.joinRefs`, de-duplicated, order-preserving). All reference ids are `r<numeric>` so they never contain a comma — guarded by `joinRefs` and covered by `ColacMappingsTest`.

**Vernacular language:** old schema stores the English language name (e.g. "English") passed through verbatim for ChecklistBank to normalise; new schema stores ISO 639-3 (`language_iso`) used directly. **Distribution** (new only): `establishmentMeans` is the raw `distribution_status` label (native, native-domesticated, alien, domesticated, uncertain, …) — **not mapped**, since ChecklistBank parses/normalises it on import; gazetteer derived from `region_standard` (TDWG → tdwg, IHO → iho, EEZ → mrgid, else text).

**Metadata** (shared template `resources/colac/metadata.yaml`, filled by `Generator.addMetadata()`): `alias = COL{2-digit year}` (e.g. COL05), `title = Catalogue of Life Checklist`, `version = Annual Checklist {year}`, `issued` = latest GSD release date, `url = https://www.catalogueoflife.org/annual-checklist/{year}/`. The **`publisher`** is *Species 2000 & ITIS Catalogue of Life* with city/country **Reading, GB** (≤2013) or **Leiden, NL** (≥2014) — Species 2000 moved to Leiden in 2013, first Leiden release was 2014. `creator` holds **only the per-year Annual Checklist editors** (the organisation is the publisher, not a creator/author). Editors are hardcoded (in `Generator.EDITORS`) from the published Annual Checklist "How to cite this work" pages, archived under `resources/colac/citations/` (`{year}-cite.html`, plus the 2005 project-team/contact pages). Citations exist for 2005 and 2010–2019; **2006–2009 have no citation page** (the legacy interface is behind a JS bot-wall) so those list the organisation plus Bisby & Roskov, who led the team throughout. Names are parsed into given/family by `ColacMappings.parseEditors` (handles particles like "Nieukerken E. van" → "van Nieukerken", "van Hertum", and multi-word surnames like "De Wever"). **ORCIDs** (`Generator.ORCID`, keyed by family) are added where a confident same-person match exists, harvested from the current CoL release metadata (`api.checklistbank.org/dataset/3LR.yaml`) and the repo's ITIS metadata: Roskov, Orrell, Nicolson, Bailly, Kirk, Ouvrard, Decock, Ower, DeWalt. **ISSN**: `1473-009X` (the CD-ROM/DVD Annual Checklist ISSN, in use 2005–2015) and `2405-884X` (2016–2019).

**ID scheme:** `t<id>` accepted taxa, `s<id>` synonyms, `r<id>` references, `d<id>` source databases.

**Pure mapping helpers** (`ColacMappings`, unit-tested in `ColacMappingsTest`): `nomCode` (kingdom→code), `status` (label→ColDP TaxonomicStatus), `establishment`, `synonymName` (atomized parts→name), `joinRefs`. The `Generator`/readers are integration-verified against the local MariaDB (`colac/GeneratorTest`, `@Ignore`d).

### ASW Generator

The ASW generator (`asw/`) scrapes [Amphibian Species of the World](https://amphibiansoftheworld.amnh.org) — the authoritative online reference for ~9,000 extant amphibian species maintained by Darrel Frost at the American Museum of Natural History.

**Recursive crawl** starting at `/Amphibia`, following `<div class="taxa">` child links depth-first. Each taxon page is cached as `taxon-{path_with_underscores}.html` in the source directory; a 200 ms delay is inserted between new downloads. The site requires a browser-like User-Agent (Cloudflare protection); Jsoup.connect() is used rather than the project's HttpUtils.

**Parsing per taxon page:**
- Rank from CSS class attribute on `#aswContent` (e.g. `rank-Subfamily`)
- Name and authorship from `<h1>`: italic text = scientificName for genera/species; for higher taxa the first word is the name and the rest is the authorship
- `<div class="synonymy">` — each `<p>` with a `<b>` tag is a synonym: creates a NameUsage with `status=synonym` and `parentID` pointing to the accepted taxon. Bold names matching the accepted name are skipped. The first reference link gives `nameReferenceID` for the accepted name.
- "Type genus: X" / "Type species: X" text in synonymy entries → deferred `NameRelation type="type genus"/"type species"` (resolved after crawl via name→ID map). Type text is excluded from citation strings.
- `<h2>Common Names</h2>` → VernacularName (language=eng), one record per `<p>` (name = text before opening parenthesis)
- `<h2>Geographic Occurrence</h2>` → Distribution records, one per country; "Natural Resident" maps to `status=native`, "Introduced" to `status=introduced`. Multi-word country names containing commas (e.g. "Congo, Democratic Republic of the") are handled by a pre-substitution step before splitting on ", ".
- `<h2>Comment</h2>` → NameUsage.remarks

**ColDP output:**
- `NameUsage` (accepted + synonyms; no `extinct` flag — all amphibians are extant)
- `NameRelation` (type genus/species, resolved post-crawl)
- `VernacularName`
- `Distribution`
- `Reference` (from bibliography links in synonymy; ID = `ref:{bibliography-path}`)

**ID scheme:** URL path without leading "/" for taxa (e.g. `Amphibia/Anura/Arthroleptidae/Arthroleptinae`); `syn:{n}` for synonym NameUsages; `ref:{bibliography-path}` for references.

### PFNR Generator

The PFNR generator (`pfnr/`) scrapes the [International Fossil Plant Names Registry](https://www.plantfossilnames.org) — the authoritative nomenclatural registry for ~1,200 fossil plant names. There is no API or bulk download; HTML scraping is the only option (robots.txt permits crawling).

**Two-phase scrape:**
- **Phase 1**: fetches `sitemap-names-1.xml.gz` to enumerate all name IDs, then downloads and parses each `/name/{id}/` page → NameUsage, NameRelation (basionyms), TypeMaterial records. Pages are cached as `name-{id}.html` under the source directory; a 100 ms delay is inserted between new downloads.
- **Phase 2**: for each unique `/reference/{id}/` encountered in phase 1, downloads the reference page (`ref-{id}.html`) and extracts the DOI → Reference records.

**ColDP output:**
- `NameUsage`: `status=accepted`, `extinct=true` for all entries; `alternativeID` carries the LSID and PFN registry number; `remarks` holds stratigraphy text.
- `NameRelation type=basionym` for new combinations (from the Basionym: link on name pages).
- `TypeMaterial`: holotype/paratype status with catalogue number and repository institution.
- `Reference`: citation text from the name page + DOI from the reference page.

**ID scheme:** `pfn:{pageId}` for taxa, `ref:{referencePageId}` for references.

### GRIN Generator

The GRIN generator (`grin/`) converts the [GRIN-Global taxonomy](https://npgsweb.ars-grin.gov/gringlobal/taxon/abouttaxonomy) for cultivated and economically important plants.

**Requires `cabextract`** to be installed on the host (`brew install cabextract`). The generator downloads a single `taxonomy_data.cab` (27 MB Windows Cabinet archive) and extracts 16 TSV files.

**Data files extracted from `taxonomy_data.cab`:**

| File | Content |
|------|---------|
| `taxonomy_family.txt` | Families (with suprafamily rank/name and subfamily/tribe/subtribe text) |
| `taxonomy_genus.txt` | Genera with `is_web_visible` flag; synonymized genera via `current_taxonomy_genus_id` |
| `taxonomy_species.txt` | Species/infraspecies; synonym code `B`=homotypic, `S`=heterotypic |
| `taxonomy_common_name.txt` | Vernacular names in many languages |
| `taxonomy_geography_map.txt` | Distribution records (52 MB, streamed) |
| `taxonomy_use.txt` | Economic uses (16 classes × 113 subclasses per Cook 1995) |
| `literature.txt` | Reference records → ColDP Reference |
| `citation.txt` | Species→literature links for `nameReferenceID` (87 MB, streamed) |
| `geography.txt` | Geography hierarchy → ISO country code lookup |

**Two-pass over `taxonomy_species.txt`:**
- **Pass 1** (`collectBasionyms`): builds `accepted_id → basionym_id` map from all `synonym_code=B` records.
- **Pass 2** (`parseSpecies`): writes NameUsage with `basionymID` set for accepted names; synonym records get `status=homotypic synonym` or `heterotypic synonym`; a `NameRelation type=basionym` row is also written for each homotypic synonym.

**ID scheme:** `fam:<taxonomy_family_id>`, `gen:<taxonomy_genus_id>`, `sp:<taxonomy_species_id>`, `ref:<literature_id>`

**Distribution status mapping:** `n`→native, `i`→introduced, `c`→cultivated, `a`→naturalised; gazetteer=ISO (country codes from `geography.country_code`).

### PBDB Generator

The PBDB generator (`pbdb/`) uses the [Paleobiology Database API](https://paleobiodb.org/data1.2/).

**Taxa API uses `all_records=true`** (not `all_taxa=true`) to include all name variants — alternative combinations, misspellings, previous ranks. Variant records are identified by `flg` containing `"V"`. They are emitted as synonyms with `status=synonym`, `nameStatus=tdf` (e.g. "recombined as"), using `vid` as their ColDP ID (fallback: `oid#name`). Canonical (non-variant) records use `oid` as ID. Concept-level data (type species, common names, ecospace facts) is only written for canonical records.

### Wikidata Generator

The Wikidata generator (`wikidata/`) is special — it processes the full Wikidata JSON dump (~160 GB compressed), which takes an entire day to download. Key design points:

Dumps are stored under `{sourceDir}/wikidata/` (default: `/tmp/coldp-generator-sources/wikidata/`). Both dumps are downloaded with a Last-Modified freshness check and re-downloaded only if the remote is newer. Use `--no-download` to skip all downloads and reuse existing local files.

#### Two-pass streaming architecture

**Pass 1** (`collectLookups`): streams the full dump to build in-memory lookup maps:
- Rank labels (P105 QIDs), area info + ISO codes (P297), IUCN status labels (P141), publication info (P1476), journal labels (P1433), nomenclatural status / gender labels (P1135, P2433 qualifiers on P225), and external identifier property metadata (P entities with formatter URL P1630).
- Pre-filter: lines containing `"P225"`, `"P297"`, `"P1476"`, `"P31"`, or `"P1630"`.
- Any unresolved QIDs are batched and resolved via SPARQL against `query.wikidata.org` between passes.

**Pass 2** (`emitColdpRecords`): streams again (filter: `"P225"`) and emits ColDP records:
- Skips entities with P31=Q17362920 (Wikimedia duplicated pages) — logs them to `wikidata-duplicates.tsv`.
- Writes `NameUsage` (accepted + synonyms via P1420), `VernacularName`, `Distribution`, `TaxonProperty`, `NameRelation`, `Reference`.
- Writes one `Media` record per taxon for P18 (representative image) directly from the dump — no HTTP calls.

#### Property mapping

| Wikidata | ColDP field | Notes |
|----------|-------------|-------|
| P225 | scientificName | primary filter |
| P105 | rank | QID resolved via rankLabels map |
| P171 | parentID | accepted taxa only |
| P1420 | parentID + status=synonym | synonym taxa |
| P835 | authorship | author citation string |
| P566 | basionymID | |
| P694 | NameRelation type="replacement name" | replaced synonym |
| P574 (P225 qualifier) | publishedInYear | year of name publication |
| P1353 (P225 qualifier) | originalSpelling | original combination |
| P1135 (P225 qualifier) | nameStatus | nom status QID, label-mapped to ColDP enum |
| P2433 (qualifier or direct) | gender | gender QID, label-mapped |
| P141 | TaxonProperty property="IUCN" | |
| P1843 | VernacularName | multilingual |
| P5588 | Distribution establishmentMeans=invasive | |
| P9714 | Distribution establishmentMeans=native | |
| sitelinks.enwiki | remarks (Wikipedia URL) | |
| P18 | Media.url (representative image) | filename → Special:FilePath URL |
| all P entities with P1630 | alternativeID (CURIEs) | dynamically discovered |

#### Output files (in addition to standard ColDP)
- `NameRelation.tsv` — replacement name relations (P694)
- `wikidata-duplicates.tsv` — skipped duplicate-page entities
- `identifier-registry.tsv` — all discovered external identifier properties with CURIE prefix, formatter URL, format regex
- `Media.tsv` — taxon images from P18 (one per taxon, from Wikidata dump) plus full gallery images from the Commons dump

#### Wikimedia Commons dump processing

**Parallel download:** At the top of `prepare()`, the Commons XML dump (`commonswiki-latest-pages-articles.xml.bz2`, ~106 GB) is downloaded in a background `CompletableFuture` thread while the Wikidata dump download (main thread) and Wikidata passes 1+2 run. Since Wikidata processing takes many hours, the Commons download is typically complete before it is needed.

**After pass 2**, `crawlCommonsMedia()` in `Generator.java` runs two passes over `CommonsXmlDumpReader`:
- **Commons pass A** (`streamGalleryPages`): namespace-0 pages → builds `Map<galleryName, List<filename>>` for all taxa with P935 gallery names
- **Commons pass B** (`streamFilePages`): namespace-6 File: pages → builds `Map<filename, FileMetadata>` for the needed files

File metadata is parsed from `{{Information|description=...|date=...|author=...}}` templates and the first recognized license template. MIME type and media type are derived from the file extension. Fields written: `url` (`Special:FilePath/{filename}`), `type`, `format`, `title`, `created`, `creator`, `license`, `link` (`File:{filename}`), `remarks`. P18 records (one per taxon) are also included; duplicates by URL within a taxon are suppressed.

## Notes

- Java 21 required
- GBIF Maven repositories are configured in `pom.xml` for COL-specific deps (`coldp`, `api`, `metadata`, `name-parser`)
- The built fat JAR (`target/coldp-generator-1.0-SNAPSHOT.jar`, ~75MB) includes all dependencies
- Tests in `src/test/java/ManualCli.java` provide a programmatic way to invoke generators during development
