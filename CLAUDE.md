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

## Supported Sources (20)

AntCat, ASW, Birdlife (HBW), BioLib, CITES, Clements, Cycads, GRIN, ICTV, IPNI, LPSN, MDD, Mites, OTL, OTT, PBDB, PFNR, WikiData, WikiSpecies, WSC

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
