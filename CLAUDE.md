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
| SWC parser | Wikitext parsing |
| coldp / dwc-api | ColDP and Darwin Core types |

## Supported Sources (18)

AntCat, Birdlife (HBW), BioLib, CITES, Clements, Cycads, GRIN, ICTV, IPNI, LPSN, MDD, Mites, OTL, OTT, PBDB, WikiData, WikiSpecies, WSC

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
