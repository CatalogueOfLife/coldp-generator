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
├── ipnicrawl/                  # IPNI crawl generator
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

## Supported Sources (17)

AntCat, Birdlife (HBW), BioLib, CITES, Clements, Cycads, ICTV, IPNI, IPNI-crawl, LPSN, MDD, Mites, OTL, OTT, PBDB, WikiData, WikiSpecies, WSC

## Wikidata Generator

The Wikidata generator (`wikidata/`) is special — it processes the full Wikidata JSON dump (~160 GB compressed), which takes an entire day to download. Key design points:

**`PREVENT_DOWNLOAD = true`** in `Generator.java` — this flag intentionally suppresses re-downloading the dump even when the remote is newer. **Do not remove this flag or change it to `false` in code.** It exists to protect against accidentally overwriting an existing dump during development and testing. The IDE will report dead code on the freshness-check branch; this is expected and harmless. To force a re-download, delete the local file manually.

The dump is stored at `{sourceDir}/wikidata/latest-all.json.gz` (default: `/tmp/coldp-generator-sources/wikidata/`).

### Two-pass streaming architecture

**Pass 1** (`collectLookups`): streams the full dump to build in-memory lookup maps:
- Rank labels (P105 QIDs), area info + ISO codes (P297), IUCN status labels (P141), publication info (P1476), journal labels (P1433), nomenclatural status / gender labels (P1135, P2433 qualifiers on P225), and external identifier property metadata (P entities with formatter URL P1630).
- Pre-filter: lines containing `"P225"`, `"P297"`, `"P1476"`, `"P31"`, or `"P1630"`.
- Any unresolved QIDs are batched and resolved via SPARQL against `query.wikidata.org` between passes.

**Pass 2** (`emitColdpRecords`): streams again (filter: `"P225"`) and emits ColDP records:
- Skips entities with P31=Q17362920 (Wikimedia duplicated pages) — logs them to `wikidata-duplicates.tsv`.
- Writes `NameUsage` (accepted + synonyms via P1420), `VernacularName`, `Distribution`, `TaxonProperty`, `NameRelation`, `Reference`.

### Property mapping

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
| all P entities with P1630 | alternativeID (CURIEs) | dynamically discovered |

### Output files (in addition to standard ColDP)
- `NameRelation.tsv` — replacement name relations (P694)
- `wikidata-duplicates.tsv` — skipped duplicate-page entities
- `identifier-registry.tsv` — all discovered external identifier properties with CURIE prefix, formatter URL, format regex

## Notes

- Java 21 required
- GBIF Maven repositories are configured in `pom.xml` for COL-specific deps (`coldp`, `api`, `metadata`, `name-parser`)
- The built fat JAR (`target/coldp-generator-1.0-SNAPSHOT.jar`, ~75MB) includes all dependencies
- Tests in `src/test/java/ManualCli.java` provide a programmatic way to invoke generators during development
