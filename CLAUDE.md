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

## Notes

- Java 21 required
- GBIF Maven repositories are configured in `pom.xml` for COL-specific deps (`coldp`, `api`, `metadata`, `name-parser`)
- The built fat JAR (`target/coldp-generator-1.0-SNAPSHOT.jar`, ~75MB) includes all dependencies
- Tests in `src/test/java/ManualCli.java` provide a programmatic way to invoke generators during development
