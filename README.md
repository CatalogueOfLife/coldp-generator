# ColDP Archive Generator

Conversion tools to create [ColDP archives](https://github.com/CatalogueOfLife/coldp/) from various online sources not readily available otherwise.
The conversion is fully automated so it can run in a scheduler.

## Build & Run

```bash
# Build fat JAR
mvn package

# Run a specific generator
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s <source>
```

## CLI Options

| Option | Default | Description                                                |
|--------|---------|------------------------------------------------------------|
| `-s, --source` | *(required)* | Source name (see below)                                    |
| `-r, --repository` | `/tmp/coldp-generator` | Output directory for generated archives                    |
| `--tmp` | `/tmp/coldp-generator-sources` | Directory for downloaded source files                      |
| `--api-key` | | API key for authenticated sources, e.g. WSC                |
| `--lpsn-user / --lpsn-pass` | | Credentials for LPSN                                       |
| `--date` | | Date filter for incremental updates for WSC                |
| `--no-download` | `false` | Skip downloading source files; reuse existing local copies |

## Supported Sources

| Source key | Name | Notes |
|------------|------|-------|
| `antcat` | [AntCat](https://antcat.org) | Online Catalog of the Ants of the World |
| `bats` | [Bats of the World](https://batnames.org) | Chiroptera taxonomy by Simmons & Cirranello (AMNH) |
| `birdlife` | [Birdlife HBW](http://datazone.birdlife.org/species/taxonomy) | Handbook of the Birds of the World |
| `biolib` | [BioLib](https://www.biolib.cz) | |
| `clements` | [Clements](https://www.birds.cornell.edu/clementschecklist/) | Clements Checklist of Birds of the World |
| `cycads` | [Cycads](https://cycadlist.org) | The World List of Cycads |
| `grin` | [GRIN](https://npgsweb.ars-grin.gov/gringlobal/taxon/abouttaxonomy) | GRIN-Global Taxonomy (cultivated plants) |
| `ictv` | [ICTV](https://talk.ictvonline.org/taxonomy/) | ICTV Master Species List |
| `ipni` | [IPNI](https://www.ipni.org) | International Plant Names Index |
| `lpsn` | [LPSN](https://lpsn.dsmz.de/) | List of Prokaryotic names with Standing in Nomenclature |
| `mdd` | [MDD](https://www.mammaldiversity.org) | Mammal Diversity Database |
| `mites` | [Mites](https://www.miteresearch.org) | |
| `otl` | [OTL](https://tree.opentreeoflife.org/about/synthesis-release) | Open Tree of Life Synthesis Tree |
| `ott` | [OTT](https://tree.opentreeoflife.org/about/taxonomy-version) | Open Tree of Life Reference Taxonomy |
| `pbdb` | [PBDB](https://paleobiodb.org/) | The Paleobiology Database |
| `pfnr` | [PFNR](https://www.plantfossilnames.org) | International Fossil Plant Names Registry |
| `wikidata` | [Wikidata](https://www.wikidata.org) | Wikidata taxonomy (downloads full Wikidata + Commons dumps, ~260 GB total) |
| `wikispecies` | [WikiSpecies](https://species.wikimedia.org) | |
| `wsc` | [WSC](https://wsc.nmbe.ch/) | World Spider Catalog |

## Generator-specific Notes

### GRIN

Requires [`cabextract`](https://www.cabextract.org.uk/) to be installed (`brew install cabextract`). Downloads a single `taxonomy_data.cab` (~27 MB) from the GRIN-Global downloads page.

### LPSN

Requires `--lpsn-user` and `--lpsn-pass` credentials.

### Wikidata

Downloads and processes two large dumps automatically:

| Dump | Size | Purpose |
|------|------|---------|
| `latest-all.json.gz` (Wikidata) | ~160 GB | Taxonomy, names, synonyms, distributions, references |
| `commonswiki-latest-pages-articles.xml.bz2` (Commons) | ~106 GB | Taxon gallery images with metadata |

Both dumps are downloaded in parallel at startup (the Commons dump in a background thread while Wikidata processing runs). Freshness is checked via `Last-Modified` headers; files are only re-downloaded when the remote is newer. Use `--no-download` to skip all downloads and reuse existing local files.

**Output includes `Media.tsv`** populated from two sources:
- **P18** (Wikidata property): one representative image per taxon, URL built directly from the filename in the Wikidata dump — no extra HTTP calls.
- **P935 gallery pages** (Commons dump): all images listed in the taxon's curated Commons gallery, with `title`, `created`, `creator`, `license`, and `remarks` extracted from the file description pages.

```bash
# Standard run (downloads everything automatically):
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s wikidata

# Re-run without re-downloading (both dumps already cached):
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s wikidata --no-download
```

### World Spider Catalog (WSC)

Find and delete 404 error files by their exact size:
```bash
find . -type f -size 131c -delete 
```