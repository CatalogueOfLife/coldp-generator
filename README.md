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

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --source` | *(required)* | Source name (see below) |
| `-r, --repository` | `/tmp/coldp-generator` | Output directory for generated archives |
| `--tmp` | `/tmp/coldp-generator-sources` | Directory for downloaded source files |
| `--api-key` | | API key for authenticated sources |
| `--lpsn-user / --lpsn-pass` | | Credentials for LPSN |
| `--date` | | Date filter for incremental updates |
| `--media-threads` | `0` | Threads for Wikimedia Commons media crawling (Wikidata only; 0 = disabled) |

## Supported Sources

| Source key | Name | Notes |
|------------|------|-------|
| `antcat` | [AntCat](https://antcat.org) | Online Catalog of the Ants of the World |
| `birdlife` | [Birdlife HBW](http://datazone.birdlife.org/species/taxonomy) | Handbook of the Birds of the World |
| `biolib` | [BioLib](https://www.biolib.cz) | |
| `cites` | [CITES](https://checklist.cites.org) | CITES species checklist |
| `clements` | [Clements](https://www.birds.cornell.edu/clementschecklist/) | Clements Checklist of Birds of the World |
| `cycads` | [Cycads](https://cycadlist.org) | The World List of Cycads |
| `grin` | [GRIN](https://npgsweb.ars-grin.gov/gringlobal/taxon/abouttaxonomy) | GRIN-Global Taxonomy (cultivated plants) |
| `ictv` | [ICTV](https://talk.ictvonline.org/taxonomy/) | ICTV Master Species List |
| `ipni` | [IPNI](https://www.ipni.org) | International Plant Names Index |
| `ipnicrawl` | [IPNI crawl](https://www.ipni.org) | IPNI via web crawl |
| `lpsn` | [LPSN](https://lpsn.dsmz.de/) | List of Prokaryotic names with Standing in Nomenclature |
| `mdd` | [MDD](https://www.mammaldiversity.org) | Mammal Diversity Database |
| `mites` | [Mites](https://www.miteresearch.org) | |
| `otl` | [OTL](https://tree.opentreeoflife.org/about/synthesis-release) | Open Tree of Life Synthesis Tree |
| `ott` | [OTT](https://tree.opentreeoflife.org/about/taxonomy-version) | Open Tree of Life Reference Taxonomy |
| `pbdb` | [PBDB](https://paleobiodb.org/) | The Paleobiology Database |
| `wikidata` | [Wikidata](https://www.wikidata.org) | Wikidata taxonomy (requires full dump, ~160 GB) |
| `wikispecies` | [WikiSpecies](https://species.wikimedia.org) | |
| `wsc` | [WSC](https://wsc.nmbe.ch/) | World Spider Catalog |

## Generator-specific Notes

### GRIN

Requires [`cabextract`](https://www.cabextract.org.uk/) to be installed (`brew install cabextract`). Downloads a single `taxonomy_data.cab` (~27 MB) from the GRIN-Global downloads page.

### LPSN

Requires `--lpsn-user` and `--lpsn-pass` credentials.

### Wikidata

Processes the full Wikidata JSON dump (~160 GB compressed), which takes an entire day to download. The dump is stored in `--tmp` and reused across runs (re-download is suppressed by default — delete the file manually to force a fresh download).

**`--media-threads N`** — after the main dump processing, crawls Wikimedia Commons gallery pages (Wikidata P935) to produce a `Media.tsv` with images, audio, and video for each taxon. Uses N parallel HTTP threads. Omitted or set to 0 to skip media crawling.

```bash
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s wikidata --media-threads 4
```
