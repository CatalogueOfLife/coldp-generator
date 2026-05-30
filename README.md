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
| `--enrich` | `false` | (USDA only) Fetch PlantProfile API for each accepted name; adds Distribution, TaxonProperty, Media |
| `--year` | | (colac only) Annual checklist year 2005–2019; selects MariaDB database `col{year}ac` |
| `--db-host / --db-port` | `localhost` / `3306` | (colac only) MariaDB host and port |
| `--db-user / --db-pass` | `root` / `root` | (colac only) MariaDB credentials |

## Supported Sources

| Source | Name | ChecklistBank | Notes |
|--------|------|---------------|-------|
| `antcat` | [AntCat](https://antcat.org) | [54937](https://www.checklistbank.org/dataset/54937) | Online Catalog of the Ants of the World |
| `bats` | [Bats of the World](https://batnames.org) | [314574](https://www.checklistbank.org/dataset/314574) | Chiroptera taxonomy by Simmons & Cirranello (AMNH) |
| `birdlife` | [Birdlife HBW](http://datazone.birdlife.org/species/taxonomy) | [170809](https://www.checklistbank.org/dataset/170809) | Handbook of the Birds of the World |
| `biolib` | [BioLib](https://www.biolib.cz) | [54592](https://www.checklistbank.org/dataset/54592) | |
| `clements` | [Clements](https://www.birds.cornell.edu/clementschecklist/) | [2013](https://www.checklistbank.org/dataset/2013) | Clements Checklist of Birds of the World |
| `colac` | [CoL Annual Checklists](https://www.catalogueoflife.org/data/download) | | Historical CoL Annual Checklists 2005–2019 (one archive per `--year`, from local MariaDB dumps) |
| `cycads` | [Cycads](https://cycadlist.org) | [1163](https://www.checklistbank.org/dataset/1163) | The World List of Cycads |
| `grin` | [GRIN](https://npgsweb.ars-grin.gov/gringlobal/taxon/abouttaxonomy) | [2018](https://www.checklistbank.org/dataset/2018) | GRIN-Global Taxonomy (cultivated plants) |
| `ioc` | [IOC World Bird List](https://www.worldbirdnames.org/) | [2036](https://www.checklistbank.org/dataset/2036) | IOC World Bird List (latest version auto-detected) |
| `ictv` | [ICTV](https://talk.ictvonline.org/taxonomy/) | [1014](https://www.checklistbank.org/dataset/1014) | ICTV Master Species List |
| `ipni` | [IPNI](https://www.ipni.org) | [2006](https://www.checklistbank.org/dataset/2006) | International Plant Names Index |
| `itis` | [ITIS](https://www.itis.gov/) | [2144](https://www.checklistbank.org/dataset/2144) | Integrated Taxonomic Information System (all 7 kingdoms) |
| `lpsn` | [LPSN](https://lpsn.dsmz.de/) | [2015](https://www.checklistbank.org/dataset/2015) | List of Prokaryotic names with Standing in Nomenclature |
| `mdd` | [MDD](https://www.mammaldiversity.org) | [9802](https://www.checklistbank.org/dataset/9802) | Mammal Diversity Database |
| `mites` | [Mites](https://www.miteresearch.org) | | |
| `otl` | [OTL](https://tree.opentreeoflife.org/about/synthesis-release) | [201891](https://www.checklistbank.org/dataset/201891) | Open Tree of Life Synthesis Tree |
| `ott` | [OTT](https://tree.opentreeoflife.org/about/taxonomy-version) | [201890](https://www.checklistbank.org/dataset/201890) | Open Tree of Life Reference Taxonomy |
| `pbdb` | [PBDB](https://paleobiodb.org/) | [1174](https://www.checklistbank.org/dataset/1174) | The Paleobiology Database |
| `pfnr` | [PFNR](https://www.plantfossilnames.org) | [314595](https://www.checklistbank.org/dataset/314595) | International Fossil Plant Names Registry |
| `wikidata` | [Wikidata](https://www.wikidata.org) | [314569](https://www.checklistbank.org/dataset/314569) | Wikidata taxonomy (downloads full Wikidata + Commons dumps, ~260 GB total) |
| `wikispecies` | [WikiSpecies](https://species.wikimedia.org) | [314570](https://www.checklistbank.org/dataset/314570) | |
| `usda` | [USDA PLANTS](https://plants.sc.egov.usda.gov/) | | USDA PLANTS Database — vascular plants, mosses, lichens of the US (~49K accepted, ~44K synonyms) |
| `wsc` | [WSC](https://wsc.nmbe.ch/) | [56185](https://www.checklistbank.org/dataset/56185) | World Spider Catalog |

## Generator-specific Notes

### CoL Annual Checklists (colac)

Converts the historical Catalogue of Life Annual Checklists **2005–2019**, one ColDP archive per year. There is no API — each year exists only as a MySQL/MariaDB dump. **The dumps are publicly downloadable from <https://www.catalogueoflife.org/data/download>**; restore them into a local MariaDB with one database per year named `col{year}ac` (e.g. `col2015ac`).

```bash
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s colac --year 2015 -r /tmp/coldp/archives
```

Two database schemas are handled and dispatched by year:

- **2005–2011** (`OldSchemaReader`): the accepted classification is the `taxa` tree (Kingdom→Infraspecies via `parent_id`); names/authors/synonymy live in `scientific_names`. Status comes from `sp2000_status_id` + the per-year `sp2000_statuses` lookup — the id→label assignment and the `scientific_name_references.reference_type` vocabulary both **drift across these years**, so accepted-vs-synonym and reference categories are derived from labels, not hardcoded ids.
- **2012–2019** (`NewSchemaReader`): the normalized Species 2000 format, read via its fully-populated denormalized helper tables `_taxon_tree` (hierarchy) and `_search_scientific` (names/authors/status/source); synonyms are `_search_scientific` rows with `accepted_species_id > 0`.

**Output:** `NameUsage` (accepted + synonyms), `VernacularName`, `Distribution`, `Reference`, and a `metadata.yaml` whose `source:` registry lists every contributing Global Species Database (GSD) as a full citation — title, author, editor, publisher, version and issue date, extracted from the source-database tables (plus a non-standard `shortTitle`, emitted ahead of its addition to the ColDP spec). Each NameUsage carries a `sourceID` pointing to its GSD. Distribution `establishmentMeans` keeps the raw source label (native, alien, domesticated, …) for ChecklistBank to normalise on import.

**Metadata** is per-year: `alias` = `COL{yy}`, `title` = "Catalogue of Life Checklist", `version` = "Annual Checklist {year}", `url` = `https://www.catalogueoflife.org/annual-checklist/{year}/`, publisher = *Species 2000 & ITIS Catalogue of Life* (Reading, GB through 2013; Leiden, NL from 2014), and creator = the per-year editor list with ORCIDs where known. Editor lists come from the archived "How to cite this work" pages kept under `src/main/resources/colac/citations/` (2005 and 2010–2019; 2006–2009 list Bisby & Roskov, who led throughout). `issn` is `1473-009X` for 2005–2015 and `2405-884X` for 2016–2019 (following the web citation, not the DVD).

**Reference links** map by category: nomenclatural references (`NomRef`/`AuthorRef`, or type 1 in the new schema) → `nameReferenceID`; taxonomic-acceptance references (`TaxAccRef`/`StatusRef`, or type 2) → `referenceID` (multiple per name are comma-joined; ids are `r<n>` and never contain a comma); common-name references (`ComNameRef` / `reference_to_common_name`) → VernacularName `referenceID`.

**IDs:** `t<id>` accepted taxa, `s<id>` synonyms, `r<id>` references, `d<id>` source databases.

Requires a MariaDB reachable over TCP (default `localhost:3306`, user/pass `root`/`root`; override with `--db-host/--db-port/--db-user/--db-pass`). The larger years (~4M name usages) benefit from a roomy heap, e.g. `java -Xmx8g`.

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

### USDA PLANTS

Downloads `plantlst.txt` from the USDA PLANTS Document Library (~93K rows). Families and some genera are absent from the file and are created as synthetic nodes (`fam:Name`, `gen:Name`). Rank is inferred from name structure via the GBIF name parser.

The optional `--enrich` flag calls `PlantProfile?symbol=SYMBOL` for each of the ~49K accepted names and adds:
- `Distribution.tsv` — native/introduced status per US region (AK, L48, HI, CAN, PR, …)
- `TaxonProperty.tsv` — duration, growth habit, and taxonomic group
- `Media.tsv` — representative plant image

Profile responses are cached as `profile-SYMBOL.json` in the sources directory; re-runs skip already-cached files. First enrichment run takes ~1.5 hours with 10 parallel threads.

```bash
# Basic run (NameUsage + VernacularName only):
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s usda

# With enrichment (adds Distribution, TaxonProperty, Media):
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s usda --enrich

# Re-run enrichment using cached profiles (no network calls):
java -jar target/coldp-generator-1.0-SNAPSHOT.jar -s usda --enrich --no-download
```

### World Spider Catalog (WSC)

Find and delete 404 error files by their exact size:
```bash
find . -type f -size 131c -delete 
```