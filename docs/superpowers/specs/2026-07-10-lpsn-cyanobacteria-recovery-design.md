# LPSN generator — recover cyanobacteria from the website

Date: 2026-07-10
Status: approved design, pre-implementation
Scope: `src/main/java/org/catalogueoflife/data/lpsn/Generator.java` (plus a new
crawler helper and a fixture-based extraction test)

## Background

The LPSN API (`api.lpsn.dsmz.de`) **intentionally withholds** cyanobacteria
(names governed by the ICN / Botanical Code), Candidatus names, and some taxa
above genus rank, until DSMZ implements code-specific nomenclatural checks
(confirmed by the LPSN developer). These records are unreachable **both** by
`/advanced_search?validly-published=yes|no` **and** by `/fetch/{id}` (which
silently returns partial results, dropping ids it does not serve). The
downloadable `lpsn_gss` CSV and `taxonomy.csv` are likewise ICNP-only — verified
absent: Aerofilum, Oculatellaceae, Planktothrix.

Consequently the generated archive is missing almost all cyanobacteria, and the
records it *does* emit contain dangling references into that gap — e.g. the ICNP
synonym *Elainellaceae* (43552) points at its ICNafp correct name *Oculatellaceae*
(6116), which no API/CSV source returns. Commit `93e7767` currently repairs those
dangling references defensively (unresolvable correct name → bare name;
unresolvable basionym/type → dropped). This spec replaces that gap with the real
data by scraping the **LPSN website**, which is the only source that has these
names.

Scope of this effort: **cyanobacteria only.** Candidatus names are deliberately
excluded — they are scattered across the whole bacterial tree rather than forming
a single subtree, so subtree-crawl discovery does not apply; they would need a
different enumeration and are left to a separate effort.

## Key facts established during investigation

The cyanobacteria form a single subtree anchored on an API-present node:

```
Phylum Cyanobacteriota (30362, ICNP)          ← in the API (anchor)
  └ Class Cyanophyceae (68953) / Chroococcophyceae … (ICNafp)   ← missing
      └ Order Oculatellales (37583, ICNafp)    ← in the API (some orders pass checks)
          └ Family Oculatellaceae (6116, ICNafp)   ← missing
              └ Genus Aerofilum (12732, ICNafp)    ← missing
                  └ Species Aerofilum fasciculatum …   ← missing
```

- The subtree root `/phylum/cyanobacteriota` (id 30362) is validly published
  under the **ICNP** and is already in the API `records` map — the anchor.
- Website taxon pages expose a machine-followable hierarchy:
  - an **accepted** ("correct name") page lists **"Child taxa"** links one rank
    down (order → families, family → genera, genus → species, species →
    subspecies) and a **"Synonyms"** section linking its synonym pages;
  - a **synonym** page has no "Child taxa"; it carries a **"Correct name:"**
    link to its accepted page.
- Every page carries `Record number: <n>` — the **same id space** as the API
  (`6116`, `12732`, `30362`, `37583`), so scraped ids drop straight into the
  existing `records` map and ColDP `ID` scheme with no remapping.
- Page fields, extractable by label: `Name`, `Author`, `Category` (rank),
  `Nomenclatural status`, `Taxonomic status`, `Correct name` (synonyms),
  `Basionym`, `Type` / `Type species`, plus the page URL (`lpsn_address`).
- The site is behind Cloudflare; a browser `User-Agent` is required (plain
  `Jsoup.connect(...)`, as the ASW generator already does — not the project
  `HttpUtils`).

## Design

### Where it runs

A new method `crawlCyanobacteria()` on `lpsn/Generator`, called from
`addData()` **after** `closeReferences()` and **before** `writeRecords()`:

```
collectNames (API crawl) → closeReferences (API closure) →
crawlCyanobacteria (website) → writeRecords (write + repair)
```

It synthesizes `FetchDetail` objects from scraped pages and inserts them into the
existing `Map<Integer,FetchDetail> records` via `putIfAbsent`. Because the
missing correct-names are now present, the existing `writeRecords()` repair logic
naturally makes those synonyms resolve to real parents instead of bare names —
**no change to `writeRecords()` is required.**

### Crawl algorithm

Top-down DFS from the phylum, carrying the parent's ColDP id so no parent
lookup is ever needed:

```
visited = Set<Integer>            // record numbers already processed
crawl(url, parentId):
    page = fetchCached(url)       // cache as lpsn-<slug>.html; 200ms delay on new
    rec  = extract(page)          // record number, name, author, category, statuses
    if rec.id in visited: return
    visited.add(rec.id)

    synthesize FetchDetail n from rec:
        n.id                    = rec.id
        n.full_name             = rec.name
        n.authority             = rec.author
        n.category              = rec.rank                 // raw label; CLB normalises
        n.nomenclatural_status  = rec.nomStatus
        n.lpsn_taxonomic_status = rec.taxStatus            // "correct name" / "synonym"
        n.lpsn_address          = url
        n.basonym_id            = rec.basionymId (nullable)
        n.nomenclatural_type_id = rec.typeId (nullable)
        if accepted:  n.lpsn_correct_name_id = n.id ; n.lpsn_parent_id = parentId
        if synonym:   n.lpsn_correct_name_id = parentId ; n.lpsn_parent_id = parentId
    records.putIfAbsent(n.id, n)  // API records stay authoritative

    // recurse regardless of whether this node was already in the map (API nodes
    // like the phylum/orders have missing children we still must reach)
    for childUrl in rec.childTaxaLinks:  crawl(childUrl, rec.id)
    if accepted:
        for synUrl in rec.synonymLinks:  crawl(synUrl, rec.id)   // parentId = accepted id
```

Notes:
- `parentId` for a synonym is the accepted page we arrived from; we set both
  `lpsn_correct_name_id` and `lpsn_parent_id` to it, so `writeRecords()` files it
  as a synonym of its real accepted name (per the ColDP synonym-parent rule — a
  synonym points at its accepted name, never an ancestor).
- `putIfAbsent` means a node already fetched from the API keeps its (authoritative)
  API record; we still recurse to discover its missing descendants.
- `basonym_id`/`nomenclatural_type_id` are resolved to record numbers from the
  respective page links when present; if the target is outside the crawled set it
  stays unresolvable and `writeRecords()` drops it exactly as today.
- A single `visited` set keyed on record number prevents re-processing and cycles
  (LPSN shows the full synonymy on every related page, so the same synonym is
  reachable from multiple pages).

### Fetch & caching

- Reuse the ASW pattern: `Jsoup.connect(url).userAgent(<browser>).get()`.
- Cache each page as `lpsn-<slug>.html` under the source dir; `--no-download`
  reuses the cache. 200 ms delay between *new* downloads only.
- First run fetches a few thousand pages (slow); subsequent runs read cache.
- Robots: confirm `https://lpsn.dsmz.de/robots.txt` permits `/phylum`, `/class`,
  `/order`, `/family`, `/genus`, `/species` before enabling by default; the ASW
  and PFNR generators set the same precedent.

### Extraction unit

A small pure `LpsnPage` parser (record number, name, author, category, nom
status, tax status, correct-name link, basionym link, type link, child-taxa
links, synonym links) that takes an HTML string and returns a value object. This
is the only piece with real parsing risk, so it is isolated and unit-tested
against saved fixture pages (order/family/genus/synonym), mirroring the
wikispecies and Commons dump-reader tests. The `Generator` orchestrates crawl +
synthesis; the parser has no I/O.

### Output

Same files the LPSN generator already emits: `NameUsage` (accepted + synonyms +
any bare names) and `NameRelation` (type). No vernaculars or distributions — parity
with the current output. Scraped `nameStatus` maps through the existing
`mapNomStatus` (already handles "validly published under the ICN (Botanical Code)"
→ available); `status` through `mapTaxStatus`.

## Error handling

- A page that fails to fetch (network / Cloudflare / 404) is logged and skipped;
  its subtree below is lost but the crawl continues (no hard failure).
- A page missing a Record number is skipped with a warning (defends against
  layout drift).
- Unresolvable basionym/type/correct-name references continue to be repaired by
  the existing `writeRecords()` logic — the crawl only shrinks that set.
- The crawl is additive: if it is disabled or fails wholesale, the generator
  still produces today's (API-only, reference-repaired) archive.

## Testing

- **Unit (fixture):** `LpsnPageTest` asserts extraction on saved HTML for an
  order (child families), a family (child genera), a genus (child species + type),
  and a synonym (correct-name link, no child taxa). Fixtures live under
  `src/test/resources/lpsn/`.
- **Integration (@Ignore):** an end-to-end crawl from the phylum against the live
  site, spot-checking that Oculatellaceae (6116), Aerofilum (12732), and the
  Elainellaceae→Oculatellaceae synonym link are present and resolve.

## Out of scope

- Candidatus names and non-cyanobacterial above-genus gaps.
- Replacing the API crawl with the GSS CSV (a possible future optimisation; the
  CSV is ICNP genus/species/subspecies only and lacks higher taxa).
- Vernaculars, distributions, type strains, etymology.
```
