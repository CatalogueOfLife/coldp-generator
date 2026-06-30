# Wikidata generator — structured authorship, authors, and taxon properties

Date: 2026-06-30
Status: approved design, pre-implementation
Scope: `src/main/java/org/catalogueoflife/data/wikidata/` (`Generator.java`, `WikidataDumpReader.java`)

## Background

The Wikidata generator currently emits a name's authorship only from `P835`
("author citation, zoology") read directly off the taxon. That property is
**absent on most taxa** (e.g. *Poa annua*, *Panthera leo*, *Panthera tigris*).
The real authorship lives in the `P225` ("taxon name") statement's qualifiers:

- `P405` taxon author → a Wikidata **item** (the author), one or more, ordered.
- `P574` year of taxon name publication.
- `P3831 = Q14594740` ("recombination") flag → the author+year must be
  parenthesised and belong to the **basionym**.
- `P1353` original spelling (basionym name as a string).
- `P1403` original combination → the basionym **item** (its own taxon, with its
  own `P225`).

`P405` is **code-agnostic**: it is used for botanical *and* zoological names
(confirmed: *Panthera leo*, *Canis lupus*, *Homo sapiens* all use `P405 → Q1043`
Linnaeus with no `P835`). There is **no per-taxon nomenclatural-code property**.
Author items split into two groups: dual-code authors and zoologists carry both
`P835` (citation surname) and `P428` (botanist abbreviation); pure botanists
(de Candolle, R.Br., Hooker, Miller) carry **only `P428`**, no `P835`.

Getting authorship right is the priority — next to the name itself it is the most
important value.

This work is split into two phases. **Phase A** (authorship + `Author` table)
ships first; **Phase B** (taxon properties, temporal range, species
interactions) follows in a second round.

---

## Phase A — Structured authorship and the `col:Author` table

### Key idea: ship atomized authorship, let ChecklistBank render

ColDP separates the flat `authorship` string from atomized fields
(`combinationAuthorship(ID)`, `combinationAuthorshipYear`, `basionymAuthorship(ID)`,
`basionymAuthorshipYear`) and provides a shared `col:Author` table carrying both
`family` ("Linnaeus") and `abbreviationBotany` ("L."). By emitting the atomized
fields plus the Author records and referencing authors by ID, **ChecklistBank
assembles the code-correct display itself** ("*Poa annua* L." vs
"*Panthera tigris* (Linnaeus, 1758)"). The generator therefore does **not** need
to determine the nomenclatural code — the abbreviation dilemma dissolves.

The atomized fields are **authoritative**. The flat `authorship` string is still
written as a human-readable fallback but is secondary.

### The `col:Author` table

A new `additionalWriter(ColdpTerm.Author, …)`. One record per author item that is
actually referenced by an emitted name (tracked like `usedExtIdPids`). Columns:

| ColDP column | Source | Notes |
|---|---|---|
| `ID` | author QID | e.g. `Q1043` |
| `given` | `P735` given-name label, else label minus family | |
| `family` | `P734` family-name label, else `P835`, else last token of label | surname used in citations |
| `abbreviationBotany` | `P428` | e.g. `L.` |
| `birth` | `P569` | year |
| `death` | `P570` | year |
| `country` | `P27` → ISO code (reuse `areaInfo`/ISO resolution) | |
| `sex` | `P21` → `male`/`female` | |
| `affiliation` | `P108` → label | employer/institution |
| `alternativeID` | `P496` → `orcid:<id>` | when present |
| `link` | `https://www.wikidata.org/wiki/<QID>` | |

### Author resolution strategy

New on `WikidataDumpReader`: `Map<String, AuthorInfo> authors` and
`Set<String> neededAuthorQids`.

- **Pass 1** collects every `P405` QID from `P225` qualifiers into
  `neededAuthorQids`. It *also* opportunistically caches `AuthorInfo` from the
  dump for any entity carrying `P428` or `P835` (unambiguously authors; catches
  prominent ones cheaply). `"P428"` is added to the pass-1 line pre-filter.
- **Between passes**, any `neededAuthorQids` not already cached are resolved via
  SPARQL in batches of 200 (same pattern as `resolvePubs`), using the
  `wikibase:label` service to resolve the QID-valued fields (`P27`, `P108`,
  `P21`) to names/codes in one query. This is the only way to catch the long tail
  of authors that have only a label.

Runtime note: this adds a bounded SPARQL phase between passes, comparable to the
existing publication resolution.

### Atomized authorship on `NameUsage`

Read the **first** `P225` statement's qualifiers:

- authors = ordered list of `P405` QIDs
- year = `P574` (extract 4-digit year)
- recombination = `P3831` qualifier item equals `Q14594740`

Then:

- **Not a recombination** → set `combinationAuthorship` (author family names
  joined: 2 authors `"A & B"`, 3+ `"A, B & C"`), `combinationAuthorshipYear` =
  year, `combinationAuthorshipID` = comma-joined QIDs.
- **Recombination** → the author+year describe the basionym, so set
  `basionymAuthorship` / `basionymAuthorshipYear` / `basionymAuthorshipID`
  instead. No combination author is recorded (Wikidata does not store the
  recombining author for zoological moves).
- **Flat `authorship` fallback** = family-names + `", " + year`, wrapped in
  parentheses for recombinations: `"Gertsch & Mulaik, 1940"`, `"(Linnaeus, 1758)"`.
- **No `P405`** → keep current behaviour: use the `P835`-on-taxon string as the
  flat `authorship`, write no atomized fields.

`*ExAuthorship*` fields stay empty (Wikidata's `P405` does not model ex-authors).

### Basionym, year, and worked examples

- `basionymID` ← `P566`, else `P1403` (original combination item — already emitted
  as its own `NameUsage` since it has `P225`). `originalSpelling` ← `P1353`.
- `namePublishedInYear` stays sourced **only** from the existing "first valid
  description" reference path (`nomRef`). Recombinations like *Panthera tigris*
  (whose `P225` references are database citations with no first-valid-description
  role) correctly get **no** spurious year; originals like *Poa annua* get 1753.

| Name | Wikidata | Output (atomized) | Flat fallback |
|---|---|---|---|
| *Poa annua* | `P405`=Q1043, `P574`=1753, no recomb | `combinationAuthorship`=Linnaeus, `combinationAuthorshipYear`=1753, `combinationAuthorshipID`=Q1043 | `Linnaeus, 1753` |
| *Loxosceles reclusa* | `P405`=[Q955086,Q22114001], `P574`=1940 | `combinationAuthorship`=Gertsch & Mulaik, year 1940, ID `Q955086,Q22114001` | `Gertsch & Mulaik, 1940` |
| *Panthera tigris* | `P405`=Q1043, `P574`=1758, `P3831`=recomb, `P1353`=Felis tigris, `P1403`=Q41083521 | `basionymAuthorship`=Linnaeus, `basionymAuthorshipYear`=1758, `basionymAuthorshipID`=Q1043, `basionymID`=Q41083521, `originalSpelling`=Felis tigris | `(Linnaeus, 1758)` |

ChecklistBank renders *Poa annua* as "L." (botanical, via
`Author.abbreviationBotany`) and *Panthera tigris* as "(Linnaeus, 1758)".

### Writers / output (Phase A)

- `initWriters()` adds the atomized columns to the `NameUsage` writer:
  `combinationAuthorship(ID)`, `combinationAuthorshipYear`,
  `basionymAuthorship(ID)`, `basionymAuthorshipYear`.
- New `Author` writer; authors written after pass 2 (so only used ones are
  emitted), like references.

---

## Phase B — TaxonProperty, temporal range, species interactions

### TaxonProperty

Which taxon properties become `col:TaxonProperty` records: **dynamic discovery
plus a curated extras allowlist**.

- **Dynamic**: any property whose P-entity is `instance of Q18609040`
  ("Wikidata property related to taxa"). Property labels are captured the same way
  external-id property metadata is (P-entities streamed in pass 1).
- **Curated extras**: a maintained allowlist of measurement properties that lack
  that class — `P2067` mass, `P462` color, `P3485` bite force quotient, and
  similar (extend over time). Plus `P788` mushroom ecological type.
- **Excluded** (special-cased elsewhere): `P523`/`P524` (temporal range, → NameUsage),
  and the interaction properties (`P1034`, `P2975`, `P1605`, `P1606`).

Emission per `col:TaxonProperty` row:

- `taxonID` = taxon QID
- `property` = the Wikidata property's English label (e.g. `habitat`, `mass`,
  `mushroom ecological type`)
- `value`:
  - item-valued (`habitat`, `diel cycle`, `P788`) → resolved QID **label**
  - quantity-valued (`mass`, `gestation period`, `lifespan`) → amount + resolved
    **unit** label/symbol (unit is a QID needing resolution)
  - string/time → verbatim
- multiple values for a property → multiple rows.

This requires resolving item-values and units (QID → label), reusing the
SPARQL/dump label-resolution infrastructure.

### Temporal range (on NameUsage, not TaxonProperty)

- `P523` temporal range start (geological-period item) → `col:temporalRangeStart`
- `P524` temporal range end (geological-period item) → `col:temporalRangeEnd`

Resolve the period QID to its label (e.g. `Pleistocene`). Add
`temporalRangeStart` / `temporalRangeEnd` columns to the `NameUsage` writer.

### Species interactions

New `additionalWriter(ColdpTerm.SpeciesInteraction, …)`. The three interaction
*categories* the user named (parasitism, predation, symbiosis) are **not** wired
to taxa as partner-bearing statements in Wikidata (the type-items Q186517/Q170430/
Q121610 appear only under `P279`/`P921`/`P788`, never as taxon→partner-taxon).
They are realized through concrete properties, mapped as:

| Wikidata property | ColDP `type` | Category |
|---|---|---|
| `P1034` main food source | `EATS` | predation |
| `P2975` has host | `HAS_HOST` | parasitism |
| `P1605` has natural reservoir | `SYMBIONT_OF` | symbiosis / reservoir |
| `P1606` natural reservoir of | `HOST_OF` | (inverse) |

The map is curated and extensible.

Emission per `col:SpeciesInteraction` row:

- `taxonID` = taxon QID
- `type` = mapped ColDP `SpeciesInteractionType`
- `relatedTaxonID` = value QID **when it is a taxon** (has `P225`; e.g. lion's
  food source `Q1231177` *ungulate* is a real taxon). When the value is not a
  taxon, fall back to `relatedTaxonScientificName` (its label) and leave
  `relatedTaxonID` empty.

---

## Testing

- **Pure mapping helpers** (unit-tested, no network), e.g. in `ColacMappings`-style
  helper or a new `WikidataMappings`:
  - author-name join (1 / 2 / 3+ authors)
  - recombination → bracketed flat string
  - family/given split from a label
  - quantity + unit formatting
  - interaction property → `SpeciesInteractionType`
- **Fixture integration check** on the three already-captured names — *Poa annua*
  (botanical original), *Loxosceles reclusa* (multi-author zoological original),
  *Panthera tigris* (zoological recombination) — asserting atomized authorship
  fields, `Author` records (including `abbreviationBotany`), `basionymID`, and
  `originalSpelling`.
- Phase B: fixture checks for a quantity TaxonProperty (mass), an item
  TaxonProperty (habitat), temporal range, and an `EATS` interaction with a taxon
  partner.

## Out of scope / non-goals

- Detecting the nomenclatural code in the generator (delegated to ChecklistBank
  via the Author table).
- Ex-author modelling.
- Interaction type-items as standalone interactions (not realizable from Wikidata
  data).
- Setting `extinct` from temporal range.
