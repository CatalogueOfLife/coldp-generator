# USDA PLANTS Database Generator — Design Spec

**Date:** 2026-04-29  
**Source key:** `usda`  
**Package:** `org.catalogueoflife.data.usda`

---

## Overview

ColDP generator for the [USDA PLANTS Database](https://plants.sc.egov.usda.gov/), the authoritative checklist of vascular plants, mosses, liverworts, hornworts, and lichens of the US and its territories (~49K accepted taxa, ~44K synonyms).

---

## Data Source

### Primary file (always downloaded)

`https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt`

CSV (comma-separated, quoted), one header row, then ~93,156 data rows:

| Column | Name | Notes |
|---|---|---|
| 1 | Symbol | Accepted symbol (e.g. `QUAL`) |
| 2 | Synonym Symbol | Empty for accepted rows; synonym symbol for synonym rows |
| 3 | Scientific Name with Author | Full name + authorship (no HTML) |
| 4 | Common Name | English common name; empty on synonym rows |
| 5 | Family | Family name string; empty on synonym rows |

Row types:
- **Accepted row**: column 2 is empty. ~48,994 rows.
- **Synonym row**: column 2 holds the synonym's own symbol; column 1 holds the accepted symbol it maps to. ~44,163 rows.

### Enrichment API (optional, `--enrich` flag)

`GET https://plantsservices.sc.egov.usda.gov/api/PlantProfile?symbol={symbol}`  
Called for each accepted symbol. Responses cached as `profile-{symbol}.json` in the sources directory; `--no-download` skips all HTTP calls. Config API base URL discovered from `https://plants.sc.egov.usda.gov/assets/config.json`.

Key response fields used:

| Field | Type | Used for |
|---|---|---|
| `Rank` | string | Rank override (in case name-parser result differs) |
| `Durations` | string[] | TaxonProperty `duration` |
| `GrowthHabits` | string[] | TaxonProperty `growth habit` |
| `Group` | string | TaxonProperty `group` (Dicot / Monocot / etc.) |
| `NativeStatuses` | object[] | Distribution rows |
| `ProfileImageFilename` | string | Media row |
| `OtherCommonNames` | string[] | Additional VernacularName rows |

`NativeStatuses` objects: `{ Region, Status, Type }`.  
`Status` values: `N` (native), `I` (introduced), `N?` (probably native), `I?` (probably introduced).  
Region codes used as-is in the `area` field (AK, L48, CAN, HI, PR, VI, GL, SPM, NA, …).

---

## Hierarchy

Three levels: **family → genus → species/infraspecific**

### Families (synthetic nodes)
Families are **not** present as entries in the TXT file; they appear only as strings in column 5.  
- ID: `fam:{FamilyName}` (e.g. `fam:Malvaceae`)  
- rank: family  
- scientificName: the family name string  
- No parentID, no link  

Collected during first pass: one synthetic family entry per distinct family name found in column 5.

### Genera
4,806 genera are present as uninomial accepted entries in the TXT file (their own Symbol is used as ID).  
278 genera are referenced by species/infraspecifics but **absent** from the file.  
- Synthetic ID: `gen:{GenusName}` (e.g. `gen:Acamptopappus`)  
- Family resolved from the Family column of the first species in that genus encountered during parsing.  

All genus entries: parentID = `fam:{FamilyName}`.

### Species and infraspecifics
- parentID = Symbol of the genus entry (real or synthetic).  
- Genus name extracted as the first token of the scientific name.

### Synonyms
- parentID = accepted Symbol (column 1 of that synonym row).  
- No link (synonyms don't have meaningful profile pages).

---

## Rank Inference

Use the **name-parser** library (`life.catalogue.api` / `org.gbif.nameparser`) to parse each scientific name:
- Uninomial → genus  
- Binomial → species  
- Trinomial with `var.` → variety; `subsp.`/`ssp.` → subspecies; `f.` → form; etc.  
- Hybrid markers (`×`) handled by name-parser automatically.

When enrichment is enabled the `Rank` field from PlantProfile can be used to cross-check, but the name-parser result is the primary source since it works without API calls.

---

## ColDP Output

### Always produced

**NameUsage.tsv**

| Field | Source |
|---|---|
| ID | Symbol / `fam:X` / `gen:X` |
| parentID | See hierarchy rules above |
| rank | From name-parser |
| scientificName | Name without authorship (from name-parser split) |
| authorship | Authorship string (from name-parser split) |
| family | Column 5 (denormalized; empty for family/genus nodes) |
| status | `accepted` / `synonym` |
| vernacularName | Column 4 (accepted rows only; empty for synthetic nodes) |
| link | `https://plants.sc.egov.usda.gov/plant-profile/{Symbol}` — real symbols only, not synthetic nodes |

**VernacularName.tsv**

One row per accepted entry where column 4 is non-empty.

| Field | Value |
|---|---|
| taxonID | Symbol |
| name | Column 4 |
| language | `eng` |

### Produced only with `--enrich`

**Distribution.tsv** — from `NativeStatuses[]`

| Field | Value |
|---|---|
| taxonID | Symbol |
| area | Region code (AK, L48, CAN, …) |
| gazetteer | `text` |
| status | `native` / `introduced` / `uncertain native` / `uncertain introduced` (mapped from N/I/N?/I?) |

**TaxonProperty.tsv** — from `Durations[]`, `GrowthHabits[]`, `Group`

| Field | Value |
|---|---|
| taxonID | Symbol |
| property | `duration` / `growth habit` / `group` |
| value | Array element or scalar value |

Multiple rows per taxon when arrays contain more than one value.

**Media.tsv** — from `ProfileImageFilename`

| Field | Value |
|---|---|
| taxonID | Symbol |
| url | `https://plants.sc.egov.usda.gov/ImageLibrary/standard/{ProfileImageFilename}` |
| type | `StillImage` |

Only written when `ProfileImageFilename` is non-null and non-empty.

**VernacularName.tsv** — additional rows from `OtherCommonNames[]`

| Field | Value |
|---|---|
| taxonID | Symbol |
| name | Each element of `OtherCommonNames` |
| language | `eng` |

---

## Enrichment Fetch Strategy

- **Concurrency**: 10 threads via `ExecutorService` / `CompletableFuture`.
- **Caching**: each profile saved as `profile-{symbol}.json` under the sources directory.  
  `--no-download` skips all HTTP — generator reads from cache only.
- **Estimated runtime**: ~1.4 hours first run at ~1 s/call with 10 threads; fast on re-runs.
- **Error handling**: HTTP errors for a single symbol are logged and skipped; the generator continues.

---

## Metadata & Resources

- `src/main/resources/usda/metadata.yaml` — standard ColDP metadata with `notes:` block documenting the PlantProfile API and further available endpoints (PlantCharacteristics, PlantNoxiousStatus, PlantInvasiveStatus, PlantWetland, PlantLegalStatus, PlantSynonyms).
- Logo: download from USDA PLANTS site or USDA/NRCS branding.
- Test: `src/test/java/org/catalogueoflife/data/usda/GeneratorTest.java` (annotated `@Ignore`).
- ManualCli entry for development runs.

---

## `GeneratorConfig` change

Add one new field:

```java
@Parameter(names = {"--enrich"},
           description = "Fetch PlantProfile API for each accepted name (adds Distribution, TaxonProperty, Media)")
public boolean enrich = false;
```

---

## ID Scheme Summary

| Taxon type | ID pattern | Example |
|---|---|---|
| Accepted (real) | Symbol from file | `QUAL` |
| Synonym | Synonym Symbol from file | `QUALS` |
| Synthetic family | `fam:{FamilyName}` | `fam:Fagaceae` |
| Synthetic genus | `gen:{GenusName}` | `gen:Acamptopappus` |
