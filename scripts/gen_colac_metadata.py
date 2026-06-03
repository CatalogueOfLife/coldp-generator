#!/usr/bin/env python3
"""
Generate the per-year ColDP metadata files for the colac (CoL Annual Checklist) source
from the editor's compilation spreadsheet (resources/colac/citations/ACs_metadata_summary_v1.xlsx).

Writes src/main/resources/colac/metadata/<year>.yaml — one explicit, hand-editable file per year.
Two placeholders remain for the generator to fill at runtime: {issued} and the trailing {sources}.

Re-run only if the spreadsheet or the author map changes; otherwise edit the YAML files directly.
"""
import openpyxl, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX = os.path.join(ROOT, "src/main/resources/colac/citations/ACs_metadata_summary_v1.xlsx")
OUT  = os.path.join(ROOT, "src/main/resources/colac/metadata")

# Expanded author names; ORCIDs only where verified (this repo's prior map + dataset/3).
# Uncertain given names (Kimani, Hernandez, Paglinawan) are intentionally left as initials.
AUTHORS = {
    "Abucay L.":            ("Luisa", "Abucay", None),
    "Appeltans W.":         ("Ward", "Appeltans", None),
    "Baillargeon G.":       ("Guy", "Baillargeon", None),
    "Bailly N.":            ("Nicolas", "Bailly", "0000-0003-4994-0653"),
    "Bisby F.":             ("Frank A.", "Bisby", None),
    "Bisby F.A.":           ("Frank A.", "Bisby", None),
    "Bourgoin T.":          ("Thierry", "Bourgoin", None),
    "Brewer P.W.":          ("Peter W.", "Brewer", None),
    "Cachuela-Palacio M.":  ("Monalisa", "Cachuela-Palacio", None),
    "Culham A.":            ("Alastair", "Culham", None),
    "De Wever A.":          ("Aaike", "De Wever", None),
    "DeWalt R.E.":          ("R. Edward", "DeWalt", "0000-0001-9985-9250"),
    "Decock W.":            ("Wim", "Decock", "0000-0002-2168-9471"),
    "Didžiulis V.":         ("Viktoras", "Didžiulis", None),
    "Flann C.":             ("Christina", "Flann", None),
    "Froese R.":            ("Rainer", "Froese", None),
    "Hernandez F.":         ("F.", "Hernandez", None),          # uncertain — initials kept
    "Kimani S.":            ("S.", "Kimani", None),             # uncertain
    "Kimani S.W.":          ("S.W.", "Kimani", None),           # uncertain
    "Kirk P.":              ("Paul M.", "Kirk", "0000-0002-0658-7338"),
    "Kirk P.M.":            ("Paul M.", "Kirk", "0000-0002-0658-7338"),
    "Kunze T.":             ("Thomas", "Kunze", None),
    "Nicolson D.":          ("David", "Nicolson", "0000-0002-7987-0679"),
    "Nieukerken E. van":    ("Erik J.", "van Nieukerken", None),
    "Orrell T.":            ("Thomas M.", "Orrell", "0000-0003-1038-3028"),
    "Orrell T.M.":          ("Thomas M.", "Orrell", "0000-0003-1038-3028"),
    "Ouvrard D.":           ("David", "Ouvrard", "0000-0003-2931-6116"),
    "Ower G.":              ("Geoff", "Ower", "0000-0002-9770-2345"),
    "Paglinawan L.":        ("L.", "Paglinawan", None),         # uncertain
    "Paglinawan L.E.":      ("L.E.", "Paglinawan", None),       # uncertain
    "Penev L.":             ("Lyubomir", "Penev", None),
    "Roskov Y.":            ("Yury R.", "Roskov", "0000-0003-2137-2690"),
    "Roskov Y.R.":          ("Yury R.", "Roskov", "0000-0003-2137-2690"),
    "Ruggiero M.":          ("Michael A.", "Ruggiero", None),
    "Ruggiero M.A.":        ("Michael A.", "Ruggiero", None),
    "Soulier-Perkins A.":   ("Adeline", "Soulier-Perkins", None),
    "Wilson K.":            ("Karen L.", "Wilson", None),
    "Wilson K.L.":          ("Karen L.", "Wilson", None),
    "Zarucchi J.":          ("James L.", "Zarucchi", None),
    "van Hertum J.":        ("Jorrit", "van Hertum", None),
}

# publisher city/country per era (ISO country codes) from the spreadsheet
def publisher(year):
    if year <= 2004: city, country = "Los Baños", "PH"
    elif year <= 2013: city, country = "Reading", "GB"
    else: city, country = "Leiden", "NL"
    org = "Species 2000 Catalogue of Life" if year == 2000 else "Species 2000 & ITIS Catalogue of Life"
    return org, city, country

KEYWORDS = ["COL", "Catalogue of Life", "species list", "community"]

def sq(s):  # single-quoted YAML scalar
    return "'" + s.replace("'", "''") + "'"

def block(s, indent):
    pad = " " * indent
    out = []
    for line in s.split("\n"):
        out.append(pad + line if line.strip() else "")
    return "\n".join(out)

def parse_editors(s):
    people = []
    for tok in s.split(","):
        tok = tok.strip()
        if not tok: continue
        if tok in AUTHORS:
            given, family, orcid = AUTHORS[tok]
        else:                                    # defensive fallback: "Surname I.I."
            parts = tok.rsplit(" ", 1)
            family, given, orcid = (parts[0], parts[1], None) if len(parts) == 2 else (tok, None, None)
        people.append((given, family, orcid))
    return people

def conversion_desc(year):
    base = ("Converted to ColDP from the original Species 2000/ITIS Annual Checklist MySQL "
            "database with the export scripts in this repository.")
    if year <= 2004:
        base += (" These early checklists were originally distributed as MS Access databases, "
                 "which were first imported into MySQL before conversion.")
    return base

def main():
    wb = openpyxl.load_workbook(XLSX, data_only=True)
    ws = wb["Sheet1"]; rows = list(ws.iter_rows(values_only=True))
    years = [int(c) for c in rows[0] if c is not None]
    F = {str(r[0]): [("" if c is None else str(c)) for c in r[1:1+len(years)]] for r in rows[1:] if r[0]}
    os.makedirs(OUT, exist_ok=True)
    for i, year in enumerate(years):
        g = lambda f: F.get(f, [""]*len(years))[i]
        org, city, country = publisher(year)
        L = []
        L.append("# yaml-language-server: $schema=https://raw.githubusercontent.com/CatalogueOfLife/coldp/master/metadata.json")
        L.append("---")
        L.append(f"alias: {g('Alias (in CLB)')}")
        L.append(f"title: {sq(g('Title'))}")
        L.append("description: |-")
        L.append(block(g('Description'), 2))
        L.append("issued: {issued}")
        L.append(f"version: Annual Checklist {year}")
        L.append(f"issn: {g('ISSN')}")
        L.append("keyword:")
        for k in KEYWORDS: L.append(f"  - {k}")
        L.append(f"url: http://www.catalogueoflife.org/annual-checklist/{year}/")
        L.append(f"taxonomicScope: {g('Taxonomic scope') or 'Biota'}")
        L.append(f"geographicScope: {g('Geographic scope') or 'Global'}")
        L.append(f"temporalScope: {g('Temporal scope') or 'Extant taxa'}")
        L.append(f"confidence: {g('Checklist Confidence') or 4}")
        if g('Completeness (% of est. extant spp)'):
            L.append(f"completeness: {g('Completeness (% of est. extant spp)')}")
        L.append("license: CC BY")
        L.append("contact:")
        L.append("  organisation: COL Secretariat")
        L.append("  city: Amsterdam")
        L.append("  country: NL")
        L.append("  email: support@catalogueoflife.org")
        L.append("publisher:")
        L.append(f"  organisation: {org}")
        L.append(f"  city: {sq(city)}")
        L.append(f"  country: {country}")
        L.append("editor:")
        for given, family, orcid in parse_editors(g('Editor')):
            L.append(f"  - family: {family}")
            if given: L.append(f"    given: {given}")
            if orcid: L.append(f"    orcid: {orcid}")
        L.append("contributor:")
        L.append("  - family: Döring")
        L.append("    given: Markus")
        L.append("    orcid: 0000-0001-7757-1889")
        L.append("    country: DE")
        L.append("    city: Berlin")
        L.append("    email: mdoering@gbif.org")
        L.append("    note: Developer of the ColDP generator code")
        L.append("conversion:")
        L.append("  url: https://github.com/CatalogueOfLife/coldp-generator")
        L.append("  description: >-")
        L.append(block(conversion_desc(year), 4))
        L.append("notes: |-")
        L.append(block(
            f"Generated from the historical CoL Annual Checklist MySQL database col{year}ac.\n"
            "IDs: t<id> accepted taxa, a<id> accepted names without classification, "
            "s<id> synonyms (and bare names), r<id> references, d<id> source databases (GSDs).\n"
            "Each NameUsage.sourceID points to its contributing Global Species Database.", 2))
        L.append("{sources}")
        path = os.path.join(OUT, f"{year}.yaml")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(L) + "\n")
        print(f"wrote {path}  ({len(F['Editor'][i].split(','))} editors)")

if __name__ == "__main__":
    main()
