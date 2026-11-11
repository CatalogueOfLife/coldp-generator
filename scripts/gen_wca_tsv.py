#!/usr/bin/env python3
"""
Builds the curated WCA 2030 crop tables in src/main/resources/wca/ from the PDF of the
World Programme for the Census of Agriculture 2030 (FAO Statistical Development Series 22):

  https://openknowledge.fao.org/handle/20.500.14283/cd9437en

  icc.tsv                  Annex 6, Indicative Crop Classification (ICC) 1.2: code, parent, label, crop type
  crops.tsv                Annex 7, alphabetical list of crops: one row per crop and botanical name
  wca2030-annex6-7.pdf     the source pages: the page with the required citation and licence, and
                           Annexes 6 and 7. The generator ships it inside the ColDP archive.

The tables only exist in the PDF, wrap over many pages and carry errors of their own, so they
are extracted once and fixed here rather than parsed on every generator run. The WCA is decennial.
Re-run only when FAO publishes a new revision or corrigendum:

  pip install pdfplumber pypdf
  python3 scripts/gen_wca_tsv.py cd9437en.pdf

The corrigendum of 23 September 2026 is already applied to the PDF currently online (e.g.
"Arracacha", Beet in ICC 2.03.90), so no corrigendum fixes are needed below.
"""
import csv
import re
import sys
from pathlib import Path

import pdfplumber
from pypdf import PdfReader, PdfWriter

OUT = Path(__file__).resolve().parent.parent / 'src/main/resources/wca'
EXCERPT = 'wca2030-annex6-7.pdf'
CITATION_PAGE = 4              # 1-based pdf page with the required citation and the CC BY 4.0 licence
ANNEX_PAGES = range(190, 205)  # 1-based pdf pages of Annex 6 and 7
ICC_PAGES = range(192, 197)    # 1-based pdf pages of the ICC 1.2 table
CROP_PAGES = range(197, 205)   # 1-based pdf pages of Annex 7

CODE = re.compile(r'^\d+(\.\d+)*$')

# Codes in Annex 7 lost their trailing zero (5.90 -> 5.9) or are plain typos.
CODE_FIXES = {
    '20.04.02': '2.04.02',  # Truffles
    '9.09.02': '9.90.02',   # Palm palmyra & Palm, sago: permanent crops, n.e.c.
}

# Botanical names that the generic parser below cannot clean: verbatim -> curated names.
# Misspellings are fixed, cross references resolved and "Genus spp." reduced to the genus.
NAME_FIXES = {
    'Musa sapientum, M. cavendishii, M.nana': ['Musa sapientum', 'Musa cavendishii', 'Musa nana'],
    'Prunus avium, cerasus avium': ['Prunus avium', 'Cerasus avium'],
    'Prunus cerasus, cerasus acida': ['Prunus cerasus', 'Cerasus acida'],
    'Capsicum spp. (annuum)': ['Capsicum annuum'],
    'Cymbopogon citrates/ Cymbopogon nardus': ['Cymbopogon citratus', 'Cymbopogon nardus'],
    'Eugenia aromatica (Syzygium aromaticum)': ['Eugenia aromatica', 'Syzygium aromaticum'],
    'Erythroxypum novogranatense, E. coca': ['Erythroxylum novogranatense', 'Erythroxylum coca'],
    'Annona reticulate': ['Annona reticulata'],
    'Sorghum bicolour': ['Sorghum bicolor'],
    'Sorghum bicolour var. Sudanense': ['Sorghum bicolor var. sudanense'],
    'Paulinia cupana': ['Paullinia cupana'],
    'Afframomum melegueta, piper guineense, xylopia aethiopica)':
        ['Aframomum melegueta', 'Piper guineense', 'Xylopia aethiopica'],
    'Cannabis sativa ssp. Indica': ['Cannabis sativa subsp. indica'],
    'Cannabis sativa (marijuana)': ['Cannabis sativa'],
    'Brassica oleracea var. Acephala': ['Brassica oleracea var. acephala'],
    'Macadamia spp. ternifolia': ['Macadamia ternifolia'],
    'Agaricus spp. ; Pleurotus spp ; Volvariella': ['Agaricus', 'Pleurotus', 'Volvariella'],
    'Mixture of Triticum spp.; Secale cereale': ['Triticum', 'Secale cereale'],
    'Raphanus sativus (inc. Cochlearia armoracia)': ['Raphanus sativus', 'Cochlearia armoracia'],
    'Rose spp.': ['Rosa'],
    'Crocus savitus': ['Crocus sativus'],
    'Hybrid of Triticum aestivum and Secale cereale': ['×Triticosecale'],
    'Genus tuber': ['Tuber'],
    'Aleurites spp.; Fordii': ['Aleurites', 'Aleurites fordii'],
    'Juglans spp. (over 20 sp.), ep. regia': ['Juglans', 'Juglans regia'],
    'Various': [],
}
# Annex 7 refers to another row instead of naming the plant.
SEE = {'see Cola nut': 'Cola nut (all varieties)', 'See Macadamia': 'Macadamia (Queensland nut)'}
FUNGI = {'Agaricus', 'Pleurotus', 'Volvariella', 'Tuber'}


def tables(pdf, pages):
    for i in pages:
        for t in pdf.pages[i - 1].extract_tables():
            for row in t:
                yield [(c or '').replace('\n', ' ').strip() for c in row]


def read_icc(pdf):
    """Annex 6 rows -> {code: (label, cropType)}, codes in table order."""
    icc = {}
    for row in tables(pdf, ICC_PAGES):
        if len(row) < 7 or row[0] == 'Group':
            continue
        label = row[4] or row[5]
        if not label:
            continue
        # a page continuation repeats the group in the first cell, the real code is the most specific one
        codes = [c for c in row[:4] if CODE.match(c)]
        code = max(codes, key=len) if codes else None
        # the PDF lost the number of group 8 and numbers group 9 as 8
        if label == 'Sugar crops':
            code = '8'
        elif label == 'Other crops':
            code = '9'
        if code is None:
            raise ValueError(f'No ICC code in {row}')
        icc[code] = (label, row[6])
    return icc


def parent(code):
    return code.rsplit('.', 1)[0] if '.' in code else ''


def fix_code(code, icc=None):
    code = CODE_FIXES.get(code) or code
    if re.match(r'^\d\.\d$', code):  # 5.9 -> 5.90, 1.1 -> 1.10
        code += '0'
    if icc is not None and code and code not in icc:
        raise ValueError(f'ICC 1.2 code {code} not in Annex 6')
    return code


def split_names(verbatim):
    if verbatim in NAME_FIXES:
        return NAME_FIXES[verbatim]
    names, genus = [], None
    for part in re.split(r'\s*(?:;|,|/| or )\s*', verbatim):
        part = re.sub(r'\([^)]*\)', '', part).strip()  # (over 30 sp.)
        if not part:
            continue
        m = re.match(r'^([A-Z])\.\s*(\S.*)$', part)  # V. corymbosum
        if m and genus and genus.startswith(m.group(1)):
            part = f'{genus} {m.group(2)}'
        part = re.sub(r'\s+spp?\.?$', '', part)  # Rubus spp.
        genus = part.split()[0]
        names.append(part)
    return names


def main(pdf_file):
    OUT.mkdir(parents=True, exist_ok=True)
    with pdfplumber.open(pdf_file) as pdf:
        icc = read_icc(pdf)
        raw = [r for r in tables(pdf, CROP_PAGES) if len(r) >= 5 and r[0] and r[0] != 'Crop name']

    with open(OUT / 'icc.tsv', 'w', newline='') as f:
        w = csv.writer(f, delimiter='\t', lineterminator='\n')
        w.writerow(['code', 'parent', 'label', 'cropType'])
        for code, (label, ctype) in icc.items():
            w.writerow([code, parent(code), label, ctype])

    by_crop = {r[0]: r for r in raw}
    rows = 0
    with open(OUT / 'crops.tsv', 'w', newline='') as f:
        w = csv.writer(f, delimiter='\t', lineterminator='\n')
        w.writerow(['crop', 'scientificName', 'kingdom', 'icc11', 'icc12', 'cpc', 'verbatimName'])
        for crop, verbatim, icc11, icc12, cpc in (r[:5] for r in raw):
            if not icc12:
                continue  # pure cross reference like "Corn (see maize)"
            crop = crop.rstrip(', ')
            names = split_names(by_crop[SEE[verbatim]][1] if verbatim in SEE else verbatim)
            # stray quotes and a missing comma between two codes (Poppy straw)
            cpc = re.sub(r'(\d\*?) (?=\d)', r'\1, ', cpc.replace("'", ''))
            cpc = re.sub(r'\s*,\s*', ', ', cpc).strip()
            for name in names:
                kingdom = 'Fungi' if name.split()[0] in FUNGI else 'Plantae'
                w.writerow([crop, name, kingdom, fix_code(icc11), fix_code(icc12, icc), cpc,
                            '' if [name] == [verbatim] else verbatim])
                rows += 1
    write_excerpt(pdf_file)
    print(f'{len(icc)} ICC classes, {rows} crop names and the source pages written to {OUT}')


def write_excerpt(pdf_file):
    """The source pages the tables come from, to ship them with the archive."""
    reader = PdfReader(pdf_file)
    writer = PdfWriter()
    for p in [CITATION_PAGE, *ANNEX_PAGES]:
        writer.add_page(reader.pages[p - 1])
    writer.add_metadata({
        '/Title': 'World Programme for the Census of Agriculture 2030 - Annexes 6 and 7 (excerpt)',
        '/Author': 'FAO',
        '/Subject': 'FAO. 2026. World Programme for the Census of Agriculture 2030 - Programme, concepts and '
                    'definitions. FAO Statistical Development Series, No. 22. Rome. https://doi.org/10.4060/cd9437en. '
                    'CC BY 4.0',
    })
    writer.compress_identical_objects(remove_duplicates=True, remove_unreferenced=True)
    with open(OUT / EXCERPT, 'wb') as f:
        writer.write(f)


if __name__ == '__main__':
    main(sys.argv[1])
