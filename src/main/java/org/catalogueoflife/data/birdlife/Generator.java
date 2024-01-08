/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.catalogueoflife.data.birdlife;

import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.catalogueoflife.data.AbstractXlsSrcGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

public class Generator extends AbstractXlsSrcGenerator {
  // to be updated manually to current version !!!
  // http://datazone.birdlife.org/species/taxonomy
  private static final URI DOWNLOAD = URI.create("http://datazone.birdlife.org/userfiles/file/Species/Taxonomy/Handbook%20of%20the%20Birds%20of%20the%20World%20and%20BirdLife%20International%20Digital%20Checklist%20of%20the%20Birds%20of%20the%20World_Version_6b.xlsx");
  // manually curated data
  private static final String ISSUED = "2022-07";
  private static final String VERSION = "6.0b";
  // SPREADSHEET FORMAT
  private static final int SHEET_IDX = 0;
  private static final int SKIP_ROWS = 4;

  private static final int COL_SORT = 0;
  private static final int COL_SSP_SORT = 1;
  private static final int COL_ORDER = 2;
  private static final int COL_FAMILY = 3;
  private static final int COL_SUBFAMILY = 5;
  private static final int COL_TRIBE = 6;
  private static final int COL_VERNACULAR = 7;
  private static final int COL_SCINAME   = 8;
  private static final int COL_AUTHORITY = 9;
  private static final int COL_REDLIST = 11;
  private static final int COL_SYNONYMS = 12;
  private static final int COL_ALT_VERNACULARS = 13;
  private static final int COL_SOURCES = 14;
  private static final int COL_SISRecID = 15;
  private static final int COL_SpcRecID = 16;
  private static final int COL_SubsppID = 17;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, DOWNLOAD);
  }

  @Override
  protected void addData() throws Exception {
    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.ordinal,
      ColdpTerm.rank,
      ColdpTerm.status,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.code,
      ColdpTerm.order,
      ColdpTerm.family,
      ColdpTerm.subfamily,
      ColdpTerm.tribe,
      ColdpTerm.alternativeID,
      ColdpTerm.referenceID
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
        ColdpTerm.ID,
        ColdpTerm.citation,
        ColdpTerm.link
    ));
    var vWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.language,
        ColdpTerm.name
    ));

    final Pattern REF_LINK_PATTERN = Pattern.compile("(?:Available at)? ?:? ?#(http.+)# ?\\.?", Pattern.CASE_INSENSITIVE);
    final Pattern NO_AUTHOR = Pattern.compile("[\\d():]");
    final Map<String, Integer> refs = new HashMap<>();
    int refID = 1;

    Sheet sheet = wb.getSheetAt(SHEET_IDX);
    int rows = sheet.getPhysicalNumberOfRows();
    LOG.info("{} rows found in excel sheet", rows);

    Iterator<Row> iter = sheet.rowIterator();
    String spID = null;
    while (iter.hasNext()) {
      Row row = iter.next();
      if (row.getRowNum()+1 <= SKIP_ROWS) continue;

      final String sort = col(row, COL_SORT);
      final String sort2 = col(row, COL_SSP_SORT);

      String id;
      Rank rank;
      if (sort2.equals("0")) {
        id = col(row, COL_SISRecID);
        spID = id;
        rank = Rank.SPECIES;
        writer.set(ColdpTerm.ordinal, sort);
        writer.set(ColdpTerm.order, col(row, COL_ORDER));
        writer.set(ColdpTerm.family, col(row, COL_FAMILY));
        writer.set(ColdpTerm.subfamily, col(row, COL_SUBFAMILY));
        writer.set(ColdpTerm.tribe, col(row, COL_TRIBE));
      } else {
        id = col(row, COL_SubsppID);
        rank = Rank.SUBSPECIES;
        writer.set(ColdpTerm.parentID, spID);
        writer.set(ColdpTerm.ordinal, sort2);
      }
      writer.set(ColdpTerm.ID, id);
      writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
      writer.set(ColdpTerm.authorship, col(row, COL_AUTHORITY));
      writer.set(ColdpTerm.scientificName, col(row, COL_SCINAME));
      writer.set(ColdpTerm.authorship, col(row, COL_AUTHORITY));
      writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
      writer.set(ColdpTerm.rank, rank.name());

      String sources = col(row, COL_SOURCES);
      List<String> refIDs = new ArrayList<>();
      if (sources != null) {
        var srcs = sources.split(";");
        StringBuilder refBuilder = new StringBuilder();
        for (String ref : srcs) {
          ref = StringUtils.trimToNull(ref);
          if (ref != null) {
            //  some authors are concatenated by semicolon :( we merge them with the next bits
            //  Baker, A. J.
            //  Dekker, R. W. R. J.
            refBuilder.append(ref);
            if (!NO_AUTHOR.matcher(ref).find()) {
              refBuilder.append("; ");
              continue;
            }
            ref = refBuilder.toString();
            refBuilder = new StringBuilder();
            String link = null;
            // Available at: #http://www.aerc.eu/DOCS/Bird_taxa_of _the_WP15.xls#.
            var m = REF_LINK_PATTERN.matcher(ref);
            if (m.find()) {
              link = m.group(1);
              ref = m.replaceFirst("");
            }

            String refKey = ref.toLowerCase().replaceAll("[ .,-]", "");
            if (refs.containsKey(refKey)) {
              refIDs.add(refs.get(refKey).toString());
            } else {
              refWriter.set(ColdpTerm.ID, refID);
              refWriter.set(ColdpTerm.citation, ref);
              refWriter.set(ColdpTerm.link, link);
              refWriter.next();
              refs.put(refKey, refID);
              refIDs.add(String.valueOf(refID));
              refID++;
            }
          }
        }
      }
      if (!refIDs.isEmpty()) {
        writer.set(ColdpTerm.referenceID, String.join(",", refIDs));
      }
      writer.next();

      String syns = col(row, COL_SYNONYMS);
      if (!StringUtils.isBlank(syns)) {
        int x = 1;
        for (String syn : syns.split(";")) {
          if (!StringUtils.isBlank(syn)) {
            writer.set(ColdpTerm.ID, String.format("%s-s%s",id,x));
            writer.set(ColdpTerm.parentID, id);
            writer.set(ColdpTerm.scientificName, syn.trim());
            writer.set(ColdpTerm.status, TaxonomicStatus.SYNONYM);
            writer.set(ColdpTerm.code, NomCode.ZOOLOGICAL.getAcronym());
            writer.next();
            x++;
          }
        }
      }

      List<String> verns = new ArrayList<>();
      verns.add(col(row, COL_VERNACULAR));
      String altV = col(row, COL_ALT_VERNACULARS);
      if (!StringUtils.isBlank(altV)) {
        verns.addAll(Arrays.asList(altV.split(",")));
      }
      for (String v : verns) {
        if (!StringUtils.isBlank(v)) {
          vWriter.set(ColdpTerm.taxonID, id);
          vWriter.set(ColdpTerm.language, "eng");
          vWriter.set(ColdpTerm.name, StringUtils.trimToNull(v));
          vWriter.next();
        }
      }
    }
  }


  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", ISSUED);
    metadata.put("version", VERSION);
    super.addMetadata();
  }

}
