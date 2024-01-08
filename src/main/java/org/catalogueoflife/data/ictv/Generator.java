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
package org.catalogueoflife.data.ictv;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import life.catalogue.api.model.DOI;
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
  // https://talk.ictvonline.org/files/master-species-lists/
  private static final URI DOWNLOAD = URI.create("https://ictv.global/msl/current");
  // manually curated data
  private static final List<DOI> SOURCES = List.of(
      new DOI("10.1093/nar/gkx932"),
      new DOI("10.1038/nrmicro.2016.177"),
      new DOI("10.1007/s00705-016-3215-y"),
      new DOI("10.1038/s41564-020-0709-x"),
      new DOI("10.1007/s00705-015-2376-4"),
      new DOI("10.1007/s00705-021-05156-1")
  );
  // SPREADSHEET FORMAT
  private static final int MD_SHEET_IDX = 0; // Version
  private static final int MD_COL_IDX = 1;
  private static final int SHEET_IDX = 1; // MSL
  private static final int SKIP_ROWS = 1;
  private static final int COL_SORT = 0;
  private static final int COL_REALM = 1;
  private static final int COL_SPECIES   = 15;
  private static final int COL_COMPOSITION = 16;
  private static final int COL_CHANGE = 17;
  private static final int COL_LINK = 20;
  private static final List<Rank> CLASSIFICATION = List.of(
    Rank.REALM,
    Rank.SUBREALM,
    Rank.KINGDOM,
    Rank.SUBKINGDOM,
    Rank.PHYLUM,
    Rank.SUBPHYLUM,
    Rank.CLASS,
    Rank.SUBCLASS,
    Rank.ORDER,
    Rank.SUBORDER,
    Rank.FAMILY,
    Rank.SUBFAMILY,
    Rank.GENUS,
    Rank.SUBGENUS
  );

  private static final int SHEET_RENAMED_IDX = 3; // Taxa Renamed in MSL38
  private static final int COL_RENAMED_RANK = 0;
  private static final int COL_RENAMED_OLD = 1;
  private static final int COL_RENAMED_NEW = 2;
  private final String rootID = "root";
  private final Set<String> ids = new HashSet<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, DOWNLOAD);
    // assert classification columns dont overlap with species col
    var i = COL_REALM + CLASSIFICATION.size();
    Preconditions.checkArgument(i <= COL_SPECIES, "Classification columns overlap with species column");
  }

  void extractMetadata() throws IOException {
    // extract metadata
    String baseversion = null;
    String version = null;
    String date = null;
    Pattern MSL = Pattern.compile("MSL\\d+");
    Sheet sheet = wb.getSheetAt(MD_SHEET_IDX);
    Iterator<Row> iter = sheet.rowIterator();
    while (iter.hasNext()) {
      Row row = iter.next();
      if (row.getRowNum() >= 60) break;
      String x = col(row, MD_COL_IDX);
      if (x != null) {
        if (baseversion == null) {
          var m = MSL.matcher(x);
          if (m.find()) {
            baseversion = m.group(0);
          }
        }
        if (x.startsWith("Version")) {
          x = col(row, MD_COL_IDX+1);
          version = baseversion + ".v"+x;
        }
        if (x.startsWith("Date")) {
          date = col(row, MD_COL_IDX+1);
        }
      }
    }
    if (version == null || date == null) {
      throw new IllegalStateException("Unable to find version or date metadata");
    }
    metadata.put("issued", date);
    metadata.put("version", version);
  }
  @Override
  protected void addData() throws Exception {
    extractMetadata();

    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.ordinal,
      ColdpTerm.status,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.code,
      ColdpTerm.link,
      ColdpTerm.remarks
    ));

    // data
    var sheet = wb.getSheetAt(SHEET_IDX);
    int rows = sheet.getPhysicalNumberOfRows();
    LOG.info("{} rows found in excel sheet", rows);

    // first add a single root
    addUsageRecord(rootID, null, null, "Viruses", null, null);

    var iter = sheet.rowIterator();
    while (iter.hasNext()) {
      Row row = iter.next();
      if (row.getRowNum()+1 <= SKIP_ROWS) continue;

      final Integer sort = colInt(row, COL_SORT);
      final String species = col(row, COL_SPECIES);
      if (Strings.isNullOrEmpty(species)) continue;

      String parentID = writeClassification(row, sort);
      // finally the species record
      writer.set(ColdpTerm.link, link(row, COL_LINK));
      writer.set(ColdpTerm.remarks, concat(row, COL_COMPOSITION, COL_CHANGE));
      String id = genID(Rank.SPECIES, species);
      addUsageRecord(id, parentID, Rank.SPECIES, species, sort, TaxonomicStatus.ACCEPTED);
    }

    // now add synonyms from the MSL38 renamer
    sheet = wb.getSheetAt(SHEET_RENAMED_IDX);
    rows = sheet.getPhysicalNumberOfRows();
    LOG.info("{} rows found in synonym sheet", rows);
    iter = sheet.rowIterator();
    while (iter.hasNext()) {
      Row row = iter.next();
      if (row.getRowNum()+1 <= SKIP_ROWS) continue;

      Rank rank = colRank(row, COL_RENAMED_RANK);
      String oldName = lastInList(col(row, COL_RENAMED_OLD));
      String newName = lastInList(col(row, COL_RENAMED_NEW));
      if (rank == null || Strings.isNullOrEmpty(oldName) || Strings.isNullOrEmpty(newName)) continue;

      // the synonym record
      writer.set(ColdpTerm.remarks, "Renamed in MSL 38");
      String id = genID(rank, oldName);
      String parentID = genID(rank, newName);
      addUsageRecord(id, parentID, rank, oldName, null, TaxonomicStatus.SYNONYM);
    }
  }

  private String lastInList(String x) {
    if (x != null) {
      int i = x.lastIndexOf(';');
      return x.substring(i+1).trim();
    }
    return null;
  }

  private String writeClassification(Row row, Integer sort) throws IOException {
    String parentID = rootID;
    int col = COL_REALM;
    for (Rank rank : CLASSIFICATION) {
      String name = col(row, col);
      if (!StringUtils.isBlank(name)) {
        String id = genID(rank, name);
        if (!ids.contains(id)) {
          addUsageRecord(id, parentID, rank, name, sort, TaxonomicStatus.ACCEPTED);
        }
        parentID = id;
      }
      col++;
    }
    return parentID;
  }

  private static String genID(Rank rank, String name) {
    return rank.name().toLowerCase() + ":" + name.toLowerCase().trim().replace(" ", "_");
  }

  private void addUsageRecord(String id, String parentID, Rank rank, String name, Integer sort, TaxonomicStatus status) throws IOException {
    // create new realm record
    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentID);
    writer.set(ColdpTerm.status, status);
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.code, NomCode.VIRUS.getAcronym());
    writer.set(ColdpTerm.ordinal, sort);
    writer.next();
    ids.add(id);
  }

  @Override
  protected void addMetadata() throws Exception {
    //   Walker PJ, Siddell SG, Lefkowitz EJ, Mushegian AR, Adriaenssens EM, Alfenas-Zerbini P, Davison AJ, Dempsey DM, Dutilh BE, García ML, Harrach B, Harrison RL, Hendrickson RC, Junglen S, Knowles NJ, Krupovic M, Kuhn JH, Lambert AJ, Łobocka M, Nibert ML, Oksanen HM, Orton RJ, Robertson DL, Rubino L, Sabanadzovic S, Simmonds P, Smith DB, Suzuki N, Van Dooerslaer K, Vandamme AM, Varsani A, Zerbini FM. Changes to virus taxonomy and to the International Code of Virus Classification and Nomenclature ratified by the International Committee on Taxonomy of Viruses (2021). Arch Virol. 2021 Jul 6. doi: 10.1007/s00705-021-05156-1. PMID: 34231026.
    for (DOI doi : SOURCES) {
      addSource(doi);
    }
    // now also use authors of the source as dataset authors!
    //if (!sources.isEmpty()) {
    //  asYaml(sources.get(0).getAuthor()).ifPresent(yaml -> {
    //    metadata.put("authors", yaml);
    //  });
    //}
    super.addMetadata();
  }

}
