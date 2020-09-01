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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import life.catalogue.api.datapackage.ColdpTerm;
import life.catalogue.api.vocab.NomRelType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.catalogueoflife.data.AbstractBuilder;
import org.catalogueoflife.data.BuilderConfig;
import org.catalogueoflife.data.utils.TermWriter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static life.catalogue.api.datapackage.ColdpTerm.*;

public class ArchiveBuilder extends AbstractBuilder {
  // to be updated manually to current version !!!
  // https://talk.ictvonline.org/files/master-species-lists/
  private static final String DOWNLOAD = "http://talk.ictvonline.org/files/master-species-lists/m/msl/9601/download";

  // SPREADSHEET METADATA FORMAT
  private static final int METADATA_SHEET_IDX = 0;
  private static final int METADATA_TITLE_ROW = 1;
  private static final int METADATA_TITLE_COL = 1;
  private static final int METADATA_VERSION_ROW = 4;
  private static final int METADATA_VERSION_COL = 2;

  // SPREADSHEET FORMAT
  private static final int SHEET_IDX = 2;
  private static final int SKIP_ROWS = 1;
  //	Realm	Subrealm	Kingdom	Subkingdom	Phylum	Subphylum	Class	Subclass	Order	Suborder	Family	Subfamily	Genus	Subgenus	Species	Type Species?	Genome Composition	Last Change	MSL of Last Change	Proposal for Last Change 	Taxon History URL
  private static final int COL_CLASSIFICATION_START  = 5;
  private static final List<ColdpTerm> CLASSIFICATION_TERMS = List.of(
      phylum,subphylum,class_,subclass,order,suborder,family,subfamily,genus,subgenus
  );
  private static final int COL_ID = 0;
  private static final int COL_KINGDOM = 3;
  private static final int COL_GENUS   = 13;
  private static final int COL_SPECIES = 15;
  private static final int COL_TYPE_SPECIES = 16;
  private static final int COL_COMPOSITION = 17;
  private static final int COL_LINK = 22;

  public ArchiveBuilder(BuilderConfig cfg) {
    super(cfg);
  }

  private void parseMetadata(Workbook wb) {
    LOG.info("Read metadata from excel sheet");
    Sheet sheet = wb.getSheetAt(METADATA_SHEET_IDX);
    Row row = sheet.getRow(METADATA_TITLE_ROW);
    String title = col(row, METADATA_TITLE_COL);
    metadata.put("title", title);
    Matcher m = Pattern.compile("MSL(\\d+)").matcher(title);
    if (m.find()) {
      metadata.put("version", m.group(1));
    } else {
      throw new IllegalStateException("Unable to extract version from spreadsheet");
    }
    row = sheet.getRow(METADATA_VERSION_ROW);
    metadata.put("released", col(row, METADATA_VERSION_COL));
  }

  protected void parseData() throws Exception {
    LOG.info("Downloading latest data from {}", DOWNLOAD);
    InputStream in = http.getStream(DOWNLOAD);
    Workbook wb = WorkbookFactory.create(in);

    parseMetadata(wb);

    Sheet sheet = wb.getSheetAt(SHEET_IDX);
    int rows = sheet.getPhysicalNumberOfRows();
    LOG.info("{} rows found in excel sheet", rows);

    Set<String> genera = Sets.newHashSet();
    Iterator<Row> iter = sheet.rowIterator();

    try (TermWriter usage = newDataFile(NameUsage, ID,
          scientificName,
          rank,
          kingdom,
          phylum,
          subphylum,
          class_,
          subclass,
          order,
          suborder,
          family,
          subfamily,
          genus,
          subgenus,
          link,
          remarks);
         TermWriter nomrel = newDataFile(NameRelation,
             nameID,
             relatedNameID,
             type)
    ) {
      while (iter.hasNext()) {
        Row row = iter.next();
        if (row.getRowNum()+1 <= SKIP_ROWS) continue;

        String name = col(row, COL_SPECIES);
        if (Strings.isNullOrEmpty(name)) continue;

        usage.set(ID, col(row, COL_ID));
        usage.set(kingdom, col(row, COL_KINGDOM));
        int col = COL_CLASSIFICATION_START;
        for (ColdpTerm t : CLASSIFICATION_TERMS) {
          usage.set(t,  col(row, col++));
        }
        usage.set(scientificName, col(row, COL_SPECIES));
        usage.set(rank, "species");
        usage.set(link, link(row, COL_LINK));
        usage.set(remarks, col(row, COL_COMPOSITION));
        String spId = usage.next();

        boolean isType = toBool(col(row, COL_TYPE_SPECIES));
        final String genus = col(row, COL_GENUS);
        if (isType && !genera.contains(genus)) {
          // there are a few cases when a genus has 2 type species listed :(
          genera.add(genus);

          // also create a genus record with this type species
          usage.set(ID, genus);
          usage.set(kingdom, col(row, COL_KINGDOM));
          usage.set(scientificName, genus);
          usage.set(rank, "genus");
          col = COL_CLASSIFICATION_START;
          for (ColdpTerm t : CLASSIFICATION_TERMS) {
            if (t == ColdpTerm.genus) {
              break;
            }
            usage.set(t,  col(row, col++));
          }
          String genId = usage.next();

          // type species nom rel
          nomrel.set(type, NomRelType.TYPE);
          nomrel.set(nameID, spId);
          nomrel.set(relatedNameID, genId);
          nomrel.next();
        }
      }
    }
  }

  private boolean toBool(String x) {
    return x != null && x.trim().equals("1");
  }

}
