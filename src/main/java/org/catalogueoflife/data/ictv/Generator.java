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
import life.catalogue.api.model.DOI;
import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.catalogueoflife.data.AbstractXlsSrcGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.HttpUtils;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Converts the ICTV Master Species List (MSL) spreadsheet into ColDP.
 * The spreadsheet provides the species with their official, stable ICTV identifiers and the full classification,
 * but no identifiers for the higher taxa. These and all previous names of a taxon are taken from the
 * ICTV ontology of the EVORA project, served by the EBI Ontology Lookup Service.
 */
public class Generator extends AbstractXlsSrcGenerator {
  // https://ictv.global/msl
  private static final URI DOWNLOAD = URI.create("https://ictv.global/msl/current");
  private static final String ID_LINK = "https://ictv.global/id/";
  private static final int OLS_PAGE_SIZE = 1000;
  // ICTV ontology, https://github.com/EVORA-project/ictv-ontology
  private static final DOI ONTOLOGY = new DOI("10.1093/gigascience/giag089");
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
  private static final String SHEET_NAME = "MSL";
  // column headers of the MSL sheet, resolved to indices at runtime as the column layout changes between releases
  private static final String COL_SORT = "Sort";
  private static final String COL_SPECIES = "Species";
  private static final String COL_ID = "ICTV_ID";
  private static final String COL_GENOME = "Genome";
  private static final String COL_CHANGE = "Last Change";
  private static final String COL_CHANGE_MSL = "MSL of Last Change";
  private static final Pattern ICTV_ID = Pattern.compile("(ICTV\\d+)");
  private static final Pattern MSL = Pattern.compile("MSL(\\d+)");
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

  private final String rootID = "root";
  private final Set<String> ids = new HashSet<>();
  // emitted usages with an official ICTV id and their rank
  private final Map<String, Rank> ictvUsages = new LinkedHashMap<>();
  private Map<String, Integer> columns;
  private String msl; // base release, e.g. MSL41
  private DOI mslDoi; // Zenodo DOI of the MSL file
  // ontology taxa by ICTV id and by rank|name
  private final Map<String, IctvOntology.Taxon> ontologyById = new HashMap<>();
  private final Map<String, IctvOntology.Taxon> ontologyByName = new HashMap<>();
  private int missingIds = 0;
  // the EBI OLS breaks off large responses over http/2
  private final HttpUtils olsHttp = new HttpUtils(HttpClient.Version.HTTP_1_1);

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, DOWNLOAD);
  }

  void extractMetadata() throws IOException {
    // extract metadata
    String version = null;
    String date = null;
    Sheet sheet = wb.getSheetAt(MD_SHEET_IDX);
    Iterator<Row> iter = sheet.rowIterator();
    while (iter.hasNext()) {
      Row row = iter.next();
      if (row.getRowNum() >= 60) break;
      String x = col(row, MD_COL_IDX);
      if (x != null) {
        if (msl == null) {
          var m = MSL.matcher(x);
          if (m.find()) {
            msl = m.group(0);
          }
        }
        if (x.startsWith("Version")) {
          version = msl + ".v" + col(row, MD_COL_IDX+1);
        } else if (x.startsWith("Date")) {
          date = isoDate(row.getCell(MD_COL_IDX+1));
          if (date == null) {
            date = col(row, MD_COL_IDX+1);
          }
        } else if (x.startsWith("DOI")) {
          mslDoi = DOI.parse(col(row, MD_COL_IDX+1)).orElse(null);
        }
      }
    }
    if (msl == null || version == null || date == null) {
      throw new IllegalStateException("Unable to find MSL, version or date metadata");
    }
    metadata.put("issued", date);
    metadata.put("version", version);
  }

  private static String isoDate(Cell cell) {
    if (cell != null && cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
      return cell.getLocalDateTimeCellValue().toLocalDate().toString();
    }
    return null;
  }

  @Override
  protected void addData() throws Exception {
    extractMetadata();
    loadOntology();

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
    var sheet = wb.getSheet(SHEET_NAME);
    if (sheet == null) {
      throw new IllegalStateException("No sheet named " + SHEET_NAME + " found");
    }
    int rows = sheet.getPhysicalNumberOfRows();
    LOG.info("{} rows found in excel sheet", rows);

    // first add a single root
    addUsageRecord(rootID, null, null, "Viruses", null, TaxonomicStatus.ACCEPTED);

    var iter = sheet.rowIterator();
    while (iter.hasNext()) {
      Row row = iter.next();
      if (columns == null) {
        columns = readHeader(row);
        continue;
      }

      final String species = col(row, COL_SPECIES);
      if (Strings.isNullOrEmpty(species)) continue;
      final Integer sort = colInt(row, column(COL_SORT));

      String parentID = writeClassification(row, sort);
      // finally the species record
      String id = ictvID(row);
      if (id == null) {
        LOG.warn("No ICTV identifier found for species {}", species);
        missingIds++;
        id = genID(Rank.SPECIES, species);
      }
      writer.set(ColdpTerm.remarks, remarks(row));
      addUsageRecord(id, parentID, Rank.SPECIES, species, sort, TaxonomicStatus.ACCEPTED);
    }
    if (missingIds > 0) {
      LOG.warn("{} taxa without an official ICTV identifier", missingIds);
    }

    addPreviousNames();
  }

  private Map<String, Integer> readHeader(Row row) {
    Map<String, Integer> header = new HashMap<>();
    for (Cell cell : row) {
      String x = StringUtils.trimToNull(cell.getStringCellValue());
      if (x != null) {
        header.put(x.toLowerCase(), cell.getColumnIndex());
      }
    }
    List<String> required = new ArrayList<>(List.of(COL_SORT, COL_SPECIES, COL_ID, COL_GENOME, COL_CHANGE, COL_CHANGE_MSL));
    CLASSIFICATION.forEach(r -> required.add(rankHeader(r)));
    for (String col : required) {
      if (!header.containsKey(col.toLowerCase())) {
        throw new IllegalStateException("Missing column " + col + " in sheet " + SHEET_NAME);
      }
    }
    return header;
  }

  private static String rankHeader(Rank rank) {
    return StringUtils.capitalize(rank.name().toLowerCase());
  }

  private int column(String header) {
    return columns.get(header.toLowerCase());
  }

  private String col(Row row, String header) {
    return col(row, column(header));
  }

  /**
   * The ICTV_ID column is a HYPERLINK formula with the identifier as its label.
   */
  private String ictvID(Row row) {
    String x = col(row, COL_ID);
    if (x != null && ICTV_ID.matcher(x).matches()) {
      return x;
    }
    Cell cell = row.getCell(column(COL_ID));
    if (cell != null && cell.getCellType() == CellType.FORMULA) {
      var m = ICTV_ID.matcher(cell.getCellFormula());
      if (m.find()) {
        return m.group(1);
      }
    }
    return null;
  }

  private String remarks(Row row) {
    List<String> parts = new ArrayList<>();
    String genome = col(row, COL_GENOME);
    if (genome != null) {
      parts.add(genome);
    }
    // values come with trailing commas, e.g. "New,"
    String change = StringUtils.trimToNull(StringUtils.stripEnd(col(row, COL_CHANGE), ", "));
    if (change != null) {
      String changeMsl = col(row, COL_CHANGE_MSL);
      parts.add("Last change: " + change + (changeMsl == null ? "" : " in MSL" + changeMsl));
    }
    return parts.isEmpty() ? null : String.join("; ", parts);
  }

  /**
   * Loads all taxa of the current MSL release from the ICTV ontology.
   * Falls back to the previous release in case the ontology has not yet been updated.
   */
  private void loadOntology() throws IOException {
    var m = MSL.matcher(msl);
    if (!m.find()) throw new IllegalStateException("Bad MSL release " + msl);
    int release = Integer.parseInt(m.group(1));
    List<IctvOntology.Taxon> taxa = loadOntology("MSL" + release);
    if (taxa.isEmpty()) {
      LOG.warn("ICTV ontology has no data for {}. Use previous release MSL{} instead", msl, release-1);
      taxa = loadOntology("MSL" + (release-1));
    }
    for (var t : taxa) {
      ontologyById.put(t.id(), t);
      ontologyByName.put(t.key(), t);
    }
    LOG.info("Loaded {} taxa from the ICTV ontology", ontologyById.size());
  }

  private List<IctvOntology.Taxon> loadOntology(String release) throws IOException {
    List<IctvOntology.Taxon> taxa = new ArrayList<>();
    int page = 0;
    int total = 1;
    while (page < total) {
      File f = olsPage(release, page);
      if (f == null) break;
      var json = mapper.readTree(f);
      var pageTaxa = IctvOntology.parsePage(json);
      if (page == 0) {
        total = IctvOntology.totalPages(json);
        if (pageTaxa.isEmpty()) {
          // do not cache an empty response, the release might show up later
          f.delete();
          break;
        }
        LOG.info("Loading {} pages of {} taxa from the ICTV ontology", total, release);
      }
      taxa.addAll(pageTaxa);
      page++;
    }
    return taxa;
  }

  private File olsPage(String release, int page) throws IOException {
    File f = sourceFile("ols-" + release + "-p" + page + ".json");
    if (!f.exists()) {
      if (cfg.noDownload) {
        LOG.warn("--no-download set but {} not cached; skipping", f.getName());
        return null;
      }
      olsHttp.download(IctvOntology.releasePage(release, page, OLS_PAGE_SIZE), f);
      crawlDelay(200);
    }
    return f;
  }

  /**
   * Adds all previous names of the emitted taxa known to the ICTV ontology as synonyms.
   */
  private void addPreviousNames() throws IOException {
    int counter = 0;
    for (var e : ictvUsages.entrySet()) {
      var t = ontologyById.get(e.getKey());
      if (t == null) continue;
      for (var prev : t.previous()) {
        String base = e.getKey() + "-" + prev.msl();
        String id = base;
        int suffix = 2;
        while (ids.contains(id)) {
          id = base + "-" + suffix++;
        }
        writer.set(ColdpTerm.remarks, "Previous name, introduced in " + prev.msl());
        addUsageRecord(id, e.getKey(), e.getValue(), prev.name(), null, TaxonomicStatus.SYNONYM);
        counter++;
      }
    }
    LOG.info("Added {} previous names as synonyms", counter);
  }

  private String writeClassification(Row row, Integer sort) throws IOException {
    String parentID = rootID;
    for (Rank rank : CLASSIFICATION) {
      String name = col(row, rankHeader(rank));
      if (!StringUtils.isBlank(name)) {
        String id = higherTaxonID(rank, name);
        if (!ids.contains(id)) {
          addUsageRecord(id, parentID, rank, name, sort, TaxonomicStatus.ACCEPTED);
        }
        parentID = id;
      }
    }
    return parentID;
  }

  private String higherTaxonID(Rank rank, String name) {
    var t = ontologyByName.get(IctvOntology.key(rank.name(), name));
    if (t != null) {
      return t.id();
    }
    String id = genID(rank, name);
    if (!ids.contains(id)) {
      LOG.warn("No ICTV identifier found for {} {}", rank, name);
      missingIds++;
    }
    return id;
  }

  private static String genID(Rank rank, String name) {
    return rank.name().toLowerCase() + ":" + name.toLowerCase().trim().replace(" ", "_");
  }

  private void addUsageRecord(String id, String parentID, Rank rank, String name, Integer sort, TaxonomicStatus status) throws IOException {
    boolean official = ICTV_ID.matcher(id).matches();
    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentID);
    writer.set(ColdpTerm.status, status);
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.code, NomCode.VIRUS.getAcronym());
    writer.set(ColdpTerm.ordinal, sort);
    if (official) {
      writer.set(ColdpTerm.link, ID_LINK + id);
      ictvUsages.put(id, rank);
    }
    writer.next();
    ids.add(id);
  }

  @Override
  protected void addMetadata() throws Exception {
    //   Walker PJ, Siddell SG, Lefkowitz EJ, Mushegian AR, Adriaenssens EM, Alfenas-Zerbini P, Davison AJ, Dempsey DM, Dutilh BE, García ML, Harrach B, Harrison RL, Hendrickson RC, Junglen S, Knowles NJ, Krupovic M, Kuhn JH, Lambert AJ, Łobocka M, Nibert ML, Oksanen HM, Orton RJ, Robertson DL, Rubino L, Sabanadzovic S, Simmonds P, Smith DB, Suzuki N, Van Dooerslaer K, Vandamme AM, Varsani A, Zerbini FM. Changes to virus taxonomy and to the International Code of Virus Classification and Nomenclature ratified by the International Committee on Taxonomy of Viruses (2021). Arch Virol. 2021 Jul 6. doi: 10.1007/s00705-021-05156-1. PMID: 34231026.
    if (mslDoi != null) {
      addSource(mslDoi);
      // Zenodo does not know the MSL release
      if (!sourceCitations.isEmpty() && sourceCitations.getLast().getVersion() == null) {
        sourceCitations.getLast().setVersion((String) metadata.get("version"));
      }
    } else {
      LOG.warn("No DOI found for {}", msl);
    }
    addSource(ONTOLOGY);
    for (DOI doi : SOURCES) {
      addSource(doi);
    }
    super.addMetadata();
  }

}
