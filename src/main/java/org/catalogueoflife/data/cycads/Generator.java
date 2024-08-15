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
package org.catalogueoflife.data.cycads;

import life.catalogue.api.model.Identifier;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.api.vocab.NomStatus;
import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.parser.NameParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.catalogueoflife.data.AbstractXlsSrcGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.AltIdBuilder;
import org.catalogueoflife.data.utils.RemarksBuilder;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

/**
 * For manual use only !
 * Sources are not published online
 */
public class Generator extends AbstractXlsSrcGenerator {
  private TermWriter dWriter;
  private TermWriter vWriter;
  private TermWriter tmWriter;
  private TermWriter relWriter;

  enum SRCFILE {FAMILY, GENUS, SPECIES}
  // to be updated manually to current version !!!
  private static final String ISSUED = "2024-07-11";
  private static final File sourceFileDir = new File("/Users/markus/Downloads/World List of Cycads_ver20240711");
  private static final Map<SRCFILE, File> sourceFiles = Map.of(
          SRCFILE.FAMILY,  new File(sourceFileDir, "Families_Export20240712_110714.xlsx"),
          SRCFILE.GENUS,   new File(sourceFileDir, "Genera_Export20240712_110746.xlsx"),
          SRCFILE.SPECIES, new File(sourceFileDir, "World List of Cycads Export20240711_190713.xlsx")
  );

  final Map<String, Integer> refs = new HashMap<>();
  int refID = 1;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, false);
  }

  Integer toReferenceID(String citation, String author, String year, String... links) throws IOException, InterruptedException {
    if (StringUtils.isBlank(citation)) return null;

    var sb = new StringBuilder();
    if (author != null) {
      var pa = NameParser.PARSER.parseAuthorship(author);
      if (pa.isPresent()) {
        var comb = pa.get().getCombinationAuthorship();
        if (comb != null && comb.exists()) {
          comb.setYear(null);
          comb.setExAuthors(List.of());
          author = comb.toString();
          sb.append(author);
          sb.append(" ");
        }
      }
    }
    if (year != null) {
      sb.append("(");
      sb.append(year);
      sb.append("). ");
    }
    sb.append("In: ");
    sb.append(citation);

    var norm = sb.toString().toLowerCase()
            .replaceAll("[.,:]", " ")
            .replaceAll("\\s+", " ")
            .trim();

    if (refs.containsKey(norm)) {
      return refs.get(norm);
    }
    int rid = refID++;
    refs.put(norm, rid);

    refWriter.set(ColdpTerm.ID, rid);
    refWriter.set(ColdpTerm.citation, sb.toString());
    refWriter.set(ColdpTerm.issued, year);
    refWriter.set(ColdpTerm.link, ObjectUtils.coalesce(links));
    refWriter.next();
    return rid;
  }

  @Override
  protected void prepare() throws IOException {
    super.prepare();
    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
            ColdpTerm.ID,
            ColdpTerm.parentID,
            ColdpTerm.basionymID,
            ColdpTerm.rank,
            ColdpTerm.status,
            ColdpTerm.nameStatus,
            ColdpTerm.scientificName,
            ColdpTerm.authorship,
            ColdpTerm.genericName,
            ColdpTerm.specificEpithet,
            ColdpTerm.infraspecificEpithet,
            ColdpTerm.order,
            ColdpTerm.referenceID,
            ColdpTerm.nameReferenceID,
            ColdpTerm.publishedInYear,
            ColdpTerm.publishedInPageLink,
            ColdpTerm.etymology,
            ColdpTerm.alternativeID,
            ColdpTerm.link,
            ColdpTerm.modified,
            ColdpTerm.remarks
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
            ColdpTerm.ID,
            ColdpTerm.citation,
            ColdpTerm.issued,
            ColdpTerm.link
    ));
    dWriter = additionalWriter(ColdpTerm.Distribution, List.of(
            ColdpTerm.taxonID,
            ColdpTerm.gazetteer,
            ColdpTerm.area
    ));
    vWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
            ColdpTerm.taxonID,
            ColdpTerm.language,
            ColdpTerm.name
    ));
    tmWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
            ColdpTerm.nameID,
            ColdpTerm.institutionCode,
            ColdpTerm.catalogNumber,
            ColdpTerm.collector,
            ColdpTerm.date,
            ColdpTerm.locality,
            ColdpTerm.altitude,
            ColdpTerm.latitude,
            ColdpTerm.longitude,
            ColdpTerm.status,
            ColdpTerm.remarks
    ));
    relWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
            ColdpTerm.nameID,
            ColdpTerm.relatedNameID,
            ColdpTerm.type,
            ColdpTerm.remarks
    ));
  }

  @Override
  protected void addData() throws Exception {
    Map<String, String> typeNames = new HashMap<>(); // genus/species name -> taxon ID

    // FAMILIES
    Iterator<Row> iter = iterSheet(SRCFILE.FAMILY);
    while (iter.hasNext()) {
      Row row = iter.next();
      var remarks = new RemarksBuilder();

      String ID = col(row, 3);
      writer.set(ColdpTerm.ID, ID);
      writer.set(ColdpTerm.rank, "family");
      writer.set(ColdpTerm.status, col(row, 4));
      writer.set(ColdpTerm.parentID, col(row, 5));
      remarks.append(col(row, 9));
      writer.set(ColdpTerm.etymology, col(row, 11));
      writer.set(ColdpTerm.order, col(row, 12));
      writer.set(ColdpTerm.scientificName, extractName(row, 13));
      String author = col(row, 14);
      writer.set(ColdpTerm.authorship, author);
      writer.set(ColdpTerm.nameReferenceID, toReferenceID(col(row, 15), author, col(row, 16)));
      writer.set(ColdpTerm.publishedInYear, col(row, 16));

      var typeGen = col(row, 17);
      if (!StringUtils.isBlank(typeGen)) {
        typeNames.put(typeGen, ID);
      }

      remarks.append(col(row, 18));
      writer.set(ColdpTerm.modified, col(row, 20));
      writer.set(ColdpTerm.remarks, remarks.toString());
      writer.next();
    }

    // GENERA
    iter = iterSheet(SRCFILE.GENUS);
    while (iter.hasNext()) {
      Row row = iter.next();
      var remarks = new RemarksBuilder();

      String ID = col(row, 2);
      writer.set(ColdpTerm.ID, ID);
      String pid = col(row, 4); // for syns only
      if (pid == null) {
        pid = col(row, 3); // family ID
      }
      writer.set(ColdpTerm.parentID, pid);
      writer.set(ColdpTerm.rank, "genus");
      writer.set(ColdpTerm.status, col(row, 5));

      NomStatus nomStat = null;
      switch (StringUtils.stripToEmpty(col(row, 10))) {
        case "leg":
          nomStat = NomStatus.ACCEPTABLE;
          break;
        case "con":
          nomStat = NomStatus.CONSERVED;
          break;
        default:
          if (!StringUtils.isBlank(col(row, 9))) {
            nomStat = NomStatus.NOT_ESTABLISHED;
          }
      }
      writer.set(ColdpTerm.nameStatus, nomStat);
      remarks.append(col(row, 11));
      writer.set(ColdpTerm.etymology, col(row, 12));
      remarks.append(col(row, 13));

      var typeName = col(row, 14);
      if (!StringUtils.isBlank(typeName)) {
        typeNames.put(typeName, ID);
      }
      String name = extractName(row, 17);
      String author = StringUtils.trimToNull(col(row, 18));
      writer.set(ColdpTerm.scientificName, name);
      writer.set(ColdpTerm.authorship, author);

      writer.set(ColdpTerm.nameReferenceID, toReferenceID(col(row, 19), author, col(row, 20)));
      writer.set(ColdpTerm.publishedInYear, col(row, 20));
      remarks.append(col(row, 21));

      writer.set(ColdpTerm.remarks, remarks.toString());
      writer.set(ColdpTerm.modified, col(row, 23));
      writer.next();

      // resolve type genera - type genera are given without authors in families
      if (typeNames.containsKey(name)) {
        relWriter.set(ColdpTerm.nameID, typeNames.get(name));
        relWriter.set(ColdpTerm.relatedNameID, ID);
        relWriter.set(ColdpTerm.type, "TYPE");
        relWriter.next();
      }
    }

    // SPECIES & BELOW
    // we need ids of all species to wire up infraspecifics
    // go through all rows once to look them up and then do the real writing
    Map<String, String> speciesIDs = new HashMap<>();
    iter = iterSheet(SRCFILE.SPECIES);
    while (iter.hasNext()) {
      Row row = iter.next();
      String ID = col(row, 5);
      String name = extractName(row, 8);
      if (name != null && ID != null) {
        speciesIDs.put(name, ID);
      }
    }

    // now do the real thing
    iter = iterSheet(SRCFILE.SPECIES);
    while (iter.hasNext()) {
      Row row = iter.next();
      var remarks = new RemarksBuilder();

      String ID = col(row, 5);
      String SPNUM = col(row, 6);
      writer.set(ColdpTerm.ID, ID);
      String genusID = col(row, 4);

      String name = extractName(row, 8);

      writer.set(ColdpTerm.scientificName, name);
      writer.set(ColdpTerm.genericName, col(row, 10));
      writer.set(ColdpTerm.specificEpithet, col(row, 11));
      String author = StringUtils.trimToNull(col(row, 12)); // species author

      Rank rank = Rank.SPECIES;
      var infra = col(row, 13); // subspecies
      if (infra != null) {
        rank = Rank.SUBSPECIES;
        writer.set(ColdpTerm.infraspecificEpithet, infra);
        author = col(row, 14);
      }

      infra = col(row, 15); // variety
      if (infra != null) {
        rank = Rank.VARIETY;
        writer.set(ColdpTerm.infraspecificEpithet, infra);
        author = col(row, 16);
      }

      infra = col(row, 17); // form
      if (infra != null) {
        rank = Rank.FORM;
        writer.set(ColdpTerm.infraspecificEpithet, infra);
        author = col(row, 18);
      }

      writer.set(ColdpTerm.rank, rank);
      writer.set(ColdpTerm.authorship, author);

      var ids = new AltIdBuilder();
      ids.add(Identifier.Scope.IPNI, col(row, 19));
      ids.add(Identifier.Scope.WFO, col(row, 21));
      ids.add("tropicos", col(row, 22));

      var dist = col(row, 23);
      if (dist != null) {
        dWriter.set(ColdpTerm.taxonID, ID);
        dWriter.set(ColdpTerm.area, dist);
        dWriter.set(ColdpTerm.gazetteer, "text");
        dWriter.next();
      }

      // 26 citation, 27 collation, 28 year, 29 month, 30 day, 31 url, 33 bhl url, 34 jstor url,
      // 35 pub doc pdf "proto/Zamia_skinneri.pdf", 37 pub license, 38 shareable "yes", 39 license

      String pdf = null;
      if (Objects.equals(col(row,38), "yes") && col(row,35) != null) {
        // shareable pdf
        pdf = "http://cycadlist.org/" + col(row,35);
      }
      writer.set(ColdpTerm.nameReferenceID, toReferenceID(col(row, 26), author, col(row, 28)
              ,pdf, col(row, 31), col(row, 34), col(row, 33)));
      writer.set(ColdpTerm.publishedInPageLink, col(row, 33));
      writer.set(ColdpTerm.publishedInYear, col(row, 20));

      writer.set(ColdpTerm.etymology, col(row, 40));

      var vname = col(row, 42);
      if (vname != null) {
        vWriter.set(ColdpTerm.taxonID, ID);
        vWriter.set(ColdpTerm.name, vname);
        vWriter.set(ColdpTerm.language, "eng");
        vWriter.next();
      }

      remarks.append(col(row, 43));
      // type material or basionym
      var tstat = col(row, 55);
      if (tstat != null) {
        if (!tstat.equalsIgnoreCase("BAS")) {
          var code = col(row, 46);
          if (code != null) {
            var typeRemarks = new RemarksBuilder();
            tmWriter.set(ColdpTerm.nameID, ID);
            tmWriter.set(ColdpTerm.institutionCode, code);
            tmWriter.set(ColdpTerm.catalogNumber, col(row, 45));
            tmWriter.set(ColdpTerm.collector, col(row, 44));
            tmWriter.set(ColdpTerm.date, col(row, 47));
            tmWriter.set(ColdpTerm.locality, col(row, 48));
            tmWriter.set(ColdpTerm.altitude, col(row, 50));
            tmWriter.set(ColdpTerm.latitude, col(row, 51));
            tmWriter.set(ColdpTerm.longitude, col(row, 52));
            typeRemarks.append(col(row, 53));
            typeRemarks.append(col(row, 57));
            tmWriter.set(ColdpTerm.remarks, typeRemarks.toString());
            tmWriter.set(ColdpTerm.status, tstat);
            tmWriter.next();
          }
        }
      }

      NomStatus nomStat = null;
      TaxonomicStatus stat = null;

      // 58 main status
      // 59=Invalid, 60=Legitimacy, 62=AcceptedSpeciesId
      var status = StringUtils.stripToEmpty(col(row, 58));
      var invalid = StringUtils.stripToEmpty(col(row, 59));
      var legitim = StringUtils.stripToEmpty(col(row, 60));
      var pid = col(row, 65); // SynOf
      if (pid != null) {
        stat = TaxonomicStatus.SYNONYM;
      } else {
        stat = TaxonomicStatus.PROVISIONALLY_ACCEPTED;
      }

      if (status.equalsIgnoreCase("Accepted")) {
        stat = TaxonomicStatus.ACCEPTED;
        pid = genusID;
        if (rank.isInfraspecific()) {
          Pattern INFRASPEC = Pattern.compile("^([A-Za-z]+ [a-z]+) ");
          var match = INFRASPEC.matcher(name);
          if (match.find()) {
            String species = match.group(1);
            if (speciesIDs.containsKey(species)) {
              pid = speciesIDs.get(species);
            } else {
              LOG.warn("Cannot find species {} for infraspecies {}", species, name);
            }
          } else {
            LOG.warn("Cannot extract species from name {}", name);
          }
        }
      } else if (status.equalsIgnoreCase("Synonyms")) {
        stat = TaxonomicStatus.SYNONYM;
        // pid is good already
      } else {
        if (pid == null) {
          stat = TaxonomicStatus.BARE_NAME;
        }
      }

      if (status.equalsIgnoreCase("Nomen dubium")) {
        nomStat = NomStatus.DOUBTFUL;
      } else if (status.equalsIgnoreCase("Invalid") || invalid.equalsIgnoreCase("inv")) {
        nomStat = NomStatus.NOT_ESTABLISHED;
      } else if (legitim.equalsIgnoreCase("Illegitimate")) {
        nomStat = NomStatus.UNACCEPTABLE;
      }

      writer.set(ColdpTerm.status, stat);
      writer.set(ColdpTerm.nameStatus, nomStat);
      writer.set(ColdpTerm.parentID, pid);
      writer.set(ColdpTerm.basionymID, col(row, 67));
      remarks.append(col(row, 68));

      ids.add("iucn", col(row, 70));
      writer.set(ColdpTerm.alternativeID, ids.toString());
      if (SPNUM != null) {
        writer.set(ColdpTerm.link, "http://cycadlist.org/taxon.php?Taxon_ID=" + SPNUM);
      }
      remarks.append(col(row, 75));
      writer.set(ColdpTerm.remarks, remarks.toString());
      writer.set(ColdpTerm.modified, col(row, 79));
      writer.next();

      // resolve types- type species are given with authors in genera!
      var fullname = author == null ? name : name + " " + author;
      if (typeNames.containsKey(fullname)) {
        relWriter.set(ColdpTerm.nameID, typeNames.get(fullname));
        relWriter.set(ColdpTerm.relatedNameID, ID);
        relWriter.set(ColdpTerm.type, "TYPE");
        relWriter.next();
      }
    }
  }

  private String extractName(Row row, int column) {
    String name = StringUtils.trimToNull(col(row, column));
    if (name != null) {
      // cleanup trailing footnotes e.g. Zamia baraquiniana [1]
      Pattern FOOTNOTE = Pattern.compile(" *\\[\\d\\] *$");
      var m = FOOTNOTE.matcher(name);
      if (m.find()) {
        name = m.replaceAll("");
      }
    }
    return name;
  }

  Iterator<Row> iterSheet(SRCFILE src) throws IOException {
    prepareWB(sourceFiles.get(src));
    Sheet sheet = wb.getSheetAt(0);
    int rows = sheet.getPhysicalNumberOfRows();
    LOG.info("{} rows found in {} sheet", rows, src);

    Iterator<Row> iter = sheet.rowIterator();
    // skip header row
    iter.next();
    return iter;
  }


    @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", ISSUED);
    super.addMetadata();
  }

}
