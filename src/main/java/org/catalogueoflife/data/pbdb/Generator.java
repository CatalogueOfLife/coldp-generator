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
package org.catalogueoflife.data.pbdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import life.catalogue.api.model.Agent;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.RemarksBuilder;
import org.catalogueoflife.data.utils.bibjson.BibRef;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;

/**
 * http://paleobiodb.org
 */
public class Generator extends AbstractColdpGenerator {
  private static final String API = "https://paleobiodb.org/data1.2";
  private static final String taxaFN = "taxa.json";
  private static final String rankFN = "ranks.csv";

  private static final String refFN = "references.json";
  private TermWriter authorWriter;
  private TermWriter factWriter;
  private TermWriter vernacularWriter;
  private TermWriter nomRelWriter;
  private TermWriter materialWriter;
  final Pattern NOT_SPECIFIED = Pattern.compile("NO_[A-Z]+_SPECIFIED");
  final Set<String> people = new HashSet<>();
  final TypeReference<Wrapper<Person>> personTYPE = new TypeReference<>() {};

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, Map.of(
            taxaFN, URI.create(API + "/taxa/list.json?all_taxa=true&show=attr,app,common,parent,immparent,classext,ecospace,ttaph,nav,img,ref,refattr,ent,entname,crmod"),
            rankFN, URI.create(API + "/config.txt?show=ranks"),
            //taxaFN, URI.create(API + "/opinions/list.json?all_taxa=true"),
            refFN, URI.create(API + "/refs/list.json?vocab=bibjson&all_records=true")
    ));
  }

  void loadPerson(String id, String name) throws IOException {
    if (!people.contains(id)) {
      // https://paleobiodb.org/data1.2/people/single.json?id=prs:18
      authorWriter.set(ColdpTerm.ID, id);
      authorWriter.set(ColdpTerm.family, name);
      try {
        String json = http.getJSON(URI.create(API + "/people/single.json?id=" + id));
        var p = mapper.readValue(json, personTYPE).records.get(0);
        LOG.info("Load person {} --> {}", id, p.getNam());
        if (p.getNam() != null) {
          name = p.getNam();
        }
        authorWriter.set(ColdpTerm.country, p.getCtr());
        authorWriter.set(ColdpTerm.affiliation, p.getIst());
        if (p.getOrc() != null) {
          authorWriter.set(ColdpTerm.alternativeID, "orcid:" + p.getOrc());
        }
      } catch (IOException e) {
        LOG.warn("Person {} not found. Use {}", id, name, e);

      } finally {
        var ag = Agent.parse(name);
        if (ag.getFamily() != null && ag.getGiven() != null) {
          authorWriter.set(ColdpTerm.family, ag.getFamily());
          authorWriter.set(ColdpTerm.given, ag.getGiven());
        }
        authorWriter.next();
        people.add(id);

      }
    }
  }

  @Override
  protected void addData() throws Exception {
    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.basionymID,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.nameStatus,
      ColdpTerm.status,
      ColdpTerm.extinct,
      ColdpTerm.environment,
      ColdpTerm.temporalRangeStart,
      ColdpTerm.temporalRangeEnd,
      ColdpTerm.referenceID,
      ColdpTerm.kingdom,
      ColdpTerm.phylum,
      ColdpTerm.class_,
      ColdpTerm.order,
      ColdpTerm.family,
      ColdpTerm.genus,
      ColdpTerm.link,
      ColdpTerm.scrutinizer,
      ColdpTerm.scrutinizerID,
      ColdpTerm.modified,
      ColdpTerm.modifiedBy,
      ColdpTerm.remarks
    ));

    nomRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
      ColdpTerm.nameID,
      ColdpTerm.relatedNameID,
      ColdpTerm.type
    ));

    authorWriter = additionalWriter(ColdpTerm.Author, List.of(
            ColdpTerm.ID,
            ColdpTerm.alternativeID,
            ColdpTerm.given,
            ColdpTerm.family,
            ColdpTerm.affiliation,
            ColdpTerm.country
    ));

    factWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
            ColdpTerm.taxonID,
            ColdpTerm.property,
            ColdpTerm.value
    ));

    vernacularWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
            ColdpTerm.taxonID,
            ColdpTerm.name,
            ColdpTerm.language
    ));

    materialWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
            ColdpTerm.ID,
            ColdpTerm.nameID,
            ColdpTerm.citation,
            ColdpTerm.status,
            ColdpTerm.country,
            ColdpTerm.locality,
            ColdpTerm.latitude,
            ColdpTerm.longitude,
            ColdpTerm.catalogNumber,
            ColdpTerm.institutionCode,
            ColdpTerm.collector
    ));

    parseDataFiles();
  }

  void parseDataFiles() throws IOException {
    TypeReference<Wrapper<BibRef>> refTYPE = new TypeReference<>() {};
    try (var in = UTF8IoUtils.readerFromFile(sourceFile(refFN))) {
      var res = mapper.readValue(in, refTYPE);
      LOG.info("{} references discovered", res.records.size());
      boolean first = true;
      try (var writer = UTF8IoUtils.writerFromFile(new File(dir,"reference.json"))) {
        writer.append('[');
        for (var bref : res.records) {
          if (!first) {
            writer.append(',');
            writer.append('\n');
          }
          mapper.writeValue(writer, bref.toCSL());
          first = false;
        }
        writer.append(']');
      }
    }

    // load rank vocab
    // https://dev.paleobiodb.org/data1.2/config.txt?show=ranks
    final Map<Integer, String> ranks = new HashMap<>();
    try (var in = UTF8IoUtils.readerFromFile(sourceFile(rankFN))) {
      var settings = new CsvParserSettings();
      settings.setMaxCharsPerColumn(1024);
      CsvParser parser = new CsvParser(settings);

      boolean first = true;
      IterableResult<String[], ParsingContext> it = parser.iterate(in);
      for (var row : it) {
        if (first) {
          first = false;
          continue; // skip header row
        }
        // "config_section","taxonomic_rank","rank_code"
        ranks.put(Integer.parseInt(row[2]), row[1]);
      }
      LOG.info("{} ranks found in vocabulary", ranks.size());
    }


    TypeReference<Wrapper<Taxon>> taxTYPE = new TypeReference<>() {};
    try (var in = UTF8IoUtils.readerFromFile(sourceFile(taxaFN))) {
      var res = mapper.readValue(in, taxTYPE);
      LOG.info("{} taxa discovered", res.records.size());
      for (var t : res.records) {
        var remarks = new RemarksBuilder();
        writer.set(ColdpTerm.ID, t.getOid());
        if (t.getRnk() != null) {
          writer.set(ColdpTerm.rank, ranks.get(t.getRnk()));
        }
        writer.set(ColdpTerm.scientificName, t.getNam());
        writer.set(ColdpTerm.authorship, t.getAtt());
        writer.set(ColdpTerm.referenceID, t.getRid());
        writer.set(ColdpTerm.environment, t.getJev());
        if (t.isExtant() != null) {
          // True if this taxon is extant on earth today, false if not, not present if unrecorded
          writer.set(ColdpTerm.extinct, !t.isExtant());
        }

        if (t.getAcc() != null && !t.getAcc().equals(t.getOid())) {
          writer.set(ColdpTerm.parentID, t.getAcc());
          writer.set(ColdpTerm.status, "synonym");
          writer.set(ColdpTerm.nameStatus, t.getTdf());
          remarks.append(t.getTdf());

        } else {
          writer.set(ColdpTerm.parentID, t.getPar());
        }

        writeHigherRank(ColdpTerm.kingdom, t.getKgl());
        writeHigherRank(ColdpTerm.phylum, t.getPhl());
        writeHigherRank(ColdpTerm.class_, t.getCll());
        writeHigherRank(ColdpTerm.order, t.getOdl());
        writeHigherRank(ColdpTerm.family, t.getFml());
        writeHigherRank(ColdpTerm.genus, t.getGnl());

        writer.set(ColdpTerm.scrutinizer, t.getEnt());
        if (t.getEni() != null) {
          loadPerson(t.getEni(), t.getEnt());
          writer.set(ColdpTerm.scrutinizerID, t.getEni());
        }
        writer.set(ColdpTerm.modified, t.getDmd());
        if (t.getMdi() != null) {
          loadPerson(t.getMdi(), t.getMdf());
          writer.set(ColdpTerm.modifiedBy, t.getMdi());
        }

        //TODO: geological times
        //Tyrannosaursu: "fea":83.6,"fla":66,   "lea":83.6,"lla":66,
        writer.set(ColdpTerm.temporalRangeStart, millionYears(t.getFea()));
        writer.set(ColdpTerm.temporalRangeEnd, millionYears(t.getLla()));


        if (!remarks.isEmpty()) {
          writer.set(ColdpTerm.remarks, remarks.toString());
        }
        writer.next();

        // types
        if (t.getTtn() != null) {
          nomRelWriter.set(ColdpTerm.nameID, t.getOid());
          nomRelWriter.set(ColdpTerm.type, "TYPE");
          nomRelWriter.set(ColdpTerm.relatedNameID, t.getTtn());
          nomRelWriter.next();
        }

        // common names
        if (t.getNm2() != null) {
          vernacularWriter.set(ColdpTerm.taxonID, t.getOid());
          vernacularWriter.set(ColdpTerm.name, t.getNm2());
          vernacularWriter.set(ColdpTerm.language, "eng");
          vernacularWriter.next();
        }

        // property values
        writeFact(t.getOid(), "motility", t.getJmo());
        writeFact(t.getOid(), "life habit", t.getJlh());
        writeFact(t.getOid(), "vision", t.getJvs());
        writeFact(t.getOid(), "diet", t.getJdt());
        writeFact(t.getOid(), "reproduction", t.getJre());
        writeFact(t.getOid(), "ontogeny", t.getJon());
        writeFact(t.getOid(), "composition", t.getJco());
      }
    }
    writer.close();
    vernacularWriter.close();
    factWriter.close();
  }

  private static String millionYears(Float fea) {
    return fea == null ? null : fea + " Ma";
  }


  private void writeHigherRank(ColdpTerm term, String value) throws IOException {
    if (value != null && !NOT_SPECIFIED.matcher(value).find()) {
      writer.set(term, value);
    }
  }

  private void writeFact(String taxID, String prop, String value) throws IOException {
    if (taxID != null && !StringUtils.isBlank(value)) {
      factWriter.set(ColdpTerm.taxonID, taxID);
      factWriter.set(ColdpTerm.property, prop);
      factWriter.set(ColdpTerm.value, value);
      factWriter.next();
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

}
