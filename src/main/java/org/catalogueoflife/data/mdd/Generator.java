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
package org.catalogueoflife.data.mdd;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import life.catalogue.api.model.DOI;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.api.vocab.Gazetteer;
import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.CompressionUtil;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class Generator extends AbstractColdpGenerator {
  private static final URI DOWNLOAD = URI.create("https://github.com/mammaldiversity/mammaldiversity.github.io/raw/refs/heads/master/assets/data/MDD.zip");

  private File fMeta;
  private File fSpecies;
  private File fSyn;
  private File fTypes;
  private int refID = 0;
  private LoadingCache<String, Integer> refCache = Caffeine.newBuilder()
          .maximumSize(10000)
          .build(this::refLookup);

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, Map.of("mdd.zip", DOWNLOAD));
  }

  @Override
  protected void prepare() throws Exception {
    /**
     * Figure our latest filenames:
     *
     * META_v2.1.csv
     * MDD_v2.1_6801species.csv
     * Species_Syn_v2.1.csv
     * TypeSpecimenMetadata_v2.1.csv
     */

    File dir = new File(sources, "mdd");
    CompressionUtil.decompressFile(dir, sourceFile("mdd.zip"));
    for (var f : dir.listFiles()) {
      if (f.isFile() && f.getName().endsWith(".csv")) {
        if (f.getName().startsWith("META_")) {
          fMeta = f;
        } else if (f.getName().startsWith("MDD_")) {
          fSpecies = f;
        } else if (f.getName().startsWith("Species_Syn")) {
          fSyn = f;
        } else if (f.getName().startsWith("TypeSpecimen")) {
          fTypes = f;
        }
      }
    }
    Preconditions.checkNotNull(fMeta, "Missing file " + fMeta);
    Preconditions.checkNotNull(fSpecies, "Missing file " + fSpecies);
    Preconditions.checkNotNull(fSyn, "Missing file " + fSyn);
    Preconditions.checkNotNull(fTypes, "Missing file " + fTypes);
  }

  private @Nullable Integer refLookup(String citation) {
    if (citation == null) return null;

    try {
      refWriter.set(ColdpTerm.ID, ++refID);
      refWriter.set(ColdpTerm.citation, citation);
      refWriter.next();
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
    return refID;
  }

  @Override
  protected void addData() throws Exception {
    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.status,
      ColdpTerm.nameStatus,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.ordinal,

      ColdpTerm.class_,
      ColdpTerm.subclass,
      ColdpTerm.order,
      ColdpTerm.suborder,
      ColdpTerm.superfamily,
      ColdpTerm.family,
      ColdpTerm.subfamily,
      ColdpTerm.tribe,
      ColdpTerm.genus,
      ColdpTerm.subgenus,

      ColdpTerm.specificEpithet,
      ColdpTerm.basionymAuthorship,
      ColdpTerm.basionymAuthorshipYear,
      ColdpTerm.combinationAuthorship,
      ColdpTerm.combinationAuthorshipYear,
      ColdpTerm.nameReferenceID,

      ColdpTerm.extinct,
      ColdpTerm.link,
      ColdpTerm.remarks
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
    var dWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.gazetteer,
        ColdpTerm.area,
        ColdpTerm.areaID
    ));
    var tWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.ID,
        ColdpTerm.nameID,
        ColdpTerm.status,
        ColdpTerm.citation,
        ColdpTerm.locality,
        ColdpTerm.latitude,
        ColdpTerm.longitude,
        ColdpTerm.link
    ));

    // create incertae_sedis record to hold unknown ones
    final String incertaeSedisID = "incertae_sedis";
    writer.set(ColdpTerm.ID, incertaeSedisID);
    writer.set(ColdpTerm.rank, "unranked");
    writer.set(ColdpTerm.scientificName, "incertae sedis");
    writer.set(ColdpTerm.status, TaxonomicStatus.PROVISIONALLY_ACCEPTED);
    writer.next();


    var csv = newCsvParser();
    try (var in = UTF8IoUtils.readerFromFile(fSpecies)) {
      IterableResult<String[], ParsingContext> it = csv.iterate(in);
      var iter = it.iterator();
      iter.next(); // header row
      while (iter.hasNext()) {
        var row = iter.next();
        var line = iter.getContext().currentLine();
        //sciName,id,phylosort,mainCommonName,otherCommonNames,subclass,infraclass,magnorder,superorder,order,suborder,infraorder,parvorder,
        //sciNa 0, 1,phyloso 2,mainCommonNa 3,otherCommonNam 4,subcla 5,infracla 6,magnord 7,superord 8,ord 9,subor 10,infraor 11,parvor 12,

        //superfamily,family,subfamily,tribe,genus,subgenus,specificEpithet,authoritySpeciesAuthor,authoritySpeciesYear,authorityParentheses,
        //superfam 13,fam 14,subfam 15,tr 16,ge 17,subge 18,specificEpit 19,authoritySpeciesAut 20,authoritySpeciesY 21,authorityParenthe 22,

        //originalNameCombination,authoritySpeciesCitation,authoritySpeciesLink,typeVoucher,typeKind,typeVoucherURIs,typeLocality,
        //originalNameCombinat 23,authoritySpeciesCitat 24,authoritySpeciesL 25,typeVouc 26,typeK 27,typeVoucherU 28,typeLocal 29,

        final var taxonID = clean(row[1]);
        final var sciname = clean(row[0].replaceAll("_", " "));
        writer.set(ColdpTerm.ID, taxonID);
        writer.set(ColdpTerm.rank, "species");
        writer.set(ColdpTerm.scientificName, sciname);
        writer.set(ColdpTerm.ordinal, clean(row[2]));
        // flat classification - not all exist as coldp terms
        writer.set(ColdpTerm.class_, "Mammalia");
        writer.set(ColdpTerm.subclass, clean(row[5]));
        //writer.set(ColdpTerm.infraclass,clean( row[6]));
        //writer.set(ColdpTerm.magnorder,clean( row[7]));
        //writer.set(ColdpTerm.superorder,clean( row[8]));
        writer.set(ColdpTerm.order, clean(row[9]));
        writer.set(ColdpTerm.suborder, clean(row[10]));
        //writer.set(ColdpTerm.infraorder,clean( row[11]));
        //writer.set(ColdpTerm.parvorder,clean( row[12]));
        writer.set(ColdpTerm.superfamily, clean(row[13]));
        writer.set(ColdpTerm.family, clean(row[14]));
        writer.set(ColdpTerm.subfamily, clean(row[15]));
        writer.set(ColdpTerm.tribe, clean(row[16]));
        writer.set(ColdpTerm.genus, clean(row[17]));
        writer.set(ColdpTerm.subgenus, clean(row[18]));

        writer.set(ColdpTerm.specificEpithet, clean(row[19]));

        writeAuthorship(bool(row[22]), clean(row[20]), clean(row[21]));

        var citation = clean(row[24]);
        if (citation != null) {
          writer.set(ColdpTerm.nameReferenceID, refCache.get(citation));
        }

        writer.set(ColdpTerm.link, clean(row[25]));

        var voucher = clean(row[26]);
        if (voucher != null) {
          tWriter.set(ColdpTerm.ID, voucher);
          tWriter.set(ColdpTerm.nameID, taxonID);
          tWriter.set(ColdpTerm.citation, voucher);
          tWriter.set(ColdpTerm.status, clean(row[27]));
          tWriter.set(ColdpTerm.link, clean(row[28]));
          tWriter.set(ColdpTerm.locality, clean(row[29]));
          tWriter.set(ColdpTerm.latitude, clean(row[30]));
          tWriter.set(ColdpTerm.longitude, clean(row[31]));
          tWriter.next();
        }
        writer.set(ColdpTerm.extinct, bool(row[43]));
        //writer.set(ColdpTerm.nameRemarks, clean(row[32]));
        writer.set(ColdpTerm.remarks, clean(row[34]));
        writer.next();

        //typeLocalityLatitude,typeLocalityLongitude,nominalNames,subspecies,taxonomyNotes,taxonomyNotesCitation,distributionNotes,
        //typeLocalityLatit 30,typeLocalityLongit 31,nominalNa 32,subspec 33,taxonomyNo 34,taxonomyNotesCitat 35,distributionNo 36,

        //distributionNotesCitation,subregionDistribution,countryDistribution,continentDistribution,biogeographicRealm,iucnStatus,
        //distributionNotesCitat 37,subregionDistribut 38,countryDistribut 39,continentDistribut 40,biogeographicRe 41,iucnSta 42,

        // DISTRIBUTION
        var realms = clean(row[41]);
        if (realms != null) {
          for (var r : realms.split("\\|")) {
            dWriter.set(ColdpTerm.taxonID, taxonID);
            dWriter.set(ColdpTerm.gazetteer, "realm");
            dWriter.set(ColdpTerm.area, r);
            dWriter.next();
          }
        }
        var countries = clean(row[39]);
        if (countries != null) {
          for (var c : countries.split("\\|")) {
            dWriter.set(ColdpTerm.taxonID, taxonID);
            dWriter.set(ColdpTerm.gazetteer, Gazetteer.ISO);
            dWriter.set(ColdpTerm.area, c);
            dWriter.next();
          }
        }
        var dnotes = clean(row[36]);
        if (dnotes != null) {
          dWriter.set(ColdpTerm.taxonID, taxonID);
          dWriter.set(ColdpTerm.gazetteer, Gazetteer.TEXT);
          dWriter.set(ColdpTerm.area, dnotes);
          dWriter.next();
        }

        //extinct,domestic,flagged,CMW_sciName,diffSinceCMW,MSW3_matchtype,MSW3_sciName,diffSinceMSW3
        //exti 43,domes 44,flag 45,CMW_sciN 46,diffSince 47,MSW3_matcht 48,MSW3_sciN 49,diffSinceM 50


        // subspecies exist?
        var subspecies = clean(row[32]);
        if (subspecies != null) {
          int sspID = 0;
          for (String ssp : subspecies.split("\\|")) {
            writer.set(ColdpTerm.ID, taxonID+"-"+sspID++);
            writer.set(ColdpTerm.parentID, taxonID);
            writer.set(ColdpTerm.rank, "subspecies");
            writer.set(ColdpTerm.scientificName, sciname + " " + ssp);
            writer.next();
          }
        }

        // VERNACULARS
        List<String> verns = new ArrayList<>();
        verns.add(clean(row[3]));
        var addVerns = clean(row[4]);
        if (addVerns != null) {
          verns.addAll(Arrays.stream(addVerns.split("\\|")).toList());
        }
        if (!verns.isEmpty()) {
          for (String vn : verns) {
            vWriter.set(ColdpTerm.taxonID, taxonID);
            vWriter.set(ColdpTerm.name, vn);
            vWriter.set(ColdpTerm.language, "eng");
            vWriter.next();
          }
        }
      }
    }

    // SYNONYM FILE
    csv = newCsvParser();
    try (var in = UTF8IoUtils.readerFromFile(fSyn)) {
      IterableResult<String[], ParsingContext> it = csv.iterate(in);
      var iter = it.iterator();
      iter.next(); // header row
      while (iter.hasNext()) {
        var row = iter.next();
        var line = iter.getContext().currentLine();

        //MDD_syn_ID,MDD_species,MDD_root_name,MDD_author,MDD_year,MDD_authority_parentheses,MDD_nomenclature_status,MDD_validity,
        //MDD_syn_ 0,MDD_speci 1,MDD_root_na 2,MDD_auth 3,MDD_ye 4,MDD_authority_parenthes 5,MDD_nomenclature_stat 6,MDD_validi 7,

        //MDD_original_combination,MDD_original_rank,MDD_authority_citation,MDD_unchecked_authority_citation,MDD_sourced_unverified_citations,MDD_citation_group,
        //MDD_original_combinati 8,MDD_original_ra 9,MDD_authority_citat 10,MDD_unchecked_authority_citat 11,MDD_sourced_unverified_citati 12,MDD_citation_gr 13,

        //MDD_citation_kind,MDD_authority_page,MDD_authority_link,MDD_authority_page_link,MDD_unchecked_authority_page_link,MDD_old_type_locality,
        //MDD_citation_k 14,MDD_authority_p 15,MDD_authority_l 16,MDD_authority_page_l 17,MDD_unchecked_authority_page_l 18,MDD_old_type_local 19,

        //MDD_original_type_locality,MDD_unchecked_type_locality,MDD_emended_type_locality,MDD_type_latitude,MDD_type_longitude,MDD_type_country,
        //MDD_original_type_local 20,MDD_unchecked_type_local 21,MDD_emended_type_local 22,MDD_type_latit 23,MDD_type_longit 24,MDD_type_coun 25,

        //MDD_type_subregion,MDD_type_subregion2,MDD_holotype,MDD_type_kind,MDD_type_specimen_link,MDD_order,MDD_family,MDD_genus,
        //MDD_type_subreg 26,MDD_type_subregi 27,MDD_holot 28,MDD_type_k 29,MDD_type_specimen_l 30,MDD_or 31,MDD_fam 32,MDD_ge 33,

        //MDD_specificEpithet,MDD_subspecificEpithet,MDD_variant_of,MDD_senior_homonym,MDD_variant_name_citations,Hesp_id,MDD_species_id,MDD_name_usages,MDD_comments
        //MDD_specificEpit 34,MDD_subspecificEpit 35,MDD_variant 36,MDD_senior_homo 37,MDD_variant_name_citati 38,Hesp 39,MDD_species 40,MDD_name_usa 41,MDD_comme 42
        var status = clean(row[7]);
        if (status != null && status.equalsIgnoreCase("species")) {
          System.out.println("Ignore bad species entry in synonyms file: " + line);
          continue;
        }
        var synRank = clean(row[9]);
        if (synRank != null && (
                synRank.equalsIgnoreCase("synonym_species") ||
                synRank.equalsIgnoreCase("informal_species")
        )) {
          System.out.println("Ignore "+synRank+" entry in synonyms file: " + line);
          continue;
        }

        writer.set(ColdpTerm.ID, clean(row[0]));
        writer.set(ColdpTerm.parentID, ObjectUtils.coalesce(clean(row[40]), incertaeSedisID));
        writeAuthorship(bool(row[5]), clean(row[3]), clean(row[4]));
        writer.set(ColdpTerm.nameStatus, clean(row[6]));
        writer.set(ColdpTerm.status, status);

        writer.set(ColdpTerm.scientificName, name(row[8]));
        writer.set(ColdpTerm.rank, synRank);

        var citation = clean(row[10]);
        if (citation != null) {
          writer.set(ColdpTerm.nameReferenceID, refCache.get(citation));
        }
        writer.set(ColdpTerm.remarks, clean(row[42]));

        writer.next();
      }
    }
  }

  private void writeAuthorship(boolean parenthis, String author, String year) {
    if (parenthis) {
      writer.set(ColdpTerm.basionymAuthorship, author);
      writer.set(ColdpTerm.basionymAuthorshipYear, year);
    } else {
      writer.set(ColdpTerm.combinationAuthorship, author);
      writer.set(ColdpTerm.combinationAuthorshipYear, year);
    }
  }
  static String name(String x) {
    x = clean(x);
    if (x != null) {
      // Genus epithet subepithet
      StringBuilder sb = new StringBuilder();
      for (String word : x.split("\\s+")) {
        if (sb.length() > 0) {
          sb.append(' ');
          sb.append(word.trim().toLowerCase());
        } else {
          sb.append(StringUtils.capitalize(word.trim().toLowerCase()));
        }
      }
      return sb.toString();
    }
    return null;
  }
  static String clean(String x) {
    return StringUtils.isBlank(x) || x.equals("NA")? null : x.trim();
  }
  static boolean bool(String x) {
    x = clean(x);
    return x != null && x.equals("1");
  }

  @Override
  protected void addMetadata() throws Exception {
    // find version from mdd file
    String version = null;
    Pattern vPattern = Pattern.compile("_v(\\d+\\.\\d+)");
    var m = vPattern.matcher(fSpecies.getName());
    if (m.find()) {
      version = m.group(1);
    }
    metadata.put("version", version);

    // read META
    Date issued = null;
    var csv = newCsvParser();
    try (var in = UTF8IoUtils.readerFromFile(fMeta)) {
      IterableResult<String[], ParsingContext> it = csv.iterate(in);
      for (var row : it) {
        if (row[0] != null) {
          if (row[0].equals("Tabs")) {
            LOG.warn("Could not find release date");
            break;
          } else {
            var format = new SimpleDateFormat("dd-MMMM-yy");
            issued = format.parse(row[0]);
            break;
          }
        }
      }
    }

    metadata.put("issued", new SimpleDateFormat("yyyy-MM-dd").format(issued));

    addSource(new DOI("https://doi.org/10.1101/2025.02.27.640393"));
    addSource(new DOI("https://doi.org/10.1093/jmammal/gyx147"));
    super.addMetadata();
  }

  private CsvParser newCsvParser() throws IOException {
    var settings = new CsvParserSettings();
    settings.setMaxCharsPerColumn(24000);
    return new CsvParser(settings);
  }
}
