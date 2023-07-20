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
package org.catalogueoflife.data.ipni;

import life.catalogue.api.vocab.NomRelType;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.util.UnicodeUtils;

import java.io.*;
import java.net.URI;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.gbif.dwc.terms.DwcTerm.month;

/**
 * ColDP generator for IPNI using their search dumps at:
 * https://storage.googleapis.com/ipni-data/ipniWebName.csv.xz
 *
 * https://storage.googleapis.com/ipni-data/ipniWebPublications.csv.xz
 * id|version_s_lower|ipni_record_type_s_lower|top_copy_b|suppressed_b|abbreviation_s_lower|title_s_lower|remarks_s_lower|bph_number_s_lower|isbn_s_lower|issn_s_lower|date_s_lower|lc_number_s_lower|preceded_by_s_lower|tl2_author_s_lower|tl2_number_s_lower|tdwg_abbreviation_s_lower|superceded_by_s_lower|sortable
 */
public class Generator extends AbstractColdpGenerator {
  private static final URI DOWNLOAD = URI.create("https://storage.googleapis.com/ipni-data/ipniWebName.csv.xz");
  private static final URI DOWNLOAD_REF = URI.create("https://storage.googleapis.com/ipni-data/ipniWebPublications.csv.xz");

  private static final String LINK_BASE = "https://www.ipni.org";
  static final Pattern PAGES = Pattern.compile("\\s*:\\s*");
  static final Pattern COLLATION = Pattern.compile("^(\\d+)(?:\\s*[(-]\\s*(\\d+(?:\\s*[,-]\\s*\\d+)?)\\s*\\)?)?\\s*$");
  static final Pattern DOI_REMARK = Pattern.compile("(?:doi:|doi.org/)(10\\.\\d+/[^ ]+?)(?:\\.? |$)");
  //urn:lsid:ipni.org:names:1000000-1
  //urn:lsid:ipni.org:publications:1071-2
  static final Pattern LSID = Pattern.compile("lsid:ipni.org:(?:names|publications):(\\d+-\\d)$");
  static final Pattern TYPE_LOC = Pattern.compile("^([a-z]+)\\s+([A-Z/]+)(?:\\s*[\\s-]\\s*(.+))?$");
  private TermWriter taxWriter;
  private TermWriter typeWriter;
  private TermWriter nameRelWriter;
  private Map<String, Set<Reference>> refCollations = new HashMap<>(); // ref id to set of collations
  private File refSrc;

  private Set<String> refIDs = new HashSet<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, DOWNLOAD);
  }

  @Override
  protected void prepare() throws IOException {
    // download refs
    refSrc = new File("/tmp/ipni-bib.src");
    refSrc.deleteOnExit();
    if (!refSrc.exists()) {
      LOG.info("Downloading latest bib data from {}", DOWNLOAD_REF);
      download.download(DOWNLOAD_REF, refSrc);
    }

    newWriter(ColdpTerm.Name, List.of(
        ColdpTerm.ID,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.status,
        ColdpTerm.referenceID,
        ColdpTerm.publishedInYear,
        ColdpTerm.publishedInPage,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
        ColdpTerm.ID,
        ColdpTerm.doi,
        ColdpTerm.alternativeID,
        ColdpTerm.citation,
        ColdpTerm.title,
        ColdpTerm.author,
        ColdpTerm.issued,
        ColdpTerm.volume,
        ColdpTerm.issue,
        ColdpTerm.page,
        ColdpTerm.issn,
        ColdpTerm.isbn,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    taxWriter = additionalWriter(ColdpTerm.Taxon, List.of(
        ColdpTerm.ID,
        ColdpTerm.nameID,
        ColdpTerm.provisional,
        ColdpTerm.status,
        ColdpTerm.family,
        ColdpTerm.link
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.ID,
        ColdpTerm.nameID,
        ColdpTerm.citation,
        ColdpTerm.status,
        ColdpTerm.institutionCode,
        ColdpTerm.catalogNumber,
        ColdpTerm.collector,
        ColdpTerm.date,
        ColdpTerm.locality,
        ColdpTerm.latitude,
        ColdpTerm.longitude,
        ColdpTerm.remarks
    ));
    nameRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));
  }
  // id|authors_t|basionym_s_lower|basionym_author_s_lower|lookup_basionym_id|bibliographic_reference_s_lower|bibliographic_type_info_s_lower|reference_collation_s_lower|collection_date_as_text_s_lower|collection_day_1_s_lower|collection_day_2_s_lower|collection_month_1_s_lower|collection_month_2_s_lower|collection_number_s_lower|collection_year_1_s_lower|collection_year_2_s_lower|collector_team_as_text_t|lookup_conserved_against_id|lookup_correction_of_id|date_created_date|date_last_modified_date|distribution_s_lower|east_or_west_s_lower|family_s_lower|taxon_scientific_name_s_lower|taxon_sci_name_suggestion|genus_s_lower|geographic_unit_as_text_s_lower|hybrid_b|hybrid_genus_b|lookup_hybrid_parent_id|hybrid_parents_s_lower|infra_family_s_lower|infra_genus_s_lower|infraspecies_s_lower|lookup_isonym_of_id|lookup_later_homonym_of_id|latitude_degrees_s_lower|latitude_minutes_s_lower|latitude_seconds_s_lower|locality_s_lower|longitude_degrees_s_lower|longitude_minutes_s_lower|longitude_seconds_s_lower|name_status_s_lower|name_status_bot_code_type_s_lower|name_status_editor_type_s_lower|nomenclatural_synonym_s_lower|lookup_nomenclatural_synonym_id|north_or_south_s_lower|original_basionym_s_lower|original_basionym_author_team_s_lower|original_hybrid_parentage_s_lower|original_remarks_s_lower|original_replaced_synonym_s_lower|original_taxon_distribution_s_lower|lookup_orthographic_variant_of_id|other_links_s_lower|lookup_parent_id|publication_s_lower|lookup_publication_id|publication_year_i|publication_year_full_s_lower|publication_year_note_s_lower|publishing_author_s_lower|rank_s_alphanum|reference_t|reference_remarks_s_lower|remarks_s_lower|lookup_replaced_synonym_id|lookup_same_citation_as_id|score_s_lower|species_s_lower|species_author_s_lower|lookup_superfluous_name_of_id|suppressed_b|top_copy_b|lookup_type_id|type_locations_s_lower|type_name_s_lower|type_remarks_s_lower|type_chosen_by_s_lower|type_note_s_lower|detail_author_team_ids|detail_species_author_team_ids|page_as_text_s_lower|citation_type_s_lower|lookup_validation_of_id|version_s_lower|powo_b|sortable|family_taxon_name_sortable|wfo_id_s_lower
  // id|version_s_lower|ipni_record_type_s_lower|top_copy_b|suppressed_b|abbreviation_s_lower|title_s_lower|remarks_s_lower|bph_number_s_lower|isbn_s_lower|issn_s_lower|date_s_lower|lc_number_s_lower|preceded_by_s_lower|tl2_author_s_lower|tl2_number_s_lower|tdwg_abbreviation_s_lower|superceded_by_s_lower|sortable
  @Override
  protected void addData() throws Exception {
    try (InputStream inNames = new XZCompressorInputStream(new FileInputStream(src));
         InputStream inRefs = new XZCompressorInputStream(new FileInputStream(refSrc));
    ){
      var iter = iterate(inNames);
      while(iter.hasNext()) {
        var row = iter.next();
        final String id = idFromLsid(row[0]); // urn:lsid:ipni.org:names:1000000-1
        if (row.length<75) {
          LOG.warn("Short row {} with {} columns", id, row.length);
          continue;
        }
        boolean suppressed = bool(row[75]);
        if (suppressed) {
           continue;
        }
        StringBuilder remarks = new StringBuilder();
        writer.set(ColdpTerm.ID, id); // 1000000-1
        writer.set(ColdpTerm.authorship, row[1]); // (Vell.) J.F.Macbr.
        writer.set(ColdpTerm.scientificName, row[24]); // Elymus × mucronatus
        writer.set(ColdpTerm.publishedInYear, row[61]); // 1997

        Reference ref = new Reference(row[60], row[7]);
        if (ref.ipniID != null) {
          refCollations.putIfAbsent(ref.ipniID, new HashSet<>());
          // move single page pointers to name
          if (ref.isSinglePage()) {
            writer.set(ColdpTerm.publishedInPage, ref.pages);
            ref.pages = null;
          }
          refCollations.get(ref.ipniID).add(ref);
          writer.set(ColdpTerm.referenceID, ref.refId());
        }
        if (!StringUtils.isBlank(row[7])) {
          append(remarks, "Reference collation: " + row[7]);
        }
        writer.set(ColdpTerm.rank, row[65]); // 1997
        writer.set(ColdpTerm.status, row[86]); // comb. nov.
        writer.set(ColdpTerm.link, LINK_BASE+"/n/"+id);
        append(remarks, row[68]);

        // taxa
        taxWriter.set(ColdpTerm.ID, id);
        taxWriter.set(ColdpTerm.nameID, id);
        taxWriter.set(ColdpTerm.family, row[23]); // Poaceae
        taxWriter.set(ColdpTerm.provisional, true); // Poaceae
        taxWriter.next();

        // name relations
        addNameRel(NomRelType.REPLACEMENT_NAME, row[69], id); // 69, lookup_replaced_synonym_id
        addNameRel("validationOf", row[87], id); // 87, lookup_validation_of_id
        addNameRel(NomRelType.SUPERFLUOUS, id, row[74]); // 74, lookup_superfluous_name_of_id
        addNameRel("orthographicVariantOf", id, row[56]); // 56, lookup_orthographic_variant_of_id
        addNameRel(NomRelType.HOMOTYPIC, id, row[48]); // 48, lookup_nomenclatural_synonym_id
        addNameRel(NomRelType.LATER_HOMONYM, id, row[36]); // 36, lookup_later_homonym_of_id
        addNameRel("isonymOf", id, row[35]); // 35, lookup_isonym_of_id
        addNameRel(NomRelType.SPELLING_CORRECTION, id, row[18]); // 18, lookup_correction_of_id
        addNameRel(NomRelType.CONSERVED, id, row[17]); // 17, lookup_conserved_against_id
        addNameRel(NomRelType.BASIONYM, id, row[4]); // 4, lookup_basionym_id

        // type
        append(remarks, row[79]); // Designated Type: F. meleagris L.

        String typeLocations = row[78]; // holotype Bolus Herbarium;isotype The Natural History Museum;isotype NBG;isotype Swedish Museum of Natural History;isotype Herbarium, Royal Botanic Gardens;isotype National Herbarium, National Botanical Institute;isotype Herbarium, Missouri Botanical Garden;isotype Bolus Herbarium;isotype The New York Botanical Garden;isotype Museum national d'Histoire naturelle
        if (!StringUtils.isBlank(typeLocations)) {
          for (String loc : typeLocations.split(";")) {
            typeWriter.set(ColdpTerm.nameID, id);
            var m = TYPE_LOC.matcher(loc.trim());
            if (m.find()) {
              typeWriter.set(ColdpTerm.status, m.group(1));
              typeWriter.set(ColdpTerm.institutionCode, m.group(2));
              typeWriter.set(ColdpTerm.catalogNumber, m.group(3));
            } else {
              typeWriter.set(ColdpTerm.citation, loc);
            }
            typeWriter.set(ColdpTerm.locality, row[40]);
            typeWriter.set(ColdpTerm.collector, row[16]);
            typeWriter.set(ColdpTerm.date, buildCollectionDate(row));
            if (row[37] != null && row[41] != null) {
              typeWriter.set(ColdpTerm.latitude, decimal(row[37], row[38], row[39]));
              typeWriter.set(ColdpTerm.longitude, decimal(row[41], row[42], row[43]));
            }
            typeWriter.next();
          }
        }

        // finish name record
        if (remarks.length()>0) {
          writer.set(ColdpTerm.remarks, remarks.toString());
        }
        writer.next();
      }

      // PUBLICATION -> REFERENCE RECORDS
      // IPNI publications are journals or books, not individual articles.
      // we use the distinct combination from publication & "collation" instead
      iter = iterate(inRefs);
      while(iter.hasNext()) {
        var row = iter.next();
        // urn:lsid:ipni.org:names:1000000-1
        String ipniID = idFromLsid(row[0]);
        if (refCollations.containsKey(ipniID)) {
          for (Reference ref : refCollations.get(ipniID)) {
            addRefRecord(ref, row);
          }
        } else {
          boolean suppressed = bool(row[4]);
          if (suppressed) {
            continue;
          }
          // write one record!
          Reference ref = new Reference(ipniID);
          addRefRecord(ref, row);
          // keep an empty list so we dont accidently create the same record again
          refCollations.put(ipniID, new HashSet<>());
        }
      }

    } finally {
      taxWriter.close();
      typeWriter.close();
      nameRelWriter.close();
    }
  }

  private static String buildCollectionDate(String[] row){
    String year1 = row[14];
    String year2 = row[15];
    if (year1 != null) {
      String month1 = row[11];
      String day1 = row[9];
      return buildCollectionDate(year1, month1, day1);
    } else if (year2 != null) {
      String month2 = row[12];
      String day2 = row[10];
      return buildCollectionDate(year2, month2, day2);
    }
    return null;
  }

  static String buildCollectionDate(String year, String month, String day){
    if (year == null) return null;

    StringBuilder sb = new StringBuilder();
    sb.append(year);
    for (String x : new String[]{month, day}) {
      if (!StringUtils.isBlank(x)) {
        sb.append("-");
        sb.append(x);
      } else {
        break;
      }
    }
    return sb.toString();
  }

  static Double decimal(String degree, String min, String sec){
    if (!StringUtils.isBlank(degree)) {
      try {
        double d = Integer.parseInt(degree);
        int m = 0;
        int s = 0;
        if (min != null) {
            m = Integer.parseInt(min);
            s = Integer.parseInt(sec);
        }
        return d + ((double)m*60+s) / 3600;
      } catch (NumberFormatException e) {
        LOG.warn("Bad lat/lon values: {}°{}`{}``", degree, min, sec);
      }
    }
    return null;
  }

  private static void append(StringBuilder sb, String x){
    if (!StringUtils.isBlank(x)) {
      if (sb.length()>0) {
        sb.append("; ");
      }
      sb.append(x);
    }
  }

  private static boolean bool(String x){
    return x != null && x.equalsIgnoreCase("t");
  }

  private void addRefRecord(Reference ref, String[] row) throws IOException {
    if (!refIDs.add(ref.refId())) {
      System.out.println("DUPLICATE REFERENCE ID: " + ref.refId());
      return;
    }
    refIDs.add(ref.refId());

    refWriter.set(ColdpTerm.ID, ref.refId());
    refWriter.set(ColdpTerm.title, row[6]);
    String remarks = row[7];
    refWriter.set(ColdpTerm.remarks, remarks);
    String  doi = extractDOI(remarks);
    refWriter.set(ColdpTerm.doi, doi);
    refWriter.set(ColdpTerm.isbn, row[9]);
    refWriter.set(ColdpTerm.issn, row[10]);
    refWriter.set(ColdpTerm.issued, row[11]);
    // alternative ids
    Map<String,String> altIDs = new HashMap<>();
    addAltID(altIDs, "bph", row[8]);
    addAltID(altIDs, "lc", row[12]);
    addAltID(altIDs, "tl2", row[15]);
    if (!altIDs.isEmpty()) {
      String ids = altIDs.entrySet().stream().map(e -> e.getKey()+":"+e.getValue()).collect(Collectors.joining(";"));
      refWriter.set(ColdpTerm.alternativeID, ids);
    }

    // add collation information
    if (ref.parsed) {
      refWriter.set(ColdpTerm.volume, ref.volume); //
      refWriter.set(ColdpTerm.issue, ref.issue); //
      refWriter.set(ColdpTerm.author, ref.authors); //
      refWriter.set(ColdpTerm.page, ref.pages); //
    }
    refWriter.next();
  }

  private void addAltID(Map<String, String> ids, String prefix, String id) {
    if (!StringUtils.isBlank(id)) {
      ids.put(prefix, id);
    }
  }
  private void addNameRel(NomRelType type, String idFrom, String idTo) throws IOException {
    addNameRel(type.name(), idFrom, idTo);
  }
  private void addNameRel(String type, String idFrom, String idTo) throws IOException {
    if (!StringUtils.isBlank(idFrom) && !StringUtils.isBlank(idTo)) {
      nameRelWriter.set(ColdpTerm.type, type);
      nameRelWriter.set(ColdpTerm.nameID, idFrom);
      nameRelWriter.set(ColdpTerm.relatedNameID, idTo);
      nameRelWriter.next();
    }
  }

  private Iterator<String[]> iterate(InputStream stream) throws IOException {
    BufferedReader br = UTF8IoUtils.readerFromStream(stream);
    Iterator<String[]> iter = br.lines().map(this::split).iterator();
    iter.next(); // skip header row
    return iter;
  }

  private String[] split(String line) {
    if (line != null) {
      var cols = (line+"|END").split("\\s*\\|\\s*");
      return cols;
    }
    return null;
  }

  static String idFromLsid(String lsid) {
    if (lsid != null) {
      var m = LSID.matcher(lsid);
      if (m.find()) {
        return m.group(1);
      }
    }
    return StringUtils.trimToNull(lsid);
  }

  static String extractDOI(String remarks) {
    if (remarks != null) {
      var m = DOI_REMARK.matcher(remarks);
      if (m.find()) {
        return m.group(1);
      }
    }
    return null;
  }

  static class Reference {
    public final String ipniID;
    public String volume;
    public String issue;
    public String authors;
    public String pages;
    public boolean parsed;

    public Reference(String ipniID) {
      this.ipniID = ipniID;
      this.parsed = false;
    }

    public Reference(String lsid, String collation) {
      this.ipniID = idFromLsid(lsid);

      if (!StringUtils.isBlank(collation)) {
        // 4(5): 8, 62 (1909):
        var parts = PAGES.split(collation.trim(), 2);
        if (parts.length > 0) {
          parsed = true;
          pages = parts.length > 1 ? parts[1] : null;
          // try with flat int first
          try {
            Integer.parseInt(collation.trim());
            volume = collation.trim();

          } catch (IllegalArgumentException e) {
            var m = COLLATION.matcher(parts[0]);
            if (m.find()) {
              volume=m.group(1);
              issue=m.group(2);
            } else {
              volume=parts[0];
            }
          }
        } else {
          parsed = false;
          volume = collation;
          LOG.info("Unparsable reference collation >>{}<<", collation);
        }
      }
    }

    boolean isSinglePage() {
      if (pages != null) {
        try {
          Integer.parseInt(pages.trim());
          return true;
        } catch (NumberFormatException e) {
          // no, just catch it
        }
      }
      return false;
    }

    private String refId() {
      StringBuilder sb = new StringBuilder();
      sb.append(ipniID);
      if (volume != null || issue != null || pages != null || authors != null) {
        sb.append("$");
        if (volume != null) {
          sb.append("v");
          sb.append(volume);
        }
        if (issue != null) {
          sb.append("(");
          sb.append(issue);
          sb.append(")");
        }
        if (pages != null) {
          sb.append("p");
          sb.append(pages.replaceAll("[\\s;:,.?-]", ""));
        }
        if (authors != null) {
          sb.append("!");
          sb.append(
              UnicodeUtils.foldToAscii(authors)
                          .replaceAll("[\\s;:,.?-]", "")
          );
        }
      }
      return sb.toString().replaceAll("\\s+", "");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Reference)) return false;
      Reference reference = (Reference) o;
      return Objects.equals(refId(), reference.refId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(refId());
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

}
