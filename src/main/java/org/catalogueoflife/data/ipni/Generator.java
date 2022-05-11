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

import life.catalogue.api.util.ObjectUtils;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.text.StringUtils;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.dwc.terms.DwcTerm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * ColDP generator for IPNI using their public API.
 * All search terms are found at https://github.com/RBGKew/pykew/blob/master/pykew/ipni_terms.py
 *
 * Currently we only use the search results to generate the archive.
 * More details are available when resolving every name detail through the API.
 * This brings back better classification and name relations!
 */
public class Generator extends AbstractGenerator {
  private static final String API = "https://beta.ipni.org/api/1/";
  static final Pattern PAGES = Pattern.compile("\\s*:\\s*");
  static final Pattern COLLATION = Pattern.compile("^(\\d+)(?:\\s*[(-]\\s*(\\d+(?:\\s*[,-]\\s*\\d+)?)\\s*\\)?)?\\s*$");
  static final Pattern DOI_REMARK = Pattern.compile("(?:doi:|doi.org/)(10\\.\\d+/[^ ]+?)(?:\\.? |$)");
  static final Pattern LSID = Pattern.compile("lsid:ipni.org:names:(\\d+-\\d)$");
  static final Pattern TYPE_LOC = Pattern.compile("^([a-z]+)\\s+([A-Z/]+)(?:\\s*[\\s-]\\s*(.+))?$");
  private static final int MIN_YEAR = 1750; //1750;
  private static final List<Integer> TEST_YEARS = List.of(); //List.of(1828,1829,1836,1753);
  private static final int PAGESIZE = 500;
  private TermWriter taxWriter;
  private TermWriter typeWriter;
  private TermWriter nameRelWriter;
  private Set<String> refIds = new HashSet();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.Name, List.of(
      ColdpTerm.ID,
      ColdpTerm.basionymID,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.status,
      ColdpTerm.referenceID,
      ColdpTerm.publishedInYear,
      ColdpTerm.publishedInPage,
      ColdpTerm.publishedInPageLink,
      ColdpTerm.remarks
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
        ColdpTerm.ID,
        ColdpTerm.doi,
        ColdpTerm.type,
        ColdpTerm.citation,
        ColdpTerm.title,
        ColdpTerm.containerTitle,
        ColdpTerm.author,
        ColdpTerm.issued,
        ColdpTerm.volume,
        ColdpTerm.issue,
        ColdpTerm.issn,
        ColdpTerm.isbn,
        ColdpTerm.page,
        ColdpTerm.remarks
    ));
    taxWriter = additionalWriter(ColdpTerm.Taxon, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.nameID,
        ColdpTerm.status,
        ColdpTerm.family
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.ID,
        ColdpTerm.nameID,
        ColdpTerm.status,
        DwcTerm.institutionCode,
        DwcTerm.catalogNumber,
        ColdpTerm.locality,
        ColdpTerm.collector,
        ColdpTerm.date,
        ColdpTerm.latitude,
        ColdpTerm.longitude,
        ColdpTerm.remarks
    ));
    nameRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));

    try {
      if (TEST_YEARS.isEmpty()) {
        int currYear = LocalDate.now().getYear();
        for (int year = currYear; year > MIN_YEAR; year--) {
          addYear(year);
        }

      } else {
        for (int year : TEST_YEARS) {
          addYear(year);
        }
      }
    } finally {
      taxWriter.close();
      typeWriter.close();
      nameRelWriter.close();
    }
  }

  /**
   * Pages through all records published in a given year using a cursor parameter
   */
  void addYear(int year) throws IOException {
    var resp = mapper.readValue(http.getStreamJSON(buildPageUri(year, null)), IpniWrapper.class);
    LOG.info("Crawl {} names published in {}", resp.totalResults, year);
    write(resp.results);
    while (resp.hasMore()) {
      resp = mapper.readValue(http.getStreamJSON(buildPageUri(year, resp)), IpniWrapper.class);
      write(resp.results);
    }
  }

  static String idFromLsid(String lsid) {
    if (lsid != null) {
      var m = LSID.matcher(lsid);
      if (m.find()) {
        return m.group(1);
      }
    }
    return lsid;
  }

  static class Collation {
    public String volume;
    public String issue;
    public String pages;

    public Collation() {
    }

    public Collation(String volume, String issue, String pages) {
      this.volume = volume;
      this.issue = issue;
      this.pages = pages;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Collation)) return false;
      Collation collation = (Collation) o;
      return Objects.equals(volume, collation.volume)
             && Objects.equals(issue, collation.issue)
             && Objects.equals(pages, collation.pages);
    }

    @Override
    public int hashCode() {
      return Objects.hash(volume, issue, pages);
    }
  }
  static Collation parseCollation (String collation) {
    Collation c = new Collation();
    parseCollation(c, collation);
    return c;
  }
  static void parseCollation (Collation c, String collation) {
    if (collation != null) {
      var parts = PAGES.split(collation, 2);
      if (parts.length > 0) {
        c.pages = parts.length > 1 ? parts[1] : null;
        // try with flat int first
        try {
          Integer.parseInt(collation.trim());
          c.volume = collation.trim();

        } catch (IllegalArgumentException e) {
          var m = COLLATION.matcher(parts[0]);
          if (m.find()) {
            c.volume=m.group(1);
            c.issue=m.group(2);
          } else {
            c.volume=parts[0];
          }
        }
      } else {
        LOG.info("Unparsable reference collation >>{}<<", collation);
      }
    }
  }
  void write(List<IpniName> results) throws IOException {
    if (results == null) return;

    for (IpniName n : results) {
      String refID = null;
      Collation collation = new Collation();
      if (n.reference != null) {
        // references are tricky in IPNI. The refID takes you to the journal, not the individual article or even issue!!!
        // instead we use a) the DOI if known, b) referenceID + authors + the collation without the pages or c) just the plain referenceID as last resort
        String  doi = extractDOI(n.remarks);
        parseCollation(collation, n.referenceCollation);
        refID = ObjectUtils.coalesce(doi, buildRefId(n.publicationId, n.publishingAuthor, collation));
        // only write once
        if (!refIds.contains(refID)) {
          refIds.add(refID);
          refWriter.set(ColdpTerm.ID, refID);
          if (collation.issue != null) {
            refWriter.set(ColdpTerm.type, "ARTICLE_JOURNAL");
          }
          refWriter.set(ColdpTerm.doi, doi);
          //refWriter.set(ColdpTerm.citation, n.reference); dont use it - the page is in it
          refWriter.set(ColdpTerm.author, n.publishingAuthor);
          refWriter.set(ColdpTerm.volume, collation.volume);
          refWriter.set(ColdpTerm.issue, collation.issue);
          refWriter.set(ColdpTerm.containerTitle, n.publication);
          String linkedPublicationRemarks = null;
          if (n.linkedPublication != null) {
            writer.set(ColdpTerm.publishedInPageLink, n.linkedPublication.bhlPageLink);
            refWriter.set(ColdpTerm.containerTitle, ObjectUtils.coalesce(n.linkedPublication.title, n.linkedPublication.abbreviation));
            refWriter.set(ColdpTerm.issn, n.linkedPublication.issn);
            refWriter.set(ColdpTerm.isbn, n.linkedPublication.isbn);
            linkedPublicationRemarks = n.linkedPublication.remarks;
          }
          refWriter.set(ColdpTerm.issued, ObjectUtils.coalesce(n.publicationYearNote, n.publicationYear));
          refWriter.set(ColdpTerm.remarks, StringUtils.concat("; ", n.referenceRemarks, linkedPublicationRemarks));
          refWriter.next();
        }
      }

      writer.set(ColdpTerm.ID, n.id);
      writer.set(ColdpTerm.rank, n.rank);
      writer.set(ColdpTerm.scientificName, n.name);
      writer.set(ColdpTerm.authorship, n.authors);
      writer.set(ColdpTerm.referenceID, refID);
      writer.set(ColdpTerm.publishedInPage, collation.pages);
      writer.set(ColdpTerm.basionymID, idFromLsid(n.basionymId));
      writer.set(ColdpTerm.status, n.nameStatusType);
      writer.set(ColdpTerm.remarks, StringUtils.concat("; ", n.remarks, n.nameStatus, valueLabel("Type", n.typeName))); // citationType contains "comb.nov."
      writer.next();

      taxWriter.set(ColdpTerm.ID, n.id);
      taxWriter.set(ColdpTerm.nameID, n.id);
      taxWriter.set(ColdpTerm.status, "provisionally accepted");
      taxWriter.set(ColdpTerm.family, n.family);
      taxWriter.next();

      if (n.isonymOf != null) {
        nameRelWriter.set(ColdpTerm.nameID, n.id);
        nameRelWriter.set(ColdpTerm.relatedNameID, n.isonymOf.id);
        nameRelWriter.set(ColdpTerm.type, "isonymOf");
        nameRelWriter.next();
      }
      if (n.replacedSynonymOf != null) {
        for (var syn : n.replacedSynonymOf) {
          nameRelWriter.set(ColdpTerm.nameID, n.id);
          nameRelWriter.set(ColdpTerm.relatedNameID, syn.id);
          nameRelWriter.set(ColdpTerm.type, "replacedSynonymOf");
          nameRelWriter.next();
        }
      }

      if (n.suppressed) {
        //writer.set(ColdpTerm.status, );
      }

      if (n.hasTypeData && n.typeLocations != null) {
        for (String tl : n.typeLocations.split(";")) {
          typeWriter.set(ColdpTerm.nameID, n.id);
          var m = TYPE_LOC.matcher(tl.trim());
          if (m.find()) {
            typeWriter.set(ColdpTerm.status, m.group(1));
            typeWriter.set(DwcTerm.institutionCode, m.group(2));
            typeWriter.set(DwcTerm.catalogNumber, m.group(3));
          } else {
            typeWriter.set(ColdpTerm.status, tl);
          }
          typeWriter.set(ColdpTerm.remarks, n.collectionNumber);// TODO: remove?
          typeWriter.set(ColdpTerm.locality, n.locality);
          typeWriter.set(ColdpTerm.collector, n.collectorTeam);
          typeWriter.set(ColdpTerm.date, n.collectionDate1);
          if (n.typeCoordinates != null) {
            // should be decimal - TODO: convert !
            typeWriter.set(ColdpTerm.latitude, n.typeCoordinates.formattedLatitude);
            typeWriter.set(ColdpTerm.longitude, n.typeCoordinates.formattedLongitude);
          }
          typeWriter.next();
        }
      }
    }
  }

  static String valueLabel(String label, String value) {
    if (!org.apache.commons.lang3.StringUtils.isBlank(value)) {
      return label + ": " + value;
    }
    return null;
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

  private static String buildRefId(String ipniID, String authors, Collation collation) {
    StringBuilder sb = new StringBuilder();
    sb.append(ipniID);
    if (collation != null) {
      if (collation.volume != null) {
        sb.append("-");
        sb.append(collation.volume);
      }
      if (collation.issue != null) {
        sb.append("(");
        sb.append(collation.issue);
        sb.append(")");
      }
    }
    if (authors != null) {
      sb.append("-");
      sb.append(
        StringUtils.foldToAscii(authors)
                   .replaceAll("[\\s;:,.?-]", "")
      );
    }
    return sb.toString();
  }
  URI buildPageUri(int year, IpniWrapper prev) throws UnsupportedEncodingException {
    String cursor = prev == null ? "*" : URLEncoder.encode(prev.cursor, StandardCharsets.UTF_8.toString());
    return URI.create(API + String.format("search?published=%s&perPage=%s&cursor=%s", year, PAGESIZE, cursor));
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

  static class IpniWrapper {
    public Integer totalResults;
    public String cursor;
    public List<IpniName> results;

    boolean hasMore() {
      // IPNI does not change the page value - it will always be 1
      // we'll get null results once we're through though...
      return results != null && results.size() < totalResults;
    }
  }
  static class IpniName {
    public String name;
    public String authors;
    public String publishingAuthor;
    public List<IpniAuthor> authorTeam;
    public String nameStatus;
    public String nameStatusType;
    // the following IpniName properties are only populated when requesting the name detail, not through the search we use!
    public IpniName isonymOf;
    public List<IpniName> basionymOf;

    public List<IpniName> nomenclaturalSynonym;
    public List<IpniName> replacedSynonymOf;
    public List<IpniName> sameCitationAs;
    public List<IpniName> parent;
    public List<IpniName> child;
    public String rank;
    public String url;
    public String family;
    public String genus;
    public String species;
    public String basionymStr;
    public String basionymAuthorStr;
    public String basionymId;
    public String citationType; // stat. nov.
    public boolean hybrid;
    public boolean hybridGenus;
    public boolean inPowo;
    public IpniPublication linkedPublication;
    public String publication;
    public Integer publicationYear;
    public String publicationYearNote;
    public String referenceCollation;
    public String publicationId;
    public String recordType;
    public String reference;
    public String referenceRemarks;
    public String collectionDate1;
    public String collectionNumber;
    public String collectorTeam;
    public String distribution;
    public boolean suppressed;
    public boolean topCopy;
    public String version;
    public String id;
    public String fqId;
    public boolean hasNomenclaturalNotes;
    public boolean hasTypeData;
    public boolean hasOriginalData;
    public boolean hasLinks;

    public IpniCoordinate typeCoordinates;
    public String locality;
    public String typeLocations;
    public String typeName; // A. alba P. Miller (Pinus picea Linnaeus, non Abies picea P. Miller, l.c.)
    public String remarks;
  }
  static class IpniAuthor {
    public String id;
    public String name;
    public int order;
    public String type;
    public String summary;
    public String remarks;
  }
  static class IpniPublication {
    public String id;
    public String abbreviation;
    public String date;
    public String fqId;
    public String lcNumber;
    public String recordType;
    public String issn;
    public String isbn;
    public String remarks;
    public boolean suppressed;
    public String title;
    public String version;
    public boolean hasBhlLinks;
    public boolean hasBhlTitleLink;
    public boolean hasBhlPageLink;
    public String bhlPageLink;
    public String bhlTitleLink;
  }
  static class IpniCoordinate {
    public String eastOrWest;
    public String formattedLatitude;
    public String formattedLongitude;
    public String latitudeDegrees;
    public String latitudeMinutes;
    public String latitudeSeconds;
    public String longitudeDegrees;
    public String longitudeMinutes;
    public String longitudeSeconds;
    public String northOrSouth;
    public boolean valid;
    public boolean validLatitude;
    public boolean validLongitude;
  }

}
