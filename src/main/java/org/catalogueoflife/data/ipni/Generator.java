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

import com.github.scribejava.apis.KeycloakApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import life.catalogue.api.model.DOI;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.nameparser.api.NomCode;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * ColDP generator for IPNI using their public API.
 * All search terms are found at https://github.com/RBGKew/pykew/blob/master/pykew/ipni_terms.py
 */
public class Generator extends AbstractGenerator {
  private static final String API = "http://beta.ipni.org/api/1/";
  private static final int MIN_YEAR = 2000; //1750;
  private static final int PAGESIZE = 500;
  private TermWriter taxWriter;
  private TermWriter typeWriter;
  private TermWriter nameRelWriter;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.Name, List.of(
      ColdpTerm.ID,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.code,
      ColdpTerm.referenceID,
      ColdpTerm.publishedInYear,
      ColdpTerm.remarks
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
        ColdpTerm.ID,
        ColdpTerm.publishedInYear,
        ColdpTerm.remarks
    ));
    taxWriter = additionalWriter(ColdpTerm.Taxon, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.ID,
        ColdpTerm.nameID,
        ColdpTerm.locality,
        ColdpTerm.collector,
        ColdpTerm.date,
        ColdpTerm.latitude,
        ColdpTerm.longitude
    ));
    nameRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));

    try {
      int currYear = LocalDate.now().getYear();
      for (int year = currYear; year > MIN_YEAR; year--) {
        addYear(year);
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

  void write(List<IpniName> results) throws IOException {
    if (results == null) return;

    for (IpniName n : results) {
      writer.set(ColdpTerm.ID, n.id);
      writer.set(ColdpTerm.rank, n.rank);
      writer.set(ColdpTerm.scientificName, n.name);
      writer.set(ColdpTerm.authorship, n.authors);
      writer.set(ColdpTerm.referenceID, n.publicationId);
      writer.set(ColdpTerm.publishedInYear, n.publicationYear);
      writer.next();

      //TODO: track ids so we only write it once!
      refWriter.set(ColdpTerm.ID, n.publicationId);
      refWriter.set(ColdpTerm.publishedInYear, n.publicationYear);
      refWriter.set(ColdpTerm.remarks, n.publicationYearNote);
      refWriter.next();

      if (n.hasTypeData) {
        typeWriter.set(ColdpTerm.ID, n.collectionNumber);
        typeWriter.set(ColdpTerm.nameID, n.id);
        typeWriter.set(ColdpTerm.locality, n.typeLocations);
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
    public String rank;
    public String url;
    public String family;
    public String genus;
    public String species;
    public String basionymStr;
    public String basionymAuthorStr;
    public String basionymId;
    public String citationType;
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
    public String typeLocations;
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
    public String remarks;
    public boolean suppressed;
    public String title;
    public String version;
    public boolean hasBhlLinks;
    public boolean hasBhlTitleLink;
    public boolean hasBhlPageLink;
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
