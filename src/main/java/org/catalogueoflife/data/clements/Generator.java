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
package org.catalogueoflife.data.clements;

import com.google.common.annotations.VisibleForTesting;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.CompressionUtil;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.*;
import java.net.URI;
import java.text.DateFormatSymbols;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Clements bird checklist
 * https://www.birds.cornell.edu/clementschecklist/
 */
public class Generator extends AbstractColdpGenerator {
  // https://www.birds.cornell.edu/clementschecklist/wp-content/uploads/2023/12/Clements-v2023-October-2023-csv.zip
  private static final String DOWNLOAD = "https://www.birds.cornell.edu/clementschecklist/wp-content/uploads/{YEAR}/{MONTH}/Clements-v{YEAR}{SUFFIX}-{MONTH_NAME}-{YEAR}-csv.zip";
  private Pattern cleanFamily = Pattern.compile("^([^ ,(]+)");
  // metadata
  private static final String TITLE = "The Clements Checklist";
  private static final URI HOMEPAGE = URI.create("https://www.birds.cornell.edu/clementschecklist");
  private static final String CITATION = "Clements, J. F., T. S. Schulenberg, M. J. Iliff, D. Roberson, T. A. Fredericks, B. L. Sullivan, and C. L. Wood. {YEAR}. The eBird/Clements checklist of birds of the world: v{YEAR}. Downloaded from http://www.birds.cornell.edu/clementschecklist/download/";
  private static final URI LOGO = null;
  private static final String DESCRIPTION = "The Clements Checklist of Birds of the World, 6th Edition was published and released by Cornell University Press in June 2007. The book was produced from a nearly completed manuscript left by James Clements upon his death in 2005.\n" +
          "\n" +
          "The Cornell Lab of Ornithology has accepted the job of maintaining the ever-changing list of species, subspecies, English names, and approximate distributions, beginning with publication of the 6th Edition. Our procedures for accomplishing this ongoing task include using the considerable expertise of our research ornithologists on staff, aided enormously by input from knowledgeable professional and amateur cooperators worldwide. We invite input on known or suspected errors or updates at any time.\n" +
          "\n" +
          "This website serves as the clearinghouse for keeping your Clements Checklist up to date. We will post all corrections once a year in August. At the same time, we’ll post updates to the taxonomy, scientific and English nomenclature, and range descriptions, to incorporate changes that have made their way into the literature and are generally accepted by the appropriate scientific body or community. In the future, we will also be posting a list of alternative English names.";
  private static final String CONTACT_ORG = "Cornell Lab of Ornithology";
  private static final String CONTACT_EMAIL = "cornellbirds@cornell.edu";

  // sort v2017	Clements v2017 change	text for website v2017	category	English name	scientific name	range	order	family	extinct	extinct year	sort v2016	page 6.0
  // sort v2023	species_code	taxon_concept_id	Clements v2023 change	text for website v2023	category	English name	scientific name	authority	name and authority	range	order	family	extinct	extinct year	sort v2022	page 6.0
  // sort v2023   species_code    taxon_concept_id    Clements v2023 change   text for website v2023  category,English name,scientific name,authority,name and authority,range,order              family  extinct extinct year    sort v2022  page 6.0
  private static final int COL_ID = 0;
  private static final int COL_AVIBASE_ID = 2;
  private static final int COL_REMARKS = 2;
  private static final int COL_RANK = 3;
  private static final int COL_EN_NAME = 4;
  private static final int COL_NAME = 5;
  private static final int COL_RANGE = 6;
  private static final int COL_ORDER = 7;
  private static final int COL_FAMILY = 8;
  private static final int COL_EXTINCT = 9;
  private static final int COL_EXTINCT_YEAR = 10;
  // reference sheet
  private static final int COL_REF_ABBREV = 0;
  private static final int COL_REF_AUTHOR = 2;
  private static final int COL_REF_YEAR = 3;
  private static final int COL_REF_TITLE = 4;
  private static final int COL_REF_JOURNAL = 5;

  private LocalDate issued;
  private String version;
  private File zip;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @VisibleForTesting
  protected static String url(LocalDate date, String versionSuffix){
    DateFormatSymbols dfs = new DateFormatSymbols();
    String[] months = dfs.getMonths();
    return DOWNLOAD
            .replace("{SUFFIX}", versionSuffix)
            .replace("{YEAR}", String.valueOf(date.get(ChronoField.YEAR)))
            .replace("{MONTH}", String.format("%02d", date.get(ChronoField.MONTH_OF_YEAR)))
            .replace("{MONTH_NAME}", months[date.get(ChronoField.MONTH_OF_YEAR) - 1]);
  }

  @Override
  protected void prepare() throws IOException {
    // find latest version
    // recently these have been published annually in october only, but there have been different month before
    // try and find a list for each month going backwards until we hit sth
    // corrections are also appending b/c/d to the version year, e.g.
    // https://www.birds.cornell.edu/clementschecklist/wp-content/uploads/2024/02/Clements-v2023b-December-2023.csv
    String url = null;
    LocalDate today = LocalDate.now();
    LOG.info("Find latest release starting with {}", today);
    for (int i=0; i<=24; i++) {
      LocalDate date = today.minus(i, ChronoUnit.MONTHS);
      url = url(date, "");
      LOG.info("Try release on {} at {}", date, url);
      if (http.exists(url)) {
        issued = date;
        version = String.valueOf(date.get(ChronoField.YEAR));
      }
      for (char sfx='b'; sfx < 'e'; sfx++) {
        url = url(date, String.valueOf(sfx));
        LOG.info("Try {} release on {} at {}", sfx, date, url);
        if (http.exists(url)) {
          issued = date;
          version = String.valueOf(date.get(ChronoField.YEAR)) + sfx;
        }
      }
    }
    if (issued == null) throw new IllegalStateException("Unable to find any publication since 2 years");

    metadata.put("issued", issued);
    metadata.put("version", version);

    // download latest CSV
    URI uri = URI.create(url);
    LOG.info("Downloading latest data from {}", uri);
    zip = download("clements.csv.zip", uri);

    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.status,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.alternativeID,
        ColdpTerm.extinct
    ));
  }

  @Override
  protected void addData() throws Exception {
    System.out.println("Process "+zip);

    try (InputStream is = new FileInputStream(zip);
         ZipInputStream zis = new ZipInputStream(is)
    ) {
      ZipEntry ze;
      while((ze= zis.getNextEntry()) != null) {
        BufferedReader br = UTF8IoUtils.readerFromStream(zis);
      }
    }


    var iter = iterate("taxonomy.tsv");
    while(iter.hasNext()) {
      var row = iter.next();
      writer.set(ColdpTerm.ID, row[0]);
      writer.set(ColdpTerm.parentID, row[1]);
      writer.set(ColdpTerm.scientificName, row[2]);
      writer.set(ColdpTerm.status, "accepted");
      writer.set(ColdpTerm.rank, translateRank(row[3]));
      writer.set(ColdpTerm.alternativeID, row[4]);
      String flags = row[5];
      if (flags != null && flags.contains("extinct")) {
        writer.set(ColdpTerm.extinct, "true");
      }
      writer.next();
    }

    System.out.println("Process synonyms.tsv");
    iter = iterate("synonyms.tsv");
    Map<String, AtomicInteger> types = new HashMap<>();
    while(iter.hasNext()) {
      var row = iter.next();
      writer.set(ColdpTerm.scientificName, row[0]);
      writer.set(ColdpTerm.parentID, row[1]);
      writer.set(ColdpTerm.status, "synonym");
      writer.set(ColdpTerm.alternativeID, row[4]);
      String type = row[2];
      if (!StringUtils.isBlank(type)) {
        types.computeIfAbsent(type, v -> new AtomicInteger(0));
        types.get(type).incrementAndGet();
      }
      writer.next();
    }
    for (var e : types.entrySet()) {
      System.out.println(e.getKey() + " -> " + e.getValue());
    }
  }

  protected static String translateRank(String value) {
    if (StringUtils.isBlank(value)) return null;
    return value;
  }

  protected Iterator<String[]> iterate(String filename) throws IOException {
    BufferedReader br = UTF8IoUtils.readerFromFile(new File(filename));
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

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued", issued);
    super.addMetadata();
  }

}
