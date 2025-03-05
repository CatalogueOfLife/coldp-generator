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
package org.catalogueoflife.data.ott;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.CompressionUtil;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Open Tree of Life Taxonomy transformer to ColDP
 * https://tree.opentreeoflife.org/about/taxonomy-version
 */
public class Generator extends AbstractColdpGenerator {
  private static final String VERSION = "3.7.2";
  private static final URI DOWNLOAD = URI.create("http://files.opentreeoflife.org/ott/ott" + VERSION + "/ott" + VERSION + ".tgz");
  private static final URI PROPERTY_FILE = URI.create("https://files.opentreeoflife.org/ott/ott" + VERSION + "/properties.json");
  private static final Map<String, String> rankMap = Map.of(
      "no rank", "unranked",
      "no rank - terminal", "unranked"
  );
  private final String version;
  protected static final String srcFN = "data.tgz";
  protected File ottSources;

  public Generator(GeneratorConfig cfg) throws IOException {
    this(cfg, DOWNLOAD, VERSION);
  }

  public Generator(GeneratorConfig cfg, URI download, String version) throws IOException {
    super(cfg, true, Map.of(srcFN, download));
    this.version = version;
  }

  @Override
  protected void prepare() throws IOException {
    System.out.println("Unpack archive " + srcFN);
    ottSources = new File(dir, "sources");
    CompressionUtil.decompressFile(ottSources, sourceFile(srcFN));

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
    System.out.println("Process taxonomy.tsv");
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

    System.out.println("Remove source files");
    FileUtils.deleteQuietly(ottSources);
  }

  protected static String translateRank(String value) {
    if (StringUtils.isBlank(value)) return null;

    value = value.toLowerCase().replace("_", " ").trim();
    if (rankMap.containsKey(value)) {
      return rankMap.get(value);
    }
    return value;
  }

  protected Iterator<String[]> iterate(String filename) throws IOException {
    BufferedReader br = UTF8IoUtils.readerFromFile(new File(ottSources, filename));
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

  protected LocalDate extractIssueDate() throws IOException {
    LOG.info("Downloading property file for version {} from {}", version, PROPERTY_FILE);
    var propF = new File(dir, "ott-properties.json");
    download.download(PROPERTY_FILE, propF);
    var prop = mapper.readValue(propF, OttProp.class);
    return LocalDate.parse(prop.date, DateTimeFormatter.BASIC_ISO_DATE);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued", extractIssueDate());
    super.addMetadata();
  }

  public static class OttProp {
    public String date;
    public String generated_on;
    public String legal;
    public String version;
    public String draft;
  }
}
