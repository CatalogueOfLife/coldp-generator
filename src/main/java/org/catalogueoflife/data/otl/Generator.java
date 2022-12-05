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
package org.catalogueoflife.data.otl;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import life.catalogue.api.model.SimpleName;
import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.CompressionUtil;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.io.FileUtils;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.newick.SimpleNode;
import org.catalogueoflife.newick.SimpleParser;
import org.gbif.nameparser.api.Rank;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Open Tree Synthesis release transformer to ColDP
 * https://tree.opentreeoflife.org/about/synthesis-release/v13.4
 */
public class Generator extends org.catalogueoflife.data.ott.Generator {
  private static final String VERSION = "13.4";
  private static final LocalDate ISSUED = LocalDate.of(2021,6,18);
  private static final URI DOWNLOAD = URI.create("http://files.opentreeoflife.org/synthesis/opentree" + VERSION + "/opentree" + VERSION + ".tgz");
  private static final Pattern OTT_PATTERN = Pattern.compile("ott(\\d+)");
  @VisibleForTesting
  protected final Int2ObjectMap<OttName> ott = new Int2ObjectOpenHashMap<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, DOWNLOAD, VERSION, ISSUED);
  }

  static class OttName {
    public final String name;
    public final String rank;

    public OttName(String name, String rank) {
      this.name = name;
      this.rank = rank;
    }
  }

  @Override
  protected void prepare() throws IOException {
    System.out.println("Unpack archive " + src);
    sources = new File(dir, "sources");
    CompressionUtil.decompressFile(sources, src);

    System.out.println("Read OTT taxonomy");
    var iter = iterate("taxonomy.tsv");
    while (iter.hasNext()) {
      var row = iter.next();
      OttName sn = new OttName(row[2], translateRank(row[3]));
      int id = Integer.parseInt(row[0]);
      ott.put(id, sn);
    }

    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.rank,
      ColdpTerm.status
    ));
  }

  private OttName lookupOTT(int id) {
    return ott.getOrDefault(id, new OttName("OTT"+id, "unranked"));
  }

  @VisibleForTesting
  protected void writeNode(SimpleNode n, @Nullable SimpleNode parent) throws IOException {
    OttName sn = null;
    if (n.getLabel().startsWith("mrc")) {
      StringBuilder name = new StringBuilder();
      name.append("[");
      var m = OTT_PATTERN.matcher(n.getLabel());
      while (m.find()) {
        int id = Integer.parseInt(m.group(1));
        var ottSN = lookupOTT(id);
        if (name.length()>1) {
          name.append(" + ");
        }
        name.append(ottSN.name);
      }
      name.append("]");
      sn = new OttName(name.toString(), "clade");

    } else if (n.getLabel().startsWith("ott")) {
      int ott = Integer.parseInt(n.getLabel().substring(3));
      sn = lookupOTT(ott);

    } else {
      System.out.println("Unknown node label " + n.getLabel());
    }
    writer.set(ColdpTerm.ID, n.getLabel());
    if (parent != null) {
      writer.set(ColdpTerm.parentID, parent.getLabel());
    }
    if (sn != null) {
      writer.set(ColdpTerm.scientificName, sn.name);
      writer.set(ColdpTerm.rank, sn.rank);
      writer.set(ColdpTerm.status, "accepted");
    }
    writer.next();

    if (n.getChildren() != null) {
      for (var c : n.getChildren()) {
        writeNode(c, n);
      }
    }
  }

  @Override
  protected void addData() throws Exception {
    System.out.println("Read Newick tree");
    Reader br = UTF8IoUtils.readerFromFile(new File(sources, "labelled_supertree.tre"));
    var p = new SimpleParser(br);
    var root = p.parse();

    System.out.println("Process tree");
    writeNode(root, null);

    System.out.println("Remove source files");
    FileUtils.deleteQuietly(sources);
  }

}
