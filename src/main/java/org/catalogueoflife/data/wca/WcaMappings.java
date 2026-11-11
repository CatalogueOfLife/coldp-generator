package org.catalogueoflife.data.wca;

import life.catalogue.common.io.Resources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the curated WCA 2030 crop tables from the classpath and holds the pure mappings used by the generator.
 * The tables are built from the PDF by scripts/gen_wca_tsv.py.
 */
public class WcaMappings {
  static final String ICC_FILE = "wca/icc.tsv";
  static final String CROPS_FILE = "wca/crops.tsv";
  // 01919.90* (fodder)
  private static final Pattern CPC = Pattern.compile("^(\\d{4,5}(?:\\.\\d{2})?)(\\*)?\\s*(?:\\((.+)\\))?$");

  /** One class of the Indicative Crop Classification. cropType is T, P or blank for mixed groups. */
  public record IccClass(String code, String parent, String label, String cropType) {}

  /** One row of Annex 7: a crop with one of its botanical names. */
  public record Crop(String crop, String scientificName, String kingdom, String icc11, String icc12, String cpc,
                     String verbatimName) {}

  /** A CPC 3.0 code of a crop. partial marks the asterisk: the CPC class is shared with other crops. */
  public record CpcCode(String code, boolean partial, String note) {}

  static Map<String, IccClass> readIcc() throws IOException {
    Map<String, IccClass> icc = new LinkedHashMap<>();
    for (String[] row : read(ICC_FILE)) {
      icc.put(row[0], new IccClass(row[0], row[1], row[2], row[3]));
    }
    return icc;
  }

  static List<Crop> readCrops() throws IOException {
    List<Crop> crops = new ArrayList<>();
    for (String[] row : read(CROPS_FILE)) {
      crops.add(new Crop(row[0], row[1], row[2], row[3], row[4], row[5], row[6]));
    }
    return crops;
  }

  private static List<String[]> read(String resource) throws IOException {
    List<String[]> rows = new ArrayList<>();
    try (var in = Resources.stream(resource)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource " + resource);
      }
      var br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      br.readLine(); // header
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.isBlank()) {
          rows.add(line.split("\t", -1));
        }
      }
    }
    return rows;
  }

  /** A stable id from a scientific name: lower case, the hybrid sign as x, everything else as hyphens. */
  static String id(String name) {
    return name.toLowerCase(Locale.ROOT)
        .replace('×', 'x')
        .replaceAll("[^a-z0-9]+", "-")
        .replaceAll("^-|-$", "");
  }

  /** A single word is a genus, e.g. Tuber; other ranks, incl. the informal "Rubus spp.", are left to the name parser. */
  static String rank(String name) {
    return name.trim().contains(" ") ? null : "genus";
  }

  /** The labels from the ICC group down to the given class, e.g. Cereals > Wheat. */
  static String path(Map<String, IccClass> icc, String code) {
    LinkedList<String> labels = new LinkedList<>();
    for (IccClass c = icc.get(code); c != null; c = c.parent().isEmpty() ? null : icc.get(c.parent())) {
      labels.addFirst(c.label());
    }
    return String.join(" > ", labels);
  }

  /**
   * Temporary or permanent, from the class itself or the nearest higher class that says so.
   * Groups mixing both (e.g. oilseed crops) leave it to their classes.
   */
  static String cropType(Map<String, IccClass> icc, String code) {
    for (IccClass c = icc.get(code); c != null; c = c.parent().isEmpty() ? null : icc.get(c.parent())) {
      switch (c.cropType()) {
        case "T": return "temporary";
        case "P": return "permanent";
        default: // ask the parent
      }
    }
    return null;
  }

  /** Splits a CPC cell like "01919.90* (fodder), 01259* (red)" into its codes. */
  static List<CpcCode> cpc(String cell) {
    List<CpcCode> codes = new ArrayList<>();
    if (cell == null || cell.isBlank()) return codes;
    for (String part : cell.split("\\s*,\\s*")) {
      Matcher m = CPC.matcher(part.trim());
      if (!m.matches()) {
        throw new IllegalArgumentException("Bad CPC code " + part + " in " + cell);
      }
      codes.add(new CpcCode(m.group(1), m.group(2) != null, m.group(3)));
    }
    return codes;
  }
}
