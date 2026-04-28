package org.catalogueoflife.data.silva;

import life.catalogue.coldp.ColdpTerm;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.*;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Shared base for the SILVA SSU and LSU taxonomy generators.
 *
 * Source file: tax_slv_{unit}_{version}.txt.gz
 * Format: tab-separated, 5 columns — path, taxid, rank, remark, release.
 * All taxa are accepted higher-rank entries; no synonyms, authors, or references.
 */
public abstract class BaseGenerator extends AbstractColdpGenerator {

  private static final String BASE_URL = "https://www.arb-silva.de/fileadmin/silva_databases/current/Exports/taxonomy/";
  private static final String VERSION_URL = "https://www.arb-silva.de/fileadmin/silva_databases/current/VERSION.txt";
  private static final Pattern VERSION_PAT = Pattern.compile("release[_-](\\d+\\.\\d+)", Pattern.CASE_INSENSITIVE);

  /** "ssu" or "lsu" */
  private final String unit;
  private String version;

  protected BaseGenerator(GeneratorConfig cfg, String unit) throws IOException {
    super(cfg, true);
    this.unit = unit;
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  @Override
  protected void prepare() throws Exception {
    version = http.get(VERSION_URL).trim();
    LOG.info("Detected SILVA version: {}", version);
  }

  @Override
  protected void addData() throws Exception {
    String filename = "tax_slv_" + unit + "_" + version + ".txt.gz";
    URI downloadUri = URI.create(BASE_URL + filename);
    File gz = download(filename, downloadUri);

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.status,
        ColdpTerm.remarks
    ));

    // Pass 1: build path → taxid map
    Map<String, Integer> pathMap = new HashMap<>();
    try (BufferedReader br = gzipReader(gz)) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] f = splitLine(line);
        if (f == null) continue;
        String path  = f[0].trim();
        int    taxid = parseId(f[1]);
        if (taxid > 0) pathMap.put(path, taxid);
      }
    }
    LOG.info("Pass 1: {} paths indexed", pathMap.size());

    // Pass 2: write NameUsage rows
    int n = 0;
    try (BufferedReader br = gzipReader(gz)) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] f = splitLine(line);
        if (f == null) continue;
        if (f.length < 3) { LOG.warn("Skipping short line: {}", line); continue; }
        String path   = f[0].trim();
        int    taxid  = parseId(f[1]);
        String rank   = f[2].trim();
        String remark = f.length > 3 ? f[3].trim() : "";
        if (taxid <= 0) continue;

        String name      = extractName(path);
        String parent    = parentPath(path);
        Integer parentId = parent != null ? pathMap.get(parent) : null;
        String remarks   = mapRemark(remark);

        writer.set(ColdpTerm.ID,            taxid);
        if (parentId != null) writer.set(ColdpTerm.parentID, parentId);
        if (!rank.isEmpty())  writer.set(ColdpTerm.rank,     rank);
        writer.set(ColdpTerm.scientificName, name);
        writer.set(ColdpTerm.status,         "accepted");
        if (remarks != null)  writer.set(ColdpTerm.remarks,  remarks);
        writer.next();
        n++;
      }
    }
    LOG.info("Pass 2: {} NameUsage records written", n);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("version", version);
    metadata.put("issued",  LocalDate.now().toString());
    // version with dot removed for the release-page URL, e.g. 138.2 → 1382
    metadata.put("versionNoDot", version.contains(".") ? version.replace(".", "") : version);
    super.addMetadata();
  }

  // ── Static helpers (package-private for testing) ──────────────────────────

  /** Splits a SILVA taxonomy line on TAB; returns null for blank lines. */
  static String[] splitLine(String line) {
    if (line == null || line.isBlank()) return null;
    return line.split("\t", -1);
  }

  /** Returns the taxon name: last non-empty semicolon-delimited segment of {@code path}. */
  static String extractName(String path) {
    String stripped = path.endsWith(";") ? path.substring(0, path.length() - 1) : path;
    int lastSemi = stripped.lastIndexOf(';');
    return lastSemi < 0 ? stripped : stripped.substring(lastSemi + 1);
  }

  /**
   * Returns the parent path (with trailing semicolon) for the given path,
   * or null if {@code path} is a root (single-segment) path.
   */
  static String parentPath(String path) {
    String stripped = path.endsWith(";") ? path.substring(0, path.length() - 1) : path;
    int lastSemi = stripped.lastIndexOf(';');
    if (lastSemi < 0) return null;
    return stripped.substring(0, lastSemi + 1);
  }

  /** Maps SILVA remark codes to human-readable ColDP remarks, or null if none. */
  static String mapRemark(String remark) {
    if (remark == null || remark.isBlank()) return null;
    return switch (remark.trim()) {
      case "a" -> "environmental origin";
      case "w" -> "scheduled for revision";
      default  -> null;
    };
  }

  // ── Private helpers ────────────────────────────────────────────────────────

  private static int parseId(String raw) {
    try { return Integer.parseInt(raw.trim()); }
    catch (NumberFormatException e) { return 0; }
  }

  static BufferedReader gzipReader(File gz) throws IOException {
    return new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(gz)), StandardCharsets.UTF_8));
  }
}
