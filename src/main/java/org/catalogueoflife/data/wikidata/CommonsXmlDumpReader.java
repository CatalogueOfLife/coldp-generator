package org.catalogueoflife.data.wikidata;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Two-pass streaming reader for the Wikimedia Commons XML dump
 * (commonswiki-latest-pages-articles.xml.bz2).
 *
 * Pass A ({@link #streamGalleryPages}): namespace-0 pages → gallery name → list of filenames.
 * Pass B ({@link #streamFilePages}):    namespace-6 File: pages → filename → FileMetadata.
 *
 * XML streaming reuses the same BZip2 + StAX pattern as wikispecies/Generator.java.
 */
public class CommonsXmlDumpReader {
  private static final Logger LOG = LoggerFactory.getLogger(CommonsXmlDumpReader.class);

  /** Metadata extracted from a File: page's {{Information}} template and license templates. */
  public record FileMetadata(String title, String created, String creator,
                             String license, String remarks) {}

  private final File dumpFile;

  public CommonsXmlDumpReader(File dumpFile) {
    this.dumpFile = dumpFile;
  }

  // ── Pass A: gallery pages (namespace 0) ──────────────────────────────────

  /**
   * Streams namespace-0 pages whose title matches a known gallery name.
   * Parses {@code <gallery>…</gallery>} blocks and calls handler with
   * (galleryName, list-of-filenames).
   */
  public void streamGalleryPages(Set<String> galleryNames,
                                  BiConsumer<String, List<String>> handler) throws Exception {
    LOG.info("Commons pass A: streaming gallery pages from {}", dumpFile.getName());
    long[] counts = {0, 0}; // [pagesScanned, galleriesFound]
    streamPages((ns, title, text) -> {
      counts[0]++;
      if (counts[0] % 500_000 == 0) {
        LOG.info("Commons pass A: {} pages scanned, {} galleries found", counts[0], counts[1]);
      }
      if (ns != 0) return;
      // Normalize title the same way gallery names from P935 are stored
      String normalized = title.replace('_', ' ');
      if (!galleryNames.contains(normalized) && !galleryNames.contains(title)) return;
      String matchedName = galleryNames.contains(normalized) ? normalized : title;
      List<String> files = parseGalleryFiles(text);
      if (!files.isEmpty()) {
        handler.accept(matchedName, files);
        counts[1]++;
      }
    });
    LOG.info("Commons pass A complete: {} pages scanned, {} galleries with files", counts[0], counts[1]);
  }

  /** Extracts File: names from all {@code <gallery>…</gallery>} blocks in wikitext. */
  private static List<String> parseGalleryFiles(String text) {
    List<String> files = new ArrayList<>();
    // Find all <gallery>…</gallery> blocks (case-insensitive, possibly with attributes)
    Pattern galleryBlock = Pattern.compile("(?is)<gallery[^>]*>(.*?)</gallery>");
    Matcher m = galleryBlock.matcher(text);
    while (m.find()) {
      for (String line : m.group(1).split("\n")) {
        line = line.trim();
        if (line.isEmpty()) continue;
        // Each line: File:Name.jpg|optional caption
        // Also accept Image: prefix (older alias)
        String lower = line.toLowerCase();
        if (!lower.startsWith("file:") && !lower.startsWith("image:")) continue;
        int colon = line.indexOf(':');
        String nameAndCaption = line.substring(colon + 1);
        int pipe = nameAndCaption.indexOf('|');
        String filename = (pipe >= 0 ? nameAndCaption.substring(0, pipe) : nameAndCaption).trim();
        if (!filename.isEmpty()) {
          // Normalize: store with spaces (underscore-to-space)
          files.add(filename.replace('_', ' '));
        }
      }
    }
    return files;
  }

  // ── Pass B: file description pages (namespace 6) ─────────────────────────

  /**
   * Streams namespace-6 (File:) pages whose normalized filename is in {@code neededFiles}.
   * Parses the {{Information}} template and license templates, then calls handler with
   * (filename, FileMetadata).
   */
  public void streamFilePages(Set<String> neededFiles,
                               BiConsumer<String, FileMetadata> handler) throws Exception {
    LOG.info("Commons pass B: streaming file pages from {}", dumpFile.getName());
    long[] counts = {0, 0}; // [pagesScanned, metadataFound]
    streamPages((ns, title, text) -> {
      counts[0]++;
      if (counts[0] % 2_000_000 == 0) {
        LOG.info("Commons pass B: {} pages scanned, {} file metadata records", counts[0], counts[1]);
      }
      if (ns != 6) return;
      // Strip "File:" prefix
      String filename = title.startsWith("File:") ? title.substring(5)
                      : title.startsWith("Image:") ? title.substring(6)
                      : title;
      // Normalize to match the names stored with spaces
      filename = filename.replace('_', ' ');
      if (!neededFiles.contains(filename)) return;
      FileMetadata meta = parseFileMetadata(text);
      handler.accept(filename, meta);
      counts[1]++;
    });
    LOG.info("Commons pass B complete: {} pages scanned, {} file metadata records", counts[0], counts[1]);
  }

  /** Parses {{Information}} fields and the first recognized license template. */
  private static FileMetadata parseFileMetadata(String text) {
    String rawDesc   = extractTemplateField(text, "description");
    String rawDate   = extractTemplateField(text, "date");
    String rawAuthor = extractTemplateField(text, "author");

    String title   = rawDesc   != null ? stripWikiMarkup(stripLang(rawDesc)).trim()   : null;
    String created = rawDate   != null ? stripWikiMarkup(rawDate).trim()              : null;
    String creator = rawAuthor != null ? stripWikiMarkup(rawAuthor).trim()            : null;
    String license = findLicense(text);

    // Put the full cleaned description in remarks if it is richer than the title
    String remarks = null;
    if (title != null && title.length() > 80) {
      remarks = title;
      // Shorten title to first sentence / line
      int nl = title.indexOf('\n');
      int dot = title.indexOf(". ");
      int cut = (nl >= 0 && dot >= 0) ? Math.min(nl, dot)
              : (nl >= 0) ? nl
              : (dot >= 0) ? dot
              : -1;
      title = (cut > 0) ? title.substring(0, cut).trim() : title;
    }

    return new FileMetadata(
        emptyToNull(title),
        emptyToNull(created),
        emptyToNull(creator),
        license,
        emptyToNull(remarks)
    );
  }

  // ── Wikitext parsing helpers ──────────────────────────────────────────────

  /**
   * Extracts the value of a named field from an {{Information|field=value|…}} template.
   * Handles multiline values.
   */
  static String extractTemplateField(String text, String field) {
    // Match |field = value up to next | or }}
    Pattern p = Pattern.compile(
        "\\|\\s*" + Pattern.quote(field) + "\\s*=\\s*(.*?)(?=\\n\\s*\\||\\}\\})",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    Matcher m = p.matcher(text);
    if (m.find()) {
      return m.group(1).trim();
    }
    return null;
  }

  /**
   * Strips language wrapper templates such as {@code {{en|text}}}, {@code {{lang|en|text}}}.
   * If an English block exists it is preferred; otherwise the first block is returned.
   */
  static String stripLang(String value) {
    if (value == null) return null;
    // {{en|...}} or {{EN|...}}
    Pattern langPat = Pattern.compile("\\{\\{([a-z]{2,3})\\|([^{}]*)\\}\\}", Pattern.CASE_INSENSITIVE);
    Matcher m = langPat.matcher(value);
    String first = null;
    while (m.find()) {
      String lang = m.group(1).toLowerCase();
      String content = m.group(2).trim();
      if (first == null) first = content;
      if (lang.equals("en")) return content;
    }
    return first != null ? first : value;
  }

  /**
   * Strips wiki markup: [[X|Y]]→Y, [[X]]→X, HTML tags, remaining {{…}} templates.
   */
  static String stripWikiMarkup(String value) {
    if (value == null) return null;
    // [[Target|Display]] → Display
    value = value.replaceAll("\\[\\[[^\\[\\]|]*\\|([^\\[\\]]*)\\]\\]", "$1");
    // [[Target]] → Target
    value = value.replaceAll("\\[\\[([^\\[\\]]*)\\]\\]", "$1");
    // HTML tags
    value = value.replaceAll("<[^>]+>", "");
    // {{…}} templates (non-nested)
    value = value.replaceAll("\\{\\{[^{}]*\\}\\}", "");
    // Trim extra whitespace
    value = value.replaceAll("\\s+", " ").trim();
    return value;
  }

  /** Returns the short name of the first recognized license template found in the page text. */
  static String findLicense(String text) {
    Pattern licPat = Pattern.compile("\\{\\{\\s*([^|}\\n]+?)\\s*(?:\\|[^}]*)?\\}\\}");
    Matcher m = licPat.matcher(text);
    while (m.find()) {
      String tname = m.group(1).trim();
      String upper = tname.toUpperCase();
      // Recognize common license template prefixes
      if (upper.startsWith("CC-BY-SA")   || upper.startsWith("CC BY-SA"))  return normLicense("CC BY-SA", tname);
      if (upper.startsWith("CC-BY")      || upper.startsWith("CC BY"))     return normLicense("CC BY",    tname);
      if (upper.startsWith("CC0")        || upper.startsWith("CC-ZERO")
                                         || upper.startsWith("CC ZERO"))   return "CC0";
      if (upper.startsWith("GFDL"))                                        return "GFDL";
      if (upper.startsWith("FAL"))                                         return "FAL";
      if (upper.startsWith("PD-"))                                         return tname;
      if (upper.startsWith("PUBLIC DOMAIN"))                               return "PD";
    }
    return null;
  }

  /** Extracts version number from template name to produce e.g. "CC BY-SA 4.0". */
  private static String normLicense(String prefix, String tname) {
    // Extract version number like "4.0", "3.0", "2.0" from the template name
    Matcher ver = Pattern.compile("(\\d+\\.\\d+)").matcher(tname);
    return ver.find() ? prefix + " " + ver.group(1) : prefix;
  }

  // ── MIME / type helpers (public static — reused by Generator.java) ────────

  private static final Map<String, String> EXT_TO_MIME = buildExtMimeMap();

  private static Map<String, String> buildExtMimeMap() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("jpg",  "image/jpeg");
    m.put("jpeg", "image/jpeg");
    m.put("png",  "image/png");
    m.put("svg",  "image/svg+xml");
    m.put("tif",  "image/tiff");
    m.put("tiff", "image/tiff");
    m.put("gif",  "image/gif");
    m.put("webp", "image/webp");
    m.put("ogg",  "audio/ogg");
    m.put("oga",  "audio/ogg");
    m.put("flac", "audio/flac");
    m.put("wav",  "audio/wav");
    m.put("mp3",  "audio/mpeg");
    m.put("ogv",  "video/ogg");
    m.put("webm", "video/webm");
    m.put("mp4",  "video/mp4");
    m.put("pdf",  "application/pdf");
    return Collections.unmodifiableMap(m);
  }

  /** Returns the MIME type for a Commons filename based on its extension, or null if unknown. */
  public static String mimeFromFilename(String filename) {
    if (filename == null) return null;
    int dot = filename.lastIndexOf('.');
    if (dot < 0) return null;
    return EXT_TO_MIME.get(filename.substring(dot + 1).toLowerCase());
  }

  /** Maps a MIME type to the ColDP media type string (image/audio/video/other). */
  public static String typeFromMime(String mime) {
    if (mime == null) return null;
    if (mime.startsWith("image/")) return "image";
    if (mime.startsWith("audio/")) return "audio";
    if (mime.startsWith("video/")) return "video";
    return "other";
  }

  // ── Core StAX streaming (same pattern as wikispecies/Generator.java) ─────

  @FunctionalInterface
  interface PageHandler {
    void accept(int ns, String title, String text);
  }

  /** Streams all pages from the bzip2-compressed XML dump, calling handler per page. */
  private void streamPages(PageHandler handler) throws Exception {
    var factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty("http://www.oracle.com/xml/jaxp/properties/totalEntitySizeLimit", 0);
    try (InputStream in = new BZip2CompressorInputStream(new FileInputStream(dumpFile), true)) {
      XMLStreamReader xml = factory.createXMLStreamReader(
          new InputStreamReader(in, StandardCharsets.UTF_8));

      int ns = -1;
      String title = null;
      String text = null;
      boolean inRevision = false;
      StringBuilder buf = null;

      int event;
      while ((event = xml.next()) != XMLStreamConstants.END_DOCUMENT) {
        switch (event) {
          case XMLStreamConstants.START_ELEMENT -> {
            buf = new StringBuilder();
            switch (xml.getLocalName()) {
              case "page" -> { ns = -1; title = null; text = null; }
              case "revision" -> inRevision = true;
            }
          }
          case XMLStreamConstants.END_ELEMENT -> {
            String localName = xml.getLocalName();
            switch (localName) {
              case "page" -> {
                if (title != null && text != null) handler.accept(ns, title, text);
                ns = -1; title = null; text = null;
              }
              case "revision" -> inRevision = false;
              case "ns"    -> { try { ns = Integer.parseInt(buf.toString().trim()); } catch (NumberFormatException ignored) {} }
              case "title" -> title = buf.toString().trim();
              case "text"  -> { if (inRevision) text = buf.toString(); }
            }
          }
          case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
            if (buf != null) {
              String s = xml.getText();
              if (!StringUtils.isBlank(s)) buf.append(s);
            }
          }
        }
      }
    }
  }

  private static String emptyToNull(String s) {
    return (s == null || s.isBlank()) ? null : s;
  }
}
