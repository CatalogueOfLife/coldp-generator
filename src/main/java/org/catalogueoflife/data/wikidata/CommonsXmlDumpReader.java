package org.catalogueoflife.data.wikidata;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Streaming reader for the Wikimedia Commons XML dump.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li>Sequential: {@link #streamAll} does a single pass over the standard
 *       {@code commonswiki-latest-pages-articles.xml.bz2} (or the two older separate-pass
 *       methods {@link #streamGalleryPages} / {@link #streamFilePages}).
 *   <li>Parallel: {@link #streamAllParallel} uses the multistream variant
 *       ({@code …-pages-articles-multistream.xml.bz2}) plus its index file to
 *       decompress and parse many independent bzip2 streams concurrently.
 * </ul>
 *
 * <p><b>Key optimization:</b> a {@link PageFilter} is evaluated once the {@code <title>}
 * and {@code <ns>} elements have been read (both precede {@code <revision><text>} in the
 * MediaWiki XML schema). Text is only buffered for pages that pass the filter, saving
 * almost all allocation work for the ~99 % of pages that are discarded.
 */
public class CommonsXmlDumpReader {
  private static final Logger LOG = LoggerFactory.getLogger(CommonsXmlDumpReader.class);

  /** Number of threads used by {@link #streamAllParallel}. */
  public static final int DEFAULT_THREADS = Math.max(4, Runtime.getRuntime().availableProcessors());

  /** Metadata extracted from a File: page's {{Information}} template and license templates. */
  public record FileMetadata(String title, String created, String creator,
                             String license, String remarks) {}

  // ── Functional interfaces ─────────────────────────────────────────────────

  /**
   * Predicate evaluated when both {@code ns} and {@code title} are known (after {@code </ns>}).
   * Return {@code true} to buffer the page's wikitext and pass it to the {@link PageHandler}.
   */
  @FunctionalInterface
  interface PageFilter {
    boolean test(int ns, String title);
  }

  @FunctionalInterface
  interface PageHandler {
    void accept(int ns, String title, String text);
  }

  // ── Internal records for multistream support ──────────────────────────────

  /** Byte range [byteOffset, endOffset) of one independent bzip2 stream in the multistream file. */
  private record StreamDescriptor(long byteOffset, long endOffset, boolean hasNs0, boolean hasNs6) {}

  /** A parsed page triple returned by per-stream parallel tasks. */
  private record ParsedPage(int ns, String title, String text) {}

  // ── State ─────────────────────────────────────────────────────────────────

  private final File dumpFile;

  private static final XMLInputFactory XML_FACTORY;
  static {
    XML_FACTORY = XMLInputFactory.newInstance();
    XML_FACTORY.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    XML_FACTORY.setProperty("http://www.oracle.com/xml/jaxp/properties/totalEntitySizeLimit", 0);
  }

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
    PageFilter filter = (ns, title) -> ns == 0
        && (galleryNames.contains(title) || galleryNames.contains(title.replace('_', ' ')));
    streamPages(filter, (ns, title, text) -> {
      counts[0]++;
      if (counts[0] % 500_000 == 0) {
        LOG.info("Commons pass A: {} pages scanned, {} galleries found", counts[0], counts[1]);
      }
      String normalized = title.replace('_', ' ');
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
  static List<String> parseGalleryFiles(String text) {
    List<String> files = new ArrayList<>();
    Pattern galleryBlock = Pattern.compile("(?is)<gallery[^>]*>(.*?)</gallery>");
    Matcher m = galleryBlock.matcher(text);
    while (m.find()) {
      for (String line : m.group(1).split("\n")) {
        line = line.trim();
        if (line.isEmpty()) continue;
        String lower = line.toLowerCase();
        if (!lower.startsWith("file:") && !lower.startsWith("image:")) continue;
        int colon = line.indexOf(':');
        String nameAndCaption = line.substring(colon + 1);
        int pipe = nameAndCaption.indexOf('|');
        String filename = (pipe >= 0 ? nameAndCaption.substring(0, pipe) : nameAndCaption).trim();
        if (!filename.isEmpty()) {
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
    PageFilter filter = (ns, title) -> {
      if (ns != 6) return false;
      String filename = title.startsWith("File:") ? title.substring(5)
                      : title.startsWith("Image:") ? title.substring(6)
                      : title;
      return neededFiles.contains(filename.replace('_', ' '));
    };
    streamPages(filter, (ns, title, text) -> {
      counts[0]++;
      if (counts[0] % 2_000_000 == 0) {
        LOG.info("Commons pass B: {} pages scanned, {} file metadata records", counts[0], counts[1]);
      }
      String filename = title.startsWith("File:") ? title.substring(5)
                      : title.startsWith("Image:") ? title.substring(6)
                      : title;
      filename = filename.replace('_', ' ');
      FileMetadata meta = parseFileMetadata(text);
      handler.accept(filename, meta);
      counts[1]++;
    });
    LOG.info("Commons pass B complete: {} pages scanned, {} file metadata records", counts[0], counts[1]);
  }

  // ── Single combined pass ──────────────────────────────────────────────────

  /**
   * Performs a single sequential pass over the dump, processing both gallery pages (ns=0)
   * and file-description pages (ns=6) in one read of the bzip2 file.
   *
   * <p><b>Precondition:</b> Wikimedia XML dumps are namespace-sorted (all ns=0 pages precede
   * ns=6 pages). The set of needed files is therefore populated from galleries before any
   * ns=6 page is encountered, allowing efficient text-buffering skipping for both namespaces.
   */
  public void streamAll(Set<String> galleryNames,
                        BiConsumer<String, List<String>> galleryHandler,
                        BiConsumer<String, FileMetadata> fileHandler) throws Exception {
    LOG.info("Commons single pass: streaming gallery + file pages from {}", dumpFile.getName());
    long[] counts = {0, 0, 0}; // [pagesScanned, galleriesFound, metadataFound]
    Set<String> neededFiles = new HashSet<>();
    boolean[] seenNs6 = {false};

    // Filter: buffer text only for gallery name matches (ns=0) or needed file matches (ns=6).
    // Works correctly because ns=0 pages precede ns=6 pages in namespace-sorted dumps,
    // so neededFiles is fully populated before any ns=6 filter call is made.
    PageFilter filter = (ns, title) -> {
      if (ns == 0) return galleryNames.contains(title) || galleryNames.contains(title.replace('_', ' '));
      if (ns == 6) {
        String filename = title.startsWith("File:") ? title.substring(5)
                        : title.startsWith("Image:") ? title.substring(6)
                        : title;
        return neededFiles.contains(filename.replace('_', ' '));
      }
      return false;
    };

    streamPages(filter, (ns, title, text) -> {
      counts[0]++;
      if (counts[0] % 1_000_000 == 0) {
        LOG.info("Commons single pass: {} pages scanned, {} galleries, {} file metadata",
            counts[0], counts[1], counts[2]);
      }
      if (ns == 0) {
        if (seenNs6[0]) {
          LOG.error("Namespace ordering violation: ns=0 gallery page '{}' found after ns=6 pages — "
              + "some file metadata may be missed.", title);
        }
        String normalized = title.replace('_', ' ');
        String matchedName = galleryNames.contains(normalized) ? normalized : title;
        List<String> files = parseGalleryFiles(text);
        if (!files.isEmpty()) {
          galleryHandler.accept(matchedName, files);
          neededFiles.addAll(files);
          counts[1]++;
        }
      } else { // ns == 6
        seenNs6[0] = true;
        String filename = title.startsWith("File:") ? title.substring(5)
                        : title.startsWith("Image:") ? title.substring(6)
                        : title;
        filename = filename.replace('_', ' ');
        fileHandler.accept(filename, parseFileMetadata(text));
        counts[2]++;
      }
    });
    LOG.info("Commons single pass complete: {} pages scanned, {} galleries, {} file metadata records",
        counts[0], counts[1], counts[2]);
  }

  // ── Parallel multistream processing ──────────────────────────────────────

  /**
   * Processes the multistream Commons dump in parallel using {@code numThreads} threads.
   *
   * <p>Two parallel phases are run:
   * <ol>
   *   <li>Phase 1 — process streams containing ns=0 gallery pages; build {@code neededFiles}.
   *   <li>Phase 2 — process streams containing ns=6 file pages; emit file metadata.
   * </ol>
   *
   * @param indexFile  the companion {@code …-multistream-index.txt.bz2} file
   * @param numThreads number of parallel decompression/parse threads
   */
  public void streamAllParallel(File indexFile,
                                 int numThreads,
                                 Set<String> galleryNames,
                                 BiConsumer<String, List<String>> galleryHandler,
                                 BiConsumer<String, FileMetadata> fileHandler) throws Exception {
    LOG.info("Commons parallel: parsing index from {}", indexFile.getName());
    List<StreamDescriptor> streams = parseIndex(indexFile);
    LOG.info("Commons parallel: {} bzip2 streams discovered", streams.size());

    List<StreamDescriptor> ns0Streams = streams.stream().filter(StreamDescriptor::hasNs0).toList();
    List<StreamDescriptor> ns6Streams = streams.stream().filter(StreamDescriptor::hasNs6).toList();
    LOG.info("Commons parallel: {} ns0 streams, {} ns6 streams", ns0Streams.size(), ns6Streams.size());

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      // ── Phase 1: gallery pages ──────────────────────────────────────────
      PageFilter galleryFilter = (ns, title) -> ns == 0
          && (galleryNames.contains(title) || galleryNames.contains(title.replace('_', ' ')));

      List<Future<List<ParsedPage>>> phase1 = submitStreams(exec, ns0Streams, galleryFilter);
      LOG.info("Commons parallel phase 1: submitted {} tasks", phase1.size());

      Set<String> neededFiles = new HashSet<>();
      long galleryCount = 0;
      for (int i = 0; i < phase1.size(); i++) {
        for (ParsedPage page : phase1.get(i).get()) {
          String normalized = page.title().replace('_', ' ');
          String matchedName = galleryNames.contains(normalized) ? normalized : page.title();
          List<String> files = parseGalleryFiles(page.text());
          if (!files.isEmpty()) {
            galleryHandler.accept(matchedName, files);
            neededFiles.addAll(files);
            galleryCount++;
          }
        }
        if ((i + 1) % 100_000 == 0) LOG.info("Commons parallel phase 1: merged {}/{} tasks", i + 1, phase1.size());
      }
      LOG.info("Commons parallel phase 1 complete: {} galleries, {} needed files", galleryCount, neededFiles.size());

      // ── Phase 2: file description pages ────────────────────────────────
      PageFilter fileFilter = (ns, title) -> {
        if (ns != 6) return false;
        String filename = title.startsWith("File:") ? title.substring(5)
                        : title.startsWith("Image:") ? title.substring(6)
                        : title;
        return neededFiles.contains(filename.replace('_', ' '));
      };

      List<Future<List<ParsedPage>>> phase2 = submitStreams(exec, ns6Streams, fileFilter);
      LOG.info("Commons parallel phase 2: submitted {} tasks", phase2.size());

      long metaCount = 0;
      for (int i = 0; i < phase2.size(); i++) {
        for (ParsedPage page : phase2.get(i).get()) {
          String filename = page.title().startsWith("File:") ? page.title().substring(5)
                          : page.title().startsWith("Image:") ? page.title().substring(6)
                          : page.title();
          filename = filename.replace('_', ' ');
          fileHandler.accept(filename, parseFileMetadata(page.text()));
          metaCount++;
        }
        if ((i + 1) % 100_000 == 0) LOG.info("Commons parallel phase 2: merged {}/{} tasks", i + 1, phase2.size());
      }
      LOG.info("Commons parallel phase 2 complete: {} file metadata records", metaCount);
    } finally {
      exec.shutdown();
    }
  }

  private List<Future<List<ParsedPage>>> submitStreams(ExecutorService exec,
                                                        List<StreamDescriptor> streams,
                                                        PageFilter filter) {
    List<Future<List<ParsedPage>>> futures = new ArrayList<>(streams.size());
    for (StreamDescriptor sd : streams) {
      futures.add(exec.submit(() -> parseStreamAt(sd.byteOffset(), sd.endOffset() - sd.byteOffset(), filter)));
    }
    return futures;
  }

  // ── Multistream index parsing ─────────────────────────────────────────────

  /**
   * Parses the multistream index file ({@code …-multistream-index.txt.bz2}).
   * Each line has the format {@code byte_offset:page_id:title}.
   * Multiple consecutive lines may share the same byte_offset (one bzip2 stream = ~100 pages).
   */
  static List<StreamDescriptor> parseIndex(File indexFile) throws IOException {
    // Collect one entry per unique byte_offset, tracking namespace presence per stream.
    List<long[]>    streamOffsets = new ArrayList<>();  // [0] = byteOffset
    List<boolean[]> streamFlags   = new ArrayList<>();  // [0]=hasNs0, [1]=hasNs6

    try (InputStream raw = new BZip2CompressorInputStream(new FileInputStream(indexFile), false);
         BufferedReader br = new BufferedReader(new InputStreamReader(raw, StandardCharsets.UTF_8))) {
      long lastOffset = -1;
      boolean lastNs0 = false, lastNs6 = false;
      String line;
      while ((line = br.readLine()) != null) {
        // Format: offset:pageId:title  (title may contain colons, split on first two only)
        int c1 = line.indexOf(':');
        if (c1 < 0) continue;
        int c2 = line.indexOf(':', c1 + 1);
        if (c2 < 0) continue;
        long offset;
        try { offset = Long.parseLong(line, 0, c1, 10); } catch (NumberFormatException e) { continue; }
        String title = line.substring(c2 + 1);

        boolean isFile = title.startsWith("File:") || title.startsWith("Image:")
                      || title.startsWith("file:") || title.startsWith("image:");
        // Simple heuristic: titles with no recognized namespace prefix are ns=0 gallery candidates
        boolean isOtherNs = !isFile && title.contains(":");

        if (offset != lastOffset) {
          if (lastOffset >= 0) {
            streamOffsets.add(new long[]{lastOffset});
            streamFlags.add(new boolean[]{lastNs0, lastNs6});
          }
          lastOffset = offset;
          lastNs0 = false;
          lastNs6 = false;
        }
        if (isFile)         lastNs6 = true;
        else if (!isOtherNs) lastNs0 = true;
      }
      // Flush last stream
      if (lastOffset >= 0) {
        streamOffsets.add(new long[]{lastOffset});
        streamFlags.add(new boolean[]{lastNs0, lastNs6});
      }
    }

    // Build StreamDescriptors: endOffset of stream[i] = byteOffset of stream[i+1];
    // last stream uses Long.MAX_VALUE as sentinel (parseStreamAt clamps to actual file size).
    List<StreamDescriptor> result = new ArrayList<>(streamOffsets.size());
    for (int i = 0; i < streamOffsets.size(); i++) {
      long off = streamOffsets.get(i)[0];
      long end = (i + 1 < streamOffsets.size()) ? streamOffsets.get(i + 1)[0] : Long.MAX_VALUE;
      result.add(new StreamDescriptor(off, end, streamFlags.get(i)[0], streamFlags.get(i)[1]));
    }
    return result;
  }

  // ── Per-stream parallel parsing ───────────────────────────────────────────

  /**
   * Decompresses and parses one bzip2 stream from the multistream dump file.
   * Each stream is wrapped with a synthetic {@code <mediawiki>…</mediawiki>} envelope
   * so that StAX sees a valid XML document.
   *
   * @param byteOffset  start byte of the bzip2 stream in the dump file
   * @param length      byte length of the stream ({@code Long.MAX_VALUE} = read to EOF)
   * @param filter      text-buffering predicate (see {@link PageFilter})
   * @return list of pages that passed the filter and were parsed
   */
  private List<ParsedPage> parseStreamAt(long byteOffset, long length, PageFilter filter) throws Exception {
    // Read the compressed bytes for this stream
    byte[] compressed;
    try (FileChannel channel = FileChannel.open(dumpFile.toPath(), StandardOpenOption.READ)) {
      long fileSize = channel.size();
      long end = (length == Long.MAX_VALUE) ? fileSize : Math.min(byteOffset + length, fileSize);
      int len = (int) (end - byteOffset);
      if (len <= 0) return Collections.emptyList();
      ByteBuffer buf = ByteBuffer.allocate(len);
      long pos = byteOffset;
      while (buf.hasRemaining()) {
        int n = channel.read(buf, pos);
        if (n < 0) break;
        pos += n;
      }
      compressed = buf.array();
    }

    // Wrap with synthetic XML envelope: each multistream stream contains complete <page> elements
    // but not a full XML document, so we add <mediawiki> wrapper for StAX.
    byte[] header = "<mediawiki>".getBytes(StandardCharsets.UTF_8);
    byte[] footer = "</mediawiki>".getBytes(StandardCharsets.UTF_8);
    List<ParsedPage> result = new ArrayList<>();
    try (InputStream decompressed = new BZip2CompressorInputStream(new ByteArrayInputStream(compressed), false)) {
      InputStream xmlStream = new SequenceInputStream(
          new SequenceInputStream(new ByteArrayInputStream(header), decompressed),
          new ByteArrayInputStream(footer));
      parseXmlStream(xmlStream, filter, (ns, title, text) -> result.add(new ParsedPage(ns, title, text)));
    }
    return result;
  }

  // ── Core StAX streaming ───────────────────────────────────────────────────

  /**
   * Streams all pages from the bzip2-compressed dump file, calling handler only for pages
   * that pass the filter. Text is buffered only for pages that pass the filter (evaluated
   * after {@code </ns>}, when both namespace and title are known).
   */
  private void streamPages(PageFilter filter, PageHandler handler) throws Exception {
    try (InputStream in = new BZip2CompressorInputStream(new FileInputStream(dumpFile), true)) {
      parseXmlStream(in, filter, handler);
    }
  }

  /**
   * Core StAX XML parser shared by sequential ({@link #streamPages}) and parallel
   * ({@link #parseStreamAt}) code paths.
   *
   * <p>Text is only buffered when the {@code PageFilter} returns {@code true} after
   * {@code </ns>} is processed (title is known from the preceding {@code </title>}).
   */
  private static void parseXmlStream(InputStream in, PageFilter filter, PageHandler handler)
      throws Exception {
    XMLStreamReader xml = XML_FACTORY.createXMLStreamReader(
        new InputStreamReader(in, StandardCharsets.UTF_8));
    try {
      int ns = -1;
      String title = null;
      String text = null;
      boolean inRevision = false;
      boolean wantText = false;
      boolean inTitle = false, inNs = false, inText = false;
      // Reusable small buffer for <title> and <ns> (always short)
      StringBuilder smallBuf = new StringBuilder(256);
      // Lazily allocated text buffer — only created when wantText is true
      StringBuilder textBuf = null;

      int event;
      while ((event = xml.next()) != XMLStreamConstants.END_DOCUMENT) {
        switch (event) {
          case XMLStreamConstants.START_ELEMENT -> {
            switch (xml.getLocalName()) {
              case "page" -> {
                ns = -1; title = null; text = null;
                wantText = false; inTitle = false; inNs = false; inText = false; textBuf = null;
              }
              case "revision" -> inRevision = true;
              case "title"    -> { inTitle = true;  smallBuf.setLength(0); }
              case "ns"       -> { inNs    = true;  smallBuf.setLength(0); }
              case "text"     -> { if (inRevision && wantText) { inText = true; textBuf = new StringBuilder(); } }
            }
          }
          case XMLStreamConstants.END_ELEMENT -> {
            switch (xml.getLocalName()) {
              case "page" -> {
                if (title != null && text != null) handler.accept(ns, title, text);
                ns = -1; title = null; text = null;
              }
              case "revision" -> inRevision = false;
              case "title"    -> { title = smallBuf.toString().trim(); inTitle = false; }
              case "ns"       -> {
                try { ns = Integer.parseInt(smallBuf.toString().trim()); } catch (NumberFormatException ignored) {}
                inNs = false;
                // Both title and ns are now known — decide whether to buffer text.
                wantText = filter.test(ns, title);
              }
              case "text" -> {
                if (inText) { text = textBuf.toString(); inText = false; }
              }
            }
          }
          case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
            if (inTitle || inNs) {
              String s = xml.getText();
              if (!StringUtils.isBlank(s)) smallBuf.append(s);
            } else if (inText) {
              textBuf.append(xml.getText()); // preserve all whitespace in wikitext
            }
          }
        }
      }
    } finally {
      xml.close();
    }
  }

  // ── Wikitext parsing helpers ──────────────────────────────────────────────

  /** Parses {{Information}} fields and the first recognized license template. */
  static FileMetadata parseFileMetadata(String text) {
    String rawDesc   = extractTemplateField(text, "description");
    String rawDate   = extractTemplateField(text, "date");
    String rawAuthor = extractTemplateField(text, "author");

    String title   = rawDesc   != null ? stripWikiMarkup(stripLang(rawDesc)).trim()   : null;
    String created = rawDate   != null ? stripWikiMarkup(rawDate).trim()              : null;
    String creator = rawAuthor != null ? stripWikiMarkup(rawAuthor).trim()            : null;
    String license = findLicense(text);

    String remarks = null;
    if (title != null && title.length() > 80) {
      remarks = title;
      int nl  = title.indexOf('\n');
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

  /**
   * Extracts the value of a named field from an {{Information|field=value|…}} template.
   * Handles multiline values.
   */
  static String extractTemplateField(String text, String field) {
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
    value = value.replaceAll("\\[\\[[^\\[\\]|]*\\|([^\\[\\]]*)\\]\\]", "$1");
    value = value.replaceAll("\\[\\[([^\\[\\]]*)\\]\\]", "$1");
    value = value.replaceAll("<[^>]+>", "");
    value = value.replaceAll("\\{\\{[^{}]*\\}\\}", "");
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

  private static String emptyToNull(String s) {
    return (s == null || s.isBlank()) ? null : s;
  }
}
