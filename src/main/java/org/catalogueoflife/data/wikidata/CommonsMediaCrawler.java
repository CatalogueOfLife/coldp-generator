package org.catalogueoflife.data.wikidata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.catalogueoflife.data.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Crawls Wikimedia Commons gallery pages for taxon media via the MediaWiki API.
 * HTTP fetches run in a configurable thread pool; results are queued and consumed
 * on the caller's thread so that TSV writing remains single-threaded.
 *
 * Usage:
 * <pre>
 *   crawler.submitAll(galleries);
 *   crawler.awaitAndDrain(record -> writer.write(record));
 * </pre>
 */
public class CommonsMediaCrawler {
  private static final Logger LOG = LoggerFactory.getLogger(CommonsMediaCrawler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String API = "https://commons.wikimedia.org/w/api.php";
  private static final Map<String, String> HEADERS = Map.of(
      "User-Agent", "ColDPGenerator/1.0 (contact@catalogueoflife.org)");
  private static final Pattern HTML_TAGS = Pattern.compile("<[^>]+>");
  // Sentinel to signal queue completion
  private static final MediaRecord DONE = new MediaRecord(null, null, null, null, null, null, null, null, null);

  private final ExecutorService executor;
  private final HttpUtils http;
  private final LinkedBlockingQueue<MediaRecord> queue;
  private final AtomicInteger pending = new AtomicInteger();
  private final AtomicInteger mediaCount = new AtomicInteger();
  private final AtomicInteger errorCount = new AtomicInteger();

  /** One crawled image record. */
  public record MediaRecord(
      String taxonID, String url, String type, String format,
      String title, String created, String creator, String license, String link) {}

  public CommonsMediaCrawler(int threads, HttpUtils http) {
    this.executor = Executors.newFixedThreadPool(threads);
    this.http = http;
    this.queue = new LinkedBlockingQueue<>(50_000);
  }

  /** Submit crawl tasks for all galleries. Non-blocking. */
  public void submitAll(Map<String, String> galleries) {
    pending.set(galleries.size());
    for (var entry : galleries.entrySet()) {
      executor.submit(() -> crawlGallery(entry.getKey(), entry.getValue()));
    }
    executor.shutdown();
  }

  /**
   * Block until all crawl tasks complete, draining the result queue by calling
   * {@code consumer} for each record. Must be called from the writer thread.
   */
  public void awaitAndDrain(java.util.function.Consumer<MediaRecord> consumer)
      throws InterruptedException {
    while (true) {
      MediaRecord r = queue.poll(5, TimeUnit.SECONDS);
      if (r == null) continue; // timeout, keep waiting
      if (r == DONE) break;
      consumer.accept(r);
    }
    LOG.info("Media crawl complete: {} images written, {} errors", mediaCount.get(), errorCount.get());
  }

  private void crawlGallery(String taxonQid, String galleryName) {
    try {
      String continueToken = null;
      do {
        String url = buildApiUrl(galleryName, continueToken);
        String json = http.get(URI.create(url), HEADERS);
        JsonNode root = MAPPER.readTree(json);
        continueToken = extractContinueToken(root);
        JsonNode pages = root.path("query").path("pages");
        for (JsonNode page : pages) {
          JsonNode arr = page.path("imageinfo");
          if (!arr.isArray() || arr.isEmpty()) continue;
          JsonNode info = arr.get(0);
          MediaRecord record = toRecord(taxonQid, info);
          if (record != null) {
            queue.put(record);
            mediaCount.incrementAndGet();
          }
        }
      } while (continueToken != null);
    } catch (Exception e) {
      LOG.warn("Failed to crawl gallery '{}' for {}: {}", galleryName, taxonQid, e.getMessage());
      errorCount.incrementAndGet();
    } finally {
      if (pending.decrementAndGet() == 0) {
        try {
          queue.put(DONE);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private String buildApiUrl(String galleryName, String continueToken) {
    String encoded = URLEncoder.encode(galleryName, StandardCharsets.UTF_8);
    StringBuilder sb = new StringBuilder(API)
        .append("?action=query&titles=").append(encoded)
        .append("&generator=images&prop=imageinfo")
        .append("&iiprop=url%7Cextmetadata%7Cmime%7Csize")
        .append("&format=json&formatversion=2&gimlimit=50");
    if (continueToken != null) {
      sb.append("&gimcontinue=").append(URLEncoder.encode(continueToken, StandardCharsets.UTF_8));
    }
    return sb.toString();
  }

  private String extractContinueToken(JsonNode root) {
    JsonNode cont = root.path("continue").path("gimcontinue");
    return cont.isMissingNode() ? null : cont.asText(null);
  }

  private MediaRecord toRecord(String taxonQid, JsonNode info) {
    String url = info.path("url").asText(null);
    if (url == null) return null;
    String link = info.path("descriptionurl").asText(null);
    String mime = info.path("mime").asText(null);
    String type = mimeToType(mime);
    JsonNode meta = info.path("extmetadata");
    String license = meta.path("LicenseShortName").path("value").asText(null);
    String creator = stripHtml(meta.path("Artist").path("value").asText(null));
    String title = stripHtml(meta.path("ImageDescription").path("value").asText(null));
    String created = meta.path("DateTimeOriginal").path("value").asText(null);
    return new MediaRecord(taxonQid, url, type, mime, title, created, creator, license, link);
  }

  private static String mimeToType(String mime) {
    if (mime == null) return null;
    if (mime.startsWith("image/")) return "image";
    if (mime.startsWith("audio/")) return "audio";
    if (mime.startsWith("video/")) return "video";
    return "other";
  }

  private static String stripHtml(String html) {
    if (html == null) return null;
    return HTML_TAGS.matcher(html).replaceAll("").trim();
  }
}
