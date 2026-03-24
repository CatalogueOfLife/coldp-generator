package org.catalogueoflife.data.utils;

import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Deduplicates ColDP Reference records written to a {@link TermWriter}.
 *
 * <p>Keyed by the normalised citation string. IDs are auto-assigned as {@code R1, R2, ...}
 * unless an explicit ID is provided. This replaces the ad-hoc {@code HashMap<String, Integer>}
 * ref-dedup patterns found in multiple generators (BirdLife, bats, MDD).
 *
 * <p>Usage:
 * <pre>{@code
 *   RefCache refCache = new RefCache(refWriter);
 *
 *   // Simple citation-only record:
 *   String refId = refCache.getOrCreate(citation);
 *
 *   // Citation with additional fields:
 *   String refId = refCache.getOrCreate(citation, w -> {
 *     w.set(ColdpTerm.link, url);
 *     w.set(ColdpTerm.doi, doi);
 *   });
 * }</pre>
 */
public class RefCache {

  private final TermWriter refWriter;
  private final Map<String, String> citationToId = new HashMap<>();
  private int counter = 1;

  public RefCache(TermWriter refWriter) {
    this.refWriter = refWriter;
  }

  /**
   * Returns the ID for a reference with the given citation, writing a new record if this
   * citation has not been seen before. Returns null for blank citations.
   */
  public String getOrCreate(String citation) throws IOException {
    return getOrCreate(citation, w -> {});
  }

  /**
   * Returns the ID for a reference with the given citation, writing a new record if this
   * citation has not been seen before. The {@code fieldSetter} is called on the writer
   * to populate additional fields (link, doi, etc.) for new records.
   * Returns null for blank citations.
   */
  public String getOrCreate(String citation, Consumer<TermWriter> fieldSetter) throws IOException {
    if (citation == null || citation.isBlank()) return null;
    String key = citation.strip();
    String existing = citationToId.get(key);
    if (existing != null) return existing;
    try {
      String id = "R" + counter++;
      refWriter.set(ColdpTerm.ID, id);
      refWriter.set(ColdpTerm.citation, citation);
      fieldSetter.accept(refWriter);
      refWriter.next();
      citationToId.put(key, id);
      return id;
    } catch (IOException e) {
      throw e;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  /** Returns the number of unique references written so far. */
  public int size() {
    return citationToId.size();
  }
}
