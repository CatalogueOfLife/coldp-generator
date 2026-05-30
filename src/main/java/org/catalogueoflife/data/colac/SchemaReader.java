package org.catalogueoflife.data.colac;

import life.catalogue.api.model.Citation;
import life.catalogue.api.model.CslName;
import life.catalogue.common.date.FuzzyDate;
import life.catalogue.common.io.TermWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

/**
 * Base for the two era-specific CoL Annual Checklist readers. Holds the shared writers and
 * provides helpers for building the GSD source registry. Each reader streams its database
 * and writes ColDP rows through the writers created by {@link Generator}.
 */
abstract class SchemaReader {
  protected static final Logger LOG = LoggerFactory.getLogger(SchemaReader.class);

  protected final Generator g;
  protected final Connection conn;
  protected final TermWriter nameW;
  protected final TermWriter refW;
  protected final TermWriter vernW;
  protected final TermWriter distW;

  SchemaReader(Generator g, Connection conn) {
    this.g = g;
    this.conn = conn;
    this.nameW = g.nameUsageWriter();
    this.refW = g.referenceWriter();
    this.vernW = g.vernWriter;
    this.distW = g.distWriter;
  }

  abstract void read() throws Exception;

  /**
   * Creates a forward-only streaming statement so the multi-million row tables are not buffered
   * entirely in client memory. MariaDB Connector/J streams progressively for any positive fetch
   * size. Only one streaming ResultSet may be open per connection at a time, so callers must
   * fully consume each result before starting the next.
   */
  Statement streamStmt() throws SQLException {
    Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    st.setFetchSize(1000);
    return st;
  }

  /**
   * Registers a contributing source database (GSD) as a ColDP source citation referenced by
   * NameUsage.sourceID, and records its release date as a candidate archive issued date.
   *
   * @param id        ColDP source id, e.g. {@code d12}
   * @param title     full GSD name
   * @param author    GSD authors/editors/custodian (may be null)
   * @param version   GSD version (may be null)
   * @param releaseDate GSD release date (may be null)
   */
  void addSourceCitation(String id, String title, String author, String version, LocalDate releaseDate) {
    Citation c = new Citation();
    c.setId(id);
    if (title != null && !title.isBlank()) c.setTitle(title.trim());
    if (author != null && !author.isBlank()) {
      CslName n = new CslName(author.trim());
      n.setIsInstitution(true);
      c.setAuthor(List.of(n));
    }
    if (version != null && !version.isBlank()) c.setVersion(version.trim());
    if (releaseDate != null) {
      c.setIssued(FuzzyDate.of(releaseDate.getYear()));
      g.noteReleaseDate(releaseDate);
    }
    g.sources().add(c);
  }

  /** Parses an SQL date string (e.g. "2015-03-01", possibly "0000-00-00" or null) to a LocalDate, or null. */
  static LocalDate parseDate(String s) {
    if (s == null || s.length() < 10 || s.startsWith("0000")) return null;
    try {
      return LocalDate.parse(s.substring(0, 10));
    } catch (Exception e) {
      return null;
    }
  }
}
