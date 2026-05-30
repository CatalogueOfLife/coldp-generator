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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
   * Registers a contributing source database (GSD) as a rich ColDP source citation referenced by
   * NameUsage.sourceID, and records its release date as a candidate archive issued date.
   *
   * @param id          ColDP source id, e.g. {@code d12}
   * @param title       full GSD name
   * @param alias       short GSD name (may be null)
   * @param agents      GSD "authors and editors" string (may be null); split into author/editor
   * @param publisher   GSD organisation/custodian (may be null)
   * @param version     GSD version (may be null)
   * @param releaseDate GSD release date (may be null)
   */
  void addSourceCitation(String id, String title, String alias, String agents, String publisher,
                         String version, LocalDate releaseDate) {
    Citation c = new Citation();
    c.setId(id);
    if (title != null && !title.isBlank()) c.setTitle(title.trim());
    ColacMappings.Agents a = ColacMappings.parseAgents(agents);
    if (!a.authors().isEmpty()) c.setAuthor(toCsl(a.authors()));
    if (!a.editors().isEmpty()) c.setEditor(toCsl(a.editors()));
    if (publisher != null && !publisher.isBlank()) c.setPublisher(publisher.trim());
    if (version != null && !version.isBlank()) c.setVersion(version.trim());
    if (releaseDate != null) {
      c.setIssued(FuzzyDate.of(releaseDate.getYear()));
      g.noteReleaseDate(releaseDate);
    }
    g.gsdSources().add(new Generator.GsdSource(c, alias));
  }

  private static List<CslName> toCsl(List<String[]> names) {
    List<CslName> out = new ArrayList<>();
    for (String[] n : names) {
      CslName c = new CslName();
      if (n[0] != null) c.setGiven(n[0]);
      c.setFamily(n[1]);
      out.add(c);
    }
    return out;
  }

  /** Returns the column names of {@code table} in the current database (lower-cased). */
  Set<String> tableColumns(String table) throws SQLException {
    Set<String> cols = new HashSet<>();
    try (ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), null, table, null)) {
      while (rs.next()) cols.add(rs.getString("COLUMN_NAME").toLowerCase());
    }
    return cols;
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
