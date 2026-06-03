package org.catalogueoflife.data.colac;

import com.fasterxml.jackson.core.JsonProcessingException;
import life.catalogue.api.model.Citation;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Generator for the historical Catalogue of Life Annual Checklists 2005–2019.
 *
 * Each annual checklist exists only as a MySQL/MariaDB dump, restored locally with one
 * database per year named {@code col{year}ac} (e.g. {@code col2015ac}). The dumps are
 * publicly available from https://www.catalogueoflife.org/data/download.
 *
 * Two database schemas are used across the years and dispatched by {@link #year}:
 *   2005–2011 → {@link OldSchemaReader} (denormalized {@code taxa} tree + {@code scientific_names})
 *   2012–2019 → {@link NewSchemaReader} (Species 2000 format with {@code _taxon_tree}/{@code _search_scientific} helper tables)
 *
 * Run with: {@code -s colac --year 2015 [--db-host ... --db-port ... --db-user ... --db-pass ...]}
 */
public class Generator extends AbstractColdpGenerator {

  // Per-year metadata (title, editors, publisher, scopes, …) lives in explicit YAML files under
  // resources/colac/metadata/<year>.yaml, compiled from the editor's metadata spreadsheet
  // (resources/colac/citations/ACs_metadata_summary_v1.xlsx via scripts/gen_colac_metadata.py).
  // Only the issued date and the GSD source: registry are filled at runtime (see addMetadata).

  private final int year;
  private final String dbName;
  // the latest GSD release date encountered, used as the archive issued date
  private LocalDate releaseDate;

  // additional ColDP writers shared with the schema readers
  TermWriter vernWriter;
  TermWriter distWriter;

  // GSD source citations, rendered into metadata.yaml ourselves (see addMetadata).
  private final List<Citation> gsdSources = new ArrayList<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
    if (cfg.year == null) {
      throw new IllegalArgumentException("--year is required for the colac source (2000-2019, no 2001)");
    }
    if (cfg.year < 2000 || cfg.year > 2019 || cfg.year == 2001) {
      throw new IllegalArgumentException("colac supports annual checklist years 2000, 2002-2019 (2001 was never released), got " + cfg.year);
    }
    this.year = cfg.year;
    this.dbName = "col" + year + "ac";
  }

  @Override
  protected void addData() throws Exception {
    // explicitly register the driver: the shaded fat JAR does not merge JDBC service files
    Class.forName("org.mariadb.jdbc.Driver");
    String url = String.format("jdbc:mariadb://%s:%d/%s", cfg.dbHost, cfg.dbPort, dbName);
    LOG.info("Connecting to {} as user {}", url, cfg.dbUser);
    try (Connection conn = DriverManager.getConnection(url, cfg.dbUser, cfg.dbPass)) {
      // allow large GROUP_CONCAT and relax ONLY_FULL_GROUP_BY for grouped helper queries
      try (Statement st = conn.createStatement()) {
        st.execute("SET SESSION group_concat_max_len = 1000000000");
        st.execute("SET SESSION sql_mode = ''");
      }

      newWriter(ColdpTerm.NameUsage, List.of(
          ColdpTerm.ID,
          ColdpTerm.sourceID,
          ColdpTerm.parentID,
          ColdpTerm.status,
          ColdpTerm.rank,
          ColdpTerm.scientificName,
          ColdpTerm.authorship,
          ColdpTerm.code,
          ColdpTerm.referenceID,
          ColdpTerm.nameReferenceID,
          ColdpTerm.remarks
      ));
      initRefWriter(List.of(
          ColdpTerm.ID,
          ColdpTerm.author,
          ColdpTerm.title,
          ColdpTerm.containerTitle,
          ColdpTerm.issued,
          ColdpTerm.citation
      ));
      vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
          ColdpTerm.taxonID,
          ColdpTerm.name,
          ColdpTerm.language,
          ColdpTerm.country,
          ColdpTerm.referenceID
      ));
      distWriter = additionalWriter(ColdpTerm.Distribution, List.of(
          ColdpTerm.taxonID,
          ColdpTerm.area,
          ColdpTerm.gazetteer,
          ColdpTerm.establishmentMeans
      ));

      if (year <= 2004) {
        // the early CD-ROM release date is the canonical "issued" date
        try (Statement st2 = conn.createStatement();
             ResultSet rs = st2.executeQuery("SELECT `Date` FROM `CD-Date` LIMIT 1")) {
          if (rs.next()) noteReleaseDate(SchemaReader.parseDate(rs.getString(1)));
        }
      }

      SchemaReader reader = year <= 2004 ? new EarlySchemaReader(this, conn)
                          : year <= 2011 ? new OldSchemaReader(this, conn)
                          : new NewSchemaReader(this, conn);
      LOG.info("Converting CoL {} Annual Checklist ({}) with {}", year, dbName, reader.getClass().getSimpleName());
      reader.read();
    }
  }

  /** Each year has its own explicit metadata file under resources/colac/metadata/. */
  @Override
  protected String metadataTemplatePath() {
    return "colac/metadata/" + year + ".yaml";
  }

  @Override
  protected void addMetadata() throws Exception {
    // The per-year metadata file is fully explicit (title, editors, publisher, scopes, …);
    // only two values are runtime-derived: the issued date (latest GSD / CD-ROM release date)
    // and the GSD source: registry appended from the actual data.
    metadata.put("issued", releaseDate != null ? releaseDate.toString() : String.valueOf(year));
    // Render the GSD source registry ourselves. The template is filled by SimpleTemplate, which
    // substitutes via Matcher.appendReplacement — that interprets the backslashes snakeyaml emits
    // (escaped quotes, wrapped-line continuations) as replacement metacharacters and corrupts the
    // YAML. Matcher.quoteReplacement makes the value literal. sourceCitations is left empty so the
    // base class does not re-inject an unescaped copy.
    metadata.put("sources", Matcher.quoteReplacement(renderSources()));
    super.addMetadata();
  }

  /** Renders the {@code source:} YAML block from the GSD citations (alias set on each). */
  private String renderSources() throws JsonProcessingException {
    return citAsYaml(gsdSources).map(y -> "source: \n" + y).orElse("");
  }

  // ── package-private accessors for the schema readers (inherited protected members) ──
  TermWriter nameUsageWriter() { return writer; }
  TermWriter referenceWriter() { return refWriter; }
  List<Citation> gsdSources() { return gsdSources; }

  /** Records the latest GSD release date seen, used as the archive issued date. */
  void noteReleaseDate(LocalDate d) {
    if (d != null && (releaseDate == null || d.isAfter(releaseDate))) {
      releaseDate = d;
    }
  }

  /** Sets a ColDP term only when the value is non-null and not blank (after trimming). */
  static void set(TermWriter w, ColdpTerm term, String value) {
    if (value != null) {
      String s = value.trim();
      if (!s.isEmpty()) {
        w.set(term, s);
      }
    }
  }
}
