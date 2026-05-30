package org.catalogueoflife.data.colac;

import life.catalogue.api.model.Citation;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

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

  private final int year;
  private final String dbName;
  // the latest GSD release date encountered, used as the archive issued date
  private LocalDate releaseDate;

  // additional ColDP writers shared with the schema readers
  TermWriter vernWriter;
  TermWriter distWriter;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
    if (cfg.year == null) {
      throw new IllegalArgumentException("--year is required for the colac source (2005-2019)");
    }
    if (cfg.year < 2005 || cfg.year > 2019) {
      throw new IllegalArgumentException("colac only supports annual checklist years 2005-2019, got " + cfg.year);
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

      SchemaReader reader = year <= 2011 ? new OldSchemaReader(this, conn) : new NewSchemaReader(this, conn);
      LOG.info("Converting CoL {} Annual Checklist ({}) with {}", year, dbName, reader.getClass().getSimpleName());
      reader.read();
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("year", String.valueOf(year));
    // explicit put() overrides the today-default applied by AbstractColdpGenerator
    metadata.put("version", String.valueOf(year));
    metadata.put("issued", releaseDate != null ? releaseDate.toString() : String.valueOf(year));
    super.addMetadata();
  }

  // ── package-private accessors for the schema readers (inherited protected members) ──
  TermWriter nameUsageWriter() { return writer; }
  TermWriter referenceWriter() { return refWriter; }
  List<Citation> sources() { return sourceCitations; }

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
