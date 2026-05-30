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
import java.util.Map;

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

  // Editor lists per year, taken verbatim from the published Annual Checklist citations
  // (Species 2000 & ITIS Catalogue of Life). Years 2005-2010 have no citation supplied here,
  // so only the organisation is listed as creator for those.
  private static final Map<Integer, String> EDITORS = Map.ofEntries(
      Map.entry(2011, "Bisby F.A., Roskov Y.R., Orrell T.M., Nicolson D., Paglinawan L.E., Bailly N., Kirk P.M., Bourgoin T., Baillargeon G., Ouvrard D."),
      Map.entry(2012, "Bisby F., Roskov Y., Culham A., Orrell T., Nicolson D., Paglinawan L., Bailly N., Appeltans W., Kirk P., Bourgoin T., Baillargeon G., Ouvrard D."),
      Map.entry(2013, "Roskov Y., Kunze T., Paglinawan L., Orrell T., Nicolson D., Culham A., Bailly N., Kirk P., Bourgoin T., Baillargeon G., Hernandez F., De Wever A."),
      Map.entry(2014, "Roskov Y., Kunze T., Orrell T., Abucay L., Paglinawan L., Culham A., Bailly N., Kirk P., Bourgoin T., Baillargeon G., Decock W., De Wever A., Didžiulis V."),
      Map.entry(2015, "Roskov Y., Abucay L., Orrell T., Nicolson D., Kunze T., Culham A., Bailly N., Kirk P., Bourgoin T., DeWalt R.E., Decock W., De Wever A."),
      Map.entry(2016, "Roskov Y., Abucay L., Orrell T., Nicolson D., Flann C., Bailly N., Kirk P., Bourgoin T., DeWalt R.E., Decock W., De Wever A."),
      Map.entry(2017, "Roskov Y., Abucay L., Orrell T., Nicolson D., Bailly N., Kirk P.M., Bourgoin T., DeWalt R.E., Decock W., De Wever A., Nieukerken E. van, Zarucchi J., Penev L."),
      Map.entry(2018, "Roskov Y., Abucay L., Orrell T., Nicolson D., Bailly N., Kirk P.M., Bourgoin T., DeWalt R.E., Decock W., De Wever A., Nieukerken E. van, Zarucchi J., Penev L."),
      Map.entry(2019, "Roskov Y., Ower G., Orrell T., Nicolson D., Bailly N., Kirk P.M., Bourgoin T., DeWalt R.E., Decock W., Nieukerken E. van, Zarucchi J., Penev L.")
  );
  // ISSN appears on the Annual Checklists from 2016 on (2018 used the DVD ISSN).
  private static final Map<Integer, String> ISSN = Map.of(
      2016, "2405-884X",
      2017, "2405-884X",
      2018, "2405-917X",
      2019, "2405-884X"
  );

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
    // Species 2000 moved from Reading (UK) to Leiden (NL) in 2013; the 2014 checklist was the
    // first released from Leiden.
    boolean leiden = year >= 2014;
    String city = leiden ? "Leiden" : "Reading";
    String country = leiden ? "NL" : "GB";

    metadata.put("year", String.valueOf(year));
    metadata.put("alias", String.format("COL%02d", year % 100));
    // explicit put() overrides the today-default applied by AbstractColdpGenerator
    metadata.put("version", "Annual Checklist " + year);
    metadata.put("issued", releaseDate != null ? releaseDate.toString() : String.valueOf(year));
    metadata.put("url", "https://www.catalogueoflife.org/annual-checklist/" + year + "/");
    String issn = ISSN.get(year);
    metadata.put("issn", issn != null ? "issn: " + issn : "");
    metadata.put("creators", buildCreators(city, country));
    super.addMetadata();
  }

  /** Builds the ColDP {@code creator:} list YAML: the organisation plus the per-year editors. */
  private String buildCreators(String city, String country) {
    StringBuilder sb = new StringBuilder();
    sb.append("  - organisation: Species 2000 & ITIS Catalogue of Life\n");
    sb.append("    city: ").append(city).append('\n');
    sb.append("    country: ").append(country);
    for (String[] ed : ColacMappings.parseEditors(EDITORS.get(year))) {
      sb.append("\n  - family: ").append(ed[1]);
      if (ed[0] != null) sb.append("\n    given: ").append(ed[0]);
    }
    return sb.toString();
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
