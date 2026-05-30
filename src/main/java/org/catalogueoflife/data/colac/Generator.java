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
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  // Editor lists per year, normalised to "Surname Initials" order from the published Annual
  // Checklist "How to cite this work" pages (archived under resources/colac/citations/).
  // 2006-2009 have no citation page available; Bisby and Roskov led the team throughout, so
  // they are listed as the confirmed core for those years.
  private static final Map<Integer, String> EDITORS = Map.ofEntries(
      Map.entry(2005, "Bisby F.A., Ruggiero M.A., Wilson K.L., Cachuela-Palacio M., Kimani S.W., Roskov Y.R., Soulier-Perkins A., van Hertum J."),
      Map.entry(2006, "Bisby F.A., Roskov Y.R."),
      Map.entry(2007, "Bisby F.A., Roskov Y.R."),
      Map.entry(2008, "Bisby F.A., Roskov Y.R."),
      Map.entry(2009, "Bisby F.A., Roskov Y.R."),
      Map.entry(2010, "Bisby F.A., Roskov Y.R., Orrell T.M., Nicolson D., Paglinawan L.E., Bailly N., Kirk P.M., Bourgoin T., Baillargeon G."),
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

  // ORCIDs keyed by family name, harvested from the current CoL release metadata
  // (api.checklistbank.org/dataset/3LR.yaml) and the repo's ITIS metadata. Only confident,
  // same-person matches are included — no guessing.
  private static final Map<String, String> ORCID = Map.ofEntries(
      Map.entry("Roskov",   "0000-0003-2137-2690"),
      Map.entry("Orrell",   "0000-0003-1038-3028"),
      Map.entry("Nicolson", "0000-0002-7987-0679"),
      Map.entry("Bailly",   "0000-0003-4994-0653"),
      Map.entry("Kirk",     "0000-0002-0658-7338"),
      Map.entry("Ouvrard",  "0000-0003-2931-6116"),
      Map.entry("Decock",   "0000-0002-2168-9471"),
      Map.entry("Ower",     "0000-0002-9770-2345"),
      Map.entry("DeWalt",   "0000-0001-9985-9250")
  );

  private final int year;
  private final String dbName;
  // the latest GSD release date encountered, used as the archive issued date
  private LocalDate releaseDate;

  // additional ColDP writers shared with the schema readers
  TermWriter vernWriter;
  TermWriter distWriter;

  // GSD source citations, rendered into metadata.yaml ourselves (see addMetadata).
  // shortTitle is not yet a documented ColDP field; we emit it now so the archived data is
  // ready once the spec adds it (current parsers ignore the unknown key).
  record GsdSource(Citation citation, String shortTitle) {}
  private final List<GsdSource> gsdSources = new ArrayList<>();

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
    // 1473-009X was the CD-ROM/DVD Annual Checklist ISSN in use from 2005; the ISSN
    // 2405-884X was used from 2016 on.
    String issn = year <= 2015 ? "1473-009X" : "2405-884X";
    metadata.put("issn", "issn: " + issn);
    metadata.put("creators", buildCreators(city, country));

    // Render the GSD source registry ourselves. The shared metadata template is filled by
    // SimpleTemplate, which substitutes via Matcher.appendReplacement — that interprets the
    // backslashes snakeyaml emits (escaped quotes, wrapped-line continuations) as replacement
    // metacharacters and corrupts the YAML. Matcher.quoteReplacement makes the value literal.
    // sourceCitations is left empty so the base class does not re-inject an unescaped copy.
    metadata.put("sources", Matcher.quoteReplacement(renderSources()));
    super.addMetadata();
  }

  /** Renders the {@code source:} YAML block, injecting a (not-yet-standard) shortTitle per GSD. */
  private String renderSources() throws JsonProcessingException {
    if (gsdSources.isEmpty()) return "";
    StringBuilder sb = new StringBuilder("source: \n");
    for (GsdSource s : gsdSources) {
      String entry = citAsYaml(List.of(s.citation())).orElse("").stripTrailing();
      if (s.shortTitle() != null && !s.shortTitle().isBlank()) {
        // single-quoted YAML scalar (no backslashes); double any internal single quote
        entry += "\n   shortTitle: '" + s.shortTitle().trim().replace("'", "''") + "'";
      }
      sb.append(entry).append('\n');
    }
    return sb.toString();
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
      String orcid = ORCID.get(ed[1]);
      if (orcid != null) sb.append("\n    orcid: ").append(orcid);
    }
    return sb.toString();
  }

  // ── package-private accessors for the schema readers (inherited protected members) ──
  TermWriter nameUsageWriter() { return writer; }
  TermWriter referenceWriter() { return refWriter; }
  List<GsdSource> gsdSources() { return gsdSources; }

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
