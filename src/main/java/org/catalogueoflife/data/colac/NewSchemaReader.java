package org.catalogueoflife.data.colac;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import life.catalogue.coldp.ColdpTerm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.catalogueoflife.data.colac.ColacMappings.*;

/**
 * Reader for the 2012–2019 CoL Annual Checklist schema (Species 2000 normalized format).
 *
 * Uses the fully-populated denormalized helper tables: {@code _taxon_tree} provides the
 * accepted classification (hierarchy via parent_id), and {@code _search_scientific} provides
 * atomized names, authorship, status, source database and synonymy (synonyms are rows with
 * {@code accepted_species_id > 0}).
 *
 * IDs: {@code t<taxon_id>} accepted, {@code s<synonym id>} synonym,
 * {@code r<reference.id>} reference, {@code d<source_database.id>} source.
 */
class NewSchemaReader extends SchemaReader {

  NewSchemaReader(Generator g, Connection conn) {
    super(g, conn);
  }

  @Override
  void read() throws Exception {
    Map<Integer, String> statusLabels = loadIntString("SELECT id, name_status FROM scientific_name_status");
    Map<Integer, String> distStatus = loadIntString("SELECT id, status FROM distribution_status");
    int provisionalStatusId = idForLabel(statusLabels, "provisionally accepted name");

    Map<Integer, String> regionName = new HashMap<>();
    Map<Integer, String> regionGazetteer = new HashMap<>();
    loadRegions(regionName, regionGazetteer);

    Int2IntOpenHashMap childParent = new Int2IntOpenHashMap();
    childParent.defaultReturnValue(0);
    Int2ObjectOpenHashMap<String> kingdomRoot = new Int2ObjectOpenHashMap<>();
    loadTaxonTree(childParent, kingdomRoot);

    IntOpenHashSet provisional = loadProvisional(provisionalStatusId);

    Int2ObjectOpenHashMap<String> nameRefTaxon = new Int2ObjectOpenHashMap<>();
    Int2ObjectOpenHashMap<Set<String>> taxonRefTaxon = new Int2ObjectOpenHashMap<>();
    loadEntityRefs("reference_to_taxon", "taxon_id", nameRefTaxon, taxonRefTaxon);

    Int2ObjectOpenHashMap<String> nameRefSyn = new Int2ObjectOpenHashMap<>();
    Int2ObjectOpenHashMap<Set<String>> taxonRefSyn = new Int2ObjectOpenHashMap<>();
    loadEntityRefs("reference_to_synonym", "synonym_id", nameRefSyn, taxonRefSyn);

    Int2ObjectOpenHashMap<Set<String>> comNameRef = loadCommonNameRefs();

    loadSources();
    writeReferences();

    int nAcc = emitAccepted(childParent, kingdomRoot, provisional, nameRefTaxon, taxonRefTaxon);
    int nSyn = emitSynonyms(statusLabels, nameRefSyn, taxonRefSyn);
    int nVern = emitVernaculars(comNameRef);
    int nDist = emitDistributions(regionName, regionGazetteer, distStatus);
    LOG.info("New schema done: {} accepted, {} synonyms, {} vernaculars, {} distributions",
        nAcc, nSyn, nVern, nDist);
  }

  private Map<Integer, String> loadIntString(String sql) throws Exception {
    Map<Integer, String> m = new HashMap<>();
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) m.put(rs.getInt(1), rs.getString(2));
    }
    return m;
  }

  private static int idForLabel(Map<Integer, String> labels, String wanted) {
    for (var e : labels.entrySet()) {
      if (wanted.equalsIgnoreCase(e.getValue())) return e.getKey();
    }
    return -1;
  }

  private void loadRegions(Map<Integer, String> regionName, Map<Integer, String> regionGazetteer) throws Exception {
    Map<Integer, String> standardGaz = new HashMap<>();
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT id, standard FROM region_standard")) {
      while (rs.next()) standardGaz.put(rs.getInt("id"), gazetteer(rs.getString("standard")));
    }
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT id, name, region_standard_id FROM region")) {
      while (rs.next()) {
        int id = rs.getInt("id");
        regionName.put(id, rs.getString("name"));
        String gaz = standardGaz.get(rs.getInt("region_standard_id"));
        regionGazetteer.put(id, gaz != null ? gaz : "text");
      }
    }
    LOG.info("Loaded {} regions", regionName.size());
  }

  private void loadTaxonTree(Int2IntOpenHashMap childParent, Int2ObjectOpenHashMap<String> kingdomRoot) throws Exception {
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT taxon_id, parent_id, name FROM _taxon_tree")) {
      while (rs.next()) {
        int id = rs.getInt("taxon_id");
        int parent = rs.getInt("parent_id");
        childParent.put(id, parent);
        if (parent == 0) kingdomRoot.put(id, rs.getString("name"));
      }
    }
    LOG.info("Loaded {} taxon tree nodes, {} kingdoms", childParent.size(), kingdomRoot.size());
  }

  private IntOpenHashSet loadProvisional(int provisionalStatusId) throws Exception {
    IntOpenHashSet set = new IntOpenHashSet();
    if (provisionalStatusId < 0) return set;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT taxon_id FROM taxon_detail WHERE scientific_name_status_id = " + provisionalStatusId)) {
      while (rs.next()) set.add(rs.getInt(1));
    }
    LOG.info("Loaded {} provisionally accepted taxa", set.size());
    return set;
  }

  /** type 1 = Nomenclatural → nameReferenceID (first); other/NULL = Acceptance → referenceID (joined). */
  private void loadEntityRefs(String table, String idCol,
                              Int2ObjectOpenHashMap<String> nameRef,
                              Int2ObjectOpenHashMap<Set<String>> taxonRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT " + idCol + ", reference_id, reference_type_id FROM " + table)) {
      while (rs.next()) {
        int key = rs.getInt(1);
        String rid = "r" + rs.getInt("reference_id");
        int type = rs.getInt("reference_type_id"); // 0 when NULL
        if (type == 1) {
          if (!nameRef.containsKey(key)) nameRef.put(key, rid);
        } else {
          taxonRef.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(rid);
        }
        n++;
      }
    }
    LOG.info("Loaded {} {} links", n, table);
  }

  private Int2ObjectOpenHashMap<Set<String>> loadCommonNameRefs() throws Exception {
    Int2ObjectOpenHashMap<Set<String>> m = new Int2ObjectOpenHashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT common_name_id, reference_id FROM reference_to_common_name")) {
      while (rs.next()) {
        m.computeIfAbsent(rs.getInt("common_name_id"), k -> new LinkedHashSet<>())
            .add("r" + rs.getInt("reference_id"));
      }
    }
    return m;
  }

  private void loadSources() throws Exception {
    int n = 0;
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT id, name, authors_and_editors, version, release_date FROM source_database")) {
      while (rs.next()) {
        addSourceCitation("d" + rs.getInt("id"), rs.getString("name"),
            rs.getString("authors_and_editors"), rs.getString("version"),
            parseDate(rs.getString("release_date")));
        n++;
      }
    }
    LOG.info("Loaded {} source databases (GSDs)", n);
  }

  private void writeReferences() throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT id, authors, year, title, text FROM `reference`")) {
      while (rs.next()) {
        Generator.set(refW, ColdpTerm.ID, "r" + rs.getInt("id"));
        Generator.set(refW, ColdpTerm.author, rs.getString("authors"));
        Generator.set(refW, ColdpTerm.issued, rs.getString("year"));
        Generator.set(refW, ColdpTerm.title, rs.getString("title"));
        Generator.set(refW, ColdpTerm.citation, rs.getString("text"));
        refW.next();
        n++;
      }
    }
    LOG.info("Wrote {} references", n);
  }

  private String kingdomOf(int id, Int2IntOpenHashMap childParent, Int2ObjectOpenHashMap<String> kingdomRoot) {
    int cur = id;
    for (int guard = 0; guard < 50; guard++) {
      int p = childParent.get(cur);
      if (p == 0) return kingdomRoot.get(cur);
      cur = p;
    }
    return null;
  }

  private int emitAccepted(Int2IntOpenHashMap childParent, Int2ObjectOpenHashMap<String> kingdomRoot,
                           IntOpenHashSet provisional, Int2ObjectOpenHashMap<String> nameRef,
                           Int2ObjectOpenHashMap<Set<String>> taxonRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT tt.taxon_id, tt.parent_id, tt.name, tt.rank, ss.author, ss.source_database_id " +
             "FROM _taxon_tree tt " +
             "LEFT JOIN _search_scientific ss ON ss.id = tt.taxon_id AND ss.accepted_species_id = 0")) {
      while (rs.next()) {
        int id = rs.getInt("taxon_id");
        int parent = rs.getInt("parent_id");
        Generator.set(nameW, ColdpTerm.ID, "t" + id);
        if (parent > 0) Generator.set(nameW, ColdpTerm.parentID, "t" + parent);
        Generator.set(nameW, ColdpTerm.rank, lc(rs.getString("rank")));
        Generator.set(nameW, ColdpTerm.scientificName, rs.getString("name"));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.code, nomCode(kingdomOf(id, childParent, kingdomRoot)));
        int dbId = rs.getInt("source_database_id");
        if (dbId > 0) Generator.set(nameW, ColdpTerm.sourceID, "d" + dbId);
        Generator.set(nameW, ColdpTerm.status, provisional.contains(id) ? "provisionally accepted" : "accepted");
        Generator.set(nameW, ColdpTerm.nameReferenceID, nameRef.get(id));
        Generator.set(nameW, ColdpTerm.referenceID, joinRefs(taxonRef.get(id)));
        nameW.next();
        n++;
      }
    }
    LOG.info("Wrote {} accepted taxa", n);
    return n;
  }

  private int emitSynonyms(Map<Integer, String> statusLabels, Int2ObjectOpenHashMap<String> nameRef,
                           Int2ObjectOpenHashMap<Set<String>> taxonRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT id, genus, species, infraspecies, infraspecific_marker, author, status, " +
             "accepted_species_id, source_database_id, kingdom " +
             "FROM _search_scientific WHERE accepted_species_id > 0")) {
      while (rs.next()) {
        int id = rs.getInt("id");
        Generator.set(nameW, ColdpTerm.ID, "s" + id);
        Generator.set(nameW, ColdpTerm.parentID, "t" + rs.getInt("accepted_species_id"));
        String marker = rs.getString("infraspecific_marker");
        String infra = rs.getString("infraspecies");
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("genus"), rs.getString("species"), marker, infra));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.rank, synonymRank(marker, infra));
        String label = statusLabels.get(rs.getInt("status"));
        Generator.set(nameW, ColdpTerm.status, label != null ? ColacMappings.status(label) : "synonym");
        Generator.set(nameW, ColdpTerm.code, nomCode(rs.getString("kingdom")));
        int dbId = rs.getInt("source_database_id");
        if (dbId > 0) Generator.set(nameW, ColdpTerm.sourceID, "d" + dbId);
        Generator.set(nameW, ColdpTerm.nameReferenceID, nameRef.get(id));
        Generator.set(nameW, ColdpTerm.referenceID, joinRefs(taxonRef.get(id)));
        nameW.next();
        n++;
      }
    }
    LOG.info("Wrote {} synonyms", n);
    return n;
  }

  private int emitVernaculars(Int2ObjectOpenHashMap<Set<String>> comNameRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT cn.id, cn.taxon_id, cne.name, cn.language_iso, cn.country_iso " +
             "FROM common_name cn JOIN common_name_element cne ON cne.id = cn.common_name_element_id")) {
      while (rs.next()) {
        String name = rs.getString("name");
        if (name == null || name.isBlank()) continue;
        Generator.set(vernW, ColdpTerm.taxonID, "t" + rs.getInt("taxon_id"));
        Generator.set(vernW, ColdpTerm.name, name);
        Generator.set(vernW, ColdpTerm.language, rs.getString("language_iso"));
        Generator.set(vernW, ColdpTerm.country, rs.getString("country_iso"));
        Generator.set(vernW, ColdpTerm.referenceID, joinRefs(comNameRef.get(rs.getInt("id"))));
        vernW.next();
        n++;
      }
    }
    LOG.info("Wrote {} vernacular names", n);
    return n;
  }

  private int emitDistributions(Map<Integer, String> regionName, Map<Integer, String> regionGazetteer,
                                Map<Integer, String> distStatus) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT taxon_detail_id, region_id, distribution_status_id FROM distribution")) {
      while (rs.next()) {
        int regionId = rs.getInt("region_id");
        String area = regionName.get(regionId);
        if (area == null || area.isBlank()) continue;
        Generator.set(distW, ColdpTerm.taxonID, "t" + rs.getInt("taxon_detail_id"));
        Generator.set(distW, ColdpTerm.area, area);
        Generator.set(distW, ColdpTerm.gazetteer, regionGazetteer.getOrDefault(regionId, "text"));
        // keep the original distribution_status label verbatim; ChecklistBank parses/maps it on import
        Generator.set(distW, ColdpTerm.establishmentMeans, distStatus.get(rs.getInt("distribution_status_id")));
        distW.next();
        n++;
      }
    }
    LOG.info("Wrote {} distributions", n);
    return n;
  }

  private static String lc(String s) {
    return s == null ? null : s.toLowerCase(Locale.ENGLISH);
  }

  private static String synonymRank(String marker, String infraspecies) {
    if (infraspecies != null && !infraspecies.isBlank()) {
      return (marker != null && !marker.isBlank()) ? marker.trim() : "infraspecific name";
    }
    return "species";
  }

  /** Maps a region_standard label to a ColDP gazetteer value. */
  private static String gazetteer(String standard) {
    if (standard == null) return "text";
    String s = standard.toLowerCase(Locale.ENGLISH);
    if (s.contains("tdwg")) return "tdwg";
    if (s.contains("iho")) return "iho";
    if (s.contains("exclusive economic")) return "mrgid";
    return "text";
  }
}
