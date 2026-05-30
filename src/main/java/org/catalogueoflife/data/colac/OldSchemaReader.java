package org.catalogueoflife.data.colac;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
 * Reader for the 2005–2011 CoL Annual Checklist schema.
 *
 * The accepted classification is the {@code taxa} tree (Kingdom→Infraspecies via parent_id).
 * Species/infraspecies names with author, status and synonymy live in {@code scientific_names},
 * keyed by {@code name_code}. Synonyms are {@code scientific_names} rows with a synonym status,
 * linked to the accepted name via {@code accepted_name_code}.
 *
 * Status is taken from {@code sp2000_status_id} (+ the {@code sp2000_statuses} lookup) rather
 * than the {@code sp2000_status} text column, which only exists in 2005.
 *
 * IDs: {@code t<taxa.record_id>} accepted, {@code s<scientific_names.record_id>} synonym,
 * {@code r<references.record_id>} reference, {@code d<databases.record_id>} source.
 */
class OldSchemaReader extends SchemaReader {

  OldSchemaReader(Generator g, Connection conn) {
    super(g, conn);
  }

  @Override
  void read() throws Exception {
    Map<Integer, String> statusLabels = loadStatusLabels();
    // The sp2000_status_id → label assignment differs across years (e.g. id 4 is "ambiguous
    // synonym" in 2005 but "provisionally accepted name" in 2008+), and 2005 lacks
    // is_accepted_name on scientific_names. So derive the accepted vs synonym id sets from the
    // per-year labels rather than hardcoding ids.
    String acceptedIds = statusIdClause(statusLabels, true);
    String synonymIds = statusIdClause(statusLabels, false);
    LOG.info("Accepted status ids {}, synonym status ids {}", acceptedIds, synonymIds);

    Map<Integer, String> familyKingdom = loadFamilyKingdom();

    // accepted classification tree: child→parent + the kingdom name at each root; plus the
    // name_code→ColDP id map used to attach synonyms, vernaculars and distributions.
    Int2IntOpenHashMap childParent = new Int2IntOpenHashMap();
    childParent.defaultReturnValue(0);
    Int2ObjectOpenHashMap<String> kingdomRoot = new Int2ObjectOpenHashMap<>();
    Map<String, String> nameCodeToId = new HashMap<>();
    loadTaxaTree(childParent, kingdomRoot, nameCodeToId);

    Map<String, String[]> accInfo = loadAcceptedInfo(statusLabels, acceptedIds);

    Map<String, String> nameRef = new HashMap<>();
    Map<String, Set<String>> taxonRef = new HashMap<>();
    Map<String, Set<String>> comNameRef = new HashMap<>();
    loadRefLinks(nameRef, taxonRef, comNameRef);

    loadSources();
    writeReferences();

    int nAcc = emitAccepted(childParent, kingdomRoot, accInfo, nameRef, taxonRef);
    int nSyn = emitSynonyms(statusLabels, synonymIds, familyKingdom, nameCodeToId, nameRef, taxonRef);
    int nVern = emitVernaculars(nameCodeToId, comNameRef);
    int nDist = emitDistributions(nameCodeToId);
    LOG.info("Old schema done: {} accepted, {} synonyms, {} vernaculars, {} distributions",
        nAcc, nSyn, nVern, nDist);
  }

  private Map<Integer, String> loadStatusLabels() throws Exception {
    Map<Integer, String> m = new HashMap<>();
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT record_id, sp2000_status FROM sp2000_statuses")) {
      while (rs.next()) m.put(rs.getInt(1), rs.getString(2));
    }
    return m;
  }

  /**
   * Builds a SQL {@code IN (...)} clause of sp2000_status_ids whose label is an accepted status
   * ("accepted name", "provisionally accepted name") when {@code accepted} is true, or a synonym
   * status otherwise. Robust to the per-year differences in id→label assignment.
   */
  private static String statusIdClause(Map<Integer, String> statusLabels, boolean accepted) {
    StringBuilder sb = new StringBuilder("(");
    for (var e : statusLabels.entrySet()) {
      String cs = ColacMappings.status(e.getValue());
      boolean isAccepted = "accepted".equals(cs) || "provisionally accepted".equals(cs);
      if (isAccepted == accepted) {
        if (sb.length() > 1) sb.append(',');
        sb.append(e.getKey());
      }
    }
    if (sb.length() == 1) sb.append("-1"); // never-match guard for an empty set
    return sb.append(')').toString();
  }

  private Map<Integer, String> loadFamilyKingdom() throws Exception {
    Map<Integer, String> m = new HashMap<>();
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT record_id, kingdom FROM families")) {
      while (rs.next()) m.put(rs.getInt(1), rs.getString(2));
    }
    return m;
  }

  private void loadTaxaTree(Int2IntOpenHashMap childParent, Int2ObjectOpenHashMap<String> kingdomRoot,
                            Map<String, String> nameCodeToId) throws Exception {
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT record_id, parent_id, name, name_code FROM taxa")) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        int parent = rs.getInt("parent_id");
        childParent.put(id, parent);
        if (parent == 0) {
          kingdomRoot.put(id, rs.getString("name")); // top-level node = kingdom
        }
        String nc = rs.getString("name_code");
        if (nc != null && !nc.isBlank()) {
          nameCodeToId.put(nc, "t" + id);
        }
      }
    }
    LOG.info("Loaded {} taxa tree nodes, {} kingdoms", childParent.size(), kingdomRoot.size());
  }

  /** accepted name_code → [author, statusLabel, comment] from accepted scientific_names rows. */
  private Map<String, String[]> loadAcceptedInfo(Map<Integer, String> statusLabels, String acceptedIds) throws Exception {
    Map<String, String[]> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT name_code, author, sp2000_status_id, comment FROM scientific_names " +
             "WHERE sp2000_status_id IN " + acceptedIds)) {
      while (rs.next()) {
        String nc = rs.getString("name_code");
        if (nc == null || nc.isBlank()) continue;
        m.put(nc, new String[]{
            rs.getString("author"),
            statusLabels.get(rs.getInt("sp2000_status_id")),
            rs.getString("comment")
        });
      }
    }
    LOG.info("Loaded {} accepted name infos", m.size());
    return m;
  }

  private void loadRefLinks(Map<String, String> nameRef, Map<String, Set<String>> taxonRef,
                            Map<String, Set<String>> comNameRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT name_code, reference_type, reference_id FROM scientific_name_references")) {
      while (rs.next()) {
        String nc = rs.getString("name_code");
        if (nc == null || nc.isBlank()) continue;
        String type = rs.getString("reference_type");
        String rid = "r" + rs.getInt("reference_id");
        if (type == null) type = "";
        // vocabulary drifts across years: 2005-06 use AuthorRef/StatusRef/StatRef,
        // 2007-11 use NomRef/TaxAccRef (+ stray types and NULL). Nomenclatural → nameReferenceID,
        // common name → vernacular, everything else → taxon-level referenceID (acceptance).
        if (type.equalsIgnoreCase("ComNameRef")) {
          comNameRef.computeIfAbsent(nc, k -> new LinkedHashSet<>()).add(rid);
        } else if (type.equals("NomRef") || type.equals("AuthorRef")) {
          nameRef.putIfAbsent(nc, rid);
        } else {
          taxonRef.computeIfAbsent(nc, k -> new LinkedHashSet<>()).add(rid);
        }
        n++;
      }
    }
    LOG.info("Loaded {} reference links ({} name, {} taxon, {} common-name keys)",
        n, nameRef.size(), taxonRef.size(), comNameRef.size());
  }

  private void loadSources() throws Exception {
    // author/publisher columns vary: 2005 has only `custodian`; 2006-2011 have
    // `authors_editors` + `organization`.
    Set<String> cols = tableColumns("databases");
    boolean hasAE = cols.contains("authors_editors");
    boolean hasOrg = cols.contains("organization");
    boolean hasCust = cols.contains("custodian");
    StringBuilder sql = new StringBuilder(
        "SELECT record_id, database_full_name, database_name, version, release_date");
    if (hasAE) sql.append(", authors_editors");
    if (hasOrg) sql.append(", organization");
    if (hasCust) sql.append(", custodian");
    sql.append(" FROM `databases`");

    int n = 0;
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql.toString())) {
      while (rs.next()) {
        String title = rs.getString("database_full_name");
        if (title == null || title.isBlank()) title = rs.getString("database_name");
        String agents = hasAE ? rs.getString("authors_editors") : null;
        String publisher = hasOrg ? rs.getString("organization")
            : (hasCust ? rs.getString("custodian") : null);
        addSourceCitation("d" + rs.getInt("record_id"), title, rs.getString("database_name"),
            agents, publisher, rs.getString("version"), parseDate(rs.getString("release_date")));
        n++;
      }
    }
    LOG.info("Loaded {} source databases (GSDs)", n);
  }

  private void writeReferences() throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, author, year, title, source FROM `references`")) {
      while (rs.next()) {
        Generator.set(refW, ColdpTerm.ID, "r" + rs.getInt("record_id"));
        Generator.set(refW, ColdpTerm.author, rs.getString("author"));
        Generator.set(refW, ColdpTerm.issued, rs.getString("year"));
        Generator.set(refW, ColdpTerm.title, rs.getString("title"));
        Generator.set(refW, ColdpTerm.containerTitle, rs.getString("source"));
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
                           Map<String, String[]> accInfo, Map<String, String> nameRef,
                           Map<String, Set<String>> taxonRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, parent_id, name, taxon, name_code, database_id FROM taxa " +
             "WHERE is_accepted_name = 1")) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        int parent = rs.getInt("parent_id");
        String nc = rs.getString("name_code");

        Generator.set(nameW, ColdpTerm.ID, "t" + id);
        if (parent > 0) Generator.set(nameW, ColdpTerm.parentID, "t" + parent);
        Generator.set(nameW, ColdpTerm.rank, lc(rs.getString("taxon")));
        Generator.set(nameW, ColdpTerm.scientificName, rs.getString("name"));
        Generator.set(nameW, ColdpTerm.code, nomCode(kingdomOf(id, childParent, kingdomRoot)));
        int dbId = rs.getInt("database_id");
        if (dbId > 0) Generator.set(nameW, ColdpTerm.sourceID, "d" + dbId);

        String status = "accepted";
        if (nc != null && !nc.isBlank()) {
          String[] info = accInfo.get(nc);
          if (info != null) {
            Generator.set(nameW, ColdpTerm.authorship, info[0]);
            String s = ColacMappings.status(info[1]);
            if (s != null) status = s;
            Generator.set(nameW, ColdpTerm.remarks, info[2]);
          }
          Generator.set(nameW, ColdpTerm.nameReferenceID, nameRef.get(nc));
          Generator.set(nameW, ColdpTerm.referenceID, joinRefs(taxonRef.get(nc)));
        }
        Generator.set(nameW, ColdpTerm.status, status);
        nameW.next();
        n++;
      }
    }
    LOG.info("Wrote {} accepted taxa", n);
    return n;
  }

  private int emitSynonyms(Map<Integer, String> statusLabels, String synonymIds, Map<Integer, String> familyKingdom,
                           Map<String, String> nameCodeToId, Map<String, String> nameRef,
                           Map<String, Set<String>> taxonRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, name_code, genus, species, infraspecies, infraspecies_marker, " +
             "author, accepted_name_code, sp2000_status_id, database_id, comment, family_id " +
             "FROM scientific_names WHERE sp2000_status_id IN " + synonymIds)) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        String nc = rs.getString("name_code");
        String synId = "s" + id;
        if (nc != null && !nc.isBlank()) {
          nameCodeToId.putIfAbsent(nc, synId); // attach vernaculars/distributions to the synonym
        }

        Generator.set(nameW, ColdpTerm.ID, synId);
        String accCode = rs.getString("accepted_name_code");
        if (accCode != null) Generator.set(nameW, ColdpTerm.parentID, nameCodeToId.get(accCode));
        String marker = rs.getString("infraspecies_marker");
        String infra = rs.getString("infraspecies");
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("genus"), rs.getString("species"), marker, infra));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.rank, synonymRank(marker, infra));
        Generator.set(nameW, ColdpTerm.status, ColacMappings.status(statusLabels.get(rs.getInt("sp2000_status_id"))));
        Generator.set(nameW, ColdpTerm.code, nomCode(familyKingdom.get(rs.getInt("family_id"))));
        int dbId = rs.getInt("database_id");
        if (dbId > 0) Generator.set(nameW, ColdpTerm.sourceID, "d" + dbId);
        if (nc != null) {
          Generator.set(nameW, ColdpTerm.nameReferenceID, nameRef.get(nc));
          Generator.set(nameW, ColdpTerm.referenceID, joinRefs(taxonRef.get(nc)));
        }
        Generator.set(nameW, ColdpTerm.remarks, rs.getString("comment"));
        nameW.next();
        n++;
      }
    }
    LOG.info("Wrote {} synonyms", n);
    return n;
  }

  private int emitVernaculars(Map<String, String> nameCodeToId, Map<String, Set<String>> comNameRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT name_code, common_name, language, country FROM common_names")) {
      while (rs.next()) {
        String nc = rs.getString("name_code");
        String taxonId = nameCodeToId.get(nc);
        String name = rs.getString("common_name");
        if (taxonId == null || name == null || name.isBlank()) continue;
        Generator.set(vernW, ColdpTerm.taxonID, taxonId);
        Generator.set(vernW, ColdpTerm.name, name);
        Generator.set(vernW, ColdpTerm.language, rs.getString("language"));
        Generator.set(vernW, ColdpTerm.country, rs.getString("country"));
        Generator.set(vernW, ColdpTerm.referenceID, joinRefs(comNameRef.get(nc)));
        vernW.next();
        n++;
      }
    }
    LOG.info("Wrote {} vernacular names", n);
    return n;
  }

  private int emitDistributions(Map<String, String> nameCodeToId) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT name_code, distribution FROM distribution")) {
      while (rs.next()) {
        String taxonId = nameCodeToId.get(rs.getString("name_code"));
        String area = rs.getString("distribution");
        if (taxonId == null || area == null || area.isBlank()) continue;
        Generator.set(distW, ColdpTerm.taxonID, taxonId);
        Generator.set(distW, ColdpTerm.area, area);
        Generator.set(distW, ColdpTerm.gazetteer, "text");
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

  /** Best-effort rank for an atomized synonym: the infraspecies marker, else species/infraspecies. */
  private static String synonymRank(String marker, String infraspecies) {
    if (infraspecies != null && !infraspecies.isBlank()) {
      return (marker != null && !marker.isBlank()) ? marker.trim() : "infraspecific name";
    }
    return "species";
  }
}
