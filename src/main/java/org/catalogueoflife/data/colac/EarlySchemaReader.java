package org.catalogueoflife.data.colac;

import life.catalogue.coldp.ColdpTerm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.catalogueoflife.data.colac.ColacMappings.*;

/** Reader for the 2000/2002–2004 Species 2000 CD-ROM schema (flat HIERARCHY + atomized SCINAMES). */
class EarlySchemaReader extends SchemaReader {
  static final String PREFIX = "h|";   // synthetic higher-taxon / genus id prefix

  EarlySchemaReader(Generator g, Connection conn) { super(g, conn); }

  /** Deterministic id for a synthetic node from its ancestor-name path (kingdom..this node). */
  static String pathId(List<String> segments) {
    if (segments == null || segments.isEmpty()) return null;
    return PREFIX + String.join("|", segments);
  }

  /** Parent id of a synthetic path id, or null when it is a top-level (kingdom) node. */
  static String parentPathId(String id) {
    if (id == null) return null;
    int cut = id.lastIndexOf('|');
    String parent = id.substring(0, cut);          // drop last segment
    return parent.equals(PREFIX.substring(0, PREFIX.length() - 1)) ? null // "h" only -> root
         : (parent.endsWith("|") ? null : parent);
  }

  @Override
  void read() throws Exception {
    // composite key normCode(HierarchyCode)+"|"+normCode(Family) -> [kingdom,phylum,class,order,family]
    Map<String, String[]> hier = loadHierarchy();
    // accepted NameCode(normCode) -> emitted id (the verbatim NameCode)
    Map<String, String> acceptedNameCodeToId = new HashMap<>();
    Set<String> emittedNodes = new LinkedHashSet<>(); // synthetic path ids already written

    int nAcc = emitAccepted(hier, emittedNodes, acceptedNameCodeToId);
    LOG.info("Early schema done: {} accepted ({} synthetic nodes)", nAcc, emittedNodes.size());
  }

  /**
   * Loads the HIERARCHY table into a map keyed by the composite
   * normCode(HierarchyCode) + "|" + normCode(Family), which is the natural PK of that table.
   * Each value is [kingdom, phylum, class, order, family].
   */
  private Map<String, String[]> loadHierarchy() throws Exception {
    Map<String, String[]> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT HierarchyCode, Kingdom, Phylum, Class, `Order`, Family FROM HIERARCHY")) {
      while (rs.next()) {
        String hc  = normCode(rs.getString("HierarchyCode"));
        String fam = normCode(rs.getString("Family"));
        if (hc == null || fam == null) continue;
        String key = hc + "|" + fam;
        m.put(key, new String[]{
            rs.getString("Kingdom"), rs.getString("Phylum"), rs.getString("Class"),
            rs.getString("Order"),   rs.getString("Family")});
      }
    }
    LOG.info("Loaded {} hierarchy rows", m.size());
    return m;
  }

  /**
   * Emits all accepted SCINAMES rows (Sp2kStatus LIKE '%accepted%').
   * For each row the higher-taxon + genus lineage is synthesised once (emitted via emittedNodes),
   * then the species/infraspecies row itself is written.
   */
  private int emitAccepted(Map<String, String[]> hier, Set<String> emitted,
                           Map<String, String> acceptedNameCodeToId) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, HierarchyCode, Family, Genus, Species, InfraSpecies, InfraSpMarker, " +
             "ScientificNameAuthor, Sp2kStatus, AuthorRefNumber, DatabaseName, Comment " +
             "FROM SCINAMES WHERE Sp2kStatus LIKE '%accepted%'")) {
      while (rs.next()) {
        String code = rs.getString("NameCode");
        String nc   = normCode(code);
        if (nc == null) continue;

        // Build the composite hierarchy key: normCode(HierarchyCode) + "|" + normCode(Family)
        String hcNorm  = normCode(rs.getString("HierarchyCode"));
        String famNorm = normCode(rs.getString("Family"));
        String hierKey = (hcNorm != null && famNorm != null) ? hcNorm + "|" + famNorm : null;
        String[] h     = hierKey != null ? hier.get(hierKey) : null;

        String kingdom = h != null ? h[0] : null;
        String marker  = blankNone(rs.getString("InfraSpMarker"));
        String infra   = blankNone(rs.getString("InfraSpecies"));
        String genusId = ensureLineage(h, rs.getString("Family"), rs.getString("Genus"), emitted);

        Generator.set(nameW, ColdpTerm.ID, code);
        if (genusId != null) Generator.set(nameW, ColdpTerm.parentID, genusId);
        Generator.set(nameW, ColdpTerm.status, status(rs.getString("Sp2kStatus")));
        Generator.set(nameW, ColdpTerm.rank, infraRank(marker, infra));
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("Genus"), rs.getString("Species"), marker, infra));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("ScientificNameAuthor"));
        Generator.set(nameW, ColdpTerm.code, nomCode(kingdom));
        Generator.set(nameW, ColdpTerm.sourceID, sourceId(rs.getString("DatabaseName")));
        Generator.set(nameW, ColdpTerm.nameReferenceID, refId(rs.getString("AuthorRefNumber")));
        Generator.set(nameW, ColdpTerm.remarks, rs.getString("Comment"));
        nameW.next();
        acceptedNameCodeToId.put(nc, code);
        n++;
      }
    }
    LOG.info("Wrote {} accepted taxa ({} synthetic higher/genus nodes)", n, emitted.size());
    return n;
  }

  /**
   * Emits Kingdom..Family (from hier, else a root Family node) and the Genus node once each
   * (guarded by the emitted set). Returns the genus node id (parent for the species/infraspecies),
   * or the family node id when genus is blank, or null when no lineage can be built.
   */
  private String ensureLineage(String[] h, String scinamesFamily, String genus,
                               Set<String> emitted) throws Exception {
    List<String> path = new ArrayList<>();
    String[] ranks = {"kingdom", "phylum", "class", "order", "family"};
    if (h != null) {
      for (int i = 0; i < 5; i++) {
        String v = blankNone(h[i]);
        if (v != null) { path.add(v); emitNode(path, ranks[i], emitted); }
      }
    } else {
      String fam = blankNone(scinamesFamily);
      if (fam != null) { path.add(fam); emitNode(path, "family", emitted); }
    }
    String g = blankNone(genus);
    if (g == null || path.isEmpty()) return path.isEmpty() ? null : pathId(path);
    path.add(g);
    emitNode(path, "genus", emitted);
    return pathId(path);
  }

  private void emitNode(List<String> path, String rank, Set<String> emitted) throws Exception {
    String id = pathId(path);
    if (!emitted.add(id)) return;                  // already written
    Generator.set(nameW, ColdpTerm.ID, id);
    String parent = parentPathId(id);
    if (parent != null) Generator.set(nameW, ColdpTerm.parentID, parent);
    Generator.set(nameW, ColdpTerm.status, "accepted");
    Generator.set(nameW, ColdpTerm.rank, rank);
    Generator.set(nameW, ColdpTerm.scientificName, path.get(path.size() - 1));
    Generator.set(nameW, ColdpTerm.code, nomCode(path.get(0)));
    nameW.next();
  }

  /**
   * Treats the early-schema "no value" sentinels as null: blank, "none" (empty atomized name
   * parts) and "Not assigned" (unplaced HIERARCHY ranks). A child then attaches to its nearest
   * real ancestor instead of to a junk node literally named "Not assigned".
   */
  static String blankNone(String s) {
    if (s == null) return null;
    String t = s.trim();
    return (t.isEmpty() || t.equalsIgnoreCase("none") || t.equalsIgnoreCase("not assigned")) ? null : t;
  }

  private static String infraRank(String marker, String infra) {
    if (infra != null) return marker != null ? marker : "infraspecific name";
    return "species";
  }

  private static String sourceId(String db) { return blankNone(db) == null ? null : "d" + db.trim(); }
  private static String refId(String ref)   { return blankNone(ref) == null ? null : "r" + ref.trim(); }
}
