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
    // 2000: no DISTRIBUTION table (distCols empty); 2002: name-keyed (no NameCode); 2003/2004: NameCode-keyed.
    Set<String> distCols = tableColumns("DISTRIBUTION");
    // col2000/col2002 use old column names; col2003/col2004 use the renamed columns
    Set<String> scinamesCols = tableColumns("SCINAMES");
    boolean newStyle = scinamesCols.contains("scientificnameauthor"); // col2003+: ScientificNameAuthor / AuthorRefNumber / AcceptedNameCode
    LOG.info("SCINAMES schema: {} style (ScientificNameAuthor={})", newStyle ? "new (2003+)" : "old (2000/2002)", newStyle);
    // accepted NameCode(normCode) -> emitted id (the verbatim NameCode)
    Map<String, String> acceptedNameCodeToId = new HashMap<>();
    Set<String> emittedNodes = new LinkedHashSet<>(); // synthetic path ids already written

    int nAcc = emitAccepted(hier, emittedNodes, acceptedNameCodeToId, newStyle);
    Map<String, String> synAcceptedCode = loadSynonymChain(newStyle);
    loadSources();
    writeReferences(newStyle);
    int[] syn = emitSynonyms(acceptedNameCodeToId, synAcceptedCode, newStyle);
    int nVern = emitVernaculars(acceptedNameCodeToId, synAcceptedCode, newStyle);
    int nDist = distCols.contains("namecode") ? emitDistributions(acceptedNameCodeToId, synAcceptedCode)
              : !distCols.isEmpty()           ? emitDistributionsByName(acceptedNameCodeToId, synAcceptedCode)
              : 0;
    LOG.info("Early schema done: {} accepted, {} synonyms, {} bare names, {} vernaculars, {} distributions ({} synthetic nodes)",
        nAcc, syn[0], syn[1], nVern, nDist, emittedNodes.size());
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
                           Map<String, String> acceptedNameCodeToId, boolean newStyle) throws Exception {
    // col2000/2002: Author(s), AuthorRef; col2003/2004: ScientificNameAuthor, AuthorRefNumber
    String authorCol   = newStyle ? "ScientificNameAuthor"     : "`Author(s)`";
    String authorRef   = newStyle ? "AuthorRefNumber"          : "AuthorRef";
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, HierarchyCode, Family, Genus, Species, InfraSpecies, InfraSpMarker, " +
             authorCol + " AS author, Sp2kStatus, " + authorRef + " AS authorRef, DatabaseName, Comment " +
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
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.code, nomCode(kingdom));
        Generator.set(nameW, ColdpTerm.sourceID, sourceId(rs.getString("DatabaseName")));
        Generator.set(nameW, ColdpTerm.nameReferenceID, refId(rs.getString("authorRef")));
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

  /** synonym NameCode → its AcceptedNameCode (both normCode'd), for chain-following. */
  private Map<String, String> loadSynonymChain(boolean newStyle) throws Exception {
    // col2000/2002: accepted-name pointer is ANCode; col2003/2004: AcceptedNameCode
    String accCol = newStyle ? "AcceptedNameCode" : "ANCode";
    Map<String, String> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, " + accCol + " AS accCode FROM SCINAMES WHERE Sp2kStatus NOT LIKE '%accepted%'")) {
      while (rs.next()) {
        String nc = normCode(rs.getString("NameCode"));
        String acc = normCode(rs.getString("accCode"));
        if (nc != null && acc != null) m.putIfAbsent(nc, acc);
      }
    }
    LOG.info("Loaded {} synonym chain links", m.size());
    return m;
  }

  /**
   * Emits all synonym rows from SCINAMES (Sp2kStatus NOT LIKE '%accepted%').
   * A synonym whose AcceptedNameCode resolves (through any synonym→synonym chain) to an emitted
   * accepted taxon is written as a real synonym pointing to it. Otherwise it is emitted as a bare
   * name (no parent, status = "bare name") with the original status kept in remarks.
   *
   * @return {@code [synonyms, bareNames]} counts
   */
  private int[] emitSynonyms(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode,
                             boolean newStyle) throws Exception {
    // col2000/2002: Author(s), AuthorRef, ANCode; col2003/2004: ScientificNameAuthor, AuthorRefNumber, AcceptedNameCode
    String authorCol = newStyle ? "ScientificNameAuthor"     : "`Author(s)`";
    String authorRef = newStyle ? "AuthorRefNumber"          : "AuthorRef";
    String accCol    = newStyle ? "AcceptedNameCode"         : "ANCode";
    int nSyn = 0, nBare = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, Genus, Species, InfraSpecies, InfraSpMarker, " +
             authorCol + " AS author, Sp2kStatus, " + accCol + " AS accCode, " +
             authorRef + " AS authorRef, DatabaseName, Comment " +
             "FROM SCINAMES WHERE Sp2kStatus NOT LIKE '%accepted%'")) {
      while (rs.next()) {
        String code = rs.getString("NameCode");
        String accCode = normCode(rs.getString("accCode"));
        String parentId = accCode == null ? null : resolveAcceptedTaxon(accCode, acceptedNameCodeToId, synAcceptedCode);
        String statusLabel = status(rs.getString("Sp2kStatus"));
        String marker = blankNone(rs.getString("InfraSpMarker"));
        String infra  = blankNone(rs.getString("InfraSpecies"));
        String comment = rs.getString("Comment");

        Generator.set(nameW, ColdpTerm.ID, code);
        Generator.set(nameW, ColdpTerm.scientificName, synonymName(rs.getString("Genus"), rs.getString("Species"), marker, infra));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.rank, infraRank(marker, infra));
        Generator.set(nameW, ColdpTerm.sourceID, sourceId(rs.getString("DatabaseName")));
        Generator.set(nameW, ColdpTerm.nameReferenceID, refId(rs.getString("authorRef")));
        if (parentId != null) {
          Generator.set(nameW, ColdpTerm.parentID, parentId);
          Generator.set(nameW, ColdpTerm.status, statusLabel);
          Generator.set(nameW, ColdpTerm.remarks, comment);
          nSyn++;
        } else {
          Generator.set(nameW, ColdpTerm.status, "bare name");
          String note = (statusLabel != null ? statusLabel : "synonym") + " without accepted name";
          Generator.set(nameW, ColdpTerm.remarks, comment == null || comment.isBlank() ? note : note + "; " + comment);
          nBare++;
        }
        nameW.next();
      }
    }
    LOG.info("Wrote {} synonyms, {} bare names", nSyn, nBare);
    return new int[]{nSyn, nBare};
  }

  private void loadSources() throws Exception {
    int n = 0;
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT DatabaseName, DbFullName, Abbr, Institute, Contact, Version, ReleaseDate FROM GSDATABASES")) {
      while (rs.next()) {
        String title = rs.getString("DbFullName");
        if (title == null || title.isBlank()) title = rs.getString("DatabaseName");
        addSourceCitation("d" + rs.getString("DatabaseName"), title, rs.getString("Abbr"),
            rs.getString("Contact"), rs.getString("Institute"),
            rs.getString("Version"), parseDate(rs.getString("ReleaseDate")));
        n++;
      }
    }
    LOG.info("Loaded {} source databases (GSDs)", n);
  }

  private void writeReferences(boolean newStyle) throws Exception {
    // col2000/2002 use "Author(s)"; col2003/2004 use "ScientificNameAuthor"
    String authorExpr = newStyle ? "ScientificNameAuthor" : "`Author(s)`";
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT RefNumber, " + authorExpr + " AS author, Year, Title, Source FROM `REFERENCES`")) {
      while (rs.next()) {
        String ref = rs.getString("RefNumber");
        if (ref == null || ref.isBlank()) continue;
        Generator.set(refW, ColdpTerm.ID, "r" + ref.trim());
        Generator.set(refW, ColdpTerm.author, rs.getString("author"));
        Generator.set(refW, ColdpTerm.issued, rs.getString("Year"));
        Generator.set(refW, ColdpTerm.title, rs.getString("Title"));
        Generator.set(refW, ColdpTerm.containerTitle, rs.getString("Source"));
        refW.next();
        n++;
      }
    }
    LOG.info("Wrote {} references", n);
  }

  private int emitVernaculars(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode,
                              boolean newStyle) throws Exception {
    // col2000/2002: reference column is ComNameRef; col2003/2004: RefNumber
    String refCol = newStyle ? "RefNumber" : "ComNameRef";
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, CommonName, Language, Country, " + refCol + " AS refNum FROM COMNAMES")) {
      while (rs.next()) {
        String name = rs.getString("CommonName");
        String taxonId = resolveAcceptedTaxon(normCode(rs.getString("NameCode")), acceptedNameCodeToId, synAcceptedCode);
        if (taxonId == null || name == null || name.isBlank()) continue;
        Generator.set(vernW, ColdpTerm.taxonID, taxonId);
        Generator.set(vernW, ColdpTerm.name, name);
        Generator.set(vernW, ColdpTerm.language, rs.getString("Language"));
        Generator.set(vernW, ColdpTerm.country, rs.getString("Country"));
        Generator.set(vernW, ColdpTerm.referenceID, refId(rs.getString("refNum")));
        vernW.next();
        n++;
      }
    }
    LOG.info("Wrote {} vernacular names", n);
    return n;
  }

  private int emitDistributions(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT NameCode, Distribution FROM DISTRIBUTION")) {
      while (rs.next()) {
        String area = rs.getString("Distribution");
        String taxonId = resolveAcceptedTaxon(normCode(rs.getString("NameCode")), acceptedNameCodeToId, synAcceptedCode);
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

  /**
   * Normalized lookup key shared by SCINAMES and the name-keyed DISTRIBUTION table:
   * HierarchyCode + Genus + Species + InfraSpecies epithet, upper-cased; "none"/blank infra omitted.
   * The infra MARKER is deliberately excluded because its format is unreliable across the two tables.
   */
  static String nameKey(String hierarchyCode, String genus, String species, String infraspecies) {
    return nz(normCode(hierarchyCode)) + "|" + nz(normCode(genus)) + "|" +
           nz(normCode(species)) + "|" + nz(normCode(blankNone(infraspecies)));
  }

  private static String nz(String s) { return s == null ? "" : s; }

  /** Builds nameKey -> accepted taxon id from all SCINAMES rows; accepted rows take precedence. */
  private Map<String, String> loadNameKeyToTaxon(Map<String, String> acceptedNameCodeToId,
                                                 Map<String, String> synAcceptedCode) throws Exception {
    Map<String, String> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT NameCode, HierarchyCode, Genus, Species, InfraSpecies, Sp2kStatus FROM SCINAMES")) {
      while (rs.next()) {
        String taxon = resolveAcceptedTaxon(normCode(rs.getString("NameCode")), acceptedNameCodeToId, synAcceptedCode);
        if (taxon == null) continue;
        String key = nameKey(rs.getString("HierarchyCode"), rs.getString("Genus"),
                             rs.getString("Species"), rs.getString("InfraSpecies"));
        boolean accepted = rs.getString("Sp2kStatus") != null && rs.getString("Sp2kStatus").toLowerCase().contains("accepted");
        if (accepted) m.put(key, taxon); else m.putIfAbsent(key, taxon);
      }
    }
    LOG.info("Built {} name-key -> taxon entries for name-keyed distributions", m.size());
    return m;
  }

  private int emitDistributionsByName(Map<String, String> acceptedNameCodeToId,
                                      Map<String, String> synAcceptedCode) throws Exception {
    Map<String, String> nameKeyToTaxon = loadNameKeyToTaxon(acceptedNameCodeToId, synAcceptedCode);
    int n = 0, unmatched = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT HierarchyCode, Genus, Species, InfraSpecies, Distribution FROM DISTRIBUTION")) {
      while (rs.next()) {
        String area = rs.getString("Distribution");
        if (area == null || area.isBlank()) continue;
        String taxonId = nameKeyToTaxon.get(nameKey(rs.getString("HierarchyCode"), rs.getString("Genus"),
                                                     rs.getString("Species"), rs.getString("InfraSpecies")));
        if (taxonId == null) { unmatched++; continue; }
        Generator.set(distW, ColdpTerm.taxonID, taxonId);
        Generator.set(distW, ColdpTerm.area, area);
        Generator.set(distW, ColdpTerm.gazetteer, "text");
        distW.next();
        n++;
      }
    }
    LOG.info("Wrote {} name-keyed distributions ({} unmatched)", n, unmatched);
    return n;
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
