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

    // accepted classification tree: child→parent + the kingdom name at each root; the set of
    // accepted taxa record_ids (used to repair dangling parent links); plus the accepted
    // name_code→ColDP id map. Only ACCEPTED taxa nodes are mapped: a non-accepted node is never
    // emitted, so a reference resolved to it would dangle (synonym parentID, vernacular/distribution
    // taxonID). Synonym-coded references are instead resolved to their accepted taxon via the chain.
    // acceptedByNameParent indexes accepted nodes by (name, parent_id) for accepted-homonym lookup;
    // neededParents collects every parent_id referenced by an accepted node (to bound the repair).
    Int2IntOpenHashMap childParent = new Int2IntOpenHashMap();
    childParent.defaultReturnValue(0);
    Int2ObjectOpenHashMap<String> kingdomRoot = new Int2ObjectOpenHashMap<>();
    IntOpenHashSet acceptedTaxa = new IntOpenHashSet();
    Map<String, String> acceptedNameCodeToId = new HashMap<>();
    Map<String, Integer> acceptedByNameParent = new HashMap<>();
    IntOpenHashSet neededParents = new IntOpenHashSet();
    loadTaxaTree(childParent, kingdomRoot, acceptedTaxa, acceptedNameCodeToId, acceptedByNameParent, neededParents);

    // synonym name_code → its declared accepted_name_code, used to follow synonym→synonym chains
    // (the 2005–2011 ITIS data has accepted_name_code values that are themselves synonyms) down to
    // an accepted name with an emitted taxon.
    Map<String, String> synAcceptedCode = loadSynonymChain(synonymIds);

    Map<String, String[]> accInfo = loadAcceptedInfo(statusLabels, acceptedIds);

    Map<String, String> nameRef = new HashMap<>();
    Map<String, Set<String>> taxonRef = new HashMap<>();
    Map<String, Set<String>> comNameRef = new HashMap<>();
    loadRefLinks(nameRef, taxonRef, comNameRef);

    loadSources();
    writeReferences();

    int nAcc = emitAccepted(childParent, kingdomRoot, acceptedTaxa,
        // repair infraspecies whose parent species is a synonym: accepted homonym of the same
        // binomial (Monarda fistulosa L., not the synonym Sims), else the synonym's accepted species
        // via accepted_name_code (e.g. Stipa nelsonii → Achnatherum nelsonii), else the genus.
        buildRepairedParents(neededParents, acceptedByNameParent, acceptedNameCodeToId, synAcceptedCode),
        accInfo, nameRef, taxonRef);
    // Some accepted names exist in scientific_names (status 1/2) but have no node in the taxa tree
    // (the source lists them with an empty Classification, e.g. Achaearanea hirta (Taczanowski)).
    // Emit them as accepted but parentless, so synonyms can link to their real accepted name.
    int nMiss = emitMissingAccepted(statusLabels, acceptedIds, familyKingdom, acceptedNameCodeToId, nameRef, taxonRef);
    int[] syn = emitSynonyms(statusLabels, synonymIds, familyKingdom, acceptedNameCodeToId, synAcceptedCode, nameRef, taxonRef);
    int nVern = emitVernaculars(acceptedNameCodeToId, synAcceptedCode, comNameRef);
    int nDist = emitDistributions(acceptedNameCodeToId, synAcceptedCode);
    LOG.info("Old schema done: {} accepted (+{} accepted without classification), {} synonyms, " +
        "{} bare names (synonym without accepted name), {} vernaculars, {} distributions",
        nAcc, nMiss, syn[0], syn[1], nVern, nDist);
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
                            IntOpenHashSet acceptedTaxa, Map<String, String> acceptedNameCodeToId,
                            Map<String, Integer> acceptedByNameParent, IntOpenHashSet neededParents) throws Exception {
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery("SELECT record_id, parent_id, name, name_code, is_accepted_name FROM taxa")) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        int parent = rs.getInt("parent_id");
        String name = rs.getString("name");
        childParent.put(id, parent);
        if (parent == 0) {
          kingdomRoot.put(id, name); // top-level node = kingdom
        }
        if (rs.getInt("is_accepted_name") == 1) {
          acceptedTaxa.add(id); // emitted by emitAccepted; valid parentID/taxonID target
          if (parent != 0) neededParents.add(parent); // candidate parent that may need repair
          String nc = normCode(rs.getString("name_code"));
          if (nc != null && !nc.isBlank()) {
            acceptedNameCodeToId.put(nc, "t" + id); // only accepted nodes are emitted
          }
          if (name != null && !name.isBlank()) {
            acceptedByNameParent.putIfAbsent(nameParentKey(name, parent), id); // for homonym lookup
          }
        }
      }
    }
    LOG.info("Loaded {} taxa tree nodes, {} accepted, {} kingdoms",
        childParent.size(), acceptedTaxa.size(), kingdomRoot.size());
  }

  /**
   * Streams the non-accepted taxa nodes that are used as parents (a species that is itself a
   * synonym) and resolves, for each, the accepted taxon its accepted children should hang under.
   * See {@link #repairParent}. Keeps only the nodes that resolve; the rest fall back to the nearest
   * accepted ancestor at emit time.
   */
  private Int2ObjectOpenHashMap<String> buildRepairedParents(
      IntOpenHashSet neededParents, Map<String, Integer> acceptedByNameParent,
      Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode) throws Exception {
    Int2ObjectOpenHashMap<String> repaired = new Int2ObjectOpenHashMap<>();
    int homonym = 0, viaAcceptedCode = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, parent_id, name, name_code FROM taxa WHERE is_accepted_name <> 1")) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        if (!neededParents.contains(id)) continue; // only nodes actually used as a parent
        String name = rs.getString("name");
        int parent = rs.getInt("parent_id");
        String nc = normCode(rs.getString("name_code"));
        Integer h = acceptedByNameParent.get(nameParentKey(name, parent));
        String r = repairParent(h, nc, acceptedNameCodeToId, synAcceptedCode);
        if (r != null) {
          repaired.put(id, r);
          if (h != null) homonym++; else viaAcceptedCode++;
        }
      }
    }
    LOG.info("Repaired {} synonym-parent links ({} via accepted homonym, {} via accepted_name_code)",
        repaired.size(), homonym, viaAcceptedCode);
    return repaired;
  }

  /** Composite key for the (name, parent_id) homonym index. */
  static String nameParentKey(String name, int parentId) {
    return name + "\u0001" + parentId; // \u0001 separator cannot occur in a scientific name
  }

  /**
   * Resolves the accepted taxon an accepted infraspecies should hang under when its parent species
   * node is itself a synonym (is_accepted_name=0, never emitted). Prefers the accepted homonym of
   * the same binomial ({@code acceptedHomonymId}); otherwise the synonym's accepted species reached
   * by following {@code accepted_name_code} ({@code parentNameCode}) through the synonym chain.
   * Returns null when neither resolves (caller falls back to {@link #acceptedAncestor}).
   */
  static String repairParent(Integer acceptedHomonymId, String parentNameCode,
                             Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode) {
    if (acceptedHomonymId != null) return "t" + acceptedHomonymId;
    if (parentNameCode != null && !parentNameCode.isBlank()) {
      return resolveAcceptedTaxon(parentNameCode, acceptedNameCodeToId, synAcceptedCode);
    }
    return null;
  }

  /** synonym name_code → accepted_name_code, for following synonym→synonym chains to an accepted name. */
  private Map<String, String> loadSynonymChain(String synonymIds) throws Exception {
    Map<String, String> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT name_code, accepted_name_code FROM scientific_names WHERE sp2000_status_id IN " + synonymIds)) {
      while (rs.next()) {
        String nc = normCode(rs.getString("name_code"));
        String acc = normCode(rs.getString("accepted_name_code"));
        if (nc != null && !nc.isBlank() && acc != null && !acc.isBlank()) {
          m.putIfAbsent(nc, acc);
        }
      }
    }
    LOG.info("Loaded {} synonym chain links", m.size());
    return m;
  }

  /** accepted name_code → [author, statusLabel, comment] from accepted scientific_names rows. */
  private Map<String, String[]> loadAcceptedInfo(Map<Integer, String> statusLabels, String acceptedIds) throws Exception {
    Map<String, String[]> m = new HashMap<>();
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT name_code, author, sp2000_status_id, comment FROM scientific_names " +
             "WHERE sp2000_status_id IN " + acceptedIds)) {
      while (rs.next()) {
        String nc = normCode(rs.getString("name_code"));
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
        String nc = normCode(rs.getString("name_code"));
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
                           IntOpenHashSet acceptedTaxa, Int2ObjectOpenHashMap<String> repairedParent,
                           Map<String, String[]> accInfo, Map<String, String> nameRef,
                           Map<String, Set<String>> taxonRef) throws Exception {
    int n = 0;
    int reparented = 0, viaAncestor = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, parent_id, name, taxon, name_code, database_id FROM taxa " +
             "WHERE is_accepted_name = 1")) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        int parent = rs.getInt("parent_id");
        String nc = normCode(rs.getString("name_code"));

        Generator.set(nameW, ColdpTerm.ID, "t" + id);
        // The 2005–2010 ITIS data attaches accepted infraspecies under a species node that is itself
        // a synonym (is_accepted_name=0) and is never emitted here. Redirect such links: prefer the
        // pre-resolved accepted homonym / accepted species (repairedParent), else the nearest
        // accepted ancestor (the genus), so the parentID always resolves.
        if (parent > 0) {
          String parentId;
          if (acceptedTaxa.contains(parent)) {
            parentId = "t" + parent;
          } else {
            parentId = repairedParent.get(parent);
            if (parentId == null) {
              int anc = acceptedAncestor(parent, childParent, acceptedTaxa);
              parentId = anc > 0 ? "t" + anc : null;
              if (anc > 0) viaAncestor++;
            }
            reparented++;
          }
          if (parentId != null) Generator.set(nameW, ColdpTerm.parentID, parentId);
        }
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
    LOG.info("Wrote {} accepted taxa ({} re-parented off a synonym species, {} of them only to the genus)",
        n, reparented, viaAncestor);
    return n;
  }

  /**
   * Walks up the {@code childParent} map from {@code parentId} (inclusive) to the nearest ancestor
   * that is an accepted taxa node. Last-resort fallback in {@link #emitAccepted} when a synonym
   * parent species has neither an accepted homonym nor a resolvable {@code accepted_name_code}
   * ({@link #repairParent}): the accepted infraspecies then attaches directly to the genus. All
   * kingdom roots are accepted, so this resolves for every node; the guard limit only protects
   * against a malformed cyclic tree. Returns 0 when no accepted ancestor exists (no parentID written).
   */
  static int acceptedAncestor(int parentId, Int2IntOpenHashMap childParent, IntOpenHashSet acceptedTaxa) {
    int cur = parentId;
    for (int guard = 0; guard < 100 && cur != 0; guard++) {
      if (acceptedTaxa.contains(cur)) return cur;
      cur = childParent.get(cur);
    }
    return 0;
  }

  /**
   * Emits accepted names that exist in {@code scientific_names} (accepted status) but have no node
   * in the {@code taxa} tree — the source lists them with an empty classification (e.g.
   * <em>Achaearanea hirta</em> (Taczanowski, 1873)). They are written as accepted but parentless,
   * and registered in {@code acceptedNameCodeToId} so synonyms can link to their real accepted name.
   * Must run before {@link #emitSynonyms}, {@link #emitVernaculars} and {@link #emitDistributions}.
   *
   * @return number of accepted-without-classification names emitted
   */
  private int emitMissingAccepted(Map<Integer, String> statusLabels, String acceptedIds,
                                  Map<Integer, String> familyKingdom, Map<String, String> acceptedNameCodeToId,
                                  Map<String, String> nameRef, Map<String, Set<String>> taxonRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, name_code, genus, species, infraspecies, infraspecies_marker, " +
             "author, sp2000_status_id, database_id, comment, family_id " +
             "FROM scientific_names WHERE sp2000_status_id IN " + acceptedIds)) {
      while (rs.next()) {
        String nc = normCode(rs.getString("name_code"));
        if (nc == null || nc.isBlank() || acceptedNameCodeToId.containsKey(nc)) continue; // already in the tree
        int id = rs.getInt("record_id");
        String accId = "a" + id;
        acceptedNameCodeToId.put(nc, accId); // so synonyms / vernaculars / distributions can link

        String marker = rs.getString("infraspecies_marker");
        String infra = rs.getString("infraspecies");
        Generator.set(nameW, ColdpTerm.ID, accId);
        // parentless on purpose: the source has no classification for these accepted names
        Generator.set(nameW, ColdpTerm.status, ColacMappings.status(statusLabels.get(rs.getInt("sp2000_status_id"))));
        Generator.set(nameW, ColdpTerm.rank, synonymRank(marker, infra));
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("genus"), rs.getString("species"), marker, infra));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.code, nomCode(familyKingdom.get(rs.getInt("family_id"))));
        int dbId = rs.getInt("database_id");
        if (dbId > 0) Generator.set(nameW, ColdpTerm.sourceID, "d" + dbId);
        Generator.set(nameW, ColdpTerm.nameReferenceID, nameRef.get(nc));
        Generator.set(nameW, ColdpTerm.referenceID, joinRefs(taxonRef.get(nc)));
        Generator.set(nameW, ColdpTerm.remarks, rs.getString("comment"));
        nameW.next();
        n++;
      }
    }
    LOG.info("Wrote {} accepted names without classification (missing from the taxa tree)", n);
    return n;
  }

  /**
   * Emits the {@code scientific_names} synonyms. A synonym whose {@code accepted_name_code} resolves
   * (through any synonym→synonym chain) to an emitted accepted taxon is written as a real synonym
   * pointing to it. A synonym whose accepted name does not exist at all in the source (the source
   * itself reports "no accepted name found", e.g. <em>Helix fulva</em>) cannot be a ColDP synonym,
   * so it is written as a <em>bare name</em> (no parent, no taxonomic data) with the original
   * synonym status kept in {@code remarks}.
   *
   * @return {@code [synonyms, bareNames]} counts
   */
  private int[] emitSynonyms(Map<Integer, String> statusLabels, String synonymIds, Map<Integer, String> familyKingdom,
                             Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode,
                             Map<String, String> nameRef, Map<String, Set<String>> taxonRef) throws Exception {
    int nSyn = 0, nBare = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT record_id, name_code, genus, species, infraspecies, infraspecies_marker, " +
             "author, accepted_name_code, sp2000_status_id, database_id, comment, family_id " +
             "FROM scientific_names WHERE sp2000_status_id IN " + synonymIds)) {
      while (rs.next()) {
        int id = rs.getInt("record_id");
        String nc = normCode(rs.getString("name_code"));
        String accCode = normCode(rs.getString("accepted_name_code"));
        String parentId = accCode == null ? null : resolveAcceptedTaxon(accCode, acceptedNameCodeToId, synAcceptedCode);
        String statusLabel = ColacMappings.status(statusLabels.get(rs.getInt("sp2000_status_id")));
        String marker = rs.getString("infraspecies_marker");
        String infra = rs.getString("infraspecies");
        String comment = rs.getString("comment");

        Generator.set(nameW, ColdpTerm.ID, "s" + id);
        Generator.set(nameW, ColdpTerm.scientificName,
            synonymName(rs.getString("genus"), rs.getString("species"), marker, infra));
        Generator.set(nameW, ColdpTerm.authorship, rs.getString("author"));
        Generator.set(nameW, ColdpTerm.rank, synonymRank(marker, infra));
        Generator.set(nameW, ColdpTerm.code, nomCode(familyKingdom.get(rs.getInt("family_id"))));
        int dbId = rs.getInt("database_id");
        if (dbId > 0) Generator.set(nameW, ColdpTerm.sourceID, "d" + dbId);
        if (nc != null) Generator.set(nameW, ColdpTerm.nameReferenceID, nameRef.get(nc));

        if (parentId != null) {
          // a real synonym pointing to its accepted taxon
          Generator.set(nameW, ColdpTerm.parentID, parentId);
          Generator.set(nameW, ColdpTerm.status, statusLabel);
          if (nc != null) Generator.set(nameW, ColdpTerm.referenceID, joinRefs(taxonRef.get(nc)));
          Generator.set(nameW, ColdpTerm.remarks, comment);
          nSyn++;
        } else {
          // no accepted name in the source → a bare name: name only, no parent / no taxon data
          Generator.set(nameW, ColdpTerm.status, "bare name");
          String note = (statusLabel != null ? statusLabel : "synonym") + " without accepted name";
          Generator.set(nameW, ColdpTerm.remarks,
              comment == null || comment.isBlank() ? note : note + "; " + comment);
          nBare++;
        }
        nameW.next();
      }
    }
    LOG.info("Wrote {} synonyms, {} bare names (synonym without an accepted name)", nSyn, nBare);
    return new int[]{nSyn, nBare};
  }

  private int emitVernaculars(Map<String, String> acceptedNameCodeToId, Map<String, String> synAcceptedCode,
                              Map<String, Set<String>> comNameRef) throws Exception {
    int n = 0;
    try (Statement st = streamStmt();
         ResultSet rs = st.executeQuery(
             "SELECT name_code, common_name, language, country FROM common_names")) {
      while (rs.next()) {
        String nc = normCode(rs.getString("name_code"));
        // attach to the accepted taxon, resolving synonym name_codes through the chain
        String taxonId = resolveAcceptedTaxon(nc, acceptedNameCodeToId, synAcceptedCode);
        String name = rs.getString("common_name");
        if (taxonId == null || name == null || name.isBlank()) continue;
        Generator.set(vernW, ColdpTerm.taxonID, taxonId);
        Generator.set(vernW, ColdpTerm.name, name);
        Generator.set(vernW, ColdpTerm.language, rs.getString("language"));
        Generator.set(vernW, ColdpTerm.country, rs.getString("country"));
        // VernacularName.referenceID is single-valued (only NameUsage may concatenate)
        Generator.set(vernW, ColdpTerm.referenceID, firstRef(comNameRef.get(nc)));
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
         ResultSet rs = st.executeQuery("SELECT name_code, distribution FROM distribution")) {
      while (rs.next()) {
        // attach to the accepted taxon, resolving synonym name_codes through the chain
        String taxonId = resolveAcceptedTaxon(normCode(rs.getString("name_code")), acceptedNameCodeToId, synAcceptedCode);
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
