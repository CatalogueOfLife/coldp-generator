/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.catalogueoflife.data.lpsn;

import life.catalogue.api.model.DOI;
import life.catalogue.api.vocab.NomStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.HttpException;
import org.catalogueoflife.data.utils.RemarksBuilder;
import org.gbif.nameparser.api.NomCode;
import org.keycloak.authorization.client.AuthzClient;
import org.keycloak.authorization.client.Configuration;
import org.keycloak.authorization.client.util.Http;
import org.keycloak.protocol.oidc.client.authentication.ClientCredentialsProvider;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.representations.adapters.config.AdapterConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * LPSN export into ColDP using the LPSN API:
 * https://api.lpsn.dsmz.de/
 *
 * Please register at https://api.lpsn.dsmz.de/login.
 */
public class Generator extends AbstractColdpGenerator {
  private static final String SSO = "https://sso.dsmz.de/auth";
  private static final String API = "https://api.lpsn.dsmz.de";
  private static final String client_id = "api.lpsn.public";

  private static final DOI SOURCE = new DOI("10.1099/ijsem.0.004332");

  private static final int FETCH_BATCH = 50;
  // website crawl for the cyanobacteria the API withholds
  private static final String WEB = "https://lpsn.dsmz.de";
  private static final String CYANO_ROOT = "/phylum/cyanobacteriota";
  private static final String USER_AGENT =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
  private static final int CRAWL_DELAY_MS = 200;

  private final AuthzClient authzClient;
  private final Configuration kc;
  private AccessTokenResponse token;
  private TermWriter nomRelWriter;
  // Records are buffered and written only after referential closure, so we know the full set
  // of resolvable ids and never emit a dangling parent/basionym/type reference.
  // LPSN advanced_search (validly-published yes/no) does not return every record reachable via
  // relationships, and some referenced records (chiefly ICN Botanical Code cyanobacteria correct
  // names) are not served by the API at all — neither by search nor by /fetch. References to
  // those unresolvable records are repaired at write time (see writeRecords()).
  private final Map<Integer, FetchDetail> records = new LinkedHashMap<>();
  private final Set<Integer> referenced = new HashSet<>();
  private final Set<Integer> attempted = new HashSet<>();
  // cyanobacteria website crawl state
  private final Map<Integer, LpsnPage> pages = new LinkedHashMap<>();
  private final Map<String, Integer> urlToId = new HashMap<>();
  private final Map<Integer, Integer> crawlParent = new HashMap<>();
  private final Set<String> visitedUrls = new HashSet<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
    kc = new Configuration();
    //kc.setHttpClient(hc); // need to migrate to hc5 or use a separate instance
    kc.setRealm("dsmz");
    kc.setAuthServerUrl(SSO);
    kc.setResource(client_id);
    kc.setCredentials(Map.of("secret", "secret"));
    authzClient = AuthzClient.create(kc);
  }

  /**
   * Unbelievable, but apparently there is no way to refresh the token with the auth client
   * https://stackoverflow.com/questions/51091376/java-client-to-refresh-keycloak-token
   */
  public void refreshToken() {
    String url = kc.getAuthServerUrl() + "/realms/" + kc.getRealm() + "/protocol/openid-connect/token";
    String secret = (String) kc.getCredentials().get("secret");
    Http http = new Http(kc, new ClientCredentialsProvider() {
      @Override
      public String getId() {
        return null;
      }

      @Override
      public void init(AdapterConfig adapterConfig, Object o) {
      }

      @Override
      public void setClientCredentials(AdapterConfig adapterConfig, Map<String, String> map, Map<String, String> map1) {
      }

    });

    LOG.info("Refresh token");
    token = http.<AccessTokenResponse>post(url)
            .authentication()
            .client()
            .form()
            .param("grant_type", "refresh_token")
            .param("refresh_token", token.getRefreshToken())
            .param("client_id", kc.getResource())
            .param("client_secret", secret)
            .response()
            .json(AccessTokenResponse.class)
            .execute();
  }

  public String callAPI(String path) {
    try {
      return callAPIInternal(path);
    } catch (HttpException e) {
      // 401 could mean an expired token
      // Access token might have expired (15 minutes life time).
      // Get new tokens using refresh token and try again.
      if (e.status == 401) {
        refreshToken();
        try {
          return callAPIInternal(path);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      throw new RuntimeException(e);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String callAPIInternal(String path) throws IOException {
    var header = new HashMap<>(Map.of(
            "Authorization", "Bearer "+token.getToken()
    ));
    var uri = URI.create(API+path);
    LOG.debug("GET {}", uri);
    return http.getJSON(uri, header);
  }

  public static class SearchResult {
    public Integer count;
    public String next;
    public List<String> results;
  }
  public static class FetchResult {
    public int count;
    public List<FetchDetail> results;
  }

  public static class FetchDetail {
    public int id;
    public Integer lpsn_parent_id;
    public Integer lpsn_correct_name_id;
    public String monomial;
    public String species_epithet;
    public String subspecies_epithet;
    public String full_name;
    public String authority;
    public String category;
    public String proposed_as;
    public String validly_published;
    public Boolean is_legitimate;
    public String nomenclatural_status;
    public Boolean is_spelling_corrected;
    public Integer nomenclatural_type_id;
    public List<String> type_strain_names;
    public Integer basonym_id;
    public String publication_text;
    public String publication_kind;
    public String ijsem_list_text;
    public String ijsem_list_kind;
    public List<Map<String, String>> emendations;
    public List<Map<String, String>> molecules;
    public String lpsn_taxonomic_status;
    public String lpsn_address;
  }

  @Override
  protected void addData() throws Exception {
    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.basionymID,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.nameStatus,
      ColdpTerm.status,
      ColdpTerm.link,
      ColdpTerm.remarks
    ));

    nomRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
      ColdpTerm.nameID,
      ColdpTerm.relatedNameID,
      ColdpTerm.type
    ));

    // get first auth token
    token = authzClient.obtainAccessToken(cfg.lpsnUsername, cfg.lpsnPassword);

    // retrieve list of all ids first, then lookup each in batches
    for (boolean valid : new boolean[]{true, false}) {
      int page = 0;
      while (page < 1000) {
        String json = callAPI("/advanced_search?validly-published=" + (valid?"yes":"no") + "&page=" + page);
        var res = mapper.readValue(json, SearchResult.class);
        if (res == null || res.results == null || res.results.isEmpty()) {
          break;
        }
        LOG.info("{} {} names from {} discovered on page {}", res.results.size(), valid? "valid":"invalid", res.count, page);
        collectNames(res.results);
        if (StringUtils.isBlank(res.next)) {
          LOG.info("last page, stop");
          break;
        }
        page++;
      }
    }

    // the yes/no facets miss records only reachable via relationships (e.g. ICN Botanical
    // Code cyanobacteria correct names); fetch as many as the API will serve
    closeReferences();
    // the API withholds cyanobacteria entirely; recover that subtree from the website
    crawlCyanobacteria();
    // write everything, repairing any references still unresolvable
    writeRecords();
  }

  //@Override
  public void runTest() {
    try {
      // get first auth token
      token = authzClient.obtainAccessToken(cfg.lpsnUsername, cfg.lpsnPassword);
      lookup("43717");

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        hc.close();
      } catch (IOException e) {
        LOG.error("Failed to close http client", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * https://lpsn.dsmz.de/text/glossary
   */
  String mapTaxStatus(String x) {
    if (x != null) {
      if (x.startsWith("orphaned")
        || x.startsWith("in need of a replacement")
        || x.startsWith("preferred")
      ) {
        return "provisionally accepted";

      } else if (x.startsWith("synonym")
              || x.startsWith("misspelling")
      ) {
        return "synonym";

      } else if (x.startsWith("correct name")) {
        return "accepted";
      }

      return switch (x){
        case "pro-correct name" -> "provisionally accepted";
        case "not in use" -> "bare name";
        default -> {
          LOG.warn("Unknown status: " + x);
          yield x;
        }
      };
    }
    return null;
  }

  String mapNomStatus(String nom, String tax) {
    NomStatus stat = null;
    if (tax != null) {
      stat = switch (tax){
        case "validly published under the ICNP" -> NomStatus.ESTABLISHED;

        case "validly published under the ICNP, rejected name" -> NomStatus.REJECTED;
        case "validly published under the ICNP, rejected name, later homonym" -> NomStatus.REJECTED;

        case "validly published under the ICNP, illegitimate name, later homonym" -> NomStatus.UNACCEPTABLE;
        case "in need of a replacement" -> NomStatus.UNACCEPTABLE;

        case "not validly published" -> NomStatus.NOT_ESTABLISHED;
        default -> null;
      };
    }
    if (stat != null) {
      return stat.name();
    } else if (nom != null) {
      return switch (nom){
        case "validly published under the ICNP" -> "available";
        case "validly published under the ICN (Botanical Code)" -> "available";
        case "not validly published" -> "unavailable";
        default -> null;
      };
    }
    return null;
  }
  NomCode code(String nomStatus) {
    return switch (nomStatus){
      case "validly published under the ICN (Botanical Code)" -> NomCode.BOTANICAL;
      default -> NomCode.BACTERIAL;
    };
  }
  void lookup(String id) throws IOException {
    String json = callAPI("/fetch/" + id);
    var resp = mapper.readValue(json, FetchResult.class);
    LOG.debug("{}", resp);
    var n = resp.results.get(0);
    LOG.debug("{}", n);
  }
  void collectNames(List<String> ids) throws IOException {
    LOG.info("Retrieve {} names from the API", ids.size());
    String json = callAPI("/fetch/" + String.join(";", ids));
    var resp = mapper.readValue(json, FetchResult.class);
    for (var n : resp.results) {
      records.putIfAbsent(n.id, n);
      addRef(n.lpsn_parent_id);
      addRef(n.lpsn_correct_name_id);
      addRef(n.basonym_id);
      addRef(n.nomenclatural_type_id);
    }
  }

  private void addRef(Integer id) {
    if (id != null) {
      referenced.add(id);
    }
  }

  /**
   * Write all buffered records once referential closure is complete. References to ids we could
   * not obtain from the API are repaired here rather than emitted as dangling pointers:
   *  - a synonym whose correct name is unresolvable becomes a bare name (no parent), never
   *    re-filed to an ancestor;
   *  - an unresolvable basionymID is dropped;
   *  - a type NameRelation to an unresolvable id is skipped.
   */
  private void writeRecords() throws IOException {
    final Set<Integer> known = records.keySet();
    int bareNames = 0, droppedBasionyms = 0;
    for (var n : records.values()) {
      RemarksBuilder remarks = new RemarksBuilder();
      boolean isSynonym = !Objects.equals(n.id, n.lpsn_correct_name_id);
      String status = mapTaxStatus(n.lpsn_taxonomic_status);
      Integer parentId;
      if (isSynonym) {
        if (n.lpsn_correct_name_id != null && known.contains(n.lpsn_correct_name_id)) {
          parentId = n.lpsn_correct_name_id; // synonym points at its accepted name
        } else {
          // accepted name not served by the API: cannot be a ColDP synonym, emit as a bare name
          parentId = null;
          status = "bare name";
          bareNames++;
        }
      } else {
        // accepted taxon: keep the hierarchy parent only if we actually have it
        parentId = (n.lpsn_parent_id != null && known.contains(n.lpsn_parent_id)) ? n.lpsn_parent_id : null;
      }

      writer.set(ColdpTerm.ID, n.id);
      writer.set(ColdpTerm.parentID, parentId);
      writer.set(ColdpTerm.rank, n.category);
      writer.set(ColdpTerm.scientificName, n.full_name);
      writer.set(ColdpTerm.authorship, n.authority);
      if (n.basonym_id != null && known.contains(n.basonym_id)) {
        writer.set(ColdpTerm.basionymID, n.basonym_id);
      } else if (n.basonym_id != null) {
        droppedBasionyms++;
      }
      writer.set(ColdpTerm.nameStatus, mapNomStatus(n.nomenclatural_status, n.lpsn_taxonomic_status));
      remarks.append(n.nomenclatural_status);
      writer.set(ColdpTerm.status, status);
      writer.set(ColdpTerm.link, n.lpsn_address);
      remarks.append(n.publication_text);
      writer.set(ColdpTerm.remarks, remarks.toString());
      writer.next();

      if (n.nomenclatural_type_id != null && known.contains(n.nomenclatural_type_id)) {
        nomRelWriter.set(ColdpTerm.type, "type"); // has type name
        nomRelWriter.set(ColdpTerm.nameID, n.id);
        nomRelWriter.set(ColdpTerm.relatedNameID, n.nomenclatural_type_id);
        nomRelWriter.next();
      }
    }
    LOG.info("Wrote {} records; {} synonyms with an unresolvable correct name emitted as bare names, {} unresolvable basionym links dropped",
             records.size(), bareNames, droppedBasionyms);
  }

  /**
   * Fetch any record referenced as a correct name, parent, basionym or type that the
   * validly-published yes/no crawl did not return. LPSN's advanced_search facets are not
   * exhaustive, so without this pass those references would dangle. The /fetch endpoint
   * resolves most ids regardless of publication status, though some referenced records
   * (chiefly ICN Botanical Code cyanobacteria correct names) are not served by the API at
   * all — those are repaired at write time. Iterates until no new ids appear, as freshly
   * fetched records may themselves reference further missing records.
   */
  private void closeReferences() throws IOException {
    int round = 0;
    while (true) {
      List<String> missing = new ArrayList<>();
      for (Integer id : referenced) {
        // negative ids are LPSN's synthetic "not assigned to X" placeholder nodes, always
        // returned by the crawl; only chase positive ids we have not seen or tried yet
        if (id != null && id > 0 && !records.containsKey(id) && !attempted.contains(id)) {
          missing.add(id.toString());
        }
      }
      if (missing.isEmpty()) {
        break;
      }
      round++;
      LOG.info("Referential closure round {}: fetch {} referenced records missing from the search crawl", round, missing.size());
      for (String idStr : missing) {
        attempted.add(Integer.valueOf(idStr));
      }
      for (int i = 0; i < missing.size(); i += FETCH_BATCH) {
        collectNames(missing.subList(i, Math.min(i + FETCH_BATCH, missing.size())));
      }
    }
    long unresolvable = referenced.stream().filter(id -> id > 0 && !records.containsKey(id)).count();
    if (unresolvable > 0) {
      LOG.warn("{} referenced records are not served by the LPSN API; their references will be repaired at write time", unresolvable);
    }
  }

  /**
   * Recover the cyanobacteria that the LPSN API withholds by crawling the website top-down
   * from the phylum. The subtree is anchored on phylum Cyanobacteriota (30362), which is
   * validly published under the ICNP and thus already present from the API crawl, so every
   * scraped node connects to an API-present ancestor. Each page's record number is reused
   * verbatim as its ColDP id (the same id space the API uses), so scraped nodes fill the
   * gaps and the previously-dangling correct-names now resolve. Records already obtained
   * from the API stay authoritative; we still recurse through them to reach their missing
   * descendants.
   */
  private void crawlCyanobacteria() {
    crawlSubtree(CYANO_ROOT);
  }

  /** Crawl and merge a website subtree from the given root path. Package-private for tests. */
  void crawlSubtree(String root) {
    LOG.info("Crawl the LPSN website subtree from root {}", root);
    crawlPage(root, null);
    LOG.info("Fetched {} website pages", pages.size());
    mergeCrawledRecords();
  }

  /** The record synthesized/fetched for an id, or null. Package-private for tests. */
  FetchDetail record(int id) {
    return records.get(id);
  }

  private void crawlPage(String path, Integer parentId) {
    if (!visitedUrls.add(path)) {
      return; // already fetched (LPSN shows the same node under multiple pages)
    }
    LpsnPage page = fetchPage(path);
    if (page == null || page.recordNo == null) {
      return;
    }
    if (pages.putIfAbsent(page.recordNo, page) != null) {
      return; // seen under another path
    }
    page.url = path;
    urlToId.put(path, page.recordNo);
    if (parentId != null) {
      crawlParent.putIfAbsent(page.recordNo, parentId);
    }
    // accepted children and synonyms are all parented to this node
    for (String child : page.childTaxaLinks) {
      crawlPage(child, page.recordNo);
    }
    for (String syn : page.synonymLinks) {
      crawlPage(syn, page.recordNo);
    }
  }

  private LpsnPage fetchPage(String path) {
    String slug = path.replaceFirst("^/", "").replace("/", "-");
    File f = sourceFile("web-" + slug + ".html");
    if (!f.exists()) {
      if (cfg.noDownload) {
        LOG.warn("LPSN: --no-download set but {} not cached; skipping", f.getName());
        return null;
      }
      try {
        Document doc = Jsoup.connect(WEB + path).userAgent(USER_AGENT).timeout(20_000).get();
        FileUtils.write(f, doc.outerHtml(), StandardCharsets.UTF_8);
      } catch (Exception e) {
        LOG.warn("LPSN: failed to download {}: {}", path, e.getMessage());
        return null;
      }
      crawlDelay(CRAWL_DELAY_MS);
    }
    try {
      return LpsnPage.parse(FileUtils.readFileToString(f, StandardCharsets.UTF_8));
    } catch (Exception e) {
      LOG.warn("LPSN: failed to parse {}: {}", path, e.getMessage());
      return null;
    }
  }

  /** Synthesize a {@link FetchDetail} per scraped page and add the ones the API lacks. */
  private void mergeCrawledRecords() {
    int added = 0;
    for (LpsnPage p : pages.values()) {
      if (records.containsKey(p.recordNo)) {
        continue; // API record is authoritative
      }
      FetchDetail n = new FetchDetail();
      n.id = p.recordNo;
      n.full_name = p.name;
      n.authority = p.author;
      n.category = p.rank;
      n.nomenclatural_status = p.nomStatus;
      n.lpsn_taxonomic_status = p.taxStatus;
      n.lpsn_address = WEB + p.url;
      n.basonym_id = resolveLink(p.basionymLink);
      n.nomenclatural_type_id = resolveLink(p.typeLink);
      if ("synonym".equals(mapTaxStatus(p.taxStatus))) {
        Integer correct = resolveLink(p.correctNameLink);
        if (correct == null) {
          correct = crawlParent.get(p.recordNo); // reached from its accepted page
        }
        n.lpsn_correct_name_id = correct;
        n.lpsn_parent_id = correct;
      } else {
        n.lpsn_correct_name_id = n.id;
        n.lpsn_parent_id = crawlParent.get(p.recordNo);
      }
      records.put(n.id, n);
      added++;
    }
    LOG.info("Added {} cyanobacteria records recovered from the website (not served by the API)", added);
  }

  /** Resolve a scraped taxon-page href to a crawled record number, or null if outside the subtree. */
  private Integer resolveLink(String href) {
    return href == null ? null : urlToId.get(href);
  }

  @Override
  protected void addMetadata() throws Exception {
    addSource(SOURCE);
    // now also use authors of the source as dataset authors!
    metadata.put("authors", "");
    if (!sourceCitations.isEmpty()) {
      asYaml(sourceCitations.get(0).getAuthor()).ifPresent(yaml -> {
        metadata.put("authors", yaml);
      });
    }
    addSource(new DOI("10.1099/00207713-47-2-590")); // List of Bacterial Names with Standing in Nomenclature: a Folder Available on the Internet
    addSource(new DOI("10.1099/ijs.0.052316-0")); // Retirement of Professor Jean Paul Euzéby as list editor
    addSource(new DOI("10.1099/ijsem.0.000778")); // International Code of Nomenclature of Prokaryotes. Prokaryotic Code (2008 revision)
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

}
