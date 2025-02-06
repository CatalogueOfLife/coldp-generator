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
import org.apache.commons.lang3.StringUtils;
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

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

  private final AuthzClient authzClient;
  private final Configuration kc;
  private AccessTokenResponse token;
  private TermWriter nomRelWriter;

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

    System.out.println("Refresh token");
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
        writeNames(res.results);
        if (StringUtils.isBlank(res.next)) {
          LOG.info("last page, stop");
          break;
        }
        page++;
      }
    }
  }

  //@Override
  public void run2() {
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

  String mapTaxStatus(String x) {
    if (x != null) {
      if (x.startsWith("orphaned")) {
        return "provisionally accepted";
      } else if (x.startsWith("synonym of")) {
        return "synonym";
      }
      return switch (x){
        case "correct name" -> "accepted";
        case "preferred name (not correct name)" -> "preferred";
        case "misspelling" -> "synonym";
        case "not in use" -> "bare name";
        default -> x;
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
        case "not validly published" -> NomStatus.NOT_ESTABLISHED;
        default -> null;
      };
    }
    if (stat != null) {
      return stat.name();
    } else if (nom != null) {
      return switch (nom){
        case "validly published under the ICNP" -> "available";
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
    System.out.println(resp);
    var n = resp.results.get(0);
    System.out.println(n);
  }
  void writeNames(List<String> ids) throws IOException {
    LOG.info("Retrieve {} names from the API", ids.size());
    String json = callAPI("/fetch/" + String.join(";", ids));
    var resp = mapper.readValue(json, FetchResult.class);
    for (var n : resp.results) {
      RemarksBuilder remarks = new RemarksBuilder();
      writer.set(ColdpTerm.ID, n.id);
      writer.set(ColdpTerm.parentID, n.lpsn_parent_id); // might be overwritten in case of synonyms
      writer.set(ColdpTerm.rank, n.category);
      writer.set(ColdpTerm.scientificName, n.full_name);
      writer.set(ColdpTerm.authorship, n.authority);
      writer.set(ColdpTerm.basionymID, n.basonym_id);
      writer.set(ColdpTerm.nameStatus, mapNomStatus(n.nomenclatural_status, n.lpsn_taxonomic_status));
      remarks.append(n.nomenclatural_status);
      writer.set(ColdpTerm.status, mapTaxStatus(n.lpsn_taxonomic_status));
      writer.set(ColdpTerm.link, n.lpsn_address);
      remarks.append(n.publication_text);
      writer.set(ColdpTerm.remarks, remarks.toString());
      if (!Objects.equals(n.id, n.lpsn_correct_name_id)) {
        writer.set(ColdpTerm.parentID, n.lpsn_correct_name_id);
      }
      writer.next();

      if (n.nomenclatural_type_id != null) {
        nomRelWriter.set(ColdpTerm.type, "type"); // has type name
        nomRelWriter.set(ColdpTerm.nameID, n.id);
        nomRelWriter.set(ColdpTerm.relatedNameID, n.nomenclatural_type_id);
        nomRelWriter.next();
      }
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    addSource(SOURCE);
    // now also use authors of the source as dataset authors!
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
