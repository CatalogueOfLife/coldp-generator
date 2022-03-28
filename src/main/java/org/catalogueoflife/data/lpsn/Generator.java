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

import com.github.scribejava.apis.KeycloakApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import life.catalogue.api.model.DOI;
import life.catalogue.coldp.ColdpTerm;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * LPSN export into ColDP using the LPSN API:
 * https://api.lpsn.dsmz.de/
 *
 * Please register at https://api.lpsn.dsmz.de/login.
 */
public class Generator extends AbstractGenerator {
  private static final String SSO = "https://sso.dsmz.de/auth/";
  private static final String API = "https://api.lpsn.dsmz.de";
  private static final String client_id = "api.lpsn.public";

  private static final DOI SOURCE = new DOI("10.1007/s00705-021-05156-1");
  private static final String USER = "mdoering@gbif.org";
  private static final String PASS = "xxx";
  private static final int batchSize = 100;
  private final OAuth20Service service;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
    service = new ServiceBuilder(USER)
      .apiSecret(PASS)
      .defaultScope("openid")// client_id ???
      //.callback(callback)
      .debug()
      .build(KeycloakApi.instance(SSO, "dsmz"));
  }

  public OAuth2AccessToken authToken() throws IOException, ExecutionException, InterruptedException {
    System.out.println("Fetching the Authorization URL...");
    final String authorizationUrl = service.getAuthorizationUrl();
    System.out.println("Got the Authorization URL!");
    System.out.println("Now go and authorize ScribeJava here:");
    System.out.println(authorizationUrl);
    System.out.println("And paste the authorization code here");
    System.out.print(">>");
    System.out.println();
    //var token = service.getRequestToken();

    return service.getAccessToken(PASS);
  }

  @Override
  protected void addData() throws Exception {
    // retrieve list of all ids first, then lookup each in batches
    LOG.info("{} records discovered in the API", 0);
    var token = authToken();

    // Now let's go and ask for a protected resource!
    System.out.println("Now we're going to access a protected resource...");
    final OAuthRequest request = new OAuthRequest(Verb.GET, API+"/fetch");
    service.signRequest(token, request);
    try (Response response = service.execute(request)) {
      System.out.println("Got it! Lets see what we found...");
      System.out.println();
      System.out.println(response.getCode());
      System.out.println(response.getBody());
    }

    // write just the NameUsage file
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.sequenceIndex,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.code,
      ColdpTerm.link,
      ColdpTerm.remarks
    ));

  }

  @Override
  protected void addMetadata() throws Exception {
    addSource(SOURCE);
    // now also use authors of the source as dataset authors!
    if (!sources.isEmpty()) {
      asYaml(sources.get(0).getAuthor()).ifPresent(yaml -> {
        metadata.put("authors", yaml);
      });
    }
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

}
