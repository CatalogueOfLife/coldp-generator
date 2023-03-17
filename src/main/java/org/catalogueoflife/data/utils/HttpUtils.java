package org.catalogueoflife.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Utilities to work with the native java http client.
 */
public class HttpUtils {
  private static Logger LOG = LoggerFactory.getLogger(HttpUtils.class);
  private final HttpClient client;
  private final String username;
  private final String password;

  public HttpUtils() {
    this(null, null);
  }

  public HttpUtils(String username, String password) {
    this.client = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.ALWAYS)
        .build();
    this.username = username;
    this.password = password;
  }

  public boolean exists(String url){
    try {
      head(url);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static Map<String, String> acceptJson(Map<String, String> header) {
    header.put("Accept", MediaType.APPLICATION_JSON);
    return header;
  }

  public HttpResponse<InputStream> head(String url) throws IOException {
    HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(url))
        .method("HEAD", HttpRequest.BodyPublishers.noBody());
    return send(req, new HashMap<>(), HttpResponse.BodyHandlers.ofInputStream());
  }


  public String get(String url) throws IOException {
    return get(URI.create(url));
  }

  public String get(URI url) throws IOException {
    return get(url, new HashMap<>());
  }

  public String get(URI url, Map<String, String> header) throws IOException {
    return send(HttpRequest.newBuilder(url), header, HttpResponse.BodyHandlers.ofString()).body();
  }

  public String getJSON(URI url) throws IOException {
    return getJSON(url, new HashMap<>());
  }

  public String getJSON(URI url, Map<String, String> header) throws IOException {
    return get(url, acceptJson(header));
  }

  public InputStream getStream(String url) throws IOException {
    return getStream(URI.create(url));
  }
  public InputStream getStream(URI url) throws IOException {
    return getStream(url, new HashMap<>());
  }
  public InputStream getStream(URI url, Map<String, String> header) throws IOException {
    return send(HttpRequest.newBuilder(url), header, HttpResponse.BodyHandlers.ofInputStream()).body();
  }
  public InputStream getStreamJSON(URI url) throws IOException {
    return send(HttpRequest.newBuilder(url), acceptJson(new HashMap<>()), HttpResponse.BodyHandlers.ofInputStream()).body();
  }
  public InputStream getStreamJSON(URI url, int retry) throws IOException {
    return send(HttpRequest.newBuilder(url), acceptJson(new HashMap<>()), HttpResponse.BodyHandlers.ofInputStream(), retry).body();
  }
  public InputStream getStreamJSON(URI url, Map<String, String> header) throws IOException {
    return send(HttpRequest.newBuilder(url), acceptJson(header), HttpResponse.BodyHandlers.ofInputStream()).body();
  }

  public void download(String url, File downloadTo) throws IOException {
    download(URI.create(url), downloadTo);
  }

  public void download(URI url, File downloadTo) throws IOException {
    download(url, new HashMap<>(), downloadTo);
  }

  public void download(URI url, Map<String, String> header, File downloadTo) throws IOException {
    // execute
    send(HttpRequest.newBuilder(url), header, HttpResponse.BodyHandlers.ofFile(downloadTo.toPath()));
  }

  public void downloadJSON(URI url, Map<String, String> header, File downloadTo) throws IOException {
    download(url, acceptJson(header), downloadTo);
  }

  public <T> HttpResponse<T> send(HttpRequest.Builder req, Map<String, String> header, HttpResponse.BodyHandler<T> bodyHandler) throws IOException {
    return sendInternal(req, header, bodyHandler, 0);
  }

  public <T> HttpResponse<T> send(HttpRequest.Builder req, Map<String, String> header, HttpResponse.BodyHandler<T> bodyHandler, int retry) throws IOException {
    return sendInternal(req, header, bodyHandler, retry);
  }

  private <T> HttpResponse<T> sendInternal(HttpRequest.Builder req, Map<String, String> header, HttpResponse.BodyHandler<T> bodyHandler, int retry) throws IOException {
    basicAuth(req);
    req.header("User-Agent", "ColDP-Generator/1.0");
    header.forEach(req::header);
    HttpResponse<T> resp = send(req, bodyHandler, 1);
    if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
      return resp;
    } else if (retry > 0) {
      try {
        LOG.info("Failed http request: {}. Retry {}", resp.statusCode(), req);
        TimeUnit.SECONDS.sleep(1);
        return sendInternal(req, header, bodyHandler, retry-1);
      } catch (InterruptedException e) {
      }
    }
    throw new HttpException(resp);
  }

  /**
   * Recursive method retrying to issue the request once in case we receive an http/w GOAWAY exception.
   */
  private  <T> HttpResponse<T> send(HttpRequest.Builder req, HttpResponse.BodyHandler<T> bodyHandler, int attempt) throws IOException {
    try {
      return client.send(req.build(), bodyHandler);
    } catch (IOException e) {
      // handle http/2 GOAWAY exceptions
      if (e.getMessage().contains("GOAWAY") && attempt <2) {
        LOG.info("GOAWAY received. Retry for {}: {}", req.build().uri(), e.getMessage());
        return send(req, bodyHandler, attempt+1);
      }
      throw e;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private HttpRequest.Builder basicAuth(HttpRequest.Builder req) {
    if (username != null) {
      String auth = "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
      req.header("Authorization", auth);
    }
    return req;
  }

  /**
   * Parses a RFC2616 compliant date string such as used in http headers.
   *
   * @see <a href="http://tools.ietf.org/html/rfc2616#section-3.3">RFC 2616</a> specification.
   *      example:
   *      Wed, 21 Jul 2010 22:37:31 GMT
   * @param rfcDate RFC2616 compliant date string
   * @return the parsed date or null if it cannot be parsed
   */
  private static Date parseHeaderDate(String rfcDate) {
    try {
      if (rfcDate != null) {
        // as its not thread safe we create a new instance each time
        return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z", Locale.US).parse(rfcDate);
      }
    } catch (ParseException e) {
      LOG.warn("Can't parse RFC2616 date");
    }
    return null;
  }

}
