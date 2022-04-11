package org.catalogueoflife.data.utils;

import com.fasterxml.jackson.databind.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
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
    header.put("Accept", MediaType.APPLICATION_JSON);
    return get(url, header);
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

  public void download(String url, File downloadTo) throws IOException {
    download(URI.create(url), downloadTo);
  }


  public void download(URI url, File downloadTo) throws IOException {

  }

  public void download(URI url, Map<String, String> header, File downloadTo) throws IOException {
    // execute
    HttpResponse<Path> resp = send(HttpRequest.newBuilder(url), header, HttpResponse.BodyHandlers.ofFile(downloadTo.toPath()));
    LOG.info("Downloaded {} to {}", url, downloadTo.getAbsolutePath());
  }

  public <T> HttpResponse<T> send(HttpRequest.Builder req, Map<String, String> header, HttpResponse.BodyHandler<T> bodyHandler) throws IOException {
    basicAuth(req);
    req.header("User-Agent", "ColDP-Generator/1.0");
    header.forEach(req::header);
    try {
      HttpResponse<T> resp = client.send(req.build(), bodyHandler);
      if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
        return resp;
      }
      throw new HttpException(resp);
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

  private HttpRequest.Builder json(String url) {
    return HttpRequest.newBuilder(URI.create(url))
          .header("Content-Type", "application/json")
          .header("Accept", "application/json");
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
