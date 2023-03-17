package org.catalogueoflife.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpResponse;

public class HttpException extends IOException {
  private static Logger LOG = LoggerFactory.getLogger(HttpException.class);
  public final URI uri;
  public final int status;

  public HttpException(HttpResponse resp) {
    this(resp.uri(), resp.statusCode(), null);
    StringBuilder sb = new StringBuilder();
    sb.append("Header:\n");
    sb.append(resp.headers());
    sb.append("Body: \n");
    sb.append(resp.body());
    LOG.debug("HTTP {} exception from {}: {}", resp.statusCode(), resp.uri(), sb);
  }

  public HttpException(URI uri, int status) {
    this(uri, status, null);
  }

  public HttpException(URI uri, int status, String message) {
    super(message);
    this.uri = uri;
    this.status = status;
  }

  @Override
  public String toString() {
    return "HttpException{" + status + " " + uri + '}';
  }
}
