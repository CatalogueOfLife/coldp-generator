package org.catalogueoflife.data.utils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpResponse;

public class HttpException extends IOException {
  public final URI uri;
  public final int status;

  public HttpException(HttpResponse resp) {
    this(resp.uri(), resp.statusCode(), null);
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
