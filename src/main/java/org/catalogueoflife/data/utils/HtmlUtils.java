package org.catalogueoflife.data.utils;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;

public class HtmlUtils {
  /**
   * Strips all html tags if they exist and optionally converts link to markdown links.
   */
  public static String replaceHtml(String x, boolean useMarkdownLinks) {
    if (StringUtils.isBlank(x)) return null;

    var doc = Jsoup.parse(x);
    if (useMarkdownLinks) {
      var links = doc.select("a");
      for (var link : links) {
        String url = link.attr("href");
        if (!StringUtils.isBlank(url)) {
          String md = String.format("[%s](%s)", link.text(), url);
          link.text(md);
        }
      }
    }
    return doc.wholeText().trim();
  }
}
