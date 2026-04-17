package org.catalogueoflife.data.wikispecies;

import org.sweble.wikitext.parser.nodes.WtBody;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.catalogueoflife.data.wikispecies.WtUtils.nodeText;

/**
 * Extracts vernacular names from a Wikispecies Vernacular names section body.
 *
 * Parses: {{VN |en=Black Wheatear |de=Trauersteinschmätzer |fr=Traquet rieur |...}}
 * Returns a list of (language, name) pairs.
 *
 * Uses regex on the raw section text because Sweble's WikitextParser (lazy mode)
 * does not produce WtTemplate nodes — {{...}} calls appear as WtText literals.
 */
public class VernacularExtractor {

  public record VernacularName(String language, String name) {}

  // Matches the full {{VN ...}} block (single or multi-line)
  private static final Pattern VN_PAT = Pattern.compile(
      "\\{\\{VN([^{}]*)\\}\\}", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  // Matches each |lang=name argument inside the VN block
  private static final Pattern ARG_PAT = Pattern.compile(
      "\\|\\s*([a-zA-Z][a-zA-Z0-9\\-]*)\\s*=\\s*([^|{}\r\n]+)");

  public static List<VernacularName> extract(WtBody body) {
    List<VernacularName> results = new ArrayList<>();
    // nodeText returns raw body text including literal {{VN|...}} since Sweble's lazy
    // parser does not convert template calls to WtTemplate nodes in article bodies.
    String text = nodeText(body);
    Matcher m = VN_PAT.matcher(text);
    if (m.find()) {
      String args = m.group(1);
      Matcher arg = ARG_PAT.matcher(args);
      while (arg.find()) {
        String lang = arg.group(1).trim();
        String name = arg.group(2).trim();
        if (!lang.isEmpty() && !name.isEmpty()) {
          results.add(new VernacularName(lang, name));
        }
      }
    }
    return results;
  }
}
