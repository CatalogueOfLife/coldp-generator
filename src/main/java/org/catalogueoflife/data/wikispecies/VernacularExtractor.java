package org.catalogueoflife.data.wikispecies;

import org.sweble.wikitext.parser.nodes.*;

import java.util.ArrayList;
import java.util.List;

import static org.catalogueoflife.data.wikispecies.WtUtils.*;

/**
 * Extracts vernacular names from a Wikispecies Vernacular names section body.
 *
 * Parses: {{VN |en=Black Wheatear |de=Trauersteinschmätzer |fr=Traquet rieur |...}}
 * Returns a list of (language, name) pairs.
 */
public class VernacularExtractor {

  public record VernacularName(String language, String name) {}

  public static List<VernacularName> extract(WtBody body) {
    List<VernacularName> results = new ArrayList<>();
    collectVN(body, results);
    return results;
  }

  private static void collectVN(WtNode node, List<VernacularName> results) {
    if (node instanceof WtTemplate) {
      WtTemplate t = (WtTemplate) node;
      String tname = templateName(t).toLowerCase();
      if (tname.equals("vn")) {
        // All named args are language=name pairs
        for (WtNode argNode : t.getArgs()) {
          if (!(argNode instanceof WtTemplateArgument)) continue;
          WtTemplateArgument arg = (WtTemplateArgument) argNode;
          if (!arg.hasName()) continue;
          String lang = arg.getName().getAsString().trim();
          String name = nodeText(arg.getValue()).trim();
          if (!lang.isEmpty() && !name.isEmpty()) {
            results.add(new VernacularName(lang, name));
          }
        }
      }
      return; // don't recurse into templates
    }
    for (WtNode child : node) {
      collectVN(child, results);
    }
  }
}
