package org.catalogueoflife.data.ictv;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Reads the EVORA ICTV ontology as served by the EBI Ontology Lookup Service (OLS4).
 * See https://evora-project.github.io/ictv-ontology/
 *
 * Every class carries the stable ICTV identifier (e.g. ICTV20040588) as its short form, the current
 * name as label, a rank and the names it was known under in earlier MSL releases as synonyms of type
 * "previous name". Plain string synonyms are virus abbreviations (RABV, SARS-CoV), not names.
 */
public class IctvOntology {
  private static final String API = "https://www.ebi.ac.uk/ols4/api/v2/ontologies/ictv/classes";
  private static final String RANK = "http://purl.obolibrary.org/obo/TAXRANK_1000000";
  private static final String VERSION_INFO = "http://www.w3.org/2002/07/owl#versionInfo";
  private static final String PREVIOUS_NAME = "previous name";
  // ICTV placeholders such as "Unnamed genus" that are not real names
  private static final Pattern PLACEHOLDER = Pattern.compile("^unnamed\\b", Pattern.CASE_INSENSITIVE);

  public record PrevName(String name, String msl) {}

  public record Taxon(String id, String name, String rank, List<PrevName> previous) {
    /**
     * @return lookup key combining the lower case rank and the name
     */
    public String key() {
      return IctvOntology.key(rank, name);
    }
  }

  public static String key(String rank, String name) {
    return (rank == null ? "" : rank.toLowerCase(Locale.ROOT)) + "|" + name;
  }

  /**
   * @return the API url listing all classes of a given MSL release, e.g. MSL41
   */
  public static URI releasePage(String msl, int page, int size) {
    return URI.create(API + "?search=" + msl
        + "&searchFields=http__%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23versionInfo"
        + "&exactMatch=true&page=" + page + "&size=" + size);
  }

  public static int totalPages(JsonNode page) {
    return page.path("totalPages").asInt(0);
  }

  public static List<Taxon> parsePage(JsonNode page) {
    List<Taxon> taxa = new ArrayList<>();
    for (JsonNode e : page.path("elements")) {
      String id = text(e.path("shortForm"));
      String name = text(e.path("label").path(0));
      if (id == null || name == null) continue;
      taxa.add(new Taxon(id, name, rank(e), previousNames(e, name)));
    }
    return taxa;
  }

  private static String rank(JsonNode e) {
    String rankIri = text(e.path(RANK));
    if (rankIri != null) {
      return text(e.path("linkedEntities").path(rankIri).path("label").path(0));
    }
    return null;
  }

  private static List<PrevName> previousNames(JsonNode e, String currentName) {
    List<PrevName> names = new ArrayList<>();
    for (JsonNode syn : e.path("synonym")) {
      // plain strings are abbreviations only
      if (!syn.isObject()) continue;
      String name = text(syn.path("value"));
      if (name == null || name.equals(currentName) || PLACEHOLDER.matcher(name).find()) continue;
      for (JsonNode ax : syn.path("axioms")) {
        if (PREVIOUS_NAME.equals(text(ax.path("oboSynonymTypeName")))) {
          names.add(new PrevName(name, text(ax.path(VERSION_INFO))));
          break;
        }
      }
    }
    return names;
  }

  private static String text(JsonNode n) {
    if (n == null || n.isMissingNode() || n.isNull()) return null;
    String x = n.asText().trim();
    return x.isEmpty() ? null : x;
  }
}
