package org.catalogueoflife.data.wikidata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Pure mapping helpers for the Wikidata generator. No IO so they unit-test. */
public class WikidataMappings {

  private WikidataMappings() {}

  static String extractYear(String date) {
    if (date == null) return null;
    String c = date.startsWith("+") ? date.substring(1) : date;
    return c.length() >= 4 ? c.substring(0, 4) : c;
  }

  static String joinFamilies(List<String> families) {
    List<String> fs = families.stream().filter(f -> f != null && !f.isBlank()).toList();
    if (fs.isEmpty()) return null;
    if (fs.size() == 1) return fs.get(0);
    if (fs.size() == 2) return fs.get(0) + " & " + fs.get(1);
    return String.join(", ", fs.subList(0, fs.size() - 1)) + " & " + fs.get(fs.size() - 1);
  }

  static String flatAuthorship(List<String> families, String year, boolean recombination) {
    String base = joinFamilies(families);
    if (base == null) return null;
    String s = (year != null && !year.isBlank()) ? base + ", " + year : base;
    return recombination ? "(" + s + ")" : s;
  }

  static String lastToken(String label) {
    if (label == null || label.isBlank()) return null;
    String[] parts = label.trim().split("\\s+");
    return parts[parts.length - 1];
  }

  static String givenFromLabel(String label, String family) {
    if (label == null) return null;
    String g = label.trim();
    if (family != null && g.endsWith(family)) {
      g = g.substring(0, g.length() - family.length()).trim();
    }
    return g.isBlank() ? null : g;
  }

  record Authorship(String combinationAuthorship, String combinationAuthorshipID,
                    String combinationAuthorshipYear, String basionymAuthorship,
                    String basionymAuthorshipID, String basionymAuthorshipYear, String flat) {}

  static Authorship assembleAuthorship(WikidataDumpReader.NameAuthorship na,
                                       Map<String, WikidataDumpReader.AuthorInfo> authors) {
    if (na == null || na.authorQids().isEmpty()) return null;
    List<String> families = new ArrayList<>();
    for (String qid : na.authorQids()) {
      WikidataDumpReader.AuthorInfo ai = authors.get(qid);
      families.add(ai != null && ai.family() != null ? ai.family() : qid);
    }
    String fam = joinFamilies(families);
    String ids = String.join(",", na.authorQids());
    String flat = flatAuthorship(families, na.year(), na.recombination());
    if (na.recombination()) {
      return new Authorship(null, null, null, fam, ids, na.year(), flat);
    }
    return new Authorship(fam, ids, na.year(), null, null, null, flat);
  }

  static String mapSex(String qid) {
    if (qid == null) return null;
    return switch (qid) {
      case "Q6581097" -> "male";
      case "Q6581072" -> "female";
      default -> null;
    };
  }
}
