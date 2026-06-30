package org.catalogueoflife.data.wikidata;

import java.util.List;

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

  static String mapSex(String qid) {
    if (qid == null) return null;
    return switch (qid) {
      case "Q6581097" -> "male";
      case "Q6581072" -> "female";
      default -> null;
    };
  }
}
