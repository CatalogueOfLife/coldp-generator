package org.catalogueoflife.data.colac;

import org.gbif.nameparser.api.NomCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pure mapping helpers shared by the old (2005–2011) and new (2012–2019) CoL Annual
 * Checklist schema readers. Kept free of any JDBC/IO so they can be unit tested.
 */
public class ColacMappings {

  private ColacMappings() {}

  /**
   * Maps a kingdom name to the nomenclatural code acronym used by ColDP, or null when the
   * kingdom is unknown / does not imply a single code.
   */
  static String nomCode(String kingdom) {
    if (kingdom == null) return null;
    return switch (kingdom.trim()) {
      case "Animalia", "Protozoa"          -> NomCode.ZOOLOGICAL.getAcronym();
      case "Plantae", "Fungi", "Chromista" -> NomCode.BOTANICAL.getAcronym();
      case "Bacteria", "Archaea"           -> NomCode.BACTERIAL.getAcronym();
      case "Viruses"                       -> NomCode.VIRUS.getAcronym();
      default                              -> null;
    };
  }

  /**
   * Maps a CoL sp2000 / scientific_name_status label (e.g. "accepted name",
   * "ambiguous synonym", "misapplied name", "provisionally accepted name", "synonym") to
   * the corresponding ColDP TaxonomicStatus value. Case-insensitive, whitespace tolerant.
   * Returns null for null/blank input.
   */
  static String status(String label) {
    if (label == null) return null;
    String l = label.trim().toLowerCase(Locale.ENGLISH);
    if (l.isEmpty()) return null;
    return switch (l) {
      case "accepted name"                -> "accepted";
      case "provisionally accepted name"  -> "provisionally accepted";
      case "synonym", "unambiguous synonym", "unambiguous synonyms" -> "synonym";
      case "ambiguous synonym", "ambiguous synonyms"                -> "ambiguous synonym";
      case "misapplied name"              -> "misapplied";
      default                             -> "synonym";
    };
  }

  /**
   * Reconstructs a scientific name (without authorship) from atomized parts. Parts are
   * emitted in the order genus, species, infraspecific marker, infraspecies; blank parts
   * are skipped and the result is single-space separated. Returns null when no part is set.
   */
  static String synonymName(String genus, String species, String marker, String infraspecies) {
    StringJoiner sj = new StringJoiner(" ");
    for (String part : new String[]{genus, species, marker, infraspecies}) {
      if (part != null && !part.isBlank()) {
        sj.add(part.trim());
      }
    }
    return sj.length() == 0 ? null : sj.toString();
  }

  /**
   * Joins reference IDs into a single ColDP multi-valued referenceID string using a comma
   * separator, de-duplicating while preserving order. Returns null for an empty input.
   *
   * @throws IllegalArgumentException if any ID contains the comma separator, which would
   *         corrupt the multi-valued field.
   */
  /**
   * Parses a comma-separated citation author list of the form "Surname I.I., Surname I., ..."
   * into a list of {@code [given, family]} pairs. Handles trailing lowercase particles
   * ("Nieukerken E. van" → family "van Nieukerken") and multi-word surnames ("De Wever A.").
   * Returns an empty list for null/blank input.
   */
  static List<String[]> parseEditors(String authors) {
    List<String[]> out = new ArrayList<>();
    if (authors == null || authors.isBlank()) return out;
    for (String name : authors.split(",")) {
      name = name.trim();
      if (name.isEmpty()) continue;
      String[] tokens = name.split("\\s+");
      int initIdx = -1;
      for (int i = 0; i < tokens.length; i++) {
        if (tokens[i].matches("(\\p{Lu}\\.)+")) { initIdx = i; break; }
      }
      if (initIdx < 0) { // no initials found: treat the whole as a family/organisation name
        out.add(new String[]{null, name});
        continue;
      }
      String given = tokens[initIdx];
      StringJoiner family = new StringJoiner(" ");
      for (int i = initIdx + 1; i < tokens.length; i++) family.add(tokens[i]); // trailing particles first
      for (int i = 0; i < initIdx; i++) family.add(tokens[i]);
      out.add(new String[]{given, family.toString()});
    }
    return out;
  }

  private static final Pattern PAREN = Pattern.compile("\\(([^)]*)\\)");

  /** Parsed source-database agents: author and editor name pairs (each {@code [given, family]}). */
  record Agents(List<String[]> authors, List<String[]> editors) {}

  /**
   * Parses a GSD "authors and editors" string into author and editor name pairs. Strips a
   * leading "©" and parenthetical scope tags (e.g. "(Pulmonata)"); a clause (";"-separated)
   * whose parenthetical is "(ed)"/"(eds)"/"(editor[s])" is treated as editors, otherwise
   * authors. Names within a clause are split on "&"/"," and parsed like {@link #parseEditors}.
   */
  static Agents parseAgents(String s) {
    List<String[]> authors = new ArrayList<>();
    List<String[]> editors = new ArrayList<>();
    if (s == null || s.isBlank()) return new Agents(authors, editors);
    for (String clause : s.replace("©", " ").split(";")) {
      if (clause.isBlank()) continue;
      boolean editor = false;
      Matcher m = PAREN.matcher(clause);
      while (m.find()) {
        String tag = m.group(1).trim().toLowerCase(Locale.ENGLISH).replace(".", "");
        if (tag.equals("ed") || tag.equals("eds") || tag.equals("editor") || tag.equals("editors")) {
          editor = true;
        }
      }
      String cleaned = PAREN.matcher(clause).replaceAll(" ").replace("&", ",");
      (editor ? editors : authors).addAll(parseEditors(cleaned));
    }
    return new Agents(authors, editors);
  }

  static String joinRefs(Collection<String> ids) {
    if (ids == null || ids.isEmpty()) return null;
    Set<String> unique = new LinkedHashSet<>();
    for (String id : ids) {
      if (id == null || id.isBlank()) continue;
      if (id.indexOf(',') >= 0) {
        throw new IllegalArgumentException("Reference ID contains the comma separator: " + id);
      }
      unique.add(id);
    }
    if (unique.isEmpty()) return null;
    return String.join(",", unique);
  }

  /**
   * Picks a single reference ID — the first non-blank one in iteration order. Only
   * {@code NameUsage.referenceID} may be multi-valued (comma-concatenated, see {@link #joinRefs});
   * every other ColDP referenceID (VernacularName, Distribution, …) takes exactly one id, so those
   * use this. Returns null for null/empty input.
   */
  static String firstRef(Collection<String> ids) {
    if (ids == null) return null;
    for (String id : ids) {
      if (id != null && !id.isBlank()) return id;
    }
    return null;
  }

  /** Canonical (upper-cased, trimmed) form of a name_code for case-insensitive matching;
   *  the early/old CoL data mixes e.g. "Mos-" and "MOS-". Returns null for null/blank. */
  static String normCode(String code) {
    if (code == null) return null;
    String s = code.trim();
    return s.isEmpty() ? null : s.toUpperCase(Locale.ENGLISH);
  }

  /** Resolves a name_code to the ColDP id of its accepted taxon, following synonym chains.
   *  acceptedNameCodeToId keys and synAcceptedCode keys/values are pre-normalised with normCode. */
  static String resolveAcceptedTaxon(String nameCode, Map<String, String> acceptedNameCodeToId,
                                     Map<String, String> synAcceptedCode) {
    String cur = nameCode;
    for (int guard = 0; guard < 25 && cur != null; guard++) {
      String tid = acceptedNameCodeToId.get(cur);
      if (tid != null) return tid;
      String next = synAcceptedCode.get(cur);
      if (next == null || next.equals(cur)) return null;
      cur = next;
    }
    return null;
  }
}
