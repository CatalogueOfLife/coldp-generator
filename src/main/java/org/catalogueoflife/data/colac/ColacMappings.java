package org.catalogueoflife.data.colac;

import org.gbif.nameparser.api.NomCode;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.StringJoiner;

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
      case "synonym"                      -> "synonym";
      case "ambiguous synonym"            -> "ambiguous synonym";
      case "misapplied name"              -> "misapplied";
      default                             -> "synonym";
    };
  }

  /**
   * Maps a new-schema distribution_status label to a ColDP establishmentMeans value, or
   * null when it carries no useful establishment information.
   */
  static String establishment(String distStatus) {
    if (distStatus == null) return null;
    return switch (distStatus.trim().toLowerCase(Locale.ENGLISH)) {
      case "native", "native-domesticated"            -> "native";
      case "alien", "alien-domesticated", "domesticated" -> "introduced";
      default                                          -> null; // uncertain / unknown
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
}
