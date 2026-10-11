package org.catalogueoflife.data.lcvp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Pure mappings from the LCVP export vocabulary to ColDP, unit tested in LcvpMappingsTest.
 */
class LcvpMappings {
  static final String ACCEPTED = "accepted";
  static final String SYNONYM = "synonym";
  static final String UNRESOLVED = "unresolved";
  static final String BARE_NAME = "bare name";
  /** "selbst" (German: self) in the Literature column marks a decision by the LCVP authors, not a publication */
  static final String SELF = "selbst";

  // LCVP marks hybrids by appending _x to the genus or epithet, once in a single case twice
  private static final Pattern HYBRID = Pattern.compile("(_x)+$");
  private static final int MAX_CHAIN = 10;

  private LcvpMappings() {}

  /** A genus or epithet with the LCVP hybrid suffix removed. */
  record Part(String value, boolean hybrid) {
    static Part of(String raw) {
      if (raw == null) return new Part(null, false);
      var m = HYBRID.matcher(raw);
      if (m.find()) {
        return new Part(raw.substring(0, m.start()), true);
      }
      return new Part(raw, false);
    }
  }

  /** Blank or a "---" / "nil" placeholder become null. */
  static String clean(String x) {
    if (x == null) return null;
    x = x.strip();
    return x.isEmpty() || x.equals("---") || x.equals("nil") ? null : x;
  }

  /**
   * The rank marker used in the scientific name, prefixed with notho for hybrids.
   * Returns null for species, which have no marker.
   */
  static String rankMarker(String rank, boolean hybrid) {
    if (rank == null) return null;
    String marker = switch (rank) {
      case "species" -> null;
      case "forma" -> "f.";
      case "subforma" -> "subf.";
      default -> rank;
    };
    return marker != null && hybrid ? "notho" + marker : marker;
  }

  /** ColDP notho value for the most specific hybrid part, or null if the name is no hybrid. */
  static String notho(Part genus, Part species, Part infra) {
    if (infra.hybrid()) return "infraspecific";
    if (species.hybrid()) return "specific";
    if (genus.hybrid()) return "generic";
    return null;
  }

  /**
   * Scientific name without authorship built from the atomized LCVP parts.
   * Hybrid genera and species get a multiplication sign, hybrid infraspecific taxa a notho rank marker.
   */
  static String scientificName(Part genus, Part species, String rank, Part infra) {
    StringBuilder sb = new StringBuilder();
    if (genus.hybrid()) sb.append("×");
    sb.append(genus.value());
    if (species.value() != null) {
      sb.append(" ");
      if (species.hybrid()) sb.append("×");
      sb.append(species.value());
    }
    if (infra.value() != null) {
      String marker = rankMarker(rank, infra.hybrid());
      if (marker != null) {
        sb.append(" ").append(marker);
      }
      sb.append(" ").append(infra.value());
    }
    return sb.toString();
  }

  /** ColDP taxonomic status. Unresolved names come with an accepted name too, so they become synonyms. */
  static String status(String lcvpStatus) {
    if (lcvpStatus == null) return null;
    return switch (lcvpStatus) {
      case ACCEPTED -> ACCEPTED;
      case SYNONYM, UNRESOLVED -> SYNONYM;
      // pass unknown values through so they show up in ChecklistBank rather than silently changing meaning
      default -> lcvpStatus;
    };
  }

  /** The individual citations of the Literature column, which separates them with semicolons. */
  static List<String> literature(String lit) {
    List<String> refs = new ArrayList<>();
    if (lit != null) {
      for (String r : lit.split(";")) {
        r = r.strip();
        if (!r.isEmpty()) {
          refs.add(r);
        }
      }
    }
    return refs;
  }

  /** Key under which an accepted species is indexed so its infraspecific taxa can find it. */
  static String speciesKey(String genus, String epithet) {
    return genus == null || epithet == null ? null : genus + "|" + epithet;
  }

  /**
   * Follows the output taxon links until an accepted name is reached.
   * LCVP should link directly to accepted names, but some synonyms point to unresolved names
   * and a few to ids that do not exist.
   *
   * @param id        the output taxon id to start from
   * @param accepted  ids of all accepted names
   * @param output    output taxon id for every name that is not accepted
   * @return the accepted id or null if the chain does not end in an accepted name
   */
  static String resolveAccepted(String id, Set<String> accepted, Map<String, String> output) {
    for (int i = 0; id != null && i < MAX_CHAIN; i++) {
      if (accepted.contains(id)) return id;
      String next = output.get(id);
      if (next == null || next.equals(id)) return null;
      id = next;
    }
    return null;
  }
}
