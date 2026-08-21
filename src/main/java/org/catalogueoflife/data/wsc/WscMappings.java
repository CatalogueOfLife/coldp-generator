package org.catalogueoflife.data.wsc;

import java.util.Locale;
import java.util.Map;

/**
 * Pure mappings from the WSC API vocabulary to ColDP, unit tested in WscMappingsTest.
 * <p>
 * WSC has a single {@code status} field mixing taxonomic and nomenclatural statements. Writing it
 * verbatim into the ColDP status fields is what caused the import problems reported in
 * <a href="https://github.com/CatalogueOfLife/data/issues/1662">data#1662</a>.
 */
class WscMappings {
  static final String ACCEPTED = "accepted";
  static final String SYNONYM = "synonym";
  static final String BARE_NAME = "bare name";

  private WscMappings() {}

  private static String norm(String wscStatus) {
    return wscStatus == null ? null : wscStatus.toUpperCase(Locale.ENGLISH).trim();
  }

  /**
   * ColDP taxonomic status for a WSC status.
   * <p>
   * HOMONYM_REPLACED matters most here: it is a junior homonym that WSC points at the replacement
   * name via validTaxon. ChecklistBank cannot parse the literal string, so it used to fall back to
   * accepted and the record surfaced as an accepted species sitting under another accepted species.
   */
  static String status(String wscStatus) {
    var s = norm(wscStatus);
    if (s == null) return null;
    return switch (s) {
      case "VALID" -> ACCEPTED;
      case "SYNONYM", "HOMONYM_REPLACED" -> SYNONYM;
      case "NOMEN_DUBIUM", "NOMEN_NUDUM", "DELETED" -> BARE_NAME;
      // pass unknown values through so a new WSC status shows up in ChecklistBank rather than
      // being silently turned into an accepted taxon
      default -> wscStatus;
    };
  }

  /**
   * ColDP nameStatus for a WSC status, or null when the status makes no nomenclatural statement.
   * SYNONYM, HOMONYM_REPLACED and DELETED are taxonomic, not nomenclatural, and emitting them here
   * produced the bulk of the "nomenclatural status invalid" issues.
   */
  static String nameStatus(String wscStatus) {
    var s = norm(wscStatus);
    if (s == null) return null;
    return switch (s) {
      case "VALID", "NOMEN_DUBIUM", "NOMEN_NUDUM" -> wscStatus;
      default -> null;
    };
  }

  /** Key under which an accepted species is indexed so its subspecies can find it. */
  static String speciesKey(String genusLsid, String specificEpithet) {
    return genusLsid == null || specificEpithet == null ? null
        : genusLsid + "|" + specificEpithet.toLowerCase(Locale.ENGLISH);
  }

  /**
   * The classification parent for a taxon that is not a synonym.
   * <p>
   * A subspecies belongs under its species. The API gives no species LSID on a subspecies record,
   * only the genus, so the species is looked up by genus + specific epithet; if it is not an
   * accepted species we fall back to the genus, which is where every subspecies used to land.
   *
   * @param acceptedSpecies {@link #speciesKey} to LSID, accepted species only
   * @param rootId          the LSID-less root the families hang under
   */
  static String parentID(Generator.Taxon t, Map<String, String> acceptedSpecies, String rootId) {
    if (t.genusObject != null && "subspecies".equalsIgnoreCase(t.taxonRank)) {
      var species = acceptedSpecies.get(speciesKey(t.genusObject.genLsid, t.species));
      if (species != null) return species;
    }
    if (t.genusObject != null) return t.genusObject.genLsid;
    if (t.familyObject != null) return t.familyObject.famLsid;
    return rootId;
  }
}
