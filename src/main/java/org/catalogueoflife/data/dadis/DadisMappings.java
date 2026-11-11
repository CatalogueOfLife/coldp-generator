package org.catalogueoflife.data.dadis;

import java.text.Normalizer;
import java.util.*;

/**
 * Pure mappings for the DAD-IS generator, unit-tested in DadisMappingsTest.
 */
public class DadisMappings {
  private static final Map<String, String> ISO3_TO_ISO2 = new HashMap<>();
  static {
    for (String iso2 : Locale.getISOCountries()) {
      ISO3_TO_ISO2.put(Locale.of("", iso2).getISO3Country(), iso2);
    }
  }

  // DAD-IS translation keys of species.names to ISO 639-3
  static final Map<String, String> SPECIES_NAME_LANGUAGES = Map.of(
      "en", "eng", "es", "spa", "fr", "fra", "ru", "rus", "ch", "zho");

  // the free text language of a breed name, as far as it can be told
  private static final Map<String, String> LANGUAGES = new HashMap<>();
  static {
    for (String l : List.of("en", "eng", "engl", "english", "anglais", "ingles", "inglés")) LANGUAGES.put(l, "eng");
    for (String l : List.of("de", "ger", "germ", "german", "deu", "deutsch")) LANGUAGES.put(l, "deu");
    for (String l : List.of("fr", "fre", "fra", "french", "francais", "français", "franc")) LANGUAGES.put(l, "fra");
    for (String l : List.of("es", "esp", "spa", "span", "spanish", "espanol", "español")) LANGUAGES.put(l, "spa");
    for (String l : List.of("it", "ita", "ital", "italian", "italiano")) LANGUAGES.put(l, "ita");
    for (String l : List.of("nl", "dut", "nld", "neth", "dutch")) LANGUAGES.put(l, "nld");
    for (String l : List.of("pt", "por", "port", "portuguese")) LANGUAGES.put(l, "por");
    for (String l : List.of("ru", "rus", "russian")) LANGUAGES.put(l, "rus");
  }

  /** ISO 3166 alpha-2 code for a DAD-IS country key (alpha-3), or null for the M49-only regions like Sark. */
  static String iso2(String iso3) {
    return iso3 == null ? null : ISO3_TO_ISO2.get(iso3.toUpperCase(Locale.ROOT));
  }

  /**
   * The key that groups spellings of the same breed within a species:
   * case, accents, apostrophes, hyphens and spacing are ignored, so Blonde d'Aquitaine = Blonde D’aquitaine.
   */
  static String key(String name) {
    if (name == null) return "";
    String n = Normalizer.normalize(name, Normalizer.Form.NFD).replaceAll("\\p{M}", "");
    return n.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N}]+", " ").trim();
  }

  /** A readable id part from a name: the key with hyphens. */
  static String slug(String name) {
    return key(name).replace(' ', '-');
  }

  /**
   * Picks the spelling to show for a breed: the most frequent one,
   * then one that is not all upper case, then the alphabetically first.
   */
  static String bestSpelling(Map<String, Integer> spellings) {
    return spellings.entrySet().stream()
        .min(Comparator.<Map.Entry<String, Integer>>comparingInt(e -> -e.getValue())
            .thenComparing(e -> isUpperCase(e.getKey()))
            .thenComparing(Map.Entry::getKey))
        .map(Map.Entry::getKey)
        .orElse(null);
  }

  private static boolean isUpperCase(String x) {
    return x.chars().anyMatch(Character::isLetter) && x.equals(x.toUpperCase(Locale.ROOT));
  }

  /** The ISO 639-3 code of a free text language like "ger.", "(ger.)" or "En", or null if unknown. */
  static String language(String raw) {
    if (raw == null) return null;
    return LANGUAGES.get(raw.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}]", ""));
  }

  /** Native or exotic breed population to ColDP establishment means. */
  static String establishmentMeans(String breedClassification) {
    if (breedClassification == null || breedClassification.isBlank()) return null;
    return switch (breedClassification.trim().toLowerCase(Locale.ROOT)) {
      case "native", "locally adapted" -> "native";
      case "exotic", "introduced" -> "introduced";
      default -> breedClassification.trim();
    };
  }

  /**
   * The FAO risk status of a national breed population to the closest IUCN threat status.
   * The original DAD-IS status is kept in the distribution remarks.
   */
  static String threatStatus(String risk) {
    if (risk == null || risk.isBlank()) return null;
    return switch (risk.trim()) {
      case "extinct" -> "extinct";
      case "cryoConservedOnly" -> "extinct in the wild";
      case "critical", "criticalMaintained" -> "critically endangered";
      case "endangered", "endangeredMaintained" -> "endangered";
      case "vulnerable" -> "vulnerable";
      case "notAtRisk" -> "least concern";
      case "unknown" -> "data deficient";
      default -> null;
    };
  }

  /** Splits a camel case DAD-IS flag like DraughtPower into "draught power". */
  static String humanize(String flag) {
    return flag.replaceAll("([a-z])([A-Z])", "$1 $2").toLowerCase(Locale.ROOT);
  }

  /**
   * The name of a breed under its parent, in the quoted form of cultivar names: Bos taurus 'Jersey'.
   * Apostrophes within the breed name become typographic so they do not close the quote.
   */
  static String breedName(String parentName, String breed) {
    return parentName + " '" + breed.replace('\'', '’') + "'";
  }

  /** A bee taxon of DAD-IS, cleaned. breed is set for the Buckfast hybrid only. */
  record BeeName(String name, String breed) {}

  /**
   * Cleans a DAD-IS bee name: drops notes in brackets like (Unspecified) or (nubica), "sp" and capital epithets.
   * "Apis mellifera buckfast hybrid" is the Buckfast breed, "Apis mellifera other hybrids" stays the species.
   */
  static BeeName beeName(String raw) {
    String n = raw.replaceAll("\\([^)]*\\)", " ").replaceAll("\\s+", " ").trim();
    String breed = null;
    if (n.endsWith(" other hybrids")) {
      n = n.substring(0, n.length() - " other hybrids".length());
    } else if (n.endsWith(" hybrid")) {
      String[] words = n.substring(0, n.length() - " hybrid".length()).split(" ");
      n = words[0] + " " + words[1];
      String b = String.join(" ", Arrays.copyOfRange(words, 2, words.length));
      breed = Character.toUpperCase(b.charAt(0)) + b.substring(1);
    }
    n = n.replaceAll(" spp?\\.?$", "");
    String[] words = n.split(" ");
    StringBuilder sb = new StringBuilder(words[0]);
    for (int i = 1; i < words.length; i++) {
      sb.append(' ').append(words[i].toLowerCase(Locale.ROOT));
    }
    return new BeeName(sb.toString(), breed);
  }

  /** genus, species or subspecies from the number of words of a cleaned name. */
  static String rank(String name) {
    return switch (name.split(" ").length) {
      case 1 -> "genus";
      case 2 -> "species";
      default -> "subspecies";
    };
  }
}
