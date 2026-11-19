package org.catalogueoflife.data.dadis;

import life.catalogue.common.io.Resources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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

  // Latin letters without a decomposition, so NFKD does not strip them to their base letter
  private static final Map<Character, String> LATIN_FOLDS = Map.ofEntries(
      Map.entry('ł', "l"), Map.entry('ø', "o"), Map.entry('æ', "ae"), Map.entry('œ', "oe"), Map.entry('ß', "ss"),
      Map.entry('đ', "d"), Map.entry('ð', "d"), Map.entry('þ', "th"), Map.entry('ı', "i"), Map.entry('ħ', "h"));
  // Cyrillic to Latin for ids, a simple BGN/PCGN like scheme without diacritics; й and ё arrive as и and е after NFKD
  private static final Map<Character, String> CYRILLIC = new HashMap<>();
  static {
    String[] pairs = {"а", "a", "б", "b", "в", "v", "г", "g", "д", "d", "е", "e", "ж", "zh", "з", "z", "и", "i",
        "к", "k", "л", "l", "м", "m", "н", "n", "о", "o", "п", "p", "р", "r", "с", "s", "т", "t", "у", "u", "ф", "f",
        "х", "kh", "ц", "ts", "ч", "ch", "ш", "sh", "щ", "shch", "ъ", "", "ы", "y", "ь", "", "э", "e", "ю", "yu",
        "я", "ya", "і", "i", "є", "ye", "ґ", "g", "ў", "u", "ј", "j", "љ", "lj", "њ", "nj", "ћ", "c", "ђ", "dj",
        "џ", "dz", "ѕ", "dz"};
    for (int i = 0; i < pairs.length; i += 2) {
      CYRILLIC.put(pairs[i].charAt(0), pairs[i + 1]);
    }
  }
  // Cyrillic letters that look like Latin ones (or the digit 3), used in Latin names by typing on a Cyrillic keyboard
  private static final Map<Character, Character> HOMOGLYPHS = new HashMap<>();
  static {
    String cyr = "АВЕЅІЈКМНОРСТХУЗаеѕіјорсхуԁԛԝ";
    String lat = "ABESIJKMHOPCTXY3aesijopcxydqw";
    for (int i = 0; i < cyr.length(); i++) {
      HOMOGLYPHS.put(cyr.charAt(i), lat.charAt(i));
    }
  }

  /**
   * Cleans a DAD-IS breed name: double quotes are dropped ("Criollo", Commercial strain "STAR-53"),
   * Cyrillic homoglyphs in a Latin name become Latin (LINE С = LINE C), spacing is normalised.
   * Returns null for an empty name.
   */
  static String clean(String name) {
    if (name == null) return null;
    String n = name.replaceAll("[\"“”„]", "");
    if (n.chars().anyMatch(c -> Character.UnicodeScript.of(c) == Character.UnicodeScript.LATIN)) {
      StringBuilder sb = new StringBuilder(n.length());
      for (char c : n.toCharArray()) {
        sb.append(HOMOGLYPHS.getOrDefault(c, c));
      }
      n = sb.toString();
    }
    n = n.replaceAll("\\s+", " ").trim();
    return n.isEmpty() ? null : n;
  }

  /**
   * The key that groups spellings of the same breed within a species:
   * case, accents, apostrophes, hyphens and spacing are ignored, so Blonde d'Aquitaine = Blonde D’aquitaine.
   * Letters without a decomposition are folded (Kłusak = Klusak) and Cyrillic is transliterated,
   * so the key is ASCII for Latin and Cyrillic names.
   */
  static String key(String name) {
    if (name == null) return "";
    String n = Normalizer.normalize(name, Normalizer.Form.NFKD).replaceAll("\\p{M}", "").toLowerCase(Locale.ROOT);
    StringBuilder sb = new StringBuilder(n.length());
    for (char c : n.toCharArray()) {
      String r = LATIN_FOLDS.get(c);
      if (r == null) r = CYRILLIC.get(c);
      if (r == null) sb.append(c); else sb.append(r);
    }
    return sb.toString().replaceAll("[^\\p{L}\\p{N}]+", " ").trim();
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
   * The breed name as it goes into the quoted cultivar-style epithet.
   * Apostrophes become typographic so they do not close the quote (Blonde d’Aquitaine). A word-final apostrophe
   * is dropped: it is a letter in DAD-IS names, a glottal stop (Gaga') or a transliterated soft sign
   * (Kazakhskaya Korridel'), but the name parser takes any apostrophe that is not followed by a letter as the
   * closing quote and then reads the epithet as authorship. The generator keeps the DAD-IS spelling in the remarks.
   */
  static String epithet(String breed) {
    return breed.replace('\'', '’')
        .replaceAll("[’ʼʹ′´`](?!\\p{L})", "")
        .replaceAll("\\s+", " ")
        .trim();
  }

  /** The name of a breed under its parent, in the quoted form of cultivar names: Bos taurus 'Jersey'. */
  static String breedName(String parentName, String breed) {
    return parentName + " '" + epithet(breed) + "'";
  }

  /** The epithet differs from the DAD-IS name in more than the typographic apostrophes. */
  static boolean epithetChanged(String breed) {
    return !epithet(breed).equals(breed.replace('\'', '’'));
  }

  /**
   * A DAD-IS population that is reported under the wrong species, from the curated refile.tsv.
   * Its population goes to the target species, as a breed or, with speciesOnly, as a population of the species.
   */
  record Refile(String species, String name, String target, boolean speciesOnly) {}

  /** The refile.tsv entries, keyed by DAD-IS species and the key of the breed name. */
  static Map<String, Refile> readRefiles() throws IOException {
    Map<String, Refile> refiles = new LinkedHashMap<>();
    try (var in = Resources.stream("dadis/refile.tsv")) {
      var br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      br.readLine(); // header
      String line;
      while ((line = br.readLine()) != null) {
        if (line.isBlank()) continue;
        String[] row = line.split("\t", -1);
        var r = new Refile(row[0].trim(), row[1].trim(), row[2].trim(), row[3].trim().equals("species"));
        refiles.put(refileKey(r.species(), r.name()), r);
      }
    }
    return refiles;
  }

  static String refileKey(String species, String name) {
    return species + "|" + key(name);
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
