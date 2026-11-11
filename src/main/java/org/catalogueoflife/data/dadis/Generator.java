package org.catalogueoflife.data.dadis;

import com.fasterxml.jackson.databind.JsonNode;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.Resources;
import life.catalogue.common.io.TermWriter;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.catalogueoflife.data.dadis.DadisMappings.*;

/**
 * FAO Domestic Animal Diversity Information System (DAD-IS), https://www.fao.org/dad-is
 * <p>
 * DAD-IS has no API, but its web apps read a public Firebase database: the species, the countries and, per country,
 * every national breed population. The 44 livestock species only have common names, so a curated mapping in
 * resources/dadis/species.tsv gives their scientific names and higher classification.
 * <p>
 * Each distinct breed of a species becomes a name usage with rank breed under the species, e.g.
 * Bos taurus 'Jersey'. The national breed populations become its distributions with their native/exotic status and
 * risk status, the national names its vernacular names. Breeds are grouped by their transboundary name if a country
 * gave one, else by the national name. Bees are listed separately with real (sub)species names.
 */
public class Generator extends AbstractColdpGenerator {
  private static final String API = "https://dadis-ws.firebaseio.com/Data/";
  private static final String BREED = "breed";
  private static final String BEE_PARENT = "Apidae";
  private static final List<String> BEE_CLASSIFICATION =
      List.of("Animalia", "Arthropoda", "Insecta", "Hymenoptera", BEE_PARENT);
  private static final List<String> HIGHER_RANKS = List.of("kingdom", "phylum", "class", "order", "family");

  record Node(String id, String parentID, String rank, String name, String authorship, Set<String> remarks) {}
  record Country(String key, String iso2, String name) {}
  record Population(Country country, String establishment, String risk, String nationalName, String language) {}

  /** A DAD-IS species with the id of the node it maps to and the names that only repeat the species. */
  record Species(String name, String nodeID, String slug, Set<String> genericKeys) {}

  static class Breed {
    final String id;
    final Species species;
    final Map<String, Integer> spellings = new HashMap<>();
    final List<Population> populations = new ArrayList<>();
    final Set<String> uses = new TreeSet<>();

    Breed(String id, Species species) {
      this.id = id;
      this.species = species;
    }
  }

  private final Map<String, Node> nodes = new LinkedHashMap<>();
  private final Map<String, Species> species = new HashMap<>();
  private final Map<String, Breed> breeds = new TreeMap<>();
  // populations of a species that have no breed name of their own, by node id
  private final Map<String, List<Population>> speciesPopulations = new HashMap<>();
  private final Map<String, Country> countries = new LinkedHashMap<>();
  private final Set<String> unknownSpecies = new TreeSet<>();

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  @Override
  protected void addData() throws Exception {
    JsonNode speciesJson = json("species.json", "species.json");
    readSpecies(speciesJson);
    readCountries(json("countries.json", "countries.json"));
    LOG.info("Reading breeds of {} countries", countries.size());
    for (Country c : countries.values()) {
      readBreeds(c, json("breeds-" + c.key() + ".json", "breeds/" + c.key() + ".json"));
    }
    if (!unknownSpecies.isEmpty()) {
      LOG.warn("Breeds of species missing in species.tsv are skipped: {}", unknownSpecies);
    }
    Map<String, List<Population>> beeDistributions = readBees(json("bees-species.json", "bees/species.json"));

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID, ColdpTerm.parentID, ColdpTerm.status, ColdpTerm.rank, ColdpTerm.scientificName,
        ColdpTerm.authorship, ColdpTerm.code, ColdpTerm.remarks));
    var vWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID, ColdpTerm.name, ColdpTerm.language, ColdpTerm.country));
    var dWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID, ColdpTerm.area, ColdpTerm.gazetteer, ColdpTerm.establishmentMeans,
        ColdpTerm.threatStatus, ColdpTerm.remarks));
    var pWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
        ColdpTerm.taxonID, ColdpTerm.property, ColdpTerm.value));

    for (Node n : nodes.values()) {
      usage(n.id(), n.parentID(), n.rank(), n.name(), n.authorship(), n.remarks());
    }
    writeSpeciesVernaculars(speciesJson, vWriter);
    for (var e : speciesPopulations.entrySet()) {
      distributions(e.getKey(), e.getValue(), dWriter);
    }
    for (var e : beeDistributions.entrySet()) {
      distributions(e.getKey(), e.getValue(), dWriter);
    }

    int populations = 0;
    for (Breed b : breeds.values()) {
      String name = bestSpelling(b.spellings);
      Node parent = nodes.get(b.species.nodeID());
      usage(b.id, parent.id(), BREED, breedName(parent.name(), name), null, Set.of());
      String breedKey = key(name);
      Set<List<String>> vernaculars = new LinkedHashSet<>();
      for (Population p : b.populations) {
        if (!key(p.nationalName()).equals(breedKey)) {
          vernaculars.add(Arrays.asList(p.nationalName(), p.language(), p.country().iso2()));
        }
      }
      for (List<String> v : vernaculars) {
        vWriter.set(ColdpTerm.taxonID, b.id);
        vWriter.set(ColdpTerm.name, v.get(0));
        vWriter.set(ColdpTerm.language, v.get(1));
        vWriter.set(ColdpTerm.country, v.get(2));
        vWriter.next();
      }
      distributions(b.id, b.populations, dWriter);
      populations += b.populations.size();
      for (String use : b.uses) {
        pWriter.set(ColdpTerm.taxonID, b.id);
        pWriter.set(ColdpTerm.property, "use");
        pWriter.set(ColdpTerm.value, use);
        pWriter.next();
      }
    }
    LOG.info("Wrote {} taxa and {} breeds with {} national populations; {} populations without a breed name",
        nodes.size(), breeds.size(), populations, speciesPopulations.values().stream().mapToInt(List::size).sum());
  }

  private JsonNode json(String filename, String path) throws IOException {
    File f = download(sourceFile(filename), URI.create(API + path), false);
    return mapper.readTree(f);
  }

  /** Builds the classification nodes of all DAD-IS species from the curated species.tsv. */
  private void readSpecies(JsonNode speciesJson) throws IOException {
    try (var in = Resources.stream("dadis/species.tsv")) {
      var br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      br.readLine(); // header
      String line;
      while ((line = br.readLine()) != null) {
        if (line.isBlank()) continue;
        // editors drop trailing empty columns
        String[] row = Arrays.stream(Arrays.copyOf(line.split("\t", -1), 10))
            .map(x -> x == null || x.isBlank() ? null : x.trim())
            .toArray(String[]::new);
        String name = row[0];
        String sciName = row[1];
        String parent = null;
        for (int i = 0; i < HIGHER_RANKS.size(); i++) {
          String higher = row[4 + i];
          if (higher != null && !higher.equals(sciName)) {
            parent = node(higher, parent, HIGHER_RANKS.get(i), null, null).id();
          }
        }
        Node n = node(sciName, parent, row[3], row[2], row[9]);
        Set<String> generic = new HashSet<>();
        generic.add(key(name));
        generic.add(key(sciName));
        for (String v : names(speciesJson.path(name).path("names"))) {
          generic.add(key(v));
          generic.add(key(v.replaceAll("\\([^)]*\\)", "")));
        }
        species.put(name, new Species(name, n.id(), slug(name), generic));
      }
    }
    speciesJson.fieldNames().forEachRemaining(sp -> {
      if (!species.containsKey(sp)) {
        LOG.warn("DAD-IS species {} is missing in species.tsv", sp);
      }
    });
  }

  private static List<String> names(JsonNode names) {
    List<String> list = new ArrayList<>();
    names.forEach(n -> {
      if (n.isTextual() && !n.asText().isBlank()) list.add(n.asText());
    });
    return list;
  }

  private Node node(String name, String parentID, String rank, String authorship, String remarks) {
    Node n = nodes.computeIfAbsent(slug(name),
        id -> new Node(id, parentID, rank, name, authorship, new LinkedHashSet<>()));
    if (remarks != null) {
      n.remarks().add(remarks);
    }
    return n;
  }

  private void readCountries(JsonNode json) {
    json.properties().forEach(e -> {
      JsonNode c = e.getValue();
      String name = c.path("name").asText(e.getKey());
      countries.put(e.getKey(), new Country(e.getKey(), iso2(c.path("iso3").asText(e.getKey())), name));
    });
  }

  private void readBreeds(Country country, JsonNode json) {
    if (!json.isObject()) return; // null for countries without breeds
    json.properties().forEach(spe -> {
      Species sp = species.get(spe.getKey());
      if (sp == null) {
        unknownSpecies.add(spe.getKey());
        return;
      }
      spe.getValue().properties().forEach(be -> {
        JsonNode rec = be.getValue();
        JsonNode nameJson = rec.path("General").path("Name");
        String national = text(nameJson.path("MostCommonName"), be.getKey());
        String transboundary = text(nameJson.path("TransboundaryOrBrandName"), null);
        var pop = new Population(country,
            establishmentMeans(text(rec.path("OriginAndDevelopment").path("BreedClassification"), null)),
            text(rec.path("General").path("Risk").path("breedRiskDetail"), null),
            national, language(text(nameJson.path("Language"), null)));
        // a transboundary name groups the breed across countries, a name that just repeats the species is none
        String name = null;
        for (String candidate : new String[]{transboundary, national}) {
          if (candidate != null && !sp.genericKeys().contains(key(candidate)) && !key(candidate).isEmpty()) {
            name = candidate;
            break;
          }
        }
        if (name == null) {
          speciesPopulations.computeIfAbsent(sp.nodeID(), k -> new ArrayList<>()).add(pop);
          return;
        }
        String id = sp.slug() + ":" + slug(name);
        Breed b = breeds.computeIfAbsent(id, k -> new Breed(k, sp));
        b.spellings.merge(name.trim(), 1, Integer::sum);
        b.populations.add(pop);
        rec.path("General").path("Uses").forEach(group -> group.properties().forEach(u -> {
          if (u.getValue().asBoolean(false)) {
            b.uses.add(humanize(u.getKey()));
          }
        }));
      });
    });
  }

  private static String text(JsonNode n, String defaultValue) {
    String x = n.isValueNode() ? n.asText().trim() : null;
    return x == null || x.isEmpty() ? defaultValue : x;
  }

  /** Bees have real names grouped by tribe, and per name the countries they are kept in. */
  private Map<String, List<Population>> readBees(JsonNode json) {
    Map<String, List<Population>> dist = new LinkedHashMap<>();
    String parent = null;
    for (int i = 0; i < BEE_CLASSIFICATION.size(); i++) {
      parent = node(BEE_CLASSIFICATION.get(i), parent, HIGHER_RANKS.get(i), null, null).id();
    }
    String apidae = parent;
    json.properties().forEach(te -> {
      String tribe = te.getKey().equals("Other") ? apidae : node(te.getKey(), apidae, "tribe", null, null).id();
      te.getValue().forEach(rec -> {
        BeeName bee = beeName(text(rec.path("full_name"), ""));
        String[] words = bee.name().split(" ");
        String p = tribe;
        for (int i = 1; i <= words.length; i++) {
          String name = String.join(" ", Arrays.copyOfRange(words, 0, i));
          p = node(name, p, DadisMappings.rank(name), null, null).id();
        }
        if (bee.breed() != null) {
          String breedId = "bee:" + slug(bee.breed());
          Node sp = nodes.get(p);
          nodes.computeIfAbsent(breedId,
              id -> new Node(id, sp.id(), BREED, breedName(sp.name(), bee.breed()), null, Set.of()));
          p = breedId;
        }
        String taxonID = p;
        rec.path("Country").fieldNames().forEachRemaining(cc -> {
          Country c = countries.getOrDefault(cc, new Country(cc, iso2(cc), cc));
          dist.computeIfAbsent(taxonID, k -> new ArrayList<>()).add(new Population(c, null, null, null, null));
        });
      });
    });
    return dist;
  }

  private void writeSpeciesVernaculars(JsonNode speciesJson, TermWriter vWriter)
      throws IOException {
    // a node shared by several DAD-IS species, like Phasianidae for Partridge and Pheasant, gets none of their names
    Map<String, Long> perNode = new HashMap<>();
    species.values().forEach(s -> perNode.merge(s.nodeID(), 1L, Long::sum));
    for (Species s : species.values()) {
      if (perNode.get(s.nodeID()) > 1) continue;
      var names = speciesJson.path(s.name()).path("names");
      for (var e : SPECIES_NAME_LANGUAGES.entrySet()) {
        String v = text(names.path(e.getKey()), null);
        if (v != null) {
          vWriter.set(ColdpTerm.taxonID, s.nodeID());
          vWriter.set(ColdpTerm.name, v);
          vWriter.set(ColdpTerm.language, e.getValue());
          vWriter.next();
        }
      }
    }
  }

  private void distributions(String taxonID, List<Population> pops, TermWriter dWriter)
      throws IOException {
    Set<List<String>> seen = new HashSet<>();
    for (Population p : pops) {
      Country c = p.country();
      String area = c.iso2() != null ? c.iso2() : c.name();
      String remarks = p.risk() == null ? null : "DAD-IS risk status: " + humanize(p.risk());
      if (seen.add(Arrays.asList(area, p.establishment(), p.risk()))) {
        dWriter.set(ColdpTerm.taxonID, taxonID);
        dWriter.set(ColdpTerm.area, area);
        dWriter.set(ColdpTerm.gazetteer, c.iso2() != null ? "iso" : "text");
        dWriter.set(ColdpTerm.establishmentMeans, p.establishment());
        dWriter.set(ColdpTerm.threatStatus, threatStatus(p.risk()));
        dWriter.set(ColdpTerm.remarks, remarks);
        dWriter.next();
      }
    }
  }

  private void usage(String id, String parentID, String rank, String name, String authorship, Set<String> remarks)
      throws IOException {
    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentID);
    writer.set(ColdpTerm.status, "accepted");
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.authorship, authorship);
    writer.set(ColdpTerm.code, "zoological");
    writer.set(ColdpTerm.remarks, remarks.isEmpty() ? null : String.join(" ", remarks));
    writer.next();
  }
}
