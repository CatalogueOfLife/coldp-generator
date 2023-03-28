/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.catalogueoflife.data.antcat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.api.vocab.NomStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.parser.RankParser;
import life.catalogue.parser.UnparsableException;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.HtmlUtils;
import org.catalogueoflife.data.utils.HttpUtils;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ColDP generator for AntCat using their public API.
 * https://antcat.org/api_docs#!/taxa/getTaxa
 */
public class Generator extends AbstractGenerator {
  private static final String API = "https://antcat.org/v1/";
  private static final String LINK_BASE = "https://antcat.org/";
  private static final URI ANTWEB_FILE = URI.create("https://www.antweb.org/web/workingdir/worldants_speciesList.txt");
  private static final TypeReference<List<Map<String, Taxon>>> taxaTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Name>>> namesTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Protonym>>> protonymsTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Reference>>> referencesTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Publisher>>> publisherTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Journal>>> journalTYPE = new TypeReference<>() {};

  private TermWriter typeWriter;
  Pattern refPattern = Pattern.compile("\\{(ref|tax|pro)(?:tt)?(?:ac)? (\\d+)\\}");
  private static final Term fossil = UnknownTerm.build("fossil", false);
  private static final Term gender = UnknownTerm.build("gender", false);
  private Int2ObjectMap<Publisher> publisher = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Journal> journals = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Reference> refs = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Taxon> taxa = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Name> names = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<CsvName> csvNames = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Protonym> protonyms = new Int2ObjectOpenHashMap<>();
  private Int2IntMap protonym2taxonID = new Int2IntOpenHashMap();
  private Map<String, String> forms = Map.ofEntries(
      Map.entry("aq", "alate queen"),
      Map.entry("dq", "dealate queen"),
      Map.entry("em", "ergatoid male"),
      Map.entry("eq", "ergatoid queen"),
      Map.entry("k", "karyotype"),
      Map.entry("l", "larvae"),
      Map.entry("m", "male"),
      Map.entry("q", "queen"),
      Map.entry("qm", "queen, male"),
      Map.entry("s", "soldier"),
      Map.entry("w", "worker")
  );
  private Map<String, String> refTypes = Map.ofEntries(
      Map.entry("nested_reference", "chapter"),
      Map.entry("book_reference", "book"),
      Map.entry("article_reference", "article_journal")
  );
  private Set<String> allRefTypes = new HashSet<>();
  private static final int FAMILY_ID = 429011; // Formicidae	Latreille, 1809


  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  String link(String id) {
    return id == null ? null : LINK_BASE + "catalog/" + id;
  }
  String reflink(int id) {
    return id > 0 ? LINK_BASE + "references/" + id : null;
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.basionymID,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.uninomial,
      ColdpTerm.genericName,
      ColdpTerm.infragenericEpithet,
      ColdpTerm.specificEpithet,
      ColdpTerm.infraspecificEpithet,
      ColdpTerm.status,
      ColdpTerm.nameStatus,
      gender,
      fossil,
      ColdpTerm.nameReferenceID,
      ColdpTerm.publishedInYear,
      ColdpTerm.family,
      ColdpTerm.subfamily,
      ColdpTerm.tribe,
      ColdpTerm.genus,
      ColdpTerm.subgenus,
      ColdpTerm.species,
      ColdpTerm.link,
      ColdpTerm.remarks
    ));
    refWriter = additionalWriter(ColdpTerm.Reference, List.of(
        ColdpTerm.ID,
        ColdpTerm.doi,
        ColdpTerm.type,
        ColdpTerm.citation,
        ColdpTerm.author,
        ColdpTerm.issued,
        ColdpTerm.title,
        ColdpTerm.containerAuthor,
        ColdpTerm.containerTitle,
        ColdpTerm.volume,
        ColdpTerm.issue,
        ColdpTerm.publisher,
        ColdpTerm.publisherPlace,
        ColdpTerm.page,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.nameID,
        ColdpTerm.locality,
        ColdpTerm.citation,
        fossil,
        ColdpTerm.remarks
    ));

    LOG.info("Use antcat csv file at {}", ANTWEB_FILE);

    // dump all taxa, protonyms and references
    load(Publisher.class, "publishers", this::readPublishers, this::cachePublisher);
    load(Journal.class, "journals", this::readJournal, this::cacheJournal);
    load(Reference.class, "references", this::readReferences, this::cacheReference);
    System.out.println("REF TYPES:");
    for (var rt : allRefTypes) {
      System.out.println(rt);
    }
    for (var ref : refs.values()) {
      writeReference(ref);
    }
    // now release memory for publisher and journals - we used them in refs
    journals.clear();
    publisher.clear();
    load(Name.class, "names", this::readName, this::cacheName);
    load(Taxon.class, "taxa", this::readTaxa, this::cacheTaxon);
    load(Protonym.class, "protonyms", this::readProtonyms, this::cacheProtonym);

    // use tsv file for taxa/synonyms
    var settings = new TsvParserSettings();
    settings.setMaxCharsPerColumn(1256000);
    TsvParser parser = new TsvParser(settings);

    // parse taxa once to just full the lookup cache
    boolean first = true;
    List<String[]> data = new ArrayList<>();
    Reader reader = new InputStreamReader(http.getStream(ANTWEB_FILE), StandardCharsets.UTF_8);
    IterableResult<String[], ParsingContext> it = parser.iterate(reader);
    for (var row : it) {
      if (first) {
        first = false;
        continue; // skip header row
      }
      int key = Integer.parseInt(row[0]);
      csvNames.put(key, new CsvName(row));
      data.add(row);
    }

    // now 2nd pass for real
    first = true;
    for (var row : data) {
      if (first) {
        first = false;
        continue; // skip header row
      }
      process(row);
    }

    typeWriter.close();
  }

  static class CsvName {
    public final String subfamily;
    public final String tribe;
    public final String genus;
    public final String subgenus;
    public final String specificEpithet;
    public final String infraspecificEpithet;
    public final String authorship;
    public final String rank;

    /**
     * 		<field index="0" term="http://rs.tdwg.org/dwc/terms/taxonID"/>
     * 		<field index="1" term="http://rs.tdwg.org/dwc/terms/subfamily"/>
     * 		<field index="2" term="http://rs.tdwg.org/dwc/terms/tribe"/>
     * 		<field index="3" term="http://rs.tdwg.org/dwc/terms/genus"/>
     * 		<field index="4" term="http://rs.tdwg.org/dwc/terms/subgenus"/>
     * 		<field index="5" term="http://rs.tdwg.org/dwc/terms/specificEpithet"/>
     * 		<field index="6" term="http://rs.tdwg.org/dwc/terms/infraspecificEpithet"/>
     * 		<field index="7" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
     */
    public CsvName(String[] row) {
      this.subfamily = row[1];
      this.tribe = row[2];
      this.genus = row[3];
      this.subgenus = row[4];
      this.specificEpithet = row[5];
      this.infraspecificEpithet = row[6];
      this.authorship = row[7];
      this.rank = row[21];
    }

    boolean hasBinomenOrInfrageneric() {
      return specificEpithet != null || subgenus != null;
    }

    String name() {
      life.catalogue.api.model.Name n = new life.catalogue.api.model.Name();
      if (hasBinomenOrInfrageneric()) {
        n.setGenus(genus);
        n.setSpecificEpithet(specificEpithet);
        n.setInfragenericEpithet(subgenus);
        n.setInfraspecificEpithet(infraspecificEpithet);

      } else {
        n.setUninomial(ObjectUtils.coalesce(genus, tribe, subfamily));
      }
      n.setAuthorship(authorship);
      try {
        RankParser.PARSER.parse(NomCode.ZOOLOGICAL, rank).ifPresent(n::setRank);
      } catch (UnparsableException e) {
      }
      n.rebuildScientificName();
      return n.getLabel();
    }
  }

  /**
   * 		<id index="0" />
   * 		<field index="10" term="http://rs.tdwg.org/dwc/terms/namePublishedInYear"/>
   * 		<field index="11" term="http://rs.tdwg.org/dwc/terms/taxonomicStatus"/>
   * 		<field index="12" term="http://rs.tdwg.org/dwc/terms/nomenclaturalStatus"/>
   * 		<field index="13" term="http://rs.tdwg.org/dwc/terms/acceptedNameUsage"/>
   * 		<field index="15" term="http://rs.tdwg.org/dwc/terms/originalNameUsage"/>
   * 		<field index="16" term="http://rs.tdwg.org/dwc/terms/fossil"/>
   * 		<field index="17" term="http://rs.tdwg.org/dwc/terms/taxonRemarks"/>
   * 		<field index="18" term="http://rs.tdwg.org/dwc/terms/namePublishedInID"/>
   * 		<field index="21" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
   * 		<field index="23" term="http://rs.tdwg.org/dwc/terms/parentNameUsage"/>
   */
  private void process(String[] row) throws IOException {
    int key = Integer.parseInt(row[0]);
    CsvName n = csvNames.get(key);
    Taxon t = taxa.get(key);

    writer.set(ColdpTerm.ID, key);
    writer.set(ColdpTerm.family, "Formicidae");
    writer.set(ColdpTerm.subfamily, n.subfamily);
    writer.set(ColdpTerm.tribe, n.tribe);
    writer.set(ColdpTerm.genus, n.genus);
    writer.set(ColdpTerm.subgenus, n.subgenus);

    if (n.hasBinomenOrInfrageneric() || n.subgenus != null) {
      writer.set(ColdpTerm.genericName, n.genus);
      writer.set(ColdpTerm.specificEpithet, n.specificEpithet);
      writer.set(ColdpTerm.infragenericEpithet, n.subgenus);
      writer.set(ColdpTerm.infraspecificEpithet, n.infraspecificEpithet);
    } else {
      writer.set(ColdpTerm.uninomial, ObjectUtils.coalesce(n.genus, n.tribe, n.subfamily));
    }
    writer.set(ColdpTerm.scientificName, n.name());
    writer.set(ColdpTerm.authorship, n.authorship);
    writer.set(ColdpTerm.publishedInYear, row[10]);
    writer.set(ColdpTerm.nameReferenceID, row[18]);
    writer.set(ColdpTerm.status, row[11]);
    boolean available = Boolean.parseBoolean(row[12]);
    if (available) {
      writer.set(ColdpTerm.nameStatus, NomStatus.ESTABLISHED);
    } else {
      writer.set(ColdpTerm.nameStatus, NomStatus.NOT_ESTABLISHED);
    }
    boolean synonym = row[13] != null;
    // synoynms lack an accepted ID, so we must look them up in the API
    if (synonym) {
      if (!csvNames.containsKey(t.current_taxon_id)) {
        LOG.warn("Accepted taxon ID for syn {} not existing: {}", n.name(), t.current_taxon_id);
      }
      writer.set(ColdpTerm.parentID, t.current_taxon_id);
    }
    boolean original = Boolean.parseBoolean(row[14]);
    if (original != t.original_combination) {
      LOG.warn("Taxon {} is listed as original={} in CSV, but original={} in Taxon from API", t.id, original, t.original_combination);
    }
    if (t.protonym_id != null) {
      Protonym p = protonyms.get(t.protonym_id);
      if (p == null) {
        LOG.warn("Protonym {} not found", t.protonym_id);
      } else if (original) {
        writeTypeMaterial(key, p);
      } else if (protonym2taxonID.containsKey(p.id)) {
        writer.set(ColdpTerm.basionymID, protonym2taxonID.get(p.id));
      } else {
        LOG.warn("Taxon not found for protonym {}", t.protonym_id);
      }
    }
    if (t.name_id > 0) {
      var name = names.get(t.name_id);
      if (name != null) {
        writer.set(gender, name.gender);
      }
    }
    writer.set(fossil, row[16]);
    writer.set(ColdpTerm.remarks, HtmlUtils.replaceHtml(row[17], true));
    writer.set(ColdpTerm.rank, row[21]);
    writer.set(ColdpTerm.link, link(row[0]));
    writer.next();
  }



  /**
   * https://antcat.org/wiki/6
   */
  String replVars(String x) {
    if (x != null) {
      var m = refPattern.matcher(x);
      StringBuilder sb = new StringBuilder();
      while (m.find()) {
        int id = Integer.parseInt(m.group(2));
        String val = null;
        if (m.group(1).equalsIgnoreCase("ref")) {
          if (refs.containsKey(id)) {
            val = refs.get(id).label(refs);
          }
        } else if (m.group(1).equalsIgnoreCase("tax")) {
          if (csvNames.containsKey(id)) {
            val = csvNames.get(id).name();
          }
        }
        m.appendReplacement(sb, ObjectUtils.coalesce(val, "???"));
      }
      m.appendTail(sb);
      return sb.toString();
    }
    return null;
  }

  private void writeTypeMaterial(int nameID, Protonym p) {
    try {
      // protonyms can be a type specimen or a type species/genus
      if (p.isSpecimen()) {
        typeWriter.set(ColdpTerm.nameID, nameID);
        typeWriter.set(ColdpTerm.citation, replVars(p.primary_type_information_taxt));
        typeWriter.set(ColdpTerm.locality, p.locality);
        typeWriter.set(fossil, p.fossil);
        // remarks
        StringBuilder sb = new StringBuilder();
        if (!StringUtils.isBlank(p.bioregion)) {
          sb.append(p.bioregion);
          sb.append(".");
        }
        if (!StringUtils.isBlank(p.forms)) {
          if (sb.length()>0) {
            sb.append(" ");
          }
          sb.append("Forms: ");
          boolean first = true;
          for (String f : p.forms.split("\\.")) {
            if (!first) {
              sb.append(", ");
            } else {
              first = false;
            }
            var val = forms.get(f.toLowerCase());
            if (val != null) {
              sb.append(val);
            } else {
              sb.append(f);
            }
          }
          sb.append(".");
        }
        if (!StringUtils.isBlank(replVars(p.notes_taxt))) {
          if (sb.length()>0) {
            sb.append(" ");
          }
          sb.append(p.notes_taxt);
        }
        if (!StringUtils.isBlank(p.secondary_type_information_taxt)) {
          if (sb.length()>0) {
            sb.append(" ");
          }
          sb.append(replVars(p.secondary_type_information_taxt));
        }
        typeWriter.set(ColdpTerm.remarks, sb.toString());
        typeWriter.next();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void cacheTaxon(Taxon t) {
    taxa.put(t.id, t);
    if (t.original_combination && t.protonym_id != null) {
      protonym2taxonID.put((int)t.protonym_id, t.id);
    }
  }
  private void cacheProtonym(Protonym p) {
    protonyms.put(p.id, p);
  }
  private void cachePublisher(Publisher p) {
    publisher.put(p.id, p);
  }
  private void cacheJournal(Journal j) {
    journals.put(j.id, j);
  }
  private void cacheReference(Reference r) {
    refs.put(r.id, r);
  }
  private void cacheName(Name n) {
    names.put(n.id, n);
  }
  private void writeReference(Reference r) {
    try {
      refWriter.set(ColdpTerm.ID, r.id);
      refWriter.set(ColdpTerm.link, reflink(r.id));
      allRefTypes.add(r.type);
      refWriter.set(ColdpTerm.type, refTypes.getOrDefault(r.type, r.type));
      refWriter.set(ColdpTerm.title, r.title);
      refWriter.set(ColdpTerm.author, r.author_names_string_cache);
      refWriter.set(ColdpTerm.issued, r.getDate());
      refWriter.set(ColdpTerm.doi, r.doi);
      refWriter.set(ColdpTerm.page, r.pagination);
      refWriter.set(ColdpTerm.volume, r.series_volume_issue);
      refWriter.set(ColdpTerm.remarks, HtmlUtils.replaceHtml(r.public_notes, true));
      if (r.journal_id != null && journals.containsKey((int)r.journal_id)) {
        var j = journals.get((int)r.journal_id);
        refWriter.set(ColdpTerm.containerTitle, j.name);
      }
      if (r.publisher_id != null && publisher.containsKey((int)r.publisher_id)) {
        var pub = publisher.get((int)r.publisher_id);
        refWriter.set(ColdpTerm.publisher, pub.name);
        refWriter.set(ColdpTerm.publisherPlace, pub.place);
      }
      if (r.nesting_reference_id != null && refs.containsKey((int)r.nesting_reference_id)) {
        var book = refs.get((int)r.nesting_reference_id);
        refWriter.set(ColdpTerm.containerTitle, book.title);
        refWriter.set(ColdpTerm.containerAuthor, book.author_names_string_cache);
      }
      refs.put(r.id, r);
      refWriter.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private List<Reference> readReferences(URI uri) {
    return read(uri, referencesTYPE);
  }
  private List<Publisher> readPublishers(URI uri) {
    return read(uri, publisherTYPE);
  }
  private List<Journal> readJournal(URI uri) {
    return read(uri, journalTYPE);
  }
  private List<Taxon> readTaxa(URI uri) {
    return read(uri, taxaTYPE);
  }
  private List<Name> readName(URI uri) {
    List<Name> names = read(uri, namesTYPE);
    names.forEach(n -> n.type = n.type.replaceAll("_name$", ""));
    return names;
  }
  private List<Protonym> readProtonyms(URI uri) {
    return read(uri, protonymsTYPE);
  }

  private <T extends IDBase> List<T> read(URI uri, TypeReference<List<Map<String, T>>> typeRef) {
    try {
      var resp = mapper.readValue(http.getStreamJSON(uri), typeRef);
      if (resp != null && !resp.isEmpty()) {
        return resp.stream().map(m -> {
          String type = m.keySet().iterator().next();
          T obj = m.get(type);
          obj.type = type;
          return obj;
        }).collect(Collectors.toList());
      }
      return Collections.emptyList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T extends IDBase> void load(Class<T> clazz, String path, Function<URI, List<T>> readFunc, Consumer<T> writeFunc) throws IllegalAccessException {
    int startID = 0;
    LOG.info("Load {}", clazz.getSimpleName());
    var resp = readFunc.apply(buildURI(path, startID));
    while (!resp.isEmpty()) {
      for (T obj : resp) {
        writeFunc.accept(obj);
      }
      startID = resp.stream().map(t -> t.id).max(Integer::compare).get() + 1;
      LOG.info("Crawl {} starting with {}", clazz.getSimpleName(), startID);
      resp = readFunc.apply(buildURI(path, startID));
    }
  }

  URI buildURI(String path, int startID){
    return URI.create(API + String.format("%s?starts_at=%s", path, startID));
  }

  static class IDBase {
    public int id;
    public String type;
  }

  static class Taxon extends IDBase {
    public String status;
    public String name_cache;
    public String author_citation;

    public int name_id;
    public Integer protonym_id;
    public Integer hol_id;

    public Integer current_taxon_id;

    public boolean unresolved_homonym;
    public boolean collective_group_name;
    public boolean original_combination;

    public Integer family_id;
    public Integer subfamily_id;
    public Integer tribe_id;
    public Integer genus_id;
    public Integer subgenus_id;
    public Integer species_id;
    public Integer subspecies_id;

    Integer getParentID() {
      // place all names ultimately into Formicidae unless status says so:
      Integer rootID = status.equalsIgnoreCase("excluded from Formicidae") ? null : FAMILY_ID;
      return ObjectUtils.coalesce(subspecies_id,species_id,subgenus_id,genus_id,tribe_id,subfamily_id,family_id, rootID);
    }
  }
  static class Protonym extends IDBase {
    public int authorship_id;
    public int name_id;

    public boolean fossil;
    public boolean sic;
    public boolean ichnotaxon;

    public String locality;
    public String primary_type_information_taxt;
    public String secondary_type_information_taxt;
    public String type_notes_taxt;
    public String bioregion;
    public String forms;
    public String notes_taxt;

    boolean isSpecimen() {
      return primary_type_information_taxt != null || locality != null;
    }
  }
  static class Name extends IDBase {
    public String name;
    public String authorship;
    public String epithet;
    public String gender;
    public String status;
    public Integer protonymID;
  }
  static class Reference extends IDBase {
    public Integer year;
    public String date;
    public Integer publisher_id;
    public Integer journal_id;
    public String series_volume_issue;
    public String pagination;
    public String author_names_string_cache;
    public String editor_notes;
    public String public_notes;
    public String taxonomic_notes;
    public String title;
    public Integer nesting_reference_id;
    public String author_names_suffix;
    public String review_state;
    public String doi;
    public String bolton_key;
    public Boolean online_early;
    public String stated_year;
    public String year_suffix;

    String getDate() {
      if (date != null) {
        if (date.length()==6) {
          return date.substring(0,4)+"-"+date.substring(4);
        } else if (date.length()==8) {
          return date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6);
        } else {
          return date;
        }
      }
      return null;
    }

    public String label(Int2ObjectMap<Reference> references) {
      return label(references, true);
    }

    private String label(Int2ObjectMap<Reference> references, boolean inclYear) {
      // keep citation cache for replacing placeholders later on
      StringBuilder sb = new StringBuilder();
      if (author_names_string_cache != null) {
        sb.append(author_names_string_cache);
      }
      if (inclYear) {
        if (year != null) {
          if (sb.length() > 0) sb.append(" ");
          sb.append("(");
          sb.append(year);
          sb.append(")");
        }
      } else {
        if (sb.length() > 0) sb.append(",");
      }
      if (title != null) {
        if (sb.length()>0) sb.append(" ");
        sb.append(title);
      }
      if (nesting_reference_id != null && references.containsKey((int)nesting_reference_id)) {
        var book = references.get((int)nesting_reference_id);
        if (sb.length() > 1 && sb.charAt(sb.length() - 1) != '.') {
          sb.append(".");
        }
        sb.append(" In ");
        sb.append(book.label(references, false));
      }
      return sb.toString();
    }
  }
  static class Publisher extends IDBase {
    public String place;
    public String name;
  }
  static class Journal extends IDBase {
    public String name;
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now().toString());
    super.addMetadata();
  }

}
