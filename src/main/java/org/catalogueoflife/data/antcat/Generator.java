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
import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import life.catalogue.parser.NameParser;
import life.catalogue.parser.RankParser;
import life.catalogue.parser.UnparsableException;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.data.utils.AltIdBuilder;
import org.catalogueoflife.data.utils.HtmlUtils;
import org.catalogueoflife.data.utils.MarkdownUtils;
import org.catalogueoflife.data.utils.RemarksBuilder;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.ParsedAuthorship;

import java.io.*;
import java.net.URI;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ColDP generator for AntCat using their public API.
 * https://antcat.org/api_docs#!/taxa/getTaxa
 */
public class Generator extends AbstractColdpGenerator {
  private static final boolean CACHE = true;
  private static final String API = "https://antcat.org/v1/";
  private static final String LINK_BASE = "https://antcat.org/";
  private static final String ANTWEB = "https://www.antweb.org/web/workingdir/";
  private static final String ANTWEB_FILE = "worldants_speciesList.txt";
  private static final URI ANTWEB_URL = URI.create(ANTWEB + ANTWEB_FILE);
  private static final TypeReference<List<Map<String, Taxon>>> taxaTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Name>>> namesTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Protonym>>> protonymsTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Reference>>> referencesTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, RefDoc>>> refDocTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Publisher>>> publisherTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Journal>>> journalTYPE = new TypeReference<>() {};

  private static final TypeReference<List<Taxon>> taxaListType = new TypeReference<>() {};
  private static final TypeReference<List<Name>> namesListType = new TypeReference<>() {};
  private static final TypeReference<List<Protonym>> protonymsListType = new TypeReference<>() {};
  private static final TypeReference<List<Reference>> referencesListType = new TypeReference<>() {};
  private static final TypeReference<List<RefDoc>> refDocListType = new TypeReference<>() {};
  private static final TypeReference<List<Publisher>> publisherListType = new TypeReference<>() {};
  private static final TypeReference<List<Journal>> journalListType = new TypeReference<>() {};

  private TermWriter typeWriter;
  Pattern refPattern = Pattern.compile("\\{(ref|tax|pro)(?:tt)?(?:ac)? (\\d+)\\}");
  private Int2ObjectMap<Publisher> publisher = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Journal> journals = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Reference> refs = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<RefDoc> refDocs = new Int2ObjectOpenHashMap<>();
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
  private Map<String, TaxonomicStatus> statusValues = new HashMap<>();
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
  protected void prepare() throws Exception {
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
            ColdpTerm.gender,
            ColdpTerm.extinct,
            ColdpTerm.nameReferenceID,
            ColdpTerm.publishedInYear,
            ColdpTerm.family,
            ColdpTerm.alternativeID,
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
            ColdpTerm.extinct,
            ColdpTerm.remarks
    ));
  }

  @Override
  protected void addData() throws Exception {
    // dump all taxa, protonyms and references
    load(Publisher.class, publisherListType, publisher, "publishers", this::readPublishers);
    load(Journal.class, journalListType, journals, "journals", this::readJournal);
    load(Reference.class, referencesListType, refs, "references", this::readReferences);
    for (var rt : allRefTypes) {
      if (refTypes.containsKey(rt)) {
        LOG.warn("Missing reference type {}", rt);
      }
    }
    load(RefDoc.class, refDocListType, refDocs, "reference_documents", this::readRefDocs);
    // add document links to references
    for (var rd : refDocs.values()) {
      if (rd.reference_id != null) {
        var ref = refs.get(rd.reference_id);
        if (ref != null) {
          ref.document = rd.getPdf();
        } else {
          LOG.warn("Missing referenc {} found in refdoc {}", rd.reference_id, rd.id);
        }
      }
    }
    for (var ref : refs.values()) {
      writeReference(ref);
    }
    // now release memory for publisher and journals - we used them in refs
    journals.clear();
    publisher.clear();
    load(Name.class, namesListType, names, "names", this::readName);
    load(Taxon.class, taxaListType, taxa, "taxa", this::readTaxa);
    var ttypes = new HashSet<>();
    for (var t : taxa.values()) {
      ttypes.add(t.type);
      if (t.protonym_id != null && t.isProtonym()) {
        protonym2taxonID.put((int)t.protonym_id, t.id);
      }
    }
    for (var tt : ttypes) {
      LOG.info("Taxon type {}", tt);
    }
    load(Protonym.class, protonymsListType, protonyms, "protonyms", this::readProtonyms);

    // use tsv file for taxa/synonyms
    var settings = new TsvParserSettings();
    settings.setMaxCharsPerColumn(1256000);

    // parse taxa once to just full the lookup cache
    File antFile = sourceFile(ANTWEB_FILE);
    if (CACHE && antFile.exists()) {
      LOG.info("Use local antcat csv file at {}", antFile);
    } else {
      LOG.info("Download antcat csv from {} to {}", ANTWEB_URL, antFile);
      download.download(ANTWEB_URL, antFile);
    }

    var reader = UTF8IoUtils.readerFromFile(antFile);

    boolean first = true;
    TsvParser parser = new TsvParser(settings);
    IterableResult<String[], ParsingContext> it = parser.iterate(reader);
    for (var row : it) {
      if (first) {
        first = false;
        continue; // skip header row
      }
      int key = Integer.parseInt(row[0]);
      csvNames.put(key, new CsvName(row));
    }

    // now 2nd pass for real
    first = true;
    parser = new TsvParser(settings);
    reader = UTF8IoUtils.readerFromFile(antFile);
    it = parser.iterate(reader);
    for (var row : it) {
      if (first) {
        first = false;
        continue; // skip header row
      }
      process(row);
    }

    for (var t : statusValues.entrySet()) {
      System.out.println(t.getKey() + " -> " + t.getValue());
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
      this.subfamily = clean(row[1]);
      this.tribe = clean(row[2]);
      this.genus = clean(row[3]);
      this.subgenus = clean(row[4]);
      this.specificEpithet = row[5];
      this.infraspecificEpithet = row[6];
      this.authorship = row[7];
      this.rank = row[21];
    }

    private String clean(String x) {
      return x != null && !x.equalsIgnoreCase("incertae_sedis") ? StringUtils.trimToNull(x) : null;
    }

    boolean hasBinomenOrInfrageneric() {
      return specificEpithet != null || subgenus != null;
    }

    /**
     * @return the species binomial if species or below. Otherwise null
     */
    String species() {
      if (genus != null && specificEpithet != null) {
        return genus + " " + specificEpithet;
      }
      return null;
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
    var remarks = new RemarksBuilder();
    String rank = row[21];

    writer.set(ColdpTerm.ID, key);
    writer.set(ColdpTerm.parentID, t.getParentID());
    writer.set(ColdpTerm.family, "Formicidae");
    writer.set(ColdpTerm.rank, rank);

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
    writer.set(ColdpTerm.extinct, row[16]);
    writer.set(ColdpTerm.publishedInYear, row[10]);
    writer.set(ColdpTerm.nameReferenceID, row[18]);

    boolean available = Boolean.parseBoolean(row[12]);
    if (available) {
      writer.set(ColdpTerm.nameStatus, NomStatus.ESTABLISHED);
    } else {
      writer.set(ColdpTerm.nameStatus, NomStatus.NOT_ESTABLISHED);
    }

    final String stateStr = StringUtils.stripToEmpty(row[11]).toLowerCase();
    TaxonomicStatus status = switch (stateStr) {
      case "valid" -> TaxonomicStatus.ACCEPTED;
      case "synonym", "obsolete combination", "unavailable misspelling", "unavailable", "homonym" -> TaxonomicStatus.SYNONYM;
      case "unidentifiable" -> TaxonomicStatus.PROVISIONALLY_ACCEPTED;
      case "excluded from formicidae" -> TaxonomicStatus.BARE_NAME;
      default -> TaxonomicStatus.PROVISIONALLY_ACCEPTED;
    };
    if (status.isSynonym() && (t.current_taxon_id == null || t.current_taxon_id == t.id)) {
      LOG.warn("Synonym {} {} without parent!", t.id, t.name_cache);
      writer.unset(ColdpTerm.parentID);
    } else if (status.isTaxon() && t.current_taxon_id != null) {
      writer.set(ColdpTerm.parentID, t.current_taxon_id);
    }
    writer.set(ColdpTerm.status, status);
    statusValues.put(stateStr, status);

    if (!status.isTaxon()) {
      remarks.append(stateStr);
    }

    boolean original = Boolean.parseBoolean(row[14]);
    if (original != t.original_combination) {
      LOG.warn("Taxon {} is listed as original={} in CSV, but original={} in Taxon from API", t.id, original, t.original_combination);
    }
    if (t.protonym_id != null) {
      Protonym p = protonyms.get(t.protonym_id);
      if (p == null) {
        LOG.warn("Protonym {} not found", t.protonym_id);
      } else {
        var pn = names.get(p.name_id);
        if (original) {
          writeTypeMaterial(key, p);
        } else if (protonym2taxonID.containsKey(p.id)) {
          writer.set(ColdpTerm.basionymID, protonym2taxonID.get(p.id));
        } else {
          LOG.warn("Taxon not found for protonym {}", t.protonym_id);
          writer.set(ColdpTerm.basionymID, p.id);
        }
      }
    }
    if (t.name_id > 0) {
      var name = names.get(t.name_id);
      if (name != null) {
        writer.set(ColdpTerm.gender, name.gender);
      }
    }
    if (t.hol_id != null) {
      AltIdBuilder ids = new AltIdBuilder();
      ids.add("hol", t.hol_id.toString());
      writer.set(ColdpTerm.alternativeID, ids.toString());
    }
    remarks.append(HtmlUtils.replaceHtml(row[17], true));
    writer.set(ColdpTerm.remarks, remarks.toString());
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

  private void writeReference(Reference r) {
    try {
      refWriter.set(ColdpTerm.ID, r.id);
      if (r.document != null) {
        refWriter.set(ColdpTerm.link, r.document);
      } else {
        refWriter.set(ColdpTerm.link, reflink(r.id));
      }
      allRefTypes.add(r.type);
      refWriter.set(ColdpTerm.type, refTypes.getOrDefault(r.type, r.type));
      refWriter.set(ColdpTerm.title, MarkdownUtils.removeMarkup(r.title));
      refWriter.set(ColdpTerm.author, r.author_names_string_cache);
      refWriter.set(ColdpTerm.issued, r.getDate());
      refWriter.set(ColdpTerm.doi, r.doi);
      refWriter.set(ColdpTerm.page, r.pagination);
      refWriter.set(ColdpTerm.volume, r.series_volume_issue);
      refWriter.set(ColdpTerm.remarks, HtmlUtils.replaceHtml(r.public_notes, true));
      if (r.journal_id != null && journals.containsKey((int)r.journal_id)) {
        var j = journals.get((int)r.journal_id);
        refWriter.set(ColdpTerm.containerTitle, MarkdownUtils.removeMarkup(j.name));
      }
      if (r.publisher_id != null && publisher.containsKey((int)r.publisher_id)) {
        var pub = publisher.get((int)r.publisher_id);
        refWriter.set(ColdpTerm.publisher, pub.name);
        refWriter.set(ColdpTerm.publisherPlace, pub.place);
      }
      if (r.nesting_reference_id != null && refs.containsKey((int)r.nesting_reference_id)) {
        var book = refs.get((int)r.nesting_reference_id);
        refWriter.set(ColdpTerm.containerTitle, MarkdownUtils.removeMarkup(book.title));
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
  private List<RefDoc> readRefDocs(URI uri) {
    return read(uri, refDocTYPE);
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

  private <T extends IDBase> void load(Class<T> clazz,
                                       TypeReference<List<T>> listType,
                                       Int2ObjectMap<T> cache,
                                       String path,
                                       Function<URI, List<T>> readFunc) throws IllegalAccessException {
    int startID = 0;
    LOG.info("Load {}", clazz.getSimpleName());
    File jsonFile = sourceFile("cache-"+clazz.getSimpleName()+".json");
    if (CACHE && jsonFile.exists()) {
      LOG.info("Load {} from local json file {}", clazz.getSimpleName(), jsonFile);
      try {
        cache.clear();
        var objects = mapper.readValue(jsonFile, listType);
        for (var obj : objects) {
          cache.put(obj.id, obj);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      LOG.info("Loaded {} {} objects from local json file {}", cache.size(), clazz.getSimpleName(), jsonFile);

    } else {
      var resp = readFunc.apply(buildURI(path, startID));
      while (!resp.isEmpty()) {
        for (T obj : resp) {
          cache.put(obj.id, obj);
        }
        startID = resp.stream().map(t -> t.id).max(Integer::compare).get() + 1;
        LOG.info("Crawl {} starting with {}", clazz.getSimpleName(), startID);
        resp = readFunc.apply(buildURI(path, startID));
      }
      LOG.info("Loaded {} {} objects from API", cache.size(), clazz.getSimpleName());

      if (CACHE) {
        LOG.info("Storing {} {} objects locally as json in {}", cache.size(), clazz.getSimpleName(), jsonFile);
        try (var w = UTF8IoUtils.writerFromFile(jsonFile)) {
          mapper.writeValue(w, cache.values());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (jsonFile.exists()){
        jsonFile.delete();
      }
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
    public String incertae_sedis_in;

    public int name_id;
    public Integer protonym_id;
    public Integer hol_id;

    public Integer current_taxon_id;
    public Integer homonym_replaced_by_id;

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
      return ObjectUtils.coalesce(current_taxon_id, subspecies_id,species_id,subgenus_id,genus_id,tribe_id,subfamily_id,family_id, rootID);
    }

    boolean isProtonym() {
      return original_combination || (
          type != null && (
          type.equalsIgnoreCase("family")
                  || type.equalsIgnoreCase("subfamily")
                  || type.equalsIgnoreCase("tribe")
                  || type.equalsIgnoreCase("subtribe")
                  || type.equalsIgnoreCase("genus")
                  || type.equalsIgnoreCase("subgenus")
          )
        ) || !hasBasionymAuthorship();
    }

    private boolean hasBasionymAuthorship() {
      if (author_citation != null) {
        try {
          var pa = NameParser.PARSER.parseAuthorship(author_citation);
          return pa.get().hasBasionymAuthorship();
        } catch (Exception e) {
        }
      }
      return true;
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
    public String document; // this does not come from the API, but is added via reading reference documents later

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
  static class RefDoc extends IDBase {
    public Integer reference_id;
    public String url;
    public String file_file_name;


    String getPdf() {
      if (file_file_name != null) {
        return "https://antcat.org/documents/" + id + "/" + file_file_name;
      } else {
        return "https://antcat.org/documents/" + id + "/" + id;
      }
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
