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
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import life.catalogue.api.model.Taxon;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.sql.Ref;
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
  private static final TypeReference<List<Map<String, Taxon>>> taxaTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Name>>> namesTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Protonym>>> protonymsTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Reference>>> referencesTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Publisher>>> publisherTYPE = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Journal>>> journalTYPE = new TypeReference<>() {};

  private TermWriter taxWriter;
  private TermWriter synWriter;
  private TermWriter typeWriter;
  private TermWriter nameRelWriter;
  Pattern refPattern = Pattern.compile("\\{ref (\\d+)\\}");
  private static final Term fossil = UnknownTerm.build("fossil", false);
  private static final Term gender = UnknownTerm.build("gender", false);
  private TsvWriter tsvWriter;
  private List<Field> columns;
  private File tmp = new File("/tmp/antcat");
  private Int2ObjectMap<Name> names = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Publisher> publisher = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<Journal> journals = new Int2ObjectOpenHashMap<>();
  private Int2ObjectMap<String> refs = new Int2ObjectOpenHashMap<>();
  private Int2IntMap protonym2nameID = new Int2IntOpenHashMap();
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


  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  String link(Integer id) {
    return id == null ? null : LINK_BASE + "catalog/" + id;
  }

  @Override
  protected void addData() throws Exception {
    newWriter(ColdpTerm.Name, List.of(
      ColdpTerm.ID,
      ColdpTerm.basionymID,
      ColdpTerm.rank,
      ColdpTerm.scientificName,
      ColdpTerm.authorship,
      ColdpTerm.status,
      ColdpTerm.referenceID,
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
        ColdpTerm.containerTitle,
        ColdpTerm.volume,
        ColdpTerm.issue,
        ColdpTerm.publisher,
        ColdpTerm.publisherPlace,
        ColdpTerm.page,
        ColdpTerm.link,
        ColdpTerm.remarks
    ));
    taxWriter = additionalWriter(ColdpTerm.Taxon, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.nameID,
        ColdpTerm.status,
        ColdpTerm.link
    ));
    synWriter = additionalWriter(ColdpTerm.Synonym, List.of(
        ColdpTerm.ID,
        ColdpTerm.taxonID,
        ColdpTerm.nameID,
        ColdpTerm.status,
        ColdpTerm.link
    ));
    typeWriter = additionalWriter(ColdpTerm.TypeMaterial, List.of(
        ColdpTerm.nameID,
        ColdpTerm.locality,
        ColdpTerm.citation,
        fossil,
        ColdpTerm.remarks
    ));
    nameRelWriter = additionalWriter(ColdpTerm.NameRelation, List.of(
        ColdpTerm.nameID,
        ColdpTerm.relatedNameID,
        ColdpTerm.type
    ));

    if (tmp.exists()) {
      FileUtils.cleanDirectory(tmp);
    }
    dump(Publisher.class, "publishers", this::readPublishers, this::writePublisher);
    dump(Journal.class, "journals", this::readJournal, this::writeJournal);
    dump(Reference.class, "references", this::readReferences, this::writeReference);
    dump(Taxon.class, "taxa", this::readTaxa, this::writeTaxon);
    dump(Protonym.class, "protonyms", this::readProtonyms, this::writeProtonym);
    dump(Name.class, "names", this::readName, this::writeName);

    taxWriter.close();
    typeWriter.close();
    nameRelWriter.close();
  }

  private void initTsvDumper(Class<? extends IDBase> clazz) {
    List<Field> fields = new ArrayList<>();
    fields.addAll(List.of(clazz.getSuperclass().getDeclaredFields()));
    fields.addAll(List.of(clazz.getDeclaredFields()));
    tsvWriter = new TsvWriter(new File(tmp, "raw/" + clazz.getSimpleName()+".tsv"), new TsvWriterSettings());
    // headers
    tsvWriter.writeHeaders(fields.stream().map(Field::getName).toArray(String[]::new));
    // columns
    this.columns = List.copyOf(fields);// immutable
  }

  private void writeTaxon(Taxon t) {
    try {
      if (!names.containsKey(t.name_id)) {
        Name n = new Name();
        n.id = t.name_id;
        n.type = t.type;
        n.name = t.name_cache;
        n.authorship = t.author_citation;
        n.protonymID = t.protonym_id;
        names.put(n.id, n);
      }
      if (t.current_taxon_id != null) {
        synWriter.set(ColdpTerm.ID, t.id);
        synWriter.set(ColdpTerm.nameID, t.name_id);
        synWriter.set(ColdpTerm.status, "synonym");
        synWriter.set(ColdpTerm.taxonID, t.current_taxon_id);
        synWriter.set(ColdpTerm.link, link(t.id));
        synWriter.next();
      } else {
        taxWriter.set(ColdpTerm.ID, t.id);
        taxWriter.set(ColdpTerm.nameID, t.name_id);
        taxWriter.set(ColdpTerm.parentID, t.getParentID());
        taxWriter.set(ColdpTerm.link, link(t.id));
        taxWriter.next();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private void writeName(Name name) {
    try {
      writer.set(ColdpTerm.ID, name.id);
      writer.set(gender, name.gender);
      writer.set(ColdpTerm.scientificName, ObjectUtils.coalesce(name.name, name.epithet));
      var n = names.remove(name.id);
      if (n == null) {
        LOG.warn("No taxon exists for name {}", name.id);
      } else {
        writer.set(ColdpTerm.scientificName, n.name);
        writer.set(ColdpTerm.authorship, n.authorship);
        writer.set(ColdpTerm.rank, n.type);
        //writer.set(ColdpTerm.status, );
        if (n.protonymID != null) {
          var pnid = protonym2nameID.get((int)n.protonymID);
          if (pnid != n.id) {
            writer.set(ColdpTerm.basionymID, pnid);
          }
        }
      }
      writer.next();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private void writeProtonym(Protonym p) {
    try {
      protonym2nameID.put(p.id, p.name_id);

      typeWriter.set(ColdpTerm.nameID, p.name_id);
      typeWriter.set(ColdpTerm.citation, p.primary_type_information_taxt);
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
      if (!StringUtils.isBlank(p.notes_taxt)) {
        if (sb.length()>0) {
          sb.append(" ");
        }
        sb.append(p.notes_taxt);
      }
      if (!StringUtils.isBlank(p.secondary_type_information_taxt)) {
        if (sb.length()>0) {
          sb.append(" ");
        }
        sb.append(p.secondary_type_information_taxt);
      }
      typeWriter.set(ColdpTerm.remarks, sb.toString());
      typeWriter.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private void writePublisher(Publisher p) {
    publisher.put(p.id, p);
  }
  private void writeJournal(Journal j) {
    journals.put(j.id, j);
  }
  private void writeReference(Reference r) {
    try {
      refWriter.set(ColdpTerm.ID, r.id);
      refWriter.set(ColdpTerm.type, r.type);
      refWriter.set(ColdpTerm.title, r.title);
      refWriter.set(ColdpTerm.author, r.author_names_string_cache);
      refWriter.set(ColdpTerm.issued, r.getDate());
      refWriter.set(ColdpTerm.doi, r.doi);
      refWriter.set(ColdpTerm.page, r.pagination);
      refWriter.set(ColdpTerm.volume, r.series_volume_issue);
      refWriter.set(ColdpTerm.remarks, r.public_notes);
      if (r.journal_id != null && journals.containsKey(r.journal_id)) {
        var j = journals.get(r.journal_id);
        refWriter.set(ColdpTerm.containerTitle, j.name);
      }
      if (r.publisher_id != null && publisher.containsKey(r.publisher_id)) {
        var pub = publisher.get(r.publisher_id);
        refWriter.set(ColdpTerm.publisher, pub.name);
        refWriter.set(ColdpTerm.publisherPlace, pub.place);
      }
      // keep citation cache for replacing placeholders later on
      StringBuilder sb = new StringBuilder();
      if (r.author_names_string_cache != null) {
        sb.append(r.author_names_string_cache);
      }
      if (r.year != null) {
        if (sb.length()>0) sb.append(" ");
        sb.append(r.year);
      }
      if (r.title != null) {
        if (sb.length()>0) sb.append(" ");
        sb.append(r.title);
      }
      refs.put(r.id, sb.toString());
      refWriter.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private List<Taxon> readTaxa(URI uri) {
    return read(uri, taxaTYPE);
  }
  private List<Name> readName(URI uri) {
    var names = read(uri, namesTYPE);
    names.forEach(n -> n.type = n.type.replaceAll("_name$", ""));
    return names;
  }
  private List<Reference> readReferences(URI uri) {
    return read(uri, referencesTYPE);
  }
  private List<Protonym> readProtonyms(URI uri) {
    return read(uri, protonymsTYPE);
  }
  private List<Publisher> readPublishers(URI uri) {
    return read(uri, publisherTYPE);
  }
  private List<Journal> readJournal(URI uri) {
    return read(uri, journalTYPE);
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

  private <T extends IDBase> void dump(Class<T> clazz, String path, Function<URI, List<T>> readFunc, Consumer<T> writeFunc) throws IllegalAccessException {
    initTsvDumper(clazz);
    try {
      int startID = 0;
      LOG.info("Dump {}", clazz.getSimpleName());
      var resp = readFunc.apply(buildURI(path, startID));
      while (!resp.isEmpty()) {
        for (T obj : resp) {
          // write to CSV generically
          String[] row = new String[columns.size()];
          int idx = 0;
          for (var f : columns) {
            row[idx++] = String.valueOf(f.get(obj));
          }
          tsvWriter.writeRow(row);
          // write to coldp writers
          writeFunc.accept(obj);
        }
        tsvWriter.flush();
        startID = resp.stream().map(t -> t.id).max(Integer::compare).get() + 1;
        LOG.info("Crawl {} starting with {}", clazz.getSimpleName(), startID);
        resp = readFunc.apply(buildURI(path, startID));
      }
    } finally {
      tsvWriter.close();
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
      return ObjectUtils.coalesce(subspecies_id,species_id,subgenus_id,genus_id,tribe_id,subfamily_id,family_id);
    }
  }
  static class Name extends IDBase {
    public String name;
    public String authorship;
    public String epithet;
    public String gender;
    public Integer protonymID;
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
    public String nesting_reference_id;
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
