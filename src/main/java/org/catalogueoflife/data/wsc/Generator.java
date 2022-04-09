package org.catalogueoflife.data.wsc;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import life.catalogue.api.vocab.Gazetteer;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class Generator extends AbstractGenerator {
  private static final URI API = URI.create("https://wsc.nmbe.ch/api/lsid/");
  private final String apiKey;

  private static final Pattern BRACKET_YEAR = Pattern.compile("\\(?([^()]+)\\)?");
  private final Client client;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, null);
    client = ClientBuilder.newClient();
    apiKey = Preconditions.checkNotNull(cfg.key, "API Key required");
  }

  @Override
  protected void addData() throws Exception {
    initRefWriter(List.of(
      ColdpTerm.ID,
      ColdpTerm.citation,
      ColdpTerm.doi
    ));
    newWriter(ColdpTerm.NameUsage, List.of(
      ColdpTerm.ID,
      ColdpTerm.parentID,
      ColdpTerm.status,
      ColdpTerm.rank,
      ColdpTerm.uninomial,
      ColdpTerm.genericName,
      ColdpTerm.specificEpithet,
      ColdpTerm.infraspecificEpithet,
      ColdpTerm.authorship,
      ColdpTerm.nameReferenceID,
      ColdpTerm.publishedInPage
    ));
    var dWriter = additionalWriter(ColdpTerm.Distribution, List.of(
        ColdpTerm.taxonID,
        ColdpTerm.gazetteer,
        ColdpTerm.area
    ));
    // loop over all taxa simply by int key ranges
    final IntSet refs = new IntOpenHashSet();
    final Set<String> higher = new HashSet<>();
    for (int id=1; id<61000; id++) {
      final String lsid = String.format("urn:lsid:nmbe.ch:spidersp:%06d", id);
      var to = fetch(lsid);
      if (to.isPresent()) {
        var tax = to.get();
        System.out.println(String.format("%06d: %s %s %s %s", id, tax.taxon.genus, tax.taxon.species, tax.taxon.subspecies, tax.taxon.author));
        writer.set(ColdpTerm.ID, lsid);
        writer.set(ColdpTerm.rank, tax.taxon.taxonRank);
        writer.set(ColdpTerm.genericName, tax.taxon.genus);
        writer.set(ColdpTerm.specificEpithet, tax.taxon.species);
        writer.set(ColdpTerm.infraspecificEpithet, tax.taxon.subspecies);
        writer.set(ColdpTerm.authorship, tax.taxon.author);
        writer.set(ColdpTerm.status, tax.taxon.status);

        if (tax.validTaxon != null) {
          writer.set(ColdpTerm.parentID, tax.validTaxon.getLSID());
        } else {
          if (tax.taxon.genusObject != null) {
            writer.set(ColdpTerm.parentID, tax.taxon.genusObject.genLsid);
          } else if (tax.taxon.familyObject != null) {
            writer.set(ColdpTerm.parentID, tax.taxon.familyObject.famLsid);
          }
        }

        if (tax.taxon.referenceObject != null) {
          // seen reference before?
          int rid = tax.taxon.referenceObject.reference.hashCode();
          if (!refs.contains(rid)) {
            tax.taxon.referenceObject.write(rid, refWriter);
          }
          writer.set(ColdpTerm.nameReferenceID, rid);
          writer.set(ColdpTerm.publishedInPage, tax.taxon.referenceObject.pageDescription);
        }
        if (!StringUtils.isBlank(tax.taxon.distribution)) {
          dWriter.set(ColdpTerm.taxonID, id);
          dWriter.set(ColdpTerm.area, tax.taxon.distribution);
          dWriter.set(ColdpTerm.gazetteer, Gazetteer.TEXT);
          dWriter.next();
        }
        writer.next();

        // new higher taxa?
        if (!higher.contains(tax.taxon.genusObject.genLsid)) {
          if (!higher.contains(tax.taxon.familyObject.famLsid)) {
            tax.taxon.genusObject.write(tax.taxon.familyObject.famLsid, null, false, writer, higher);
          }
          tax.taxon.familyObject.write(tax.taxon.familyObject.genLsid, tax.taxon.familyObject.famLsid, true, writer, higher);
        }
      }
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", "Version 23.0");
    super.addMetadata();
  }

  private Optional<NameUsage> fetch(String lsid) {
    WebTarget target = client.target(API+lsid).queryParam("apiKey", apiKey);
    try {
      return Optional.of(target.request(MediaType.APPLICATION_JSON).get(NameUsage.class));
    } catch (WebApplicationException e) {
      LOG.warn("Failed to fetch {}. HTTP {}", lsid, e.getResponse().getStatus(), e);
    }
    return Optional.empty();
  }

  static class NameUsage {
    public Taxon taxon;
    public Synonym validTaxon;
  }
  static class Taxon {
    public String species;
    public String subspecies;
    public String author;
    public String taxonRank;
    public String status;
    public String genus;
    public TaxonObject genusObject;
    public String family;
    public TaxonObject familyObject;
    public String distribution;
    public String lsid;
    public ReferenceObject referenceObject;
  }
  static class Synonym {
    private static Pattern LINK2LSID = Pattern.compile("urn:lsid.+$");
    public String _href;
    String getLSID() {
      if (_href != null) {
        var m = LINK2LSID.matcher(_href);
        if (m.find()) {
          return m.group();
        }
      }
      return null;
    }
  }
  static class TaxonObject {
    public String genus;
    public String genLsid;
    public String family;
    public String famLsid;
    public String author;

    void write(String id, String parentID, boolean isGenus, TermWriter w, Set<String> higher) throws IOException {
      w.set(ColdpTerm.ID, id);
      w.set(ColdpTerm.parentID, parentID);
      if (isGenus) {
        w.set(ColdpTerm.uninomial, genus);
        w.set(ColdpTerm.rank, "genus");
      } else {
        w.set(ColdpTerm.uninomial, family);
        w.set(ColdpTerm.rank, "family");
      }
      w.set(ColdpTerm.authorship, author);
      w.next();
      higher.add(id);
    }
  }
  static class ReferenceObject {
    public String reference;
    public String doi;
    public String pageDescription;

    void write(int rid, TermWriter w) throws IOException {
      w.set(ColdpTerm.citation, reference);
      w.set(ColdpTerm.doi, doi);
      w.set(ColdpTerm.ID, rid);
      w.next();
    }
  }

}
