package org.catalogueoflife.data.wca;

import life.catalogue.api.model.DOI;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.Resources;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static org.catalogueoflife.data.wca.WcaMappings.*;

/**
 * Crops of the FAO World Programme for the Census of Agriculture 2030 (WCA 2030).
 * <p>
 * Annex 7 of the WCA lists about 300 crops with their botanical names, the Indicative Crop Classification (ICC)
 * and the Central Product Classification (CPC). The tables only exist in the PDF and are curated once into
 * resources/wca by scripts/gen_wca_tsv.py, so the generator downloads nothing.
 * <p>
 * Each distinct botanical name becomes an accepted name usage. The ICC groups crops by agricultural use, not by
 * botany, so it is not used as the classification: names hang under Plantae > Tracheophyta or Fungi, the crop names
 * become vernacular names, and ICC, CPC and the temporary/permanent crop type become taxon properties.
 */
public class Generator extends AbstractColdpGenerator {
  static final String PLANTAE = "Plantae";
  static final String TRACHEOPHYTA = "Tracheophyta";
  static final String FUNGI = "Fungi";
  static final DOI WCA_DOI = new DOI("10.4060/cd9437en");
  // the citation page and Annexes 6 and 7 of the WCA 2030 PDF, cut out by scripts/gen_wca_tsv.py
  static final String SOURCE_PDF = "wca/wca2030-annex6-7.pdf";

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  /** Collects the property rows of one taxon, merging the crops that share a value. */
  private static class Props {
    final Map<List<String>, Set<String>> crops = new LinkedHashMap<>();

    void add(String property, String value, String remarks, String crop) {
      if (value != null && !value.isBlank()) {
        crops.computeIfAbsent(Arrays.asList(property, value, remarks), k -> new LinkedHashSet<>()).add(crop);
      }
    }
  }

  @Override
  protected void addData() throws Exception {
    Map<String, IccClass> icc = readIcc();
    Map<String, List<Crop>> byName = new TreeMap<>();
    for (Crop c : readCrops()) {
      byName.computeIfAbsent(c.scientificName(), k -> new ArrayList<>()).add(c);
    }

    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID, ColdpTerm.parentID, ColdpTerm.status, ColdpTerm.rank, ColdpTerm.scientificName,
        ColdpTerm.code, ColdpTerm.remarks));
    var vWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
        ColdpTerm.taxonID, ColdpTerm.name, ColdpTerm.language));
    var pWriter = additionalWriter(ColdpTerm.TaxonProperty, List.of(
        ColdpTerm.taxonID, ColdpTerm.property, ColdpTerm.value, ColdpTerm.remarks));

    usage(PLANTAE, null, "kingdom", PLANTAE, null);
    usage(TRACHEOPHYTA, PLANTAE, "phylum", TRACHEOPHYTA, null);
    usage(FUNGI, null, "kingdom", FUNGI, null);

    int vernaculars = 0, properties = 0;
    for (var e : byName.entrySet()) {
      String name = e.getKey();
      List<Crop> crops = e.getValue();
      String id = id(name);
      Set<String> verbatim = new LinkedHashSet<>();
      Props props = new Props();
      for (Crop c : crops) {
        if (!c.verbatimName().isBlank()) {
          verbatim.add(c.verbatimName());
        }
        props.add("ICC 1.2", c.icc12(), path(icc, c.icc12()), c.crop());
        props.add("ICC 1.1", c.icc11(), null, c.crop());
        props.add("crop type", cropType(icc, c.icc12()), null, c.crop());
        for (CpcCode cpc : cpc(c.cpc())) {
          StringJoiner sj = new StringJoiner("; ");
          if (cpc.note() != null) sj.add(cpc.note());
          if (cpc.partial()) sj.add("CPC class shared with other crops");
          props.add("CPC 3.0", cpc.code(), sj.length() == 0 ? null : sj.toString(), c.crop());
        }
      }
      String parent = FUNGI.equals(crops.getFirst().kingdom()) ? FUNGI : TRACHEOPHYTA;
      String remarks = verbatim.isEmpty() ? null : "Given in WCA 2030 as: " + String.join(" | ", verbatim);
      usage(id, parent, rank(name), name, remarks);

      for (String crop : new LinkedHashSet<>(crops.stream().map(Crop::crop).toList())) {
        vWriter.set(ColdpTerm.taxonID, id);
        vWriter.set(ColdpTerm.name, crop);
        vWriter.set(ColdpTerm.language, "eng");
        vWriter.next();
        vernaculars++;
      }
      for (var p : props.crops.entrySet()) {
        List<String> key = p.getKey();
        StringJoiner sj = new StringJoiner("; ");
        if (key.get(2) != null) sj.add(key.get(2));
        sj.add("crop: " + String.join(", ", p.getValue()));
        pWriter.set(ColdpTerm.taxonID, id);
        pWriter.set(ColdpTerm.property, key.get(0));
        pWriter.set(ColdpTerm.value, key.get(1));
        pWriter.set(ColdpTerm.remarks, sj.toString());
        pWriter.next();
        properties++;
      }
    }
    LOG.info("Wrote {} names with {} vernacular names and {} properties", byName.size(), vernaculars, properties);
    copySourcePages();
    if (!cfg.noDownload) {
      addSource(WCA_DOI);
    }
  }

  /** Ships the PDF pages the curated tables were extracted from, so the archive documents its own source. */
  private void copySourcePages() throws IOException {
    try (InputStream in = Resources.stream(SOURCE_PDF)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource " + SOURCE_PDF);
      }
      File f = new File(dir, new File(SOURCE_PDF).getName());
      Files.copy(in, f.toPath(), StandardCopyOption.REPLACE_EXISTING);
      LOG.info("Added source pages {}", f.getName());
    }
  }

  private void usage(String id, String parentID, String rank, String name, String remarks) throws IOException {
    writer.set(ColdpTerm.ID, id);
    writer.set(ColdpTerm.parentID, parentID);
    writer.set(ColdpTerm.status, "accepted");
    writer.set(ColdpTerm.rank, rank);
    writer.set(ColdpTerm.scientificName, name);
    writer.set(ColdpTerm.code, "botanical");
    writer.set(ColdpTerm.remarks, remarks);
    writer.next();
  }
}
