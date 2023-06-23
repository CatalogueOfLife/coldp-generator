package org.catalogueoflife.data.mites;

import life.catalogue.coldp.ColdpTerm;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Pattern;

public class Generator extends AbstractColdpGenerator {
  private static final URI BASE = URI.create("http://www.miteresearch.org/");
  private static final URI INDEX = BASE.resolve("/genusindex.php");
  private static final URI GENUS = BASE.resolve("/db/main.php?genus=445");

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true);
  }

  private void initWriter() throws IOException {
    initRefWriter(List.of(
        ColdpTerm.ID,
        ColdpTerm.author,
        ColdpTerm.containerTitle,
        ColdpTerm.issued,
        ColdpTerm.volume,
        ColdpTerm.issue,
        ColdpTerm.page
    ));
    newWriter(ColdpTerm.NameUsage, List.of(
        ColdpTerm.ID,
        ColdpTerm.parentID,
        ColdpTerm.basionymID,
        ColdpTerm.status,
        ColdpTerm.nameStatus,
        ColdpTerm.rank,
        ColdpTerm.scientificName,
        ColdpTerm.authorship,
        ColdpTerm.family,
        ColdpTerm.genus,
        //ColdpTerm.genericName,
        //ColdpTerm.specificEpithet,
        //ColdpTerm.infraspecificEpithet,
        ColdpTerm.nameRemarks,
        ColdpTerm.nameReferenceID
    ));
  }

  @Override
  protected void addData() throws Exception {
    // read & write core file
    initWriter();
    // read genus index
    var html = http.get(GENUS);
    final Document doc = Jsoup.parse(html);

    Pattern genusID = Pattern.compile("genus=([0-9]+)");
    var x = doc.select("body > div.zone-mitte > div.inhalt.indexlist > ul > li > a");
    x = doc.select("div.indexlist ul");
    x = doc.select("div.indexlist ul li");
    for (var a : doc.select("div.indexlist ul li a")) {
      var genus = a.text();
      var gm = genusID.matcher(a.attr("href"));
      if (gm.find()) {
        var id = gm.group(1);
        System.out.println(genus + "  -->  " + id);
      }
    }
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now());
    metadata.put("version", LocalDate.now());
    super.addMetadata();
  }
}
