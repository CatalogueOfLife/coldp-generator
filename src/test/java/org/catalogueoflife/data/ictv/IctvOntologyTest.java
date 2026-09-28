package org.catalogueoflife.data.ictv;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class IctvOntologyTest {

  @Test
  public void parsePage() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/ictv/ols-page.json")) {
      var json = new ObjectMapper().readTree(in);
      assertEquals(1, IctvOntology.totalPages(json));

      Map<String, IctvOntology.Taxon> taxa = IctvOntology.parsePage(json).stream()
          .collect(Collectors.toMap(IctvOntology.Taxon::id, Function.identity()));
      assertEquals(3, taxa.size());

      var sars = taxa.get("ICTV20040588");
      assertEquals("Betacoronavirus pandemicum", sars.name());
      assertEquals("species", sars.rank());
      assertEquals("species|Betacoronavirus pandemicum", sars.key());
      // the plain SARS-CoV abbreviation is not a previous name
      assertEquals(List.of(
          new IctvOntology.PrevName("Severe acute respiratory syndrome coronavirus", "MSL22"),
          new IctvOntology.PrevName("Severe acute respiratory syndrome-related coronavirus", "MSL25")
      ), sars.previous());

      var simplex = taxa.get("ICTV19790066");
      assertEquals("genus", simplex.rank());
      // the "Unnamed genus 1" placeholder is dropped
      assertEquals(List.of(new IctvOntology.PrevName("Human herpesvirus 1 group", "MSL8")), simplex.previous());

      var realm = taxa.get("ICTV201907161");
      assertEquals("realm", realm.rank());
      assertEquals(List.of(new IctvOntology.PrevName("Monodnaviria", "MSL35")), realm.previous());
    }
  }

  @Test
  public void releasePage() {
    assertEquals("https://www.ebi.ac.uk/ols4/api/v2/ontologies/ictv/classes?search=MSL41"
            + "&searchFields=http__%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23versionInfo&exactMatch=true&page=2&size=1000",
        IctvOntology.releasePage("MSL41", 2, 1000).toString());
  }
}
