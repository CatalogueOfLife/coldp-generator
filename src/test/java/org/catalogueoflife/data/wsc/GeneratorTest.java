package org.catalogueoflife.data.wsc;

import life.catalogue.common.io.Resources;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class GeneratorTest {

    @Test
    public void scrapeVersion() throws IOException {
        var html = Resources.toString("wsc.html");
        assertEquals("25.5", Generator.scrapeVersion(html));
    }

    @Test
    public void authorship() {
        // recombination: WSC already provides the year in parentheses, leave untouched
        assertEquals("(Blackwall, 1841)",
                Generator.authorship("(Blackwall, 1841)",
                        "Blackwall, J. (1841). The difference in the number of eyes..."));

        // original combination with a correctly formed authorship (year present) is left untouched
        assertEquals("C. L. Koch, 1841",
                Generator.authorship("C. L. Koch, 1841", "Koch, C. L. (1841). Die Arachniden..."));

        // original combination missing the year (WSC API bug): recover it from the original description reference
        assertEquals("Galán-Sánchez & Álvarez-Padilla, 2022",
                Generator.authorship("Galán-Sánchez & Álvarez-Padilla,",
                        "Galán-Sánchez, M. A. & Álvarez-Padilla, F. (2022). A new genus of caponiid spiders. Zootaxa 5128(4): 547-573."));

        assertEquals("Clerck, 1757",
                Generator.authorship("Clerck,",
                        "Clerck, C. (1757). Aranei Svecici. Svenska spindlar. Laurentius Salvius, Stockholmiae, 154 pp."));

        // reference year carries a letter suffix and a later parenthesised number - take the leading publication year
        assertEquals("Simon, 1898",
                Generator.authorship("Simon,",
                        "Simon, E. (1898i). On the spiders of the island of St Vincent. III. Proceedings of the Zoological Society of London 65(4, 1897): 860-890."));

        // missing year but no reference available: strip the dangling comma rather than emit it
        assertEquals("Clerck", Generator.authorship("Clerck,", null));

        // blank author passes through
        assertNull(Generator.authorship(null, "whatever"));
        assertEquals("", Generator.authorship("", "whatever"));
    }
}