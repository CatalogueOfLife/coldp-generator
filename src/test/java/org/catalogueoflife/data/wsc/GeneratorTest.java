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
}