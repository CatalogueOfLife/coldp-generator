package org.catalogueoflife.data.otl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import life.catalogue.coldp.ColdpTerm;
import org.catalogueoflife.data.GeneratorConfig;
import org.catalogueoflife.newick.SimpleNode;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class GeneratorTest {

  @Test
  public void parseMrca() throws Exception {
    GeneratorConfig cfg = new GeneratorConfig();
    cfg.source = "abba";
    cfg.repository = new File("/tmp/repo");
    Generator gen = new Generator(cfg);
    gen.newWriter(ColdpTerm.NameUsage);
    gen.ott.put(42, new Generator.OttName("My42", "unranked"));
    gen.ott.put(150, new Generator.OttName("My150", "unranked"));

    SimpleNode n = new SimpleNode();
    n.setLabel("mrcaott42ott150");

    gen.writeNode(n, null);
  }
}