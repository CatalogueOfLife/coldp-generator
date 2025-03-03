package org.catalogueoflife.data;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.List;

/**
 *
 */
public class GeneratorConfig {

  @Parameter(names = {"-r", "--repository"})
  @NotNull
  public File repository = new File("/tmp/coldp-generator");

  @Parameter(names = {"-s", "--source"}, required = true)
  @NotNull
  public String source;

  @Parameter(names = {"--api-key"})
  public String apiKey;

  @Parameter(names = {"--lpsn-user"})
  public String lpsnUsername;
  @Parameter(names = {"--lpsn-pass"})
  public String lpsnPassword;
  @Parameter(names = {"--biolib-root-id"})
  public int biolibRootID = 10713; // Coccinellidae, 4801=Coleoptera, 14955=Arthropoda, 10726=small test group

  @Parameter(names = {"--wsc-data-repo"})
  public File wscDataRepo;

  @Parameter(names = {"--wsc-max-key"})
  public int wscMaxKey = 70000; // max on 21.2.2025 is 64070

  @Parameter(names = {"--date"})
  public String date;

  /**
   * Returns the directory with the decompressed archive folder created by the checklist builder
   */
  public File archiveDir() {
    return new File(repository, source);
  }

  public Class<? extends AbstractColdpGenerator> builderClass() {
    try {
      String classname = GeneratorConfig.class.getPackage().getName() + "." + source.toLowerCase() + ".Generator";
      return (Class<? extends AbstractColdpGenerator>) GeneratorConfig.class.getClassLoader().loadClass(classname);

    } catch (ClassNotFoundException e) {
      List<String> sources = Lists.newArrayList();
      Joiner joiner = Joiner.on(",").skipNulls();
      throw new IllegalArgumentException(source + " not a valid source. Please use one of: " + joiner.join(sources), e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
