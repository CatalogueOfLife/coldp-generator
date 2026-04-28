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

  @Parameter(names = {"--tmp"})
  @NotNull
  public File tmpSourceDir = new File("/tmp/coldp-generator-sources");

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
  public int wscMaxKey = 66850; // max on 21.2.2025 is 64070

  @Parameter(names = {"--date"})
  public String date;

  @Parameter(names = {"--no-download"},
             description = "Skip downloading source files; use existing local copies only")
  public boolean noDownload = false;

  @Parameter(names = {"--enrich"},
             description = "Fetch PlantProfile API for each accepted name; adds Distribution, TaxonProperty, Media")
  public boolean enrich = false;

  @Parameter(names = {"--clear-sources"},
             description = "Delete cached source files before running, forcing a fresh download of everything")
  public boolean clearSources = false;

  /**
   * Returns the directory with the decompressed archive folder created by the checklist builder
   */
  public File archiveDir() {
    return new File(repository, source);
  }

  public File tmpDir() {
    return new File(tmpSourceDir, source);
  }

  public Class<? extends AbstractColdpGenerator> builderClass() {
    try {
      String classname = GeneratorConfig.class.getPackage().getName() + "." + source.toLowerCase().replace("-", "") + ".Generator";
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
