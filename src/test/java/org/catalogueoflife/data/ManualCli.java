package org.catalogueoflife.data;

import org.junit.Ignore;

@Ignore
public class ManualCli {

  public static void main(String[] args) throws Exception {
    //GeneratorCLI.main( new String[]{"-s", "wcvp", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "lpsn", "-r", "/tmp/coldp/archives"} );
    GeneratorCLI.main( new String[]{"-s", "birdlife", "-r", "/tmp/coldp/archives"} );
  }
}