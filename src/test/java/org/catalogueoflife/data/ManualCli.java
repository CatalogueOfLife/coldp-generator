package org.catalogueoflife.data;

import org.junit.Ignore;

@Ignore
public class ManualCli {

  public static void main(String[] args) throws Exception {
    //GeneratorCLI.main( new String[]{"-s", "ictv", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "biolib", "-r", "/tmp/coldp/archives"} );
    GeneratorCLI.main( new String[]{"-s", "lpsn", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "otl", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "antcat", "-r", "/tmp/coldp/archives"} );
  }
}