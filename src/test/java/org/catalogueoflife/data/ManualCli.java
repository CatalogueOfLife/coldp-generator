package org.catalogueoflife.data;

import org.junit.Ignore;

@Ignore
public class ManualCli {

  public static void main(String[] args) throws Exception {
    //GeneratorCLI.main( new String[]{"-s", "ictv", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "biolib", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "lpsn", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "wikispecies", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "otl", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "antcat", "-r", "/tmp/coldp/archives"} );
    GeneratorCLI.main( new String[]{"-s", "wsc", "-r", "/tmp/coldp/archives",
            "--api-key", "4a4b93272b6e9ce923d6dfabfab27f67KSWZBp4RCv",
            "--wsc-data-repo", "/Users/markus/code/data/data-wsc/json"
            //"--date", "2024-01-01"
    } );
  }
}