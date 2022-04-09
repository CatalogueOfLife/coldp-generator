package org.catalogueoflife.data;

import org.junit.Ignore;

@Ignore
public class ManualCli {

  public static void main(String[] args) throws Exception {
    //GeneratorCLI.main( new String[]{"-s", "wcvp", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "lpsn", "-r", "/tmp/coldp/archives"} );
    GeneratorCLI.main( new String[]{"-s", "wsc", "-r", "/tmp/coldp/archives", "--key", "4a4b93272b6e9ce923d6dfabfab27f67KSWZBp4RCv"} );
  }
}