package org.catalogueoflife.data;

import org.junit.Ignore;

@Ignore
public class ManualCli {

  public static void main(String[] args) throws Exception {
    //GeneratorCLI.main( new String[]{"-s", "ncbi", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "ictv", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "cycads", "-r", "/tmp/coldp/archives"});
    //GeneratorCLI.main( new String[]{"-s", "lpsn", "-r", "/tmp/coldp/archives",
    //                "--lpsn-user", "mdoering@gbif.org",
    //                "--lpsn-pass", "xxx"
    //} );
    //GeneratorCLI.main( new String[]{"-s", "wikispecies", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "wikidata", "-r", "/tmp/coldp/archives", "--tmp", "/Users/markus/Downloads/coldp-gen-src", "--no-download"} );
    GeneratorCLI.main( new String[]{"-s", "clements", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "antcat", "-r", "/tmp/coldp/archives"} );
    //GeneratorCLI.main( new String[]{"-s", "wsc", "-r", "/tmp/coldp/archives",
    //        "--wsc-data-repo", "/Users/markus/code/data/data-wsc/json2",
    //        //"--date", "skip",
    //        "--api-key", "xxx"
    //} );
  }
}