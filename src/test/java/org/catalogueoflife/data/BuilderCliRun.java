package org.catalogueoflife.data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BuilderCliRun {

  public static void main(String[] args) throws Exception {
    BuilderCli.main( new String[]{"-s", "ictv",   "-r", "/tmp/checklist_builder/archives"} );
  }
}