package org.catalogueoflife.data;

import life.catalogue.api.model.DatasetWithSettings;
import life.catalogue.metadata.coldp.ColdpMetadataParser;
import life.catalogue.metadata.coldp.YamlMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertNotNull;

public class Utils {

    public static void verifyMetadata(File file) throws IOException {
        var opt = ColdpMetadataParser.readYAML(Files.newInputStream(file.toPath()));
        if (!opt.isPresent()) {
            throw new IllegalStateException("Metadata file " + file + " not found");
        }
        var metadata = opt.get();
        assertNotNull(metadata.getAlias());
        assertNotNull(metadata.getTitle());
        assertNotNull(metadata.getVersion());
        assertNotNull(metadata.getIssued());
        assertNotNull(metadata.getDescription());
    }
}
