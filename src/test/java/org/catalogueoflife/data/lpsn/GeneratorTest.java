package org.catalogueoflife.data.lpsn;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.catalogueoflife.data.GeneratorConfig;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.catalogueoflife.data.Utils.verifyMetadata;

public class GeneratorTest {

    @Test
    public void metadata() throws Exception {
        var cfg = new GeneratorConfig();
        cfg.source = "lpsn";
        var gen = new Generator(cfg);
        gen.addMetadata();
        System.out.println(gen.getMetadataFile());
        var x = FileUtils.readFileToString(gen.getMetadataFile(), StandardCharsets.UTF_8);
        System.out.println(x);
        verifyMetadata(gen.getMetadataFile());
    }

    @Test
    public void deserialize() throws Exception {
        final String json = "{\"count\":30106,\"next\":\"https://api.lpsn.dsmz.de/advanced_search?validly-published=yes&page=1\",\"previous\":null,\"results\":[10,89,240,348,368,404,443,539,543,547,548,553,555,556,560,561,562,597,603,609,612,635,668,881,886,1058,1119,1121,1123,1135,1209,1223,1624,1652,1653,1660,1703,1704,1716,1752,1759,1773,1774,1791,1830,2135,2158,2181,2185,2341,2355,2518,2554,2559,2568,2617,2654,2741,2766,2768,2794,2822,2893,2898,2902,2906,2917,2970,2974,2980,2985,2986,2988,2990,2992,3000,3004,3015,3041,3042,3051,3068,3070,3093,3094,3095,3099,3110,3125,3126,3141,3143,3147,3148,3154,3155,3158,3166,3176,3209]}";
        final ObjectMapper mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        var res = mapper.readValue(json, Generator.SearchResult.class);
        Assert.assertEquals(100, res.results.size());
    }
}