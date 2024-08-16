package org.catalogueoflife.data.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class MarkdownUtilsTest {

    @Test
    public void removeMarkup() throws Exception {
        Assert.assertNull(MarkdownUtils.removeMarkup(null));
        Assert.assertNull(MarkdownUtils.removeMarkup(""));
        Assert.assertNull(MarkdownUtils.removeMarkup("  "));

        Assert.assertEquals("x", MarkdownUtils.removeMarkup("x"));
        Assert.assertEquals("*", MarkdownUtils.removeMarkup("*"));
        Assert.assertEquals("==", MarkdownUtils.removeMarkup("=="));
        Assert.assertEquals("=markus*", MarkdownUtils.removeMarkup("=markus*"));
        Assert.assertEquals("*markus**", MarkdownUtils.removeMarkup("*markus**"));

        Assert.assertEquals("markus", MarkdownUtils.removeMarkup("=markus="));
        Assert.assertEquals("markus", MarkdownUtils.removeMarkup("==markus=="));
        Assert.assertEquals("markus", MarkdownUtils.removeMarkup("**markus**"));
        Assert.assertEquals("Monomorium pharaonis", MarkdownUtils.removeMarkup("*Monomorium pharaonis*"));
        Assert.assertEquals("2010 (\"2009\"). Monomorium kugleri n. sp., a new fossil ant species", MarkdownUtils.removeMarkup("2010 (\"2009\"). *Monomorium kugleri* n. sp., a new fossil ant species"));
    }

}