package org.catalogueoflife.data.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

public class MarkdownUtils {

    private static final Pattern TAGS = Pattern.compile("(\\*+|=+)([a-zA-Z0-9;:,. _-]+)\\1(?![*=])");

    /**
     * Strips all markdown tags if they exist.
     * Currently only supports: *xxx* and =xxx=
     */
    public static String removeMarkup(String x) {
        if (!StringUtils.isBlank(x)) {
            return TAGS.matcher(x).replaceAll("$2");
        }
        return null;
    }
}
