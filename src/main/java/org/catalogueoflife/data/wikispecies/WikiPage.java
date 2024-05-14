package org.catalogueoflife.data.wikispecies;

public class WikiPage {

    public static String id(String title) {
        return title.replaceAll(" +", "_");
    }

    public String id;
    public String title;
    public String redirect;
    public String contributor;
    public String timestamp;
    public String contributorID;
    public String model;
    public String format;
    public String text;
    public String id() {
        return id(title);
    }
}
