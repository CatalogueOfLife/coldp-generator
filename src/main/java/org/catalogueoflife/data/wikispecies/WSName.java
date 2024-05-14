package org.catalogueoflife.data.wikispecies;

import life.catalogue.api.vocab.TaxonomicStatus;
import org.gbif.nameparser.api.Rank;

import java.util.ArrayList;
import java.util.List;

public class WSName {
    public String id;
    public String parentID;
    public TaxonomicStatus status;
    public String scientificName;
    public String authorship;
    public Rank rank;
    public String remarks;
    public boolean extinct;
    public WSReference publishedIn = new WSReference();
    public WSType type;

    public List<WSVernacular> vernaculars = new ArrayList<>();
    public List<WSReference> references = new ArrayList<>();

    public static class WSVernacular {
        public String language;
        public String name;
    }

    public static class WSReference {
        public String citation;
        public String author;
        public String year;
        public String title;
        public String link;
    }

    public static class WSType {
        public String status;
        public String citation;
        public String designation;
    }
}
