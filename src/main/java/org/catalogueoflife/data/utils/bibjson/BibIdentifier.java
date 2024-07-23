package org.catalogueoflife.data.utils.bibjson;

import de.undercouch.citeproc.csl.CSLItemDataBuilder;
import org.apache.jena.base.Sys;

import java.util.Objects;

public class BibIdentifier {
    private String type;
    private String id;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BibIdentifier that)) return false;
        return Objects.equals(type, that.type) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id);
    }

    public void addToCsl(CSLItemDataBuilder builder) {
        switch (type.trim().toLowerCase()) {
            case "doi":
                builder.DOI(id);
                break;
            case "isbn":
                builder.ISBN(id);
                break;
            case "issn":
                builder.ISSN(id);
                break;
            default:
                System.out.println("Unknown identifier type " + type +": " + id);
        }
    }
}
