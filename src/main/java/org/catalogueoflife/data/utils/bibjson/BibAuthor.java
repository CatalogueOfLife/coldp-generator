package org.catalogueoflife.data.utils.bibjson;

import de.undercouch.citeproc.csl.CSLName;
import de.undercouch.citeproc.csl.CSLNameBuilder;

import java.util.Objects;

public class BibAuthor {
    private String firstname;
    private String lastname;

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public CSLName toCSL() {
        return new CSLNameBuilder()
                .family(lastname)
                .given(firstname)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BibAuthor bibAuthor)) return false;
        return Objects.equals(firstname, bibAuthor.firstname) && Objects.equals(lastname, bibAuthor.lastname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstname, lastname);
    }
}
