package org.catalogueoflife.data.utils.bibjson;

import de.undercouch.citeproc.bibtex.BibTeXConverter;
import de.undercouch.citeproc.bibtex.DateParser;
import de.undercouch.citeproc.csl.*;
import life.catalogue.api.util.ObjectUtils;
import org.jbibtex.Key;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static life.catalogue.api.util.ObjectUtils.coalesce;

public class BibRef {
    private static BibTeXConverter BC = new BibTeXConverter();
    private String id;
    private String type; // The type of publication this entry represents. Examples include: article, book, unpublished, etc.
    private String title;
    private String year;
    private List<BibAuthor> author = new ArrayList<>();
    private List<BibAuthor> editor = new ArrayList<>();
    private String journal;
    private String booktitle;
    private String school;
    private String publisher; // Name of the publisher
    private String address; // City of publication
    private String volume;
    private String number; // The series number within the volume, if any
    private String pages; // This field holds page numbers in responses generated with the BibJSON vocabulary
    private String language;
    private BibIdentifier identifier;  // DOI
    private String _comments;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public List<BibAuthor> getAuthor() {
        return author;
    }

    public void setAuthor(List<BibAuthor> author) {
        this.author = author;
    }

    public List<BibAuthor> getEditor() {
        return editor;
    }

    public void setEditor(List<BibAuthor> editor) {
        this.editor = editor;
    }

    public String getJournal() {
        return journal;
    }

    public void setJournal(String journal) {
        this.journal = journal;
    }

    public String getBooktitle() {
        return booktitle;
    }

    public void setBooktitle(String booktitle) {
        this.booktitle = booktitle;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getPages() {
        return pages;
    }

    public void setPages(String pages) {
        this.pages = pages;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public BibIdentifier getIdentifier() {
        return identifier;
    }

    public void setIdentifier(BibIdentifier identifier) {
        this.identifier = identifier;
    }

    public String get_comments() {
        return _comments;
    }

    public void set_comments(String _comments) {
        this._comments = _comments;
    }

    public CSLItemData toCSL() {
        CSLItemDataBuilder builder = new CSLItemDataBuilder();
        builder .id(id)
                .type(getCslType())
                .title(title)
                .issued(getCslYear())
                .author(author.isEmpty() ? null :
                        author.stream().map(BibAuthor::toCSL).toList().toArray(CSLName[]::new)
                )
                .editor(editor.isEmpty() ? null :
                        editor.stream().map(BibAuthor::toCSL).toList().toArray(CSLName[]::new)
                )
                .containerTitle(coalesce(journal, booktitle))
                .publisher(coalesce(publisher, school))
                .publisherPlace(address)
                .volume(volume)
                .number(number)
                .page(pages)
                .language(language)
                .note(_comments);
        if (identifier != null) {
            identifier.addToCsl(builder);
        }
        return builder.build();
    }

    private CSLDate getCslYear() {
        if (year != null) {
            var date = DateParser.toDate(year, null);
            if (date == null) {
                // this puts it into raw
                date = DateParser.toDate(year);
            }
            return date;
        }
        return null;
    }

    private CSLType getCslType() {
        var key = new Key(type);
        return BC.toType(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BibRef bibRef)) return false;
        return Objects.equals(id, bibRef.id) && Objects.equals(type, bibRef.type) && Objects.equals(title, bibRef.title) && Objects.equals(year, bibRef.year) && Objects.equals(author, bibRef.author) && Objects.equals(editor, bibRef.editor) && Objects.equals(journal, bibRef.journal) && Objects.equals(booktitle, bibRef.booktitle) && Objects.equals(school, bibRef.school) && Objects.equals(publisher, bibRef.publisher) && Objects.equals(address, bibRef.address) && Objects.equals(volume, bibRef.volume) && Objects.equals(number, bibRef.number) && Objects.equals(pages, bibRef.pages) && Objects.equals(language, bibRef.language) && Objects.equals(identifier, bibRef.identifier) && Objects.equals(_comments, bibRef._comments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, title, year, author, editor, journal, booktitle, school, publisher, address, volume, number, pages, language, identifier, _comments);
    }
}
