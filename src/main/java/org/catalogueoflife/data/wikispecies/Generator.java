package org.catalogueoflife.data.wikispecies;

import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.coldp.ColdpTerm;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import org.apache.commons.lang3.StringUtils;
import org.catalogueoflife.data.AbstractColdpGenerator;
import org.catalogueoflife.data.GeneratorConfig;
import org.sweble.wikitext.parser.ParserConfig;
import org.sweble.wikitext.parser.WikitextParser;
import org.sweble.wikitext.parser.nodes.*;
import org.sweble.wikitext.parser.utils.SimpleParserConfig;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

public class Generator extends AbstractColdpGenerator {
  static final URI DOWNLOAD = URI.create("https://dumps.wikimedia.org/specieswiki/latest/specieswiki-latest-pages-articles.xml.bz2");
  int counterTax = 0;
  int counterTmpl = 0;
  BufferedWriter titles;
  WikitextParser p;
  private TermWriter vernWriter;

  public Generator(GeneratorConfig cfg) throws IOException {
    super(cfg, true, DOWNLOAD);
    ParserConfig pcfg = new SimpleParserConfig();
    p = new WikitextParser(pcfg);
  }

  @Override
  protected void addData() throws Exception {
    System.out.println("\nStart Wikispecies parsing");
    var factory = XMLInputFactory.newInstance();
    titles = UTF8IoUtils.writerFromFile(new File("/tmp/wikispecies-titles.txt"));
    try (var input = new FileInputStream(src)) {
    //try (var input = new BZip2CompressorInputStream(new FileInputStream(src), true)) {
      XMLStreamReader parser = factory.createXMLStreamReader(new InputStreamReader(input, StandardCharsets.UTF_8));

      int event;
      WikiPage page = null;
      boolean revision = false;
      boolean contributor = false;
      StringBuilder text = null;
      while ((event = parser.next()) != XMLStreamConstants.END_DOCUMENT) {
        switch (event) {
          case XMLStreamConstants.START_ELEMENT:
            //System.out.println("> " + parser.getLocalName());
            text = new StringBuilder();
            switch (parser.getLocalName()) {
              case "page":
                page = new WikiPage();
                break;
              case "revision":
                revision = true;
                break;
              case "contributor":
                contributor = true;
                break;
              case "redirect":
                if (page != null) {
                  page.redirect = parser.getAttributeValue(null, "title");
                }
                break;
            }
            break;

          case XMLStreamConstants.END_ELEMENT:
            //System.out.println("< " + parser.getLocalName());
            switch (parser.getLocalName()) {
              case "page":
                if (page != null) {
                  processPage(page);
                  page = null;
                }
                break;
              case "revision":
                revision = false;
                break;
              case "contributor":
                contributor = false;
                break;
              case "title":
                if (page != null) {
                  page.title = text.toString();
                }
                break;
              case "id":
                if (contributor) {
                  page.contributorID = text.toString();
                } else if (!revision && page != null) {
                  page.id = text.toString();
                }
                break;
              case "timestamp":
                if (page != null) {
                  page.timestamp = text.toString();
                }
                break;
              case "username":
                if (contributor) {
                  page.contributor = text.toString();
                }
                break;
              case "model":
                if (page != null) {
                  page.model = text.toString();
                }
                break;
              case "format":
                if (page != null) {
                  page.format = text.toString();
                }
                break;
              case "text":
                if (page != null) {
                  page.text = text.toString();
                }
                break;
            }
            break;

          case XMLStreamConstants.CHARACTERS:
            if (page != null) {
              String x = parser.getText();
              if (!StringUtils.isBlank(x)) {
                text.append(x);
              }
            }
            break;
        }
      }
    } finally {
      titles.close();
      vernWriter.close();
    }

    // at the end
    System.out.println("\nEnded Wikispecies parsing");
  }

  @Override
  protected void prepare() throws Exception {
    initRefWriter(List.of(
            ColdpTerm.ID,
            ColdpTerm.citation,
            ColdpTerm.doi
    ));
    newWriter(ColdpTerm.NameUsage, List.of(
            ColdpTerm.ID,
            ColdpTerm.parentID,
            ColdpTerm.status,
            ColdpTerm.nameStatus,
            ColdpTerm.rank,
            ColdpTerm.scientificName,
            ColdpTerm.authorship,

            //ColdpTerm.uninomial,
            //ColdpTerm.genericName,
            //ColdpTerm.specificEpithet,
            //ColdpTerm.infraspecificEpithet,
            //ColdpTerm.nameReferenceID,
            //ColdpTerm.publishedInPage,
            ColdpTerm.remarks
    ));

    vernWriter = additionalWriter(ColdpTerm.VernacularName, List.of(
            ColdpTerm.ID,
            ColdpTerm.language,
            ColdpTerm.name
    ));
  }
  private void processPage(WikiPage page) throws Exception {
    if (page.title.startsWith("Wikispecies:") || page.title.startsWith("Category:")) {
      //titles.write("\n--- " + page.title);

    } else if (Set.of("Main Page").contains(page.title)) {
      //titles.write("\n--- " + page.title);

    } else if (page.model.equalsIgnoreCase("wikitext")) {
      String txt = page.text.toLowerCase().trim();
      if (page.title.startsWith("Template:")) {
        titles.write("\nTMPL " + page.title);
        processTemplate(page);

      } else if (page.redirect != null) {
        titles.write("\nSYN " + page.title);
        processRedirect(page);

      } else if (txt.contains("{{int:name}}")) {
        titles.write("\nNAME " + page.title);
        processName(page);

      } else if (txt.contains("[[category:taxon authorities]]")) {
        titles.write("\nAUTH " + page.title);
        processAuthor(page);

      } else if (txt.contains("[[category:series identifiers]]")) {
        titles.write("\nSERIES " + page.title);
        processSeries(page);

      } else if (txt.contains("[[category:sources]]")) {
        titles.write("\nSOURCE " + page.title);
        processSeries(page);

      } else if (txt.startsWith("{{repository")) {
        titles.write("\nCOLL " + page.title);

      } else if (txt.contains("{{int disambiguation|cat=taxon}}")) {
        titles.write("\nAMBIG " + page.title);

      } else if (txt.contains("{{int disambig|cat=taxon}}")) {
        titles.write("\nAMBIG " + page.title);

      } else if (txt.contains("{{disambig}}")) {
        titles.write("\nAMBIG " + page.title);

      } else if (txt.contains("{{int:Taxonavigation}}")) {
        processName(page);

      } else {
        titles.write("\n??? " + page.title);
      }

    } else {
      titles.write("\n??? " + page.title + " --> " + page.model+ " | " + page.format);
    }
  }

  private void processSeries(WikiPage page) {

  }

  private void processAuthor(WikiPage page) {

  }

  private void processRedirect(WikiPage page) throws IOException {
    //TODO: redirects can also be authors - need to check the target page to be sure its a taxon!!!
    // store in memory first before writing as synonyms ???
    writer.set(ColdpTerm.ID, page.id());
    writer.set(ColdpTerm.parentID, WikiPage.id(page.redirect));
    writer.set(ColdpTerm.status, TaxonomicStatus.SYNONYM);
    writer.set(ColdpTerm.scientificName, page.title);
    writer.set(ColdpTerm.authorship, "");
    writer.set(ColdpTerm.rank, "");
    writer.next();
  }

  private void processTemplate(WikiPage page) throws Exception {
    counterTmpl++;
  }

  private void processName(WikiPage page) throws Exception {
    counterTax++;
    boolean nameFound = false;
    System.out.println("\n\n----------------------------------------------------------------START");
    System.out.println(page.text);
    System.out.println("----------------------------------------------------------------");

    final WSName name = new WSName();
    var article = p.parseArticle(page.text, page.title);
    for (WtNode node : article) {
      if (node instanceof WtSection) {
        WtSection sect = (WtSection) node;
        if (!sect.isEmpty()) {
          var head = (WtText) sect.getHeading().get(0);
          var h = head.getContent().trim().toLowerCase();
          System.out.println("========== " + h);
          var txt = (String) new TextExtractor().go(sect.getBody());
          System.out.println("TEXT");
          System.out.println(txt);
          switch (h) {
            case "{{int:name}}":
              new NameExtractor(name).go(sect.getBody());
              name.remarks = txt;
            case "{{int:taxonavigation}}":
              name.parentID = txt;
          }
        }
      }
    }
    System.out.println("----------------------------------------------------------------END");
    // finally write all data to coldp files
    if (name.scientificName.length()>1) {
      writer.set(ColdpTerm.ID, page.id());
      writer.set(ColdpTerm.parentID, name.parentID);
      writer.set(ColdpTerm.status, TaxonomicStatus.ACCEPTED);
      writer.set(ColdpTerm.scientificName, name.scientificName);
      writer.set(ColdpTerm.authorship, name.authorship);
      writer.set(ColdpTerm.rank, name.rank);
      writer.set(ColdpTerm.remarks, name.remarks);
      writer.next();

      for (var vn : name.vernaculars) {
        vernWriter.set(ColdpTerm.taxonID, page.id());
        vernWriter.set(ColdpTerm.language, vn.language);
        vernWriter.set(ColdpTerm.name, vn.name);
        vernWriter.next();
      }

      for (var r : name.references) {
        refWriter.set(ColdpTerm.ID, "null");
        refWriter.set(ColdpTerm.citation, r.citation);
        refWriter.next();
      }
    }


    // {{Image|Black Wheatear, Garraf, Barcelona, Spain 1.jpg|''Oenanthe leucura'', Barcelona, Spain}}

    // =={{int:Name}}==
    // {{a|Carolus Linnaeus}}   linked author names that exist as pages
    // {{aut|Carolus Linnaeus}}   styled author names, no pages

    // =={{int:Taxonavigation}}==

    // ==={{int:Synonyms}}===
    // ==={{int:Synonymy}}===

    // {{int:Type locality}}ː "Vorder Indien, Kirinde". "Ceylon" in Lepindex. [https://www.google.co.uk/maps/@6.2233674,81.2979471,13z Map].

    // =={{int:Vernacular names}}==
    // {{VN |als=Weisstanne |an=Abet común |ar=شوح أبيض |az=Avropa ağ şamı |azb=آوروپا آغ شامی |be=Піхта белая |be-x-old=Піхта белая |bg=Обикновена ела |ca=Avet blanc |co=Ghjaddicu |cs=Jedle bělokorá

    // ==={{int:Links}}===
    // {{POWO|2023|Feb|20}}

    //=={{int:References}}==
    //==={{int:Primary references}}===
    //{{Miller, 1768}}
    //==={{int:Additional references}}===
    //{{Greuter, 1993}}  --> Template:Greuter, 1993

    //System.out.println(page.text);
  }

  @Override
  protected void addMetadata() throws Exception {
    metadata.put("issued", LocalDate.now().toString());
    metadata.put("version", "Version 23.0");
    super.addMetadata();
  }
}
