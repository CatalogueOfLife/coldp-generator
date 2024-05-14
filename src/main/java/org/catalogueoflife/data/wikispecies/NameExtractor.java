package org.catalogueoflife.data.wikispecies;

import org.sweble.wikitext.parser.nodes.*;

import de.fau.cs.osr.ptk.common.AstVisitor;

import java.util.regex.Pattern;

/**
 * ''Oenanthe'' {{a|Louis Pierre Vieillot|Vieillot}}, 1816
 *
 * * {{int:Typus}}: ''Turdus leucurus'' {{a|Johann Friedrich Gmelin|Gmelin}}, 1789 = ''[[Oenanthe leucura]]''
 */
public class NameExtractor extends AstVisitor<WtNode> {

    final StringBuilder scientificName = new StringBuilder();
    final StringBuilder authorship = new StringBuilder();

    private final WSName name;
    private boolean firstLine = true;

    public NameExtractor(WSName name) {
        this.name = name;
    }
    @Override
    protected Object after(WtNode node, Object result) {
        // This method is called by go() after visitation has finished
        // The return value will be passed to go() which passes it to the caller
        name.scientificName = scientificName.toString();
        name.authorship = authorship.toString();
        return name;
    }

    public void visit(WtNode n) {
        iterate(n);
    }

    public void visit(WtText text) {
        Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{([^|]+)(?:|([^|]+))*\\}\\}}", Pattern.CASE_INSENSITIVE);
        var m = TEMPLATE_PATTERN.matcher(text.getContent());
        while (m.find()) {
            if (m.group(1).equalsIgnoreCase("a")) {
                m.appendReplacement(scientificName, m.group(3));
            } else if (m.group(1).equalsIgnoreCase("aut")) {
                m.appendReplacement(scientificName, m.group(2));
            } else if (m.group(1).equalsIgnoreCase("BHL page")) {
                System.out.println("BHL!");
                name.publishedIn.link = "12345678";
                m.appendReplacement(scientificName, m.group(2));
            }
        }
        m.appendTail(scientificName);
        scientificName.append(text.getContent());
    }

    public void visit(WtWhitespace w) {
        scientificName.append(" ");
    }

    @Override
    protected void iterate(WtNode node) {
        for (WtNode n : node) {
            if (n instanceof WtNewline) {
                if (firstLine) {
                    firstLine = false;
                } else {
                    // the real name is on the fist line only - back out!
                    break;
                }
            }
            dispatch(n);
        }
    }

    public void visit(WtTemplate t) {
        System.out.println(t);
    }

}
