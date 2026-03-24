package org.catalogueoflife.data.wikispecies;

import de.fau.cs.osr.ptk.common.AstVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.parser.nodes.*;

import java.util.stream.Collectors;

/**
 * ''Oenanthe'' {{a|Louis Pierre Vieillot|Vieillot}}, 1816
 *
 * * {{int:Typus}}: ''Turdus leucurus'' {{a|Johann Friedrich Gmelin|Gmelin}}, 1789 = ''[[Oenanthe leucura]]''
 */
public class TextExtractor extends AstVisitor<WtNode> {
    private static final Logger LOG = LoggerFactory.getLogger(TextExtractor.class);
    private final StringBuilder sb = new StringBuilder();

    @Override
    protected Object after(WtNode node, Object result) {
        // This method is called by go() after visitation has finished
        // The return value will be passed to go() which passes it to the caller
        return sb.toString();
    }

    public void visit(WtNode n) {
        LOG.debug("{}  {}", n.getNodeTypeName(), n.getNodeName());
        LOG.debug("{}", n.getAttributes().entrySet().stream()
                .map(e -> e.getKey()+"="+e.getValue())
                .collect(Collectors.joining("|")));
        iterate(n);
    }

    public void visit(WtText text) {
        sb.append(text.getContent());
    }

    public void visit(WtWhitespace w) {
        sb.append("");
    }

    public void visit(WtNewline n) {
        sb.append("\n");
    }
}
