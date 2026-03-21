package org.catalogueoflife.data.wikispecies;

import org.sweble.wikitext.parser.nodes.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static utility methods for extracting plain text and structured data
 * from Sweble AST nodes.
 */
public class WtUtils {
  private static final Pattern YEAR_PAT = Pattern.compile("\\b(1[0-9]{3}|20[0-2][0-9])\\b");

  /**
   * Extract all plain text from a node.
   * - WtItalics / WtBold: traverse children
   * - WtInternalLink: use display title if present, else link target (without disambiguation)
   * - WtTemplate: skip (do not expand)
   * - WtText / WtWhitespace / WtNewline: literal text
   */
  public static String nodeText(WtNode node) {
    StringBuilder sb = new StringBuilder();
    appendText(node, sb);
    return sb.toString().trim();
  }

  static void appendText(WtNode node, StringBuilder sb) {
    if (node instanceof WtText) {
      sb.append(((WtText) node).getContent());
    } else if (node instanceof WtWhitespace) {
      sb.append(" ");
    } else if (node instanceof WtNewline) {
      sb.append("\n");
    } else if (node instanceof WtItalics || node instanceof WtBold) {
      for (WtNode child : node) appendText(child, sb);
    } else if (node instanceof WtInternalLink) {
      WtInternalLink link = (WtInternalLink) node;
      if (link.hasTitle() && !link.getTitle().isEmpty()) {
        for (WtNode child : link.getTitle()) appendText(child, sb);
      } else {
        String target = link.getTarget().getAsString();
        // strip disambiguation suffix e.g. "Oenanthe (Muscicapidae)" → "Oenanthe"
        int paren = target.indexOf(" (");
        sb.append(paren >= 0 ? target.substring(0, paren) : target);
      }
    } else if (node instanceof WtTemplate || node instanceof WtTagExtension
               || node instanceof WtXmlElement) {
      // skip template content in plain text extraction
    } else {
      for (WtNode child : node) appendText(child, sb);
    }
  }

  /** Get template name as a trimmed string, or empty string if unresolved. */
  public static String templateName(WtTemplate t) {
    try {
      WtName n = t.getName();
      return n.isResolved() ? n.getAsString().trim() : "";
    } catch (Exception e) {
      return "";
    }
  }

  /**
   * Get the n-th positional argument (0-based) of a template as trimmed plain text.
   * Returns null if the argument is not present.
   */
  public static String templateArg(WtTemplate t, int index) {
    int pos = 0;
    for (WtNode argNode : t.getArgs()) {
      if (!(argNode instanceof WtTemplateArgument)) continue;
      WtTemplateArgument arg = (WtTemplateArgument) argNode;
      if (!arg.hasName()) {
        if (pos == index) return nodeText(arg.getValue()).trim();
        pos++;
      }
    }
    return null;
  }

  /**
   * Get a named argument of a template as trimmed plain text.
   * Returns null if not present.
   */
  public static String templateNamedArg(WtTemplate t, String name) {
    for (WtNode argNode : t.getArgs()) {
      if (!(argNode instanceof WtTemplateArgument)) continue;
      WtTemplateArgument arg = (WtTemplateArgument) argNode;
      if (arg.hasName() && arg.getName().getAsString().trim().equals(name)) {
        return nodeText(arg.getValue()).trim();
      }
    }
    return null;
  }

  /**
   * Identify the section type from its heading content.
   * "{{int:Name}}" → "name", "{{int:Taxonavigation}}" → "taxonavigation", etc.
   * Falls back to lowercased plain text.
   */
  public static String sectionKey(WtHeading heading) {
    for (WtNode node : heading) {
      if (node instanceof WtTemplate) {
        String name = templateName((WtTemplate) node).toLowerCase();
        if (name.startsWith("int:")) return name.substring(4).trim();
        return name;
      }
    }
    return nodeText(heading).toLowerCase();
  }

  /** Find the first 4-digit year in the given text, or null. */
  public static String extractYear(String text) {
    Matcher m = YEAR_PAT.matcher(text);
    return m.find() ? m.group(1) : null;
  }

  /**
   * Find the name of the first WtTemplate node in the given node list.
   * Returns null if none found.
   */
  public static String firstTemplateName(WtNodeList nodes) {
    for (WtNode node : nodes) {
      if (node instanceof WtTemplate) {
        String name = templateName((WtTemplate) node);
        if (!name.isEmpty()) return name;
      }
      // also look inside paragraphs / bodies one level deep
      if (!(node instanceof WtSection)) {
        for (WtNode child : node) {
          if (child instanceof WtTemplate) {
            String name = templateName((WtTemplate) child);
            if (!name.isEmpty()) return name;
          }
        }
      }
    }
    return null;
  }
}
