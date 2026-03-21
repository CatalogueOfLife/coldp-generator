package org.catalogueoflife.data.wikispecies;

import org.sweble.wikitext.parser.nodes.*;

import static org.catalogueoflife.data.wikispecies.WtUtils.*;

/**
 * Extracts scientific name, authorship, and year from the first line of a
 * Wikispecies Name section body.
 *
 * Handles formats:
 *   ''Oenanthe leucura'' ([[Johann Friedrich Gmelin|Gmelin]], 1789)
 *   ''Abies alba'' {{a|Philip Miller|Mill.}}, Gard. Dict. (1768).
 *   Muscicapidae {{a|John Fleming|Fleming}}, 1822
 */
public class NameExtractor {

  public record ParsedName(String scientificName, String authorship, String year) {}

  public static ParsedName extract(WtBody body) {
    String sciName = null;
    StringBuilder authorBuf = new StringBuilder();
    String year = null;

    // Sweble wraps the first content line in a WtParagraph when a sub-section follows.
    // Unwrap it so we can walk the inline nodes directly.
    WtNode container = body;
    for (WtNode n : body) {
      if (n instanceof WtParagraph) { container = n; break; }
      if (n instanceof WtItalics || n instanceof WtTemplate || n instanceof WtInternalLink
          || (n instanceof WtText && !((WtText) n).getContent().isBlank())) break;
    }

    // Walk first-line nodes only (stop at first newline)
    for (WtNode node : container) {
      if (node instanceof WtNewline) break;

      if (node instanceof WtItalics) {
        if (sciName == null) {
          sciName = nodeText(node).trim();
        }
        // italic content after first name is part of synonymy — skip

      } else if (node instanceof WtTemplate) {
        WtTemplate t = (WtTemplate) node;
        String tname = templateName(t).toLowerCase();
        if (tname.equals("a")) {
          // {{a|Full Name|Display}} or {{a|Display}}
          String display = templateArg(t, 1);
          String auth = display != null ? display : templateArg(t, 0);
          if (auth != null) appendAuthor(authorBuf, auth);
        } else if (tname.equals("aut")) {
          String auth = templateArg(t, 0);
          if (auth != null) appendAuthor(authorBuf, auth);
        }
        // All other templates (BHL, etc.) are skipped

      } else if (node instanceof WtInternalLink) {
        // [[Full Name|Display]] — used for author links when no {{a}} template
        // Only capture as author if the name has already been found (to avoid capturing genus links)
        if (sciName != null) {
          WtInternalLink link = (WtInternalLink) node;
          String text;
          if (link.hasTitle() && !link.getTitle().isEmpty()) {
            text = nodeText(link.getTitle()).trim();
          } else {
            String target = link.getTarget().getAsString();
            int paren = target.indexOf(" (");
            text = paren >= 0 ? target.substring(0, paren) : target;
          }
          // Skip status/nomenclatural terms
          if (!text.isEmpty() && !isNomTerm(text)) {
            appendAuthor(authorBuf, text);
          }
        }

      } else if (node instanceof WtText) {
        String text = ((WtText) node).getContent();
        if (year == null) year = extractYear(text);
        // Don't add to authorBuf — year already extracted; plain text is context
      }
    }

    String auth = authorBuf.toString().trim();
    // Normalise: strip leading/trailing punctuation
    auth = auth.replaceAll("^[,\\s]+", "").replaceAll("[,\\s]+$", "").trim();
    return new ParsedName(
        sciName,
        auth.isEmpty() ? null : auth,
        year);
  }

  /** Extract italic text from a list item — used for synonym names. */
  public static String extractItalics(WtNode node) {
    for (WtNode child : node) {
      if (child instanceof WtItalics) return nodeText(child).trim();
      String nested = extractItalics(child);
      if (nested != null) return nested;
    }
    return null;
  }

  /** Extract first author ({{a}} or {{aut}} template) from a node tree. */
  public static String extractAuthor(WtNode node) {
    for (WtNode child : node) {
      if (child instanceof WtTemplate) {
        WtTemplate t = (WtTemplate) child;
        String tname = templateName(t).toLowerCase();
        if (tname.equals("a")) {
          String display = templateArg(t, 1);
          return display != null ? display : templateArg(t, 0);
        } else if (tname.equals("aut")) {
          return templateArg(t, 0);
        }
      }
      if (child instanceof WtInternalLink) {
        WtInternalLink link = (WtInternalLink) child;
        if (link.hasTitle() && !link.getTitle().isEmpty()) {
          String text = nodeText(link.getTitle()).trim();
          if (!text.isEmpty() && !isNomTerm(text)) return text;
        }
      }
      String fromChild = extractAuthor(child);
      if (fromChild != null) return fromChild;
    }
    return null;
  }

  private static void appendAuthor(StringBuilder buf, String auth) {
    if (buf.length() > 0) buf.append(" & ");
    buf.append(auth);
  }

  private static boolean isNomTerm(String text) {
    String lower = text.toLowerCase();
    return lower.contains("protonym") || lower.contains("nomen") || lower.contains("pro syn")
        || lower.equals("bm") || lower.equals("k") || lower.equals("p");
  }
}
