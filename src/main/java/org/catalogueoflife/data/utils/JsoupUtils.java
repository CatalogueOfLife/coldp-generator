package org.catalogueoflife.data.utils;

import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

/**
 * Shared Jsoup DOM traversal helpers for HTML-scraping generators.
 *
 * All methods accept {@link Element} (Jsoup's {@code Document} extends {@code Element},
 * so document-scoped calls work without any overloads).
 */
public final class JsoupUtils {

  private JsoupUtils() {}

  // ── Section helpers ──────────────────────────────────────────────────────

  /**
   * Returns the concatenated text of all sibling elements that follow the first
   * {@code <h2>} whose trimmed text equals {@code heading} (case-insensitive),
   * stopping at the next heading element. Returns null if the heading is not found
   * or the section is empty.
   */
  public static String sectionText(Element parent, String heading) {
    Element h2 = findH2(parent, heading);
    if (h2 == null) return null;
    StringBuilder sb = new StringBuilder();
    for (Element sib : h2.nextElementSiblings()) {
      if (isHeading(sib)) break;
      String t = sib.text().trim();
      if (!t.isEmpty()) {
        if (sb.length() > 0) sb.append(" ");
        sb.append(t);
      }
    }
    return sb.isEmpty() ? null : sb.toString();
  }

  /**
   * Returns the text of the first non-empty element after the {@code <h2>} with
   * the given heading, stopping before any subsequent heading or element with a
   * colon-terminated {@code <strong>} label. Useful for single-paragraph sections.
   */
  public static String firstSectionParagraph(Element parent, String heading) {
    Element h2 = findH2(parent, heading);
    if (h2 == null) return null;
    for (Element sib : h2.nextElementSiblings()) {
      if (isHeading(sib)) break;
      boolean hasLabel = sib.select("strong").stream().anyMatch(s -> s.text().trim().endsWith(":"));
      if (hasLabel) break;
      String t = sib.text().trim();
      if (!t.isEmpty()) return t;
    }
    return null;
  }

  /**
   * Returns the first {@code <h2>} element whose trimmed text equals {@code heading}
   * (case-insensitive), or null if not found.
   */
  public static Element findH2(Element parent, String heading) {
    for (Element h2 : parent.select("h2")) {
      if (h2.text().trim().equalsIgnoreCase(heading)) return h2;
    }
    return null;
  }

  /**
   * Returns the first {@code <h2>} element whose trimmed text starts with {@code prefix},
   * or null if not found. Use this when the heading has variable trailing content.
   */
  public static Element findH2StartsWith(Element parent, String prefix) {
    for (Element h2 : parent.select("h2")) {
      if (h2.text().trim().startsWith(prefix)) return h2;
    }
    return null;
  }

  /** Returns true if {@code el} is a heading element (h1, h2 or h3). */
  public static boolean isHeading(Element el) {
    String tag = el.tagName();
    return tag.equals("h1") || tag.equals("h2") || tag.equals("h3");
  }

  // ── Label helpers (for PFNR-style "Label: value" patterns) ───────────────

  /**
   * Finds the first {@code <strong>} whose text starts with {@code labelPrefix}
   * and returns all text that follows it in the same parent element, stopping
   * before the next {@code <strong>} or {@code <h2>} sibling. Returns null if
   * the label is not found or the following text is empty.
   */
  public static String afterLabel(Element container, String labelPrefix) {
    Element strong = findStrong(container, labelPrefix);
    if (strong == null) return null;
    StringBuilder sb = new StringBuilder();
    boolean after = false;
    for (Node node : strong.parent().childNodes()) {
      if (!after) {
        if (node == strong) after = true;
        continue;
      }
      if (node instanceof TextNode tn) {
        sb.append(tn.text());
      } else if (node instanceof Element el) {
        if ("strong".equalsIgnoreCase(el.tagName()) || "h2".equalsIgnoreCase(el.tagName())) break;
        sb.append(el.text());
      }
    }
    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  /**
   * Returns the first {@code <strong>} element whose text starts with {@code labelPrefix},
   * searching within {@code container} and all its descendants.
   */
  public static Element findStrong(Element container, String labelPrefix) {
    for (Element s : container.select("strong")) {
      if (s.text().startsWith(labelPrefix)) return s;
    }
    return null;
  }

  /**
   * Returns the concatenated text of all elements between the parent of
   * {@code afterLabel}'s {@code <strong>} and the parent of {@code beforeLabel}'s
   * {@code <strong>}, skipping siblings that contain their own label {@code <strong>}.
   * Returns null if either label is missing or they have no intermediate siblings.
   */
  public static String textBetweenLabels(Element container, String afterLabel, String beforeLabel) {
    Element afterStrong  = findStrong(container, afterLabel);
    Element beforeStrong = findStrong(container, beforeLabel);
    if (afterStrong == null || beforeStrong == null) return null;
    Element afterEl  = afterStrong.parent();
    Element beforeEl = beforeStrong.parent();
    if (afterEl == null || beforeEl == null || afterEl == beforeEl) return null;
    if (afterEl.parent() == null || afterEl.parent() != beforeEl.parent()) return null;
    StringBuilder sb = new StringBuilder();
    boolean between = false;
    for (Element sib : afterEl.parent().children()) {
      if (sib == afterEl)  { between = true; continue; }
      if (sib == beforeEl) break;
      if (!between) continue;
      boolean hasLabel = sib.select("strong").stream().anyMatch(s -> s.text().trim().endsWith(":"));
      if (hasLabel) continue;
      String t = sib.text().trim();
      if (!t.isEmpty()) { if (sb.length() > 0) sb.append(" "); sb.append(t); }
    }
    return sb.isEmpty() ? null : sb.toString();
  }
}
