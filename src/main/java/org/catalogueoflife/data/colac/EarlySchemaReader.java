package org.catalogueoflife.data.colac;

import java.sql.Connection;
import java.util.List;
import static org.catalogueoflife.data.colac.ColacMappings.*;

/** Reader for the 2000/2002–2004 Species 2000 CD-ROM schema (flat HIERARCHY + atomized SCINAMES). */
class EarlySchemaReader extends SchemaReader {
  static final String PREFIX = "h|";   // synthetic higher-taxon / genus id prefix

  EarlySchemaReader(Generator g, Connection conn) { super(g, conn); }

  /** Deterministic id for a synthetic node from its ancestor-name path (kingdom..this node). */
  static String pathId(List<String> segments) {
    if (segments == null || segments.isEmpty()) return null;
    return PREFIX + String.join("|", segments);
  }

  /** Parent id of a synthetic path id, or null when it is a top-level (kingdom) node. */
  static String parentPathId(String id) {
    if (id == null) return null;
    int cut = id.lastIndexOf('|');
    String parent = id.substring(0, cut);          // drop last segment
    return parent.equals(PREFIX.substring(0, PREFIX.length() - 1)) ? null // "h" only -> root
         : (parent.endsWith("|") ? null : parent);
  }

  @Override void read() throws Exception { /* implemented in later tasks */ }
}
