package org.catalogueoflife.data.utils;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

/**
 * Factory methods for univocity CSV/TSV parsers with sensible defaults.
 *
 * <p>Defaults applied to all parsers: null values preserved ({@code setNullValue(null)}),
 * auto line-separator detection enabled. Pass {@code maxCharsPerColumn} appropriate
 * for your data; typical values are 1024 (PBDB), 8192 (Clements), 24000 (MDD),
 * 65536 (GRIN), 1256000 (AntCat).
 */
public final class CsvUtils {

  private CsvUtils() {}

  /** CSV parser with the given max column size. */
  public static CsvParser newCsvParser(int maxCharsPerColumn) {
    var s = new CsvParserSettings();
    s.setMaxCharsPerColumn(maxCharsPerColumn);
    s.setNullValue(null);
    s.setLineSeparatorDetectionEnabled(true);
    return new CsvParser(s);
  }

  /** TSV parser with the given max column size. */
  public static TsvParser newTsvParser(int maxCharsPerColumn) {
    return newTsvParser(maxCharsPerColumn, false);
  }

  /**
   * TSV parser with the given max column size.
   *
   * @param extractHeader if true, the first row is treated as a header row and accessible
   *                      via {@link com.univocity.parsers.common.ParsingContext#headers()}
   */
  public static TsvParser newTsvParser(int maxCharsPerColumn, boolean extractHeader) {
    var s = new TsvParserSettings();
    s.setMaxCharsPerColumn(maxCharsPerColumn);
    s.setNullValue(null);
    s.setLineSeparatorDetectionEnabled(true);
    s.setHeaderExtractionEnabled(extractHeader);
    return new TsvParser(s);
  }
}
