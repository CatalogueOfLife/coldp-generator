package org.catalogueoflife.data.utils;

import com.google.common.base.Strings;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.poi.ss.usermodel.Cell.CELL_TYPE_FORMULA;

/**
 *
 */
public class ExcelUtils {
  private final static Pattern HYPERLINK = Pattern.compile("HYPERLINK *\\( *\"(.+)\" *,", Pattern.CASE_INSENSITIVE);

  public static String col(Row row, int column) {
    Cell c = row.getCell(column);
    if (c == null) {
      return null;
    }
    String val;
    switch (c.getCellType()) {
      case Cell.CELL_TYPE_NUMERIC:
        // dates are also stored as numeric types, check formatting
        if (HSSFDateUtil.isCellDateFormatted(c)) {
          Date d = c.getDateCellValue();
          val = DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(d);
        } else {
          val = String.valueOf((int) c.getNumericCellValue());
        }
        break;
      case Cell.CELL_TYPE_STRING:
        val = c.getStringCellValue();
        break;
      default:
        val = c.toString();
    }

    return Strings.nullToEmpty(val.trim()).replace("Unassigned", "");
  }

  /**
   * @return link URL or null if none exists
   */
  public static String link(Row row, int column) {
    Cell c = row.getCell(column);
    if (c != null) {
      if (c.getHyperlink() != null) {
        return c.getHyperlink().getAddress();

      } else if (c.getCellType() == CELL_TYPE_FORMULA && c.getCellFormula() != null) {
        Matcher m = HYPERLINK.matcher(c.getCellFormula());
        if (m.find()) {
          return m.group(1);
        }
      }
    }
    return null;
  }
}
