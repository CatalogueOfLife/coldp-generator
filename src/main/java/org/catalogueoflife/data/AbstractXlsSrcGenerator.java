package org.catalogueoflife.data;

import life.catalogue.parser.RankParser;
import life.catalogue.parser.SafeParser;
import life.catalogue.parser.UnparsableException;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.formula.FormulaParseException;
import org.apache.poi.ss.usermodel.*;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.Rank;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;

public abstract class AbstractXlsSrcGenerator extends AbstractColdpGenerator {
  private static final String srcFN = "data.xls";
  protected Workbook wb;
  private DataFormatter formatter;
  private FormulaEvaluator evaluator;

  public AbstractXlsSrcGenerator(GeneratorConfig cfg, boolean addMetadata, URI downloadUri) throws IOException {
    super(cfg, addMetadata, Map.of(srcFN, downloadUri));
  }

  @Override
  protected void prepare() throws IOException {
    wb = WorkbookFactory.create(sourceFile(srcFN));
    formatter = new DataFormatter(Locale.US);
    evaluator = wb.getCreationHelper().createFormulaEvaluator();;
  }

  protected Integer colInt(Row row, int column) {
    var val = col(row, column);
    if (!StringUtils.isBlank(val)) {
      return Integer.parseInt(val.trim());
    }
    return null;
  }

  protected String col(Row row, int column) {
    try {
      Cell cell = row.getCell(column);
      return formatter.formatCellValue(cell, evaluator);
    } catch (FormulaParseException e) {
      LOG.warn("Error evaluating excel formula in sheet {}, row {} and column {}: {}", row.getSheet().getSheetName(), row.getRowNum(), column, e.getMessage());
    }
    return null;
  }

  protected Rank colRank(Row row, int column) {
    String val = col(row, column);
    if (val != null) {
      try {
        return RankParser.PARSER.parse(NomCode.VIRUS, val).orElse(null);
      } catch (Exception e) {
        LOG.warn("Invalid rank {} in row {} and column {}", val, row.getRowNum(), column);
      }
    }
    return null;
  }

  protected String concat(Row row, int ... columns) {
    StringBuilder sb = new StringBuilder();
    for (int col : columns) {
      String val = col(row, col);
      if (val != null) {
        if (sb.length() > 0) {
          sb.append("; ");
        }
        sb.append(val);
      }
    }
    return sb.length() > 0 ? sb.toString() : null;
  }

  protected String link(Row row, int column) {
    try {
      Cell cell = row.getCell(column);
      var link = cell.getHyperlink();
      if (link != null) {
        return link.getAddress();
      }
    } catch (FormulaParseException e) {
      LOG.warn("Error evaluating excel formula in sheet {}, row {} and column {}: {}", row.getSheet().getSheetName(), row.getRowNum(), column, e.getMessage());
    }
    return null;
  }
}
