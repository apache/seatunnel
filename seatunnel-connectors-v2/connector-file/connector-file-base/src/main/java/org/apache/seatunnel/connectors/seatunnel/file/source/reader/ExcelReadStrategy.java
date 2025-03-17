/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.ExcelEngine;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.excel.ExcelCellUtils;
import org.apache.seatunnel.connectors.seatunnel.file.excel.ExcelReaderListener;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.read.builder.ExcelReaderBuilder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

@Getter
@Slf4j
public class ExcelReadStrategy extends AbstractReadStrategy {

    private String dateFormatterPattern = DateUtils.Formatter.YYYY_MM_DD.getValue();

    private String dateTimeFormatterPattern =
            DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS.getValue();

    private String timeFormatterPattern = TimeUtils.Formatter.HH_MM_SS.getValue();

    private int[] indexes;

    private int cellCount;

    @SneakyThrows
    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        resolveArchiveCompressedInputStream(path, tableId, output, partitionsMap, FileFormat.EXCEL);
    }

    @Override
    protected void readProcess(
            String path,
            String tableId,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName)
            throws IOException {

        if (skipHeaderNumber > Integer.MAX_VALUE || skipHeaderNumber < Integer.MIN_VALUE) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
        }

        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
            dateFormatterPattern =
                    pluginConfig.getString(BaseSourceConfigOptions.DATE_FORMAT.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
            dateTimeFormatterPattern =
                    pluginConfig.getString(BaseSourceConfigOptions.DATETIME_FORMAT.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
            timeFormatterPattern =
                    pluginConfig.getString(BaseSourceConfigOptions.TIME_FORMAT.key());
        }

        ExcelCellUtils excelCellUtils =
                new ExcelCellUtils(
                        pluginConfig,
                        dateFormatterPattern,
                        dateTimeFormatterPattern,
                        timeFormatterPattern);

        if (pluginConfig.hasPath(BaseSourceConfigOptions.EXCEL_ENGINE.key())
                && pluginConfig
                        .getString(BaseSourceConfigOptions.EXCEL_ENGINE.key())
                        .equals(ExcelEngine.EASY_EXCEL.getExcelEngineName())) {
            log.info("Parsing Excel with EasyExcel");

            ExcelReaderBuilder read =
                    EasyExcel.read(
                            inputStream,
                            new ExcelReaderListener(
                                    tableId, output, excelCellUtils, seaTunnelRowType));
            if (pluginConfig.hasPath(BaseSourceConfigOptions.SHEET_NAME.key())) {
                read.sheet(pluginConfig.getString(BaseSourceConfigOptions.SHEET_NAME.key()))
                        .headRowNumber((int) skipHeaderNumber)
                        .doReadSync();
            } else {
                read.sheet(0).headRowNumber((int) skipHeaderNumber).doReadSync();
            }
        } else {
            log.info("Parsing Excel with POI");

            Workbook workbook;
            FormulaEvaluator formulaEvaluator;
            if (currentFileName.endsWith(".xls")) {
                workbook = new HSSFWorkbook(inputStream);
                formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
            } else if (currentFileName.endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(inputStream);
                formulaEvaluator = new XSSFFormulaEvaluator((XSSFWorkbook) workbook);
            } else {
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Only support read excel file");
            }
            DataFormatter formatter = new DataFormatter();
            Sheet sheet =
                    pluginConfig.hasPath(BaseSourceConfigOptions.SHEET_NAME.key())
                            ? workbook.getSheet(
                                    pluginConfig.getString(
                                            BaseSourceConfigOptions.SHEET_NAME.key()))
                            : workbook.getSheetAt(0);
            cellCount = seaTunnelRowType.getTotalFields();
            cellCount = partitionsMap.isEmpty() ? cellCount : cellCount + partitionsMap.size();
            SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
            int rowCount = sheet.getPhysicalNumberOfRows();
            if (skipHeaderNumber > rowCount) {
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
            }
            IntStream.range((int) skipHeaderNumber, rowCount)
                    .mapToObj(sheet::getRow)
                    .filter(Objects::nonNull)
                    .forEach(
                            rowData -> {
                                int[] cellIndexes =
                                        indexes == null
                                                ? IntStream.range(0, cellCount).toArray()
                                                : indexes;
                                int z = 0;
                                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(cellCount);
                                for (int j : cellIndexes) {
                                    Cell cell = rowData.getCell(j);
                                    seaTunnelRow.setField(
                                            z++,
                                            cell == null
                                                    ? null
                                                    : excelCellUtils.convert(
                                                            getCellValue(
                                                                    cell.getCellType(),
                                                                    cell,
                                                                    formulaEvaluator,
                                                                    formatter),
                                                            fieldTypes[z - 1],
                                                            null));
                                }
                                if (isMergePartition) {
                                    int index = seaTunnelRowType.getTotalFields();
                                    for (String value : partitionsMap.values()) {
                                        seaTunnelRow.setField(index++, value);
                                    }
                                }
                                seaTunnelRow.setTableId(tableId);
                                output.collect(seaTunnelRow);
                            });
        }
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        if (isNullOrEmpty(rowType.getFieldNames()) || isNullOrEmpty(rowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Schema information is not set or incorrect Schema settings");
        }
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), rowType);
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = rowType.indexOf(readColumns.get(i));
                fields[i] = rowType.getFieldName(indexes[i]);
                types[i] = rowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = rowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }

    private Object getCellValue(
            CellType cellType,
            Cell cell,
            FormulaEvaluator formulaEvaluator,
            DataFormatter formatter) {
        switch (cellType) {
            case STRING:
                return cell.getStringCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getLocalDateTimeCellValue();
                }
                return formatter.formatCellValue(cell);
            case BLANK:
                return "";
            case ERROR:
                break;
            case FORMULA:
                CellValue evaluate = formulaEvaluator.evaluate(cell);
                if (evaluate.getCellType().equals(CellType.NUMERIC)) {
                    return NumberToTextConverter.toText(evaluate.getNumberValue());
                } else {
                    return evaluate.formatAsString();
                }
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format("[%s] type not support ", cellType));
        }
        return null;
    }

    private <T> boolean isNullOrEmpty(T[] arr) {
        return arr == null || arr.length == 0;
    }
}
