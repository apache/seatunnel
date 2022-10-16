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

package org.apache.seatunnel.connectors.seatunnel.file.sink.util;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public class ExcelGenerator {
    private List<Integer> sinkColumnsIndexInRow;
    private SeaTunnelRowType seaTunnelRowType;
    private final DateUtils.Formatter dateFormat;
    private final DateTimeUtils.Formatter dateTimeFormat;
    private final TimeUtils.Formatter timeFormat;
    private final String fieldDelimiter;
    private Workbook wb;
    private CellStyle wholeNumberCellStyle;
    private CellStyle stringCellStyle;
    private CellStyle dateCellStyle;
    private CellStyle dateTimeCellStyle;
    private CellStyle timeCellStyle;
    private Sheet st;
    private int row = 0;
    public ExcelGenerator(List<Integer> sinkColumnsIndexInRow, SeaTunnelRowType seaTunnelRowType, TextFileSinkConfig textFileSinkConfig) {
        this.sinkColumnsIndexInRow = sinkColumnsIndexInRow;
        this.seaTunnelRowType = seaTunnelRowType;
        if (textFileSinkConfig.getMaxRowsInMemory() > 0) {
            wb = new SXSSFWorkbook(textFileSinkConfig.getMaxRowsInMemory());
        } else {
            wb = new XSSFWorkbook();
        }
        this.st = wb.createSheet("Sheet1");
        Row row = st.createRow(this.row);
        for (Integer i : sinkColumnsIndexInRow) {
            String fieldName = seaTunnelRowType.getFieldName(i);
            row.createCell(i).setCellValue(fieldName);
        }
        this.dateFormat = textFileSinkConfig.getDateFormat();
        this.dateTimeFormat = textFileSinkConfig.getDatetimeFormat();
        this.timeFormat = textFileSinkConfig.getTimeFormat();
        this.fieldDelimiter = textFileSinkConfig.getFieldDelimiter();
        wholeNumberCellStyle = createStyle(wb, "General");
        stringCellStyle = createStyle(wb, "@");
        dateCellStyle = createStyle(wb, dateFormat.getValue());
        dateTimeCellStyle = createStyle(wb, dateTimeFormat.getValue());
        timeCellStyle = createStyle(wb, timeFormat.getValue());

        this.row += 1;
    }

    public void writeData(SeaTunnelRow seaTunnelRow) {
        Row excelRow = this.st.createRow(this.row);
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (Integer i : sinkColumnsIndexInRow) {
            Cell cell = excelRow.createCell(i);
            Object value = seaTunnelRow.getField(i);
            setCellValue(fieldTypes[i], value, cell);
        }
        this.row += 1;
    }

    public void flushAndCloseExcel(OutputStream output) throws IOException {
        wb.write(output);
        wb.close();
    }

    private void setCellValue(SeaTunnelDataType<?> type, Object value, Cell cell) {
        if (value == null) {
            cell.setBlank();
        } else {
            switch (type.getSqlType()) {
                case STRING:
                    cell.setCellValue((String) value);
                    cell.setCellStyle(stringCellStyle);
                    break;
                case BOOLEAN:
                    cell.setCellValue((Boolean) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case SMALLINT:
                    cell.setCellValue((short) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case TINYINT:
                    cell.setCellValue((byte) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case INT:
                    cell.setCellValue((int) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case BIGINT:
                    cell.setCellValue((long) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case FLOAT:
                    cell.setCellValue((float) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case DOUBLE:
                    cell.setCellValue((double) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case DECIMAL:
                    cell.setCellValue(Double.parseDouble(value.toString()));
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case BYTES:
                    List<String> arrayData = new ArrayList<>();
                    for (int i = 0; i < Array.getLength(value); i++) {
                        arrayData.add(String.valueOf(Array.get(value, i)));
                    }
                    cell.setCellValue(arrayData.toString());
                    cell.setCellStyle(stringCellStyle);
                    break;
                case MAP:
                case ARRAY:
                    cell.setCellValue(JsonUtils.toJsonString(value));
                    cell.setCellStyle(stringCellStyle);
                    break;
                case ROW:
                    Object[] fields = ((SeaTunnelRow) value).getFields();
                    String[] strings = new String[fields.length];
                    for (int i = 0; i < fields.length; i++) {
                        strings[i] = convert(fields[i], ((SeaTunnelRowType) type).getFieldType(i));
                    }
                    cell.setCellValue(String.join(fieldDelimiter, strings));
                    cell.setCellStyle(stringCellStyle);
                    break;
                case DATE:
                    cell.setCellValue((LocalDate) value);
                    cell.setCellStyle(dateCellStyle);
                    break;
                case TIMESTAMP:
                case TIME:
                    setTimestampColumn(value, cell);
                    break;
                default:
                    String errorMsg = String.format("[%s] type not support ", type.getSqlType());
                    throw new RuntimeException(errorMsg);

            }
        }
    }

    private String convert(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(field);
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return field.toString();
            case DATE:
                return DateUtils.toString((LocalDate) field, dateFormat);
            case TIME:
                return TimeUtils.toString((LocalTime) field, timeFormat);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) field, dateTimeFormat);
            case NULL:
                return "";
            case BYTES:
                return new String((byte[]) field);
            case ROW:
                Object[] fields = ((SeaTunnelRow) field).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] = convert(fields[i], ((SeaTunnelRowType) fieldType).getFieldType(i));
                }
                return String.join(fieldDelimiter, strings);
            default:
                throw new UnsupportedOperationException("SeaTunnel format text not supported for parsing this type");
        }
    }

    private void setTimestampColumn(Object value, Cell cell) {
        if (value instanceof Timestamp) {
            cell.setCellValue((Timestamp) value);
            cell.setCellStyle(dateTimeCellStyle);
        } else if (value instanceof LocalDate) {
            cell.setCellValue((LocalDate) value);
            cell.setCellStyle(dateCellStyle);
        } else if (value instanceof LocalDateTime) {
            cell.setCellValue(Timestamp.valueOf((LocalDateTime) value));
            cell.setCellStyle(dateTimeCellStyle);
        } else if (value instanceof LocalTime) {
            cell.setCellValue(Timestamp.valueOf(((LocalTime) value).atDate(LocalDate.ofEpochDay(0))));
            cell.setCellStyle(timeCellStyle);
        } else {
            throw new RuntimeException("Time series type expected for field");
        }

    }

    private CellStyle createStyle(Workbook wb, String format) {
        CreationHelper creationHelper = wb.getCreationHelper();
        CellStyle cellStyle = wb.createCellStyle();
        cellStyle.setDataFormat(creationHelper.createDataFormat().getFormat(format));
        return cellStyle;
    }
}
