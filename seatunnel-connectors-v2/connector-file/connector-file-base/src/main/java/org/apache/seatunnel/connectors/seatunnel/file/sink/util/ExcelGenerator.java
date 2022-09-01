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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ExcelGenerator {
    private Workbook wb;
    private CellStyle wholeNumberCellStyle;
    private CellStyle stringCellStyle;
    private int row = 0;
    private List<Integer> sinkColumnsIndexInRow;
    private SeaTunnelRowType seaTunnelRowType;

    public ExcelGenerator(List<Integer> sinkColumnsIndexInRow, SeaTunnelRowType seaTunnelRowType) {
        this.sinkColumnsIndexInRow = sinkColumnsIndexInRow;
        this.seaTunnelRowType = seaTunnelRowType;
        wb = new XSSFWorkbook();
        Sheet st = wb.createSheet("Sheet1");
        Row row = st.createRow(this.row);
        for (Integer i : sinkColumnsIndexInRow) {
            String fieldName = seaTunnelRowType.getFieldName(i);
            row.createCell(i).setCellValue(fieldName);
        }

        wholeNumberCellStyle = createStyle(wb, "General");
        stringCellStyle = createStyle(wb, "@");
        this.row += 1;
    }

    public void writeData(SeaTunnelRow seaTunnelRow) {
        Sheet st = wb.getSheet("Sheet1");
        Row excelRow = st.createRow(this.row);
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (Integer i : sinkColumnsIndexInRow) {
            Cell cell = excelRow.createCell(i);
            Object value = seaTunnelRow.getField(i);
            makeConverter(fieldTypes[i], value, cell);
        }
        this.row += 1;
    }

    public void flushAndCloseExcel(OutputStream output) throws IOException {
        wb.write(output);
        wb.close();
    }

    private void makeConverter(SeaTunnelDataType<?> type, Object value, Cell cell) {
        if (value == null) {
            cell.setBlank();
        } else if (BasicType.STRING_TYPE.equals(type)) {
            cell.setCellValue((String) value);
            cell.setCellStyle(stringCellStyle);
        } else if (BasicType.BOOLEAN_TYPE.equals(type)) {
            cell.setCellValue((Boolean) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (BasicType.BYTE_TYPE.equals(type)) {
            cell.setCellValue((byte) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (BasicType.SHORT_TYPE.equals(type)) {
            cell.setCellValue((short) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (BasicType.INT_TYPE.equals(type)) {
            cell.setCellValue((int) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (BasicType.LONG_TYPE.equals(type) || type.getSqlType().equals(SqlType.TIMESTAMP)) {
            cell.setCellValue((long) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (BasicType.FLOAT_TYPE.equals(type)) {
            cell.setCellValue((float) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (BasicType.DOUBLE_TYPE.equals(type)) {
            cell.setCellValue((double) value);
            cell.setCellStyle(wholeNumberCellStyle);
        } else if (type.getSqlType().equals(SqlType.BYTES) || type.getSqlType().equals(SqlType.ARRAY)) {
            List<String> arrayData = new ArrayList<>();
            for (int i = 0; i < Array.getLength(value); i++) {
                arrayData.add(String.valueOf(Array.get(value, i)));
            }
            cell.setCellValue(arrayData.toString());
            cell.setCellStyle(stringCellStyle);
        } else if (type.getSqlType().equals(SqlType.MAP)) {
            cell.setCellValue(JsonUtils.toJsonString(value));
            cell.setCellStyle(stringCellStyle);
        } else if (type.getSqlType().equals(SqlType.DATE) || type.getSqlType().equals(SqlType.TIME)) {
            cell.setCellValue((String) value);
            cell.setCellStyle(stringCellStyle);
        } else {
            String errorMsg = String.format("[%s] type not support ", type.getSqlType());
            throw new RuntimeException(errorMsg);
        }
    }

    private CellStyle createStyle(Workbook wb, String format) {
        CreationHelper creationHelper = wb.getCreationHelper();
        CellStyle cellStyle = wb.createCellStyle();
        cellStyle.setDataFormat(creationHelper.createDataFormat().getFormat(format));
        return cellStyle;
    }
}
