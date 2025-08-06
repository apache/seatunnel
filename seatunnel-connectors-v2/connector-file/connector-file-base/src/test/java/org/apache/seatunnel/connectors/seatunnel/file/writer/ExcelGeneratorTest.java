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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.ExcelGenerator;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExcelGeneratorTest {

    private FileSinkConfig fileSinkConfig;
    private SeaTunnelRowType rowType;
    private List<Integer> sinkColumnsIndexInRow;

    @BeforeEach
    public void setUp() {
        fileSinkConfig = mock(FileSinkConfig.class);
        when(fileSinkConfig.getMaxRowsInMemory()).thenReturn(100);
        when(fileSinkConfig.getSheetName()).thenReturn("TestSheet");
        when(fileSinkConfig.getDateFormat()).thenReturn(DateUtils.Formatter.YYYY_MM_DD);
        when(fileSinkConfig.getDatetimeFormat())
                .thenReturn(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        when(fileSinkConfig.getTimeFormat()).thenReturn(TimeUtils.Formatter.HH_MM_SS);
        when(fileSinkConfig.getSheetMaxRows()).thenReturn(1048576);
        rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age", "email"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE
                        });

        sinkColumnsIndexInRow = Arrays.asList(0, 1, 2, 3);
    }

    @Test
    public void testGenerateBasicExcelFile() throws IOException {
        File outputDir = new File("target/test-output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        File outputFile = new File(outputDir, "basic-test.xlsx");

        ExcelGenerator excelGenerator =
                new ExcelGenerator(sinkColumnsIndexInRow, rowType, fileSinkConfig);

        SeaTunnelRow[] testData = {
            new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"}),
            new SeaTunnelRow(new Object[] {2, "Bob", 30, "bob@test.com"}),
            new SeaTunnelRow(new Object[] {3, "Charlie", 35, "charlie@test.com"}),
            new SeaTunnelRow(new Object[] {4, "Diana", 28, "diana@test.com"}),
            new SeaTunnelRow(new Object[] {5, null, 22, null})
        };

        for (SeaTunnelRow row : testData) {
            excelGenerator.writeData(row);
        }

        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            excelGenerator.flushAndCloseExcel(fos);
        }

        assertTrue("File should exist", outputFile.exists());
        assertTrue("File should not be empty", outputFile.length() > 0);

        validateGeneratedFile(outputFile, 5);
    }

    @Test
    public void testGenerateLargeDataFile() throws IOException {
        File outputDir = new File("target/test-output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        File outputFile = new File(outputDir, "large-test.xlsx");

        ExcelGenerator excelGenerator =
                new ExcelGenerator(sinkColumnsIndexInRow, rowType, fileSinkConfig);

        int totalRows = 1200000;
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= totalRows; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, "User" + i, 20 + (i % 50), "user" + i + "@example.com"
                            });
            excelGenerator.writeData(row);
        }

        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            excelGenerator.flushAndCloseExcel(fos);
        }

        long endTime = System.currentTimeMillis();

        assertTrue("Large file should exist", outputFile.exists());
        //        validateGeneratedFile(outputFile, 1048575);
    }

    private void validateGeneratedFile(File file, int expectedDataRows) throws IOException {
        try (FileInputStream fis = new FileInputStream(file);
                Workbook workbook = new XSSFWorkbook(fis)) {

            assertTrue("Should have at least 1 sheet", workbook.getNumberOfSheets() >= 1);

            Sheet sheet = workbook.getSheetAt(0);
            assertEquals(
                    "Should have correct number of rows", expectedDataRows, sheet.getLastRowNum());

            if (sheet.getLastRowNum() >= 0) {
                assertEquals(
                        "Header should have correct column",
                        "id",
                        sheet.getRow(0).getCell(0).getStringCellValue());
                assertEquals(
                        "Header should have correct column",
                        "name",
                        sheet.getRow(0).getCell(1).getStringCellValue());
                assertEquals(
                        "Header should have correct column",
                        "email",
                        sheet.getRow(0).getCell(3).getStringCellValue());
            }
        }
    }
}
