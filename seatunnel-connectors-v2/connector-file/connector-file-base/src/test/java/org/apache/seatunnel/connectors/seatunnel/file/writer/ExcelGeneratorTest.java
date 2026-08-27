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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
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

        validateGeneratedFile(outputFile, 5, 0);
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

        assertTrue("Large file should exist", outputFile.exists());
        validateGeneratedFile(outputFile, 1048575, 0);
        validateGeneratedFile(outputFile, totalRows - 1048575, 1);
    }

    private void validateGeneratedFile(File file, int expectedDataRows, int sheetNo)
            throws IOException {
        AtomicInteger rowCount = new AtomicInteger(0);
        AtomicBoolean headerValid = new AtomicBoolean(false);
        EasyExcel.read(file)
                .registerReadListener(
                        new AnalysisEventListener<Map<Integer, String>>() {
                            @Override
                            public void invoke(Map<Integer, String> data, AnalysisContext context) {
                                rowCount.incrementAndGet();
                                if (rowCount.get() % 50000 == 0) {
                                    log.info("Processed " + rowCount.get() + " rows");
                                }
                            }

                            @Override
                            public void invokeHeadMap(
                                    Map<Integer, String> headMap, AnalysisContext context) {
                                headerValid.set(
                                        "id".equals(headMap.get(0))
                                                && "name".equals(headMap.get(1))
                                                && "age".equals(headMap.get(2))
                                                && "email".equals(headMap.get(3)));
                            }

                            @Override
                            public void doAfterAllAnalysed(AnalysisContext context) {
                                log.info("Validation completed. Total rows: " + rowCount.get());
                            }
                        })
                .sheet(sheetNo)
                .doRead();

        assertTrue("Headers should be valid", headerValid.get());
        assertEquals("Should have correct number of rows", expectedDataRows, rowCount.get());
    }

    @Test
    public void testGenerateExcelFileWithReorderedColumns() throws IOException {
        File outputDir = new File("target/test-output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        File outputFile = new File(outputDir, "reordered-columns-test.xlsx");

        // Use non-consecutive indices to test the fix
        List<Integer> reorderedColumnsIndexInRow = Arrays.asList(3, 1, 2, 0);
        ExcelGenerator excelGenerator =
                new ExcelGenerator(reorderedColumnsIndexInRow, rowType, fileSinkConfig);

        SeaTunnelRow[] testData = {
            new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"}),
            new SeaTunnelRow(new Object[] {2, "Bob", 30, "bob@test.com"})
        };

        for (SeaTunnelRow row : testData) {
            excelGenerator.writeData(row);
        }

        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            excelGenerator.flushAndCloseExcel(fos);
        }

        assertTrue("File should exist", outputFile.exists());
        assertTrue("File should not be empty", outputFile.length() > 0);

        // Validate the reordered columns
        validateReorderedColumnsFile(outputFile, 2, 0);
    }

    private void validateReorderedColumnsFile(File file, int expectedDataRows, int sheetNo)
            throws IOException {
        AtomicInteger rowCount = new AtomicInteger(0);
        AtomicBoolean headerValid = new AtomicBoolean(false);
        EasyExcel.read(file)
                .registerReadListener(
                        new AnalysisEventListener<Map<Integer, String>>() {
                            @Override
                            public void invoke(Map<Integer, String> data, AnalysisContext context) {
                                rowCount.incrementAndGet();
                                // For reordered columns [3, 1, 2, 0], the values should be in this
                                // order

                                // Check that first column is email (index 3 in original row)
                                // Second column is name (index 1 in original row)
                                // Third column is age (index 2 in original row)
                                // Fourth column is id (index 0 in original row)
                                String email = data.get(0);
                                String name = data.get(1);
                                String age = data.get(2);
                                String id = data.get(3);
                                assertTrue(
                                        "Email should be valid",
                                        email != null && email.endsWith("@test.com"));
                                assertTrue(
                                        "Name should be valid",
                                        name != null
                                                && (name.equals("Alice") || name.equals("Bob")));
                                assertTrue(
                                        "Age should be valid",
                                        age != null && (age.equals("25") || age.equals("30")));
                                assertTrue(
                                        "Id should be valid",
                                        id != null && (id.equals("1") || id.equals("2")));
                            }

                            @Override
                            public void invokeHeadMap(
                                    Map<Integer, String> headMap, AnalysisContext context) {
                                // For reordered columns [3, 1, 2, 0], headers should be in this
                                // order
                                headerValid.set(
                                        "email".equals(headMap.get(0))
                                                && "name".equals(headMap.get(1))
                                                && "age".equals(headMap.get(2))
                                                && "id".equals(headMap.get(3)));
                            }

                            @Override
                            public void doAfterAllAnalysed(AnalysisContext context) {
                                log.info("Validation completed. Total rows: " + rowCount.get());
                            }
                        })
                .sheet(sheetNo)
                .doRead();

        assertTrue("Headers should be valid", headerValid.get());
        assertEquals("Should have correct number of rows", expectedDataRows, rowCount.get());
    }
}
