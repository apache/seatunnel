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

package org.apache.seatunnel.connectors.seatunnel.file.local;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.LocalFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.LocalFileSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;
import org.apache.seatunnel.connectors.seatunnel.source.SourceFlowTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.CommonOptions.EVENT_TIME;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason =
                "Hadoop has windows problem, please refer https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems")
public class LocalFileTest {

    CatalogTable catalogTable =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "test", BasicType.STRING_TYPE, 1L, true, null, ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    void testSingleFileMode() throws IOException {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "only_one_file");
                        put("file_format_type", "text");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                    }
                };
        options.put("single_file_mode", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})));
        Assertions.assertEquals(
                2,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/only_one_file.txt"));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SinkFlowTestUtils.runBatchWithCheckpointEnabled(
                                        catalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Arrays.asList(
                                                new SeaTunnelRow(new Object[] {"test"}),
                                                new SeaTunnelRow(new Object[] {"test"}))));
        Assertions.assertEquals(
                "Single file mode is not supported when checkpoint is enabled or in streaming mode.",
                exception.getMessage());

        IllegalArgumentException exception2 =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SinkFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                                        catalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Arrays.asList(
                                                new SeaTunnelRow(new Object[] {"test"}),
                                                new SeaTunnelRow(new Object[] {"test"})),
                                        2));
        Assertions.assertEquals(
                "Single file mode is not supported when file_name_expression not contains ${transactionId} but has parallel subtasks.",
                exception2.getMessage());

        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        options.put("file_name_expression", "${transactionId}_2");
        SinkFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})),
                2);
        Assertions.assertFalse(
                FileUtils.isFileExist("/tmp/seatunnel/LocalFileTest/only_one_file.txt"));
        Assertions.assertEquals(2, FileUtils.listFile("/tmp/seatunnel/LocalFileTest").size());

        options.put("single_file_mode", false);
        options.put("file_name_expression", "only_one_file");
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})));
        Assertions.assertFalse(
                FileUtils.isFileExist("/tmp/seatunnel/LocalFileTest/only_one_file.txt"));
        Assertions.assertEquals(
                1,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/only_one_file_0.txt"));
        Assertions.assertEquals(
                1,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/only_one_file_1.txt"));
    }

    @Test
    void testCreateEmptyFileWhenNoData() throws IOException {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "empty_file");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                        put("create_empty_file_when_no_data", true);
                    }
                };
        options.put("file_format_type", "text");
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.emptyList());
        Assertions.assertEquals(
                0,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/empty_file_0.txt"));

        options.put("file_format_type", "csv");
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.emptyList());
        Assertions.assertEquals(
                0,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/empty_file_0.csv"));

        options.put("enable_header_write", true);
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.emptyList());
        Assertions.assertEquals(
                "test\n",
                FileUtils.readFileToStr(
                        Paths.get("/tmp/seatunnel/LocalFileTest/empty_file_0.csv")));

        options.put("file_format_type", "parquet");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.emptyList());
        Assertions.assertEquals(
                300, new File("/tmp/seatunnel/LocalFileTest/empty_file_0.parquet").length());

        options.put("file_format_type", "binary");
        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                                        catalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Collections.emptyList()));
        Assertions.assertEquals(
                "ErrorCode:[FILE-07], ErrorDescription:[Format not support] - BinaryWriteStrategy does not support generating empty files when no data is written.",
                exception.getMessage());
    }

    @Test
    void testWriteFileWithCustomFileExtension() throws Exception {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "testFile");
                        put("is_enable_transaction", false);
                        put("file_format_type", "text");
                    }
                };
        options.put("filename_extension", "txt2");
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})));
        Assertions.assertEquals(
                2,
                (long) FileUtils.getFileLineNumber("/tmp/seatunnel/LocalFileTest/testFile_0.txt2"));

        options.put("filename_extension", ".ppp");
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})));
        Assertions.assertEquals(
                2,
                (long) FileUtils.getFileLineNumber("/tmp/seatunnel/LocalFileTest/testFile_0.ppp"));

        Map<String, Object> readOptions =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_format_type", "text");
                    }
                };
        readOptions.put("filename_extension", "ppp");
        List<SeaTunnelRow> rows =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(readOptions), new LocalFileSourceFactory());
        Assertions.assertEquals(2, rows.size());

        readOptions.put("filename_extension", "ppp2");
        List<SeaTunnelRow> emptyRows =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(readOptions), new LocalFileSourceFactory());

        Assertions.assertEquals(0, emptyRows.size());
    }

    @Test
    void testReadOneFileButHasTwoParallelism() throws Exception {
        Map<String, Object> readOptions =
                new HashMap<String, Object>() {
                    {
                        put("path", LocalFileTest.class.getResource("/test_data.txt").getPath());
                        put("file_format_type", "text");
                    }
                };
        List<SeaTunnelRow> rows =
                SourceFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(readOptions), new LocalFileSourceFactory(), 2);
        Assertions.assertEquals(3, rows.size());
    }

    @Test
    void testCanalJsonSink() throws IOException {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "canal_json_file");
                        put("file_format_type", "canal_json");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                    }
                };
        options.put("single_file_mode", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", TablePath.DEFAULT.getFullName()),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "a", BasicType.LONG_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "b", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "c", BasicType.INT_TYPE, 1L, true, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");

        Map<String, Object> rowOptions = new HashMap<>();
        rowOptions.put(EVENT_TIME.getName(), 1L);

        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1.setRowKind(RowKind.INSERT);
        row1.setTableId(TablePath.DEFAULT.getFullName());
        row1.setOptions(rowOptions);
        SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2.setRowKind(RowKind.INSERT);
        row2.setTableId(TablePath.DEFAULT.getFullName());
        row2.setOptions(rowOptions);
        SeaTunnelRow row3 = new SeaTunnelRow(new Object[] {3L, "C", 100});
        row3.setRowKind(RowKind.INSERT);
        row3.setTableId(TablePath.DEFAULT.getFullName());
        row3.setOptions(rowOptions);
        SeaTunnelRow row1UpdateBefore = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1UpdateBefore.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        row1UpdateBefore.setOptions(rowOptions);
        SeaTunnelRow row1UpdateAfter = new SeaTunnelRow(new Object[] {1L, "A_1", 100});
        row1UpdateAfter.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateAfter.setRowKind(RowKind.UPDATE_AFTER);
        row1UpdateAfter.setOptions(rowOptions);
        SeaTunnelRow row2Delete = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2Delete.setTableId(TablePath.DEFAULT.getFullName());
        row2Delete.setRowKind(RowKind.DELETE);
        row2Delete.setOptions(rowOptions);

        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete));
        Assertions.assertEquals(
                6,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/canal_json_file.canal_json"));
        Path path = Paths.get("/tmp/seatunnel/LocalFileTest/canal_json_file.canal_json");
        String dataStr = FileUtils.readFileToStr(path);
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":1,\"b\":\"A\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":2,\"b\":\"B\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":3,\"b\":\"C\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":1,\"b\":\"A\",\"c\":100}],\"type\":\"DELETE\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":1,\"b\":\"A_1\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":2,\"b\":\"B\",\"c\":100}],\"type\":\"DELETE\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));

        // test merge_update_event
        options.put("merge_update_event", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete));
        Assertions.assertEquals(
                5,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/canal_json_file.canal_json"));
        path = Paths.get("/tmp/seatunnel/LocalFileTest/canal_json_file.canal_json");
        dataStr = FileUtils.readFileToStr(path);
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":1,\"b\":\"A\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":2,\"b\":\"B\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":3,\"b\":\"C\",\"c\":100}],\"type\":\"INSERT\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":[{\"a\":1,\"b\":\"A\",\"c\":100}],\"data\":[{\"a\":1,\"b\":\"A_1\",\"c\":100}],\"type\":\"UPDATE\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":[{\"a\":2,\"b\":\"B\",\"c\":100}],\"type\":\"DELETE\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
    }

    @Test
    void testDebeziumJsonSink() throws IOException {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "debezium_json_file");
                        put("file_format_type", "debezium_json");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                    }
                };
        options.put("single_file_mode", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", TablePath.DEFAULT.getFullName()),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "a", BasicType.LONG_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "b", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "c", BasicType.INT_TYPE, 1L, true, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");

        Map<String, Object> rowOptions = new HashMap<>();
        rowOptions.put(EVENT_TIME.getName(), 1L);

        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1.setRowKind(RowKind.INSERT);
        row1.setTableId(TablePath.DEFAULT.getFullName());
        row1.setOptions(rowOptions);
        SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2.setRowKind(RowKind.INSERT);
        row2.setTableId(TablePath.DEFAULT.getFullName());
        row2.setOptions(rowOptions);
        SeaTunnelRow row3 = new SeaTunnelRow(new Object[] {3L, "C", 100});
        row3.setRowKind(RowKind.INSERT);
        row3.setTableId(TablePath.DEFAULT.getFullName());
        row3.setOptions(rowOptions);
        SeaTunnelRow row1UpdateBefore = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1UpdateBefore.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        row1UpdateBefore.setOptions(rowOptions);
        SeaTunnelRow row1UpdateAfter = new SeaTunnelRow(new Object[] {1L, "A_1", 100});
        row1UpdateAfter.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateAfter.setRowKind(RowKind.UPDATE_AFTER);
        row1UpdateAfter.setOptions(rowOptions);
        SeaTunnelRow row2Delete = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2Delete.setTableId(TablePath.DEFAULT.getFullName());
        row2Delete.setRowKind(RowKind.DELETE);
        row2Delete.setOptions(rowOptions);

        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete));
        Assertions.assertEquals(
                6,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/debezium_json_file.debezium_json"));
        Path path = Paths.get("/tmp/seatunnel/LocalFileTest/debezium_json_file.debezium_json");
        String dataStr = FileUtils.readFileToStr(path);
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":1,\"b\":\"A\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":2,\"b\":\"B\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":3,\"b\":\"C\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":{\"a\":1,\"b\":\"A\",\"c\":100},\"after\":null,\"op\":\"d\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":1,\"b\":\"A_1\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":{\"a\":2,\"b\":\"B\",\"c\":100},\"after\":null,\"op\":\"d\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));

        // test merge_update_event
        options.put("merge_update_event", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete));
        Assertions.assertEquals(
                5,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/debezium_json_file.debezium_json"));
        path = Paths.get("/tmp/seatunnel/LocalFileTest/debezium_json_file.debezium_json");
        dataStr = FileUtils.readFileToStr(path);
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":1,\"b\":\"A\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":2,\"b\":\"B\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":null,\"after\":{\"a\":3,\"b\":\"C\",\"c\":100},\"op\":\"c\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":{\"a\":1,\"b\":\"A\",\"c\":100},\"after\":{\"a\":1,\"b\":\"A_1\",\"c\":100},\"op\":\"u\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"before\":{\"a\":2,\"b\":\"B\",\"c\":100},\"after\":null,\"op\":\"d\",\"source\":{\"schema\":\"default\",\"database\":\"default\",\"table\":\"default\"},\"ts_ms\":1}"));
    }

    @Test
    void testMaxWellJsonSink() throws IOException {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "maxwell_json_file");
                        put("file_format_type", "maxwell_json");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                    }
                };
        options.put("single_file_mode", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", TablePath.DEFAULT.getFullName()),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "a", BasicType.LONG_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "b", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "c", BasicType.INT_TYPE, 1L, true, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        Map<String, Object> rowOptions = new HashMap<>();
        rowOptions.put(EVENT_TIME.getName(), 1L);

        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1.setRowKind(RowKind.INSERT);
        row1.setTableId(TablePath.DEFAULT.getFullName());
        row1.setOptions(rowOptions);
        SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2.setRowKind(RowKind.INSERT);
        row2.setTableId(TablePath.DEFAULT.getFullName());
        row2.setOptions(rowOptions);
        SeaTunnelRow row3 = new SeaTunnelRow(new Object[] {3L, "C", 100});
        row3.setRowKind(RowKind.INSERT);
        row3.setTableId(TablePath.DEFAULT.getFullName());
        row3.setOptions(rowOptions);
        SeaTunnelRow row1UpdateBefore = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1UpdateBefore.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        row1UpdateBefore.setOptions(rowOptions);
        SeaTunnelRow row1UpdateAfter = new SeaTunnelRow(new Object[] {1L, "A_1", 100});
        row1UpdateAfter.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateAfter.setRowKind(RowKind.UPDATE_AFTER);
        row1UpdateAfter.setOptions(rowOptions);
        SeaTunnelRow row2Delete = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2Delete.setTableId(TablePath.DEFAULT.getFullName());
        row2Delete.setRowKind(RowKind.DELETE);
        row2Delete.setOptions(rowOptions);

        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete));
        Assertions.assertEquals(
                6,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/maxwell_json_file.maxwell_json"));
        Path path = Paths.get("/tmp/seatunnel/LocalFileTest/maxwell_json_file.maxwell_json");
        String dataStr = FileUtils.readFileToStr(path);
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":1,\"b\":\"A\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":2,\"b\":\"B\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":3,\"b\":\"C\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":1,\"b\":\"A\",\"c\":100},\"type\":\"delete\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":1,\"b\":\"A_1\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":2,\"b\":\"B\",\"c\":100},\"type\":\"delete\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));

        // test merge_update_event
        options.put("merge_update_event", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete));
        Assertions.assertEquals(
                5,
                (long)
                        FileUtils.getFileLineNumber(
                                "/tmp/seatunnel/LocalFileTest/maxwell_json_file.maxwell_json"));
        path = Paths.get("/tmp/seatunnel/LocalFileTest/maxwell_json_file.maxwell_json");
        dataStr = FileUtils.readFileToStr(path);
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":1,\"b\":\"A\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":2,\"b\":\"B\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":3,\"b\":\"C\",\"c\":100},\"type\":\"insert\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":{\"a\":1,\"b\":\"A\",\"c\":100},\"data\":{\"a\":1,\"b\":\"A_1\",\"c\":100},\"type\":\"update\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
        Assertions.assertTrue(
                dataStr.contains(
                        "{\"old\":null,\"data\":{\"a\":2,\"b\":\"B\",\"c\":100},\"type\":\"delete\",\"database\":\"default\",\"table\":\"default\",\"ts\":1}"));
    }

    @Test
    void testFileFilterByModificationDate() throws Exception {
        // create test path
        String testPath = "/tmp/seatunnel/LocalFileTest";
        FileUtils.deleteFile(testPath);
        FileUtils.createNewDir(testPath);

        // create test files
        String file1Path = testPath + "/test1.txt";
        String file2Path = testPath + "/test2.txt";
        String file3Path = testPath + "/test3.txt";

        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", testPath);
                        put("file_format_type", "text");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                        put("single_file_mode", true);
                    }
                };

        // create file1
        options.put("file_name_expression", "test1");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.singletonList(new SeaTunnelRow(new Object[] {"test1"})));

        // create file2
        options.put("file_name_expression", "test2");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.singletonList(new SeaTunnelRow(new Object[] {"test2"})));

        // create file3
        options.put("file_name_expression", "test3");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.singletonList(new SeaTunnelRow(new Object[] {"test3"})));

        File file1 = Paths.get(file1Path).toFile();
        File file2 = Paths.get(file2Path).toFile();
        File file3 = Paths.get(file3Path).toFile();

        long now = System.currentTimeMillis();
        // set file1 modification time is today
        boolean isModified1 = file1.setLastModified(now);

        // set file2 modification time is yesterday
        long yesterday = now - 24 * 60 * 60 * 1000;
        boolean isModified2 = file2.setLastModified(yesterday);

        // set file3 modification time is day before yesterday
        long dayBeforeYesterday = now - 48 * 60 * 60 * 1000;
        boolean isModified3 = file3.setLastModified(dayBeforeYesterday);

        // modified time success
        Assertions.assertTrue(isModified1 && isModified2 && isModified3);

        // test case1: return all file if not set time filter
        Map<String, Object> readOptions1 =
                new HashMap<String, Object>() {
                    {
                        put("path", testPath);
                        put("file_format_type", "text");
                    }
                };

        // file1, file2  and file3, all file can be read.
        Assertions.assertEquals(
                3,
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                ReadonlyConfig.fromMap(readOptions1), new LocalFileSourceFactory())
                        .size());

        // test case2: only file2 can be read, if set filter time is yesterday
        Map<String, Object> readOptions2 =
                new HashMap<String, Object>() {
                    {
                        put("path", testPath);
                        put("file_format_type", "text");
                        put(
                                "file_filter_modified_start",
                                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                        .format(new Date(yesterday)));
                        put(
                                "file_filter_modified_end",
                                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                        .format(new Date(yesterday + 1000)));
                    }
                };
        List<SeaTunnelRow> readContext =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(readOptions2), new LocalFileSourceFactory());
        Assertions.assertEquals(1, readContext.size());
        Assertions.assertEquals("test2", readContext.get(0).getField(0));

        // test case 3: only file3 can be read, if set filter time is day before yesterday
        Map<String, Object> readOptions3 =
                new HashMap<String, Object>() {
                    {
                        put("path", testPath);
                        put("file_format_type", "text");
                        put(
                                "file_filter_modified_start",
                                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                        .format(new Date(dayBeforeYesterday)));

                        put(
                                "file_filter_modified_end",
                                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                        .format(new Date(dayBeforeYesterday + 1000)));
                    }
                };

        List<SeaTunnelRow> rows3 =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(readOptions3), new LocalFileSourceFactory());

        Assertions.assertEquals(1, rows3.size());
        Assertions.assertEquals("test3", rows3.get(0).getField(0));

        // clean up
        FileUtils.deleteFile(testPath);
    }
}
