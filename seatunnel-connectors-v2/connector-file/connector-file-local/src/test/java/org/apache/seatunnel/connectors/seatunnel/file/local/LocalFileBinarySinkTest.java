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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.LocalFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason =
                "Hadoop has windows problem, please refer https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems")
class LocalFileBinarySinkTest {

    private final CatalogTable binaryCatalogTable =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .columns(
                                    Arrays.asList(
                                            PhysicalColumn.of(
                                                    "data",
                                                    PrimitiveByteArrayType.INSTANCE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    ""),
                                            PhysicalColumn.of(
                                                    "relativePath",
                                                    BasicType.STRING_TYPE,
                                                    1L,
                                                    true,
                                                    null,
                                                    ""),
                                            PhysicalColumn.of(
                                                    "partIndex",
                                                    BasicType.LONG_TYPE,
                                                    1L,
                                                    true,
                                                    null,
                                                    "")))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    void testBinarySinkUsesCustomFilename() throws Exception {
        String testPath = "/tmp/seatunnel/LocalFileBinarySinkTest/customFilename";
        FileUtils.deleteFile(testPath);

        Map<String, Object> options = createBinarySinkOptions(testPath);
        options.put("single_file_mode", true);
        options.put("custom_filename", true);
        options.put("file_name_expression", "expected_binary_file");
        options.put("filename_extension", ".raw");

        byte[] data = "binary-content".getBytes(StandardCharsets.UTF_8);
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                binaryCatalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Collections.singletonList(new SeaTunnelRow(new Object[] {data, "source.raw", 0L})));

        Assertions.assertArrayEquals(
                data, Files.readAllBytes(Paths.get(testPath, "expected_binary_file.raw")));
        Assertions.assertFalse(FileUtils.isFileExist(Paths.get(testPath, "source.raw").toString()));
    }

    @Test
    void testBinarySinkKeepsSourceRelativePathByDefault() throws Exception {
        String testPath = "/tmp/seatunnel/LocalFileBinarySinkTest/defaultRelativePath";
        FileUtils.deleteFile(testPath);

        byte[] data = "binary-content".getBytes(StandardCharsets.UTF_8);
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                binaryCatalogTable,
                ReadonlyConfig.fromMap(createBinarySinkOptions(testPath)),
                new LocalFileSinkFactory(),
                Collections.singletonList(
                        new SeaTunnelRow(new Object[] {data, "nested/source.raw", 0L})));

        Assertions.assertArrayEquals(
                data, Files.readAllBytes(Paths.get(testPath, "nested", "source.raw")));
    }

    @Test
    void testBinarySinkRejectsMultipleSourceFilesWhenUsingCustomFilename() throws Exception {
        String testPath = "/tmp/seatunnel/LocalFileBinarySinkTest/customFilenameMultipleFiles";
        FileUtils.deleteFile(testPath);

        Map<String, Object> options = createBinarySinkOptions(testPath);
        options.put("single_file_mode", true);
        options.put("custom_filename", true);
        options.put("file_name_expression", "expected_binary_file");
        options.put("filename_extension", ".raw");

        byte[] data = "binary-content".getBytes(StandardCharsets.UTF_8);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                                        binaryCatalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Arrays.asList(
                                                new SeaTunnelRow(
                                                        new Object[] {data, "source-1.raw", 0L}),
                                                new SeaTunnelRow(
                                                        new Object[] {data, "source-2.raw", 0L}))));

        Assertions.assertInstanceOf(FileConnectorException.class, exception.getCause());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains("only supports a single source file"));
    }

    @Test
    void testBinarySinkRejectsUnsafeCustomFilenameWithParallelSubtasks() throws Exception {
        String testPath = "/tmp/seatunnel/LocalFileBinarySinkTest/parallelCustomFilename";
        FileUtils.deleteFile(testPath);

        Map<String, Object> options = createBinarySinkOptions(testPath);
        options.put("custom_filename", true);
        options.put("file_name_expression", "expected_binary_file");
        options.put("filename_extension", ".raw");

        byte[] data = "binary-content".getBytes(StandardCharsets.UTF_8);
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SinkFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                                        binaryCatalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Collections.singletonList(
                                                new SeaTunnelRow(
                                                        new Object[] {data, "source.raw", 0L})),
                                        2));

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Binary custom filename requires a unique filename expression"));
    }

    private Map<String, Object> createBinarySinkOptions(String path) {
        Map<String, Object> options = new HashMap<>();
        options.put("path", path);
        options.put("file_format_type", "binary");
        options.put("is_enable_transaction", false);
        return options;
    }
}
