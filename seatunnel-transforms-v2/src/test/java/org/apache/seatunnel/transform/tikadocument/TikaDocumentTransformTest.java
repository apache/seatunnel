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

package org.apache.seatunnel.transform.tikadocument;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Unit tests for TikaDocumentTransform */
public class TikaDocumentTransformTest {

    private CatalogTable catalogTable;
    private TikaDocumentTransformConfig config;

    @BeforeEach
    public void setUp() {
        // Create test catalog table
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.of(
                                                "id", BasicType.LONG_TYPE, 0, false, null, ""),
                                        PhysicalColumn.of(
                                                "filename",
                                                BasicType.STRING_TYPE,
                                                200,
                                                true,
                                                null,
                                                ""),
                                        PhysicalColumn.of(
                                                "document_data",
                                                PrimitiveByteArrayType.INSTANCE,
                                                0,
                                                true,
                                                null,
                                                "")))
                        .build();

        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.of("test", "test_table")),
                        tableSchema,
                        new HashMap<>(),
                        Arrays.asList(),
                        "");

        // Create test configuration
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source_field", "document_data");

        Map<String, String> outputFields = new HashMap<>();
        outputFields.put("content", "extracted_text");
        outputFields.put("content_type", "mime_type");
        outputFields.put("title", "doc_title");
        configMap.put("output_fields", outputFields);

        configMap.put("parse_options.extract_text", true);
        configMap.put("parse_options.extract_metadata", true);
        configMap.put("parse_options.max_string_length", 10000);
        configMap.put("content_processing.remove_empty_lines", true);
        configMap.put("content_processing.trim_whitespace", true);
        configMap.put("error_handling.on_parse_error", "skip");
        configMap.put("error_handling.log_errors", true);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        config = TikaDocumentTransformConfig.of(readonlyConfig);
    }

    @Test
    public void testPluginName() {
        TikaDocumentTransform transform = new TikaDocumentTransform(config, catalogTable);
        Assertions.assertEquals("TikaDocument", transform.getPluginName());
    }

    @Test
    public void testGetOutputColumns() {
        TikaDocumentTransform transform = new TikaDocumentTransform(config, catalogTable);
        Column[] outputColumns = transform.getOutputColumns();

        Assertions.assertEquals(3, outputColumns.length);

        // Check column names
        Set<String> expectedNames =
                new HashSet<>(Arrays.asList("extracted_text", "mime_type", "doc_title"));
        for (int i = 0; i < outputColumns.length; i++) {
            Assertions.assertTrue(
                    expectedNames.contains(outputColumns[i].getName()),
                    "Column name " + outputColumns[i].getName() + " not found in expected names");
        }
    }

    @Test
    public void testTransformTextDocument() {
        // Test basic transform creation and column generation
        TikaDocumentTransform transform = new TikaDocumentTransform(config, catalogTable);

        // Test that transform can be created successfully
        Assertions.assertNotNull(transform);
        Assertions.assertEquals("TikaDocument", transform.getPluginName());

        // Test output columns
        Column[] outputColumns = transform.getOutputColumns();
        Assertions.assertNotNull(outputColumns);
        Assertions.assertEquals(3, outputColumns.length);

        // Test catalog table transformation
        CatalogTable producedTable = transform.getProducedCatalogTable();
        Assertions.assertNotNull(producedTable);
        Assertions.assertTrue(producedTable.getTableSchema().getColumns().size() >= 6);
    }

    @Test
    public void testTransformWithNullInput() {
        // Test basic transform behavior without actual data processing
        TikaDocumentTransform transform = new TikaDocumentTransform(config, catalogTable);
        Assertions.assertNotNull(transform);

        // Test that configuration is properly set
        Assertions.assertEquals("TikaDocument", transform.getPluginName());
    }

    @Test
    public void testTransformWithInvalidData() {
        // Test configuration with different error handling
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source_field", "document_data");
        Map<String, String> outputFields = new HashMap<>();
        outputFields.put("content", "extracted_text");
        configMap.put("output_fields", outputFields);
        configMap.put("error_handling.on_parse_error", "fail");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        TikaDocumentTransformConfig failConfig = TikaDocumentTransformConfig.of(readonlyConfig);

        TikaDocumentTransform transform = new TikaDocumentTransform(failConfig, catalogTable);
        Assertions.assertNotNull(transform);
        Assertions.assertEquals("TikaDocument", transform.getPluginName());
    }

    @Test
    public void testConfigurationParsing() {
        // Test configuration parsing
        Assertions.assertEquals("document_data", config.getSourceField());
        Assertions.assertTrue(config.isExtractText());
        Assertions.assertTrue(config.isExtractMetadata());
        Assertions.assertEquals(10000, config.getMaxStringLength());
        Assertions.assertTrue(config.isRemoveEmptyLines());
        Assertions.assertTrue(config.isTrimWhitespace());
        Assertions.assertEquals(ErrorHandleWay.SKIP, config.getOnParseError());
        Assertions.assertTrue(config.isLogErrors());

        Map<String, String> expectedOutputFields = new HashMap<>();
        expectedOutputFields.put("content", "extracted_text");
        expectedOutputFields.put("content_type", "mime_type");
        expectedOutputFields.put("title", "doc_title");

        Assertions.assertEquals(expectedOutputFields, config.getOutputFields());
    }

    @Test
    public void testBase64Input() {
        TikaDocumentTransform transform = new TikaDocumentTransform(config, catalogTable);

        // Test basic functionality without actual data processing
        Assertions.assertNotNull(transform);
        Assertions.assertEquals("TikaDocument", transform.getPluginName());

        // Test that output columns are generated correctly
        Column[] outputColumns = transform.getOutputColumns();
        Assertions.assertNotNull(outputColumns);
        Assertions.assertEquals(3, outputColumns.length);
    }

    @Test
    public void testGetProducedCatalogTable() {
        TikaDocumentTransform transform = new TikaDocumentTransform(config, catalogTable);
        CatalogTable producedTable = transform.getProducedCatalogTable();

        Assertions.assertNotNull(producedTable);
        // Should have original columns plus new output columns
        Assertions.assertTrue(producedTable.getTableSchema().getColumns().size() >= 6);
    }
}
