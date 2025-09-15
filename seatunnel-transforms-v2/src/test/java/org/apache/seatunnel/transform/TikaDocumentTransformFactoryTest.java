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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.transform.tikadocument.TikaDocumentTransform;
import org.apache.seatunnel.transform.tikadocument.TikaDocumentTransformFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Unit tests for TikaDocumentTransformFactory */
public class TikaDocumentTransformFactoryTest {

    private TikaDocumentTransformFactory factory;
    private CatalogTable catalogTable;

    @BeforeEach
    public void setUp() {
        factory = new TikaDocumentTransformFactory();

        // Create a test catalog table
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
    }

    @Test
    public void testFactoryIdentifier() {
        String identifier = factory.factoryIdentifier();
        Assertions.assertEquals(TikaDocumentTransform.PLUGIN_NAME, identifier);
    }

    @Test
    public void testOptionRule() {
        // Test that option rule is not null
        Assertions.assertNotNull(factory.optionRule());
        // Basic check that the factory can be created successfully
        Assertions.assertEquals(TikaDocumentTransform.PLUGIN_NAME, factory.factoryIdentifier());
    }

    @Test
    public void testCreateTransform() {
        // Create configuration
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source_field", "document_data");
        Map<String, String> outputFields = new HashMap<>();
        outputFields.put("content", "extracted_text");
        outputFields.put("content_type", "mime_type");
        configMap.put("output_fields", outputFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Create factory context
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Arrays.asList(catalogTable), config, getClass().getClassLoader());

        // Create transform
        TableTransform transform = factory.createTransform(context);

        Assertions.assertNotNull(transform);
        Assertions.assertNotNull(transform.createTransform());
    }

    @Test
    public void testCreateTransformWithMinimalConfig() {
        // Test with minimal configuration
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source_field", "document_data");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Arrays.asList(catalogTable), config, getClass().getClassLoader());

        // Should not throw exception with minimal config
        TableTransform transform = factory.createTransform(context);
        Assertions.assertNotNull(transform);
    }
}
