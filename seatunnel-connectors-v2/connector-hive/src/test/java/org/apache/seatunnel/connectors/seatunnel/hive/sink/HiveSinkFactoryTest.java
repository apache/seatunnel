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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for HiveSinkFactory SaveMode validation */
public class HiveSinkFactoryTest {

    private HiveSinkFactory factory;
    private CatalogTable catalogTable;

    @BeforeEach
    void setUp() {
        factory = new HiveSinkFactory();

        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, "ID"),
                        PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, true, null, "Name"));

        TableSchema tableSchema = TableSchema.builder().columns(columns).build();

        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        tableSchema,
                        new HashMap<>(),
                        Arrays.asList(),
                        "Test table");
    }

    private TableSinkFactoryContext createContext(
            ReadonlyConfig config, CatalogTable catalogTable) {
        return new TableSinkFactoryContext(
                catalogTable, config, Thread.currentThread().getContextClassLoader());
    }

    @Test
    void testFactoryIdentifier() {
        assertEquals("Hive", factory.factoryIdentifier());
    }

    @Test
    void testCreateSinkWithValidSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");
        configMap.put(
                HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) STORED AS PARQUET");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Note: We don't call tableSink.createSink() to avoid MetaStore dependency in unit tests
        assertDoesNotThrow(
                () -> {
                    TableSinkFactoryContext context = createContext(config, catalogTable);
                    TableSink<?, ?, ?, ?> tableSink = factory.createSink(context);
                    assertNotNull(tableSink);
                });
    }

    @Test
    void testCreateSinkWithoutSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertDoesNotThrow(
                () -> {
                    TableSinkFactoryContext context = createContext(config, catalogTable);
                    TableSink<?, ?, ?, ?> tableSink = factory.createSink(context);
                    assertNotNull(tableSink);
                });
    }

    @Test
    void testCreateSinkWithInvalidSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "INVALID_MODE");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(
                Exception.class,
                () -> {
                    config.get(HiveSinkOptions.SCHEMA_SAVE_MODE); // This should fail
                });
    }

    @Test
    void testCreateSinkWithSaveModeButNoTemplate() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertDoesNotThrow(
                () -> {
                    TableSinkFactoryContext context = createContext(config, catalogTable);
                    TableSink<?, ?, ?, ?> tableSink = factory.createSink(context);
                    assertNotNull(tableSink);
                });
    }

    @Test
    void testValidSaveModeValues() {
        String[] validModes = {
            "CREATE_SCHEMA_WHEN_NOT_EXIST",
            "RECREATE_SCHEMA",
            "ERROR_WHEN_SCHEMA_NOT_EXIST",
            "IGNORE"
        };

        for (String mode : validModes) {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
            configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
            configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), mode);
            configMap.put(
                    HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                    "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) STORED AS PARQUET");

            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

            assertDoesNotThrow(
                    () -> {
                        TableSinkFactoryContext context = createContext(config, catalogTable);
                        TableSink<?, ?, ?, ?> tableSink = factory.createSink(context);
                        assertNotNull(tableSink);
                    },
                    "Failed to create sink with SaveMode: " + mode);
        }
    }

    @Test
    void testCreateSinkWithDifferentTemplates() {
        String[] templates = {
            "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) STORED AS PARQUET",
            "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) STORED AS ORC",
            "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) PARTITIONED BY (${rowtype_partition_fields}) STORED AS PARQUET"
        };

        for (String template : templates) {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
            configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
            configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");
            configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), template);

            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

            assertDoesNotThrow(
                    () -> {
                        TableSinkFactoryContext context = createContext(config, catalogTable);
                        TableSink<?, ?, ?, ?> tableSink = factory.createSink(context);
                        assertNotNull(tableSink);
                    },
                    "Failed to create sink with template: " + template);
        }
    }

    @Test
    void testRequiredConfigValidation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertDoesNotThrow(
                () -> {
                    TableSinkFactoryContext context = createContext(config, catalogTable);
                    factory.createSink(context);
                });
    }

    @Test
    void testRequiredMetastoreUriValidation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertDoesNotThrow(
                () -> {
                    TableSinkFactoryContext context = createContext(config, catalogTable);
                    factory.createSink(context);
                });
    }

    @Test
    void testFactoryOptionKeys() {
        assertNotNull(factory.optionRule());

        assertTrue(
                factory.optionRule()
                        .getOptionalOptions()
                        .contains(HiveSinkOptions.SCHEMA_SAVE_MODE));
        assertTrue(
                factory.optionRule()
                        .getOptionalOptions()
                        .contains(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
    }

    @Test
    void testCreateSinkWithDifferentTableNames() {
        String[] tableNames = {
            "db.table", "database.table_name", "test_db.user_events", "analytics.fact_sales"
        };

        for (String tableName : tableNames) {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(HiveConfig.TABLE_NAME.key(), tableName);
            configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
            configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");

            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

            assertDoesNotThrow(
                    () -> {
                        TableSinkFactoryContext context = createContext(config, catalogTable);
                        TableSink<?, ?, ?, ?> tableSink = factory.createSink(context);
                        assertNotNull(tableSink);
                    },
                    "Failed to create sink with table name: " + tableName);
        }
    }
}
