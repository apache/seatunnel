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
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveSaveModeHandlerTest {

    @Mock private HiveMetaStoreProxy mockHiveMetaStoreProxy;

    private ReadonlyConfig readonlyConfig;
    private CatalogTable catalogTable;
    private TableSchema tableSchema;

    @BeforeEach
    void setUp() {
        // Create test table schema
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, "Primary key"),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, 0, true, null, "User name"),
                        PhysicalColumn.of("age", BasicType.INT_TYPE, 0, true, null, "User age"),
                        PhysicalColumn.of(
                                "salary", new DecimalType(10, 2), 0, true, null, "User salary"),
                        PhysicalColumn.of(
                                "birth_date",
                                LocalTimeType.LOCAL_DATE_TYPE,
                                0,
                                true,
                                null,
                                "Birth date"),
                        PhysicalColumn.of(
                                "created_at",
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                0,
                                true,
                                null,
                                "Creation timestamp"));

        tableSchema = TableSchema.builder().columns(columns).build();

        // Create catalog table
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "user_table"),
                        tableSchema,
                        new HashMap<>(),
                        Arrays.asList(),
                        "Test user table");

        // Create readonly config
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");
        configMap.put(
                HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                        + "  ${rowtype_fields}\n"
                        + ") \n"
                        + "STORED AS PARQUET\n"
                        + "LOCATION '${table_location}'\n"
                        + "TBLPROPERTIES (\n"
                        + "  'parquet.compression'='SNAPPY'\n"
                        + ")");

        readonlyConfig = ReadonlyConfig.fromMap(configMap);
    }

    @Test
    void testConstructor() {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        "CREATE TABLE ${database}.${table} (${rowtype_fields})");

        assertNotNull(handler);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
        assertEquals(TablePath.of("test_db.user_table"), handler.getHandleTablePath());
        assertNull(handler.getHandleCatalog()); // Hive doesn't use Catalog interface
    }

    @Test
    void testGenerateColumnDefinitions() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        "CREATE TABLE ${database}.${table} (${rowtype_fields})");

        // Use reflection to access private method for testing
        java.lang.reflect.Method method =
                HiveSaveModeHandler.class.getDeclaredMethod("generateColumnDefinitions");
        method.setAccessible(true);
        String result = (String) method.invoke(handler);

        // Verify column definitions
        assertTrue(result.contains("`id` bigint COMMENT 'Primary key'"));
        assertTrue(result.contains("`name` string COMMENT 'User name'"));
        assertTrue(result.contains("`age` int COMMENT 'User age'"));
        assertTrue(result.contains("`salary` decimal(10,2) COMMENT 'User salary'"));
        assertTrue(result.contains("`birth_date` date COMMENT 'Birth date'"));
        assertTrue(result.contains("`created_at` timestamp COMMENT 'Creation timestamp'"));
    }

    @Test
    void testProcessCreateTemplate() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                                + "  ${rowtype_fields}\n"
                                + ") \n"
                                + "STORED AS PARQUET\n"
                                + "LOCATION '${table_location}'");

        // Use reflection to access private method
        java.lang.reflect.Method method =
                HiveSaveModeHandler.class.getDeclaredMethod("processCreateTemplate");
        method.setAccessible(true);
        String result = (String) method.invoke(handler);

        // Verify template processing
        assertTrue(result.contains("CREATE TABLE IF NOT EXISTS `test_db`.`user_table`"));
        assertTrue(result.contains("STORED AS PARQUET"));
        assertTrue(result.contains("LOCATION '/user/hive/warehouse/test_db.db/user_table'"));
        assertTrue(result.contains("`id` bigint COMMENT 'Primary key'"));
    }

    @Test
    void testBuildTableFromSchema() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        "CREATE TABLE ${database}.${table} (${rowtype_fields})");

        // Use reflection to access private method
        java.lang.reflect.Method method =
                HiveSaveModeHandler.class.getDeclaredMethod("buildTableFromSchema");
        method.setAccessible(true);
        Table table = (Table) method.invoke(handler);

        // Verify table properties
        assertEquals("test_db", table.getDbName());
        assertEquals("user_table", table.getTableName());
        assertEquals("MANAGED_TABLE", table.getTableType());
        assertNotNull(table.getSd());

        // Verify columns
        List<FieldSchema> cols = table.getSd().getCols();
        assertEquals(6, cols.size());

        FieldSchema idCol = cols.get(0);
        assertEquals("id", idCol.getName());
        assertEquals("bigint", idCol.getType());
        assertEquals("Primary key", idCol.getComment());

        FieldSchema nameCol = cols.get(1);
        assertEquals("name", nameCol.getName());
        assertEquals("string", nameCol.getType());
        assertEquals("User name", nameCol.getComment());

        // Verify storage descriptor
        assertEquals(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                table.getSd().getInputFormat());
        assertEquals(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                table.getSd().getOutputFormat());
        assertEquals("/user/hive/warehouse/test_db.db/user_table", table.getSd().getLocation());

        // Verify table properties
        assertEquals("SNAPPY", table.getParameters().get("parquet.compression"));
        assertEquals("seatunnel", table.getParameters().get("created_by"));
    }

    @Test
    void testHandleSchemaSaveModeCreateWhenNotExist() throws Exception {
        try (MockedStatic<HiveMetaStoreProxy> mockedStatic = mockStatic(HiveMetaStoreProxy.class)) {
            mockedStatic
                    .when(() -> HiveMetaStoreProxy.getInstance(any()))
                    .thenReturn(mockHiveMetaStoreProxy);

            when(mockHiveMetaStoreProxy.tableExists(anyString(), anyString())).thenReturn(false);

            HiveSaveModeHandler handler =
                    new HiveSaveModeHandler(
                            readonlyConfig,
                            catalogTable,
                            SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                            "CREATE TABLE ${database}.${table} (${rowtype_fields})");

            handler.open();
            handler.handleSchemaSaveMode();

            verify(mockHiveMetaStoreProxy).tableExists("test_db", "user_table");
            verify(mockHiveMetaStoreProxy).createDatabaseIfNotExists(any());
            verify(mockHiveMetaStoreProxy).createTableIfNotExists(any(Table.class));
        }
    }

    @Test
    void testHandleSchemaSaveModeRecreateSchema() throws Exception {
        try (MockedStatic<HiveMetaStoreProxy> mockedStatic = mockStatic(HiveMetaStoreProxy.class)) {
            mockedStatic
                    .when(() -> HiveMetaStoreProxy.getInstance(any()))
                    .thenReturn(mockHiveMetaStoreProxy);

            when(mockHiveMetaStoreProxy.tableExists(anyString(), anyString())).thenReturn(true);

            HiveSaveModeHandler handler =
                    new HiveSaveModeHandler(
                            readonlyConfig,
                            catalogTable,
                            SchemaSaveMode.RECREATE_SCHEMA,
                            "CREATE TABLE ${database}.${table} (${rowtype_fields})");

            handler.open();
            handler.handleSchemaSaveMode();

            verify(mockHiveMetaStoreProxy).tableExists("test_db", "user_table");
            verify(mockHiveMetaStoreProxy).dropTable("test_db", "user_table");
            verify(mockHiveMetaStoreProxy).createDatabaseIfNotExists(any());
            verify(mockHiveMetaStoreProxy).createTableIfNotExists(any(Table.class));
        }
    }

    @Test
    void testHandleDataSaveMode() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        "CREATE TABLE ${database}.${table} (${rowtype_fields})");

        // Data save mode should not throw exception and should log message
        assertDoesNotThrow(() -> handler.handleDataSaveMode());
    }

    @Test
    void testCommentEscaping() throws Exception {
        // Create table with special characters in comments
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of(
                                "test_col",
                                BasicType.STRING_TYPE,
                                0,
                                true,
                                null,
                                "Comment with 'single quotes' and \"double quotes\""));

        TableSchema specialSchema = TableSchema.builder().columns(columns).build();
        CatalogTable specialCatalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "special_table"),
                        specialSchema,
                        new HashMap<>(),
                        Arrays.asList(),
                        "Test table with special comments");

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig,
                        specialCatalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        "CREATE TABLE ${database}.${table} (${rowtype_fields})");

        // Use reflection to test column definition generation
        java.lang.reflect.Method method =
                HiveSaveModeHandler.class.getDeclaredMethod("generateColumnDefinitions");
        method.setAccessible(true);
        String result = (String) method.invoke(handler);

        // Verify that single quotes are escaped
        assertTrue(result.contains("Comment with \\'single quotes\\'"));
    }
}
