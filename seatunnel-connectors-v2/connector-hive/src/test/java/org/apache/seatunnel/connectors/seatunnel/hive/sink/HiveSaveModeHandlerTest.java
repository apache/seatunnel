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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        configMap.put(HiveSinkOptions.TABLE_FORMAT.key(), "PARQUET");
        configMap.put(HiveSinkOptions.PARTITION_FIELDS.key(), Arrays.asList());

        readonlyConfig = ReadonlyConfig.fromMap(configMap);
    }

    @Test
    void testConstructor() {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertNotNull(handler);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
        assertEquals(TablePath.of("test_db.user_table"), handler.getHandleTablePath());
        assertNull(handler.getHandleCatalog()); // Hive doesn't use Catalog interface
    }

    // Removed testGenerateColumnDefinitions and testProcessCreateTemplate
    // as these methods no longer exist in the simplified implementation

    @Test
    void testBuildTableFromSchema() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

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
                            SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

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
                            readonlyConfig, catalogTable, SchemaSaveMode.RECREATE_SCHEMA);

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
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        // Data save mode should not throw exception and should log message
        assertDoesNotThrow(() -> handler.handleDataSaveMode());
    }

    // Removed testCommentEscaping as generateColumnDefinitions method no longer exists

    @Test
    void testPartitionFieldsValidation() {
        // Test with partition fields from source
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.PARTITION_FIELDS.key(), Arrays.asList("age", "created_at"));
        ReadonlyConfig configWithPartitions = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithPartitions,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        // Test partition field validation methods
        assertTrue(handler.isPartitionedTable());
        assertEquals(Arrays.asList("age", "created_at"), handler.getPartitionFieldsFromSource());
        assertEquals(
                Arrays.asList("id", "name", "salary", "birth_date"),
                handler.getNonPartitionFields());
    }

    @Test
    void testPartitionFieldsWithNewFields() {
        // Test with new partition fields not in source
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.PARTITION_FIELDS.key(), Arrays.asList("year", "month"));
        ReadonlyConfig configWithNewPartitions = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithNewPartitions,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        // Test partition field validation methods
        assertTrue(handler.isPartitionedTable());
        assertEquals(Collections.emptyList(), handler.getPartitionFieldsFromSource());
        assertEquals(
                Arrays.asList("id", "name", "age", "salary", "birth_date", "created_at"),
                handler.getNonPartitionFields());
    }

    @Test
    void testNonPartitionedTable() {
        // Test without partition fields
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.PARTITION_FIELDS.key(), Collections.emptyList());
        ReadonlyConfig configNonPartitioned = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configNonPartitioned,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        // Test non-partitioned table
        assertFalse(handler.isPartitionedTable());
        assertEquals(Collections.emptyList(), handler.getPartitionFieldsFromSource());
        assertEquals(
                Arrays.asList("id", "name", "age", "salary", "birth_date", "created_at"),
                handler.getNonPartitionFields());
    }

    // Removed testGenerateNonPartitionColumnDefinitions and testGeneratePartitionByClause
    // as these methods no longer exist in the simplified implementation

    @Test
    void testBuildTableFromSchemaWithPartitions() throws Exception {
        // Test with partition fields
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.PARTITION_FIELDS.key(), Arrays.asList("age", "year"));
        ReadonlyConfig configWithPartitions = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithPartitions,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        // Use reflection to access private method
        java.lang.reflect.Method method =
                HiveSaveModeHandler.class.getDeclaredMethod("buildTableFromSchema");
        method.setAccessible(true);
        Table table = (Table) method.invoke(handler);

        // Verify regular columns (should exclude partition fields)
        List<FieldSchema> cols = table.getSd().getCols();
        assertEquals(5, cols.size()); // 6 original columns - 1 partition field (age)

        // Verify partition keys
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        assertEquals(2, partitionKeys.size());

        FieldSchema agePartition = partitionKeys.get(0);
        assertEquals("age", agePartition.getName());
        assertEquals("int", agePartition.getType());

        FieldSchema yearPartition = partitionKeys.get(1);
        assertEquals("year", yearPartition.getName());
        assertEquals("string", yearPartition.getType());
    }
}
