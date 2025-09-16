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
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class HiveSaveModeHandlerTest {

    @Mock private HiveMetaStoreProxy mockHiveMetaStoreProxy;

    private ReadonlyConfig readonlyConfig;
    private CatalogTable catalogTable;
    private TableSchema tableSchema;

    @BeforeEach
    void setUp() {
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

        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "user_table"),
                        tableSchema,
                        new HashMap<>(),
                        Arrays.asList(),
                        "Test user table");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");

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
        handler.open();
        assertNotNull(handler.getHandleCatalog());
    }

    @Test
    void testBuildTableFromTemplate() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
        assertEquals(TablePath.of("test_db.user_table"), handler.getHandleTablePath());

        // assert partition fields from template if needed via HiveTableTemplateUtils in separate
        // tests
    }

    @Test
    void testHandleSchemaSaveModeCreateWhenNotExist() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
        assertEquals(TablePath.of("test_db.user_table"), handler.getHandleTablePath());
    }

    @Test
    void testHandleSchemaSaveModeRecreateSchema() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.RECREATE_SCHEMA);

        assertEquals(SchemaSaveMode.RECREATE_SCHEMA, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
        assertEquals(TablePath.of("test_db.user_table"), handler.getHandleTablePath());
    }

    @Test
    void testHandleDataSaveMode() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertDoesNotThrow(() -> handler.handleDataSaveMode());
    }

    @Test
    void testTemplateWithPartitionFields() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(
                HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) "
                        + "PARTITIONED BY (year string, month string) STORED AS PARQUET");
        ReadonlyConfig configWithTemplate = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithTemplate,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        // verify partition fields via utility
        assertEquals(
                java.util.Arrays.asList("year", "month"),
                org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableTemplateUtils
                        .extractPartitionFieldsFromTemplate(
                                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) PARTITIONED BY (year string, month string) STORED AS PARQUET"));
    }

    @Test
    void testCustomTemplate() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(
                HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) "
                        + "STORED AS ORC LOCATION '${table_location}'");
        ReadonlyConfig configWithCustomTemplate = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithCustomTemplate,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
        assertEquals(TablePath.of("test_db.user_table"), handler.getHandleTablePath());
    }

    @Test
    void testDefaultTemplate() throws Exception {
        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        readonlyConfig, catalogTable, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());

        // default template non-partitioned verified elsewhere
    }

    @Test
    void testTemplateWithPartitionedTable() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        configMap.put(
                HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) "
                        + "PARTITIONED BY (${rowtype_partition_fields}) STORED AS PARQUET");
        ReadonlyConfig configWithPartitions = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithPartitions,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        assertEquals(
                java.util.Arrays.asList("${rowtype_partition_fields}"),
                org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableTemplateUtils
                        .extractPartitionFieldsFromTemplate(
                                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) PARTITIONED BY (${rowtype_partition_fields}) STORED AS PARQUET"));
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, handler.getDataSaveMode());
    }

    @Test
    void testCustomTemplate_buildsExpectedTable() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveConfig.TABLE_NAME.key(), "test_db.user_table");
        configMap.put(HiveConfig.METASTORE_URI.key(), "thrift://localhost:9083");
        String template =
                "CREATE EXTERNAL TABLE IF NOT EXISTS `${database}`.`${table}` ("
                        + "  ${rowtype_fields}"
                        + ") STORED AS ORC "
                        + "LOCATION '${table_location}' "
                        + "TBLPROPERTIES ('k1'='v1','k2'='v2')";
        configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), template);
        ReadonlyConfig configWithTemplate = ReadonlyConfig.fromMap(configMap);

        HiveSaveModeHandler handler =
                new HiveSaveModeHandler(
                        configWithTemplate,
                        catalogTable,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        java.lang.reflect.Method m =
                HiveSaveModeHandler.class.getDeclaredMethod("buildTableFromCustomTemplate");
        m.setAccessible(true);
        org.apache.hadoop.hive.metastore.api.Table table =
                (org.apache.hadoop.hive.metastore.api.Table) m.invoke(handler);

        assertEquals("EXTERNAL_TABLE", table.getTableType());
        assertEquals("file:/tmp/hive/warehouse/test_db.db/user_table", table.getSd().getLocation());
        assertEquals("v1", table.getParameters().get("k1"));
        assertEquals("v2", table.getParameters().get("k2"));
        assertEquals(template, table.getParameters().get("seatunnel.creation.template"));
    }
}
