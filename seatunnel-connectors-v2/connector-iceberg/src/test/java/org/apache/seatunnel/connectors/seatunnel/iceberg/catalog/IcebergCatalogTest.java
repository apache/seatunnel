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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;

@DisabledOnOs(OS.WINDOWS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergCatalogTest {
    private static final String CATALOG_NAME = "seatunnel";
    private static final IcebergCatalogType CATALOG_TYPE = HADOOP;
    private static final String CATALOG_DIR = "/tmp/seatunnel/iceberg/hadoop-test/";
    private static final String WAREHOUSE = "file://" + CATALOG_DIR;

    private static IcebergCatalog icebergCatalog;

    private static String databaseName = "default";
    private static String tableName = "tbl6";

    private TablePath tablePath = TablePath.of(databaseName, null, tableName);
    private TableIdentifier tableIdentifier =
            TableIdentifier.of(CATALOG_NAME, databaseName, null, tableName);

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        // build catalog props
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", CATALOG_TYPE.getType());
        catalogProps.put("warehouse", WAREHOUSE);
        configs.put(CommonConfig.KEY_CATALOG_NAME.key(), CATALOG_NAME);
        configs.put(CommonConfig.CATALOG_PROPS.key(), catalogProps);
        configs.put(SinkConfig.TABLE_DEFAULT_PARTITION_KEYS.key(), "dt_col");
        // hadoop config directory
        configs.put(CommonConfig.HADOOP_CONF_PATH_PROP.key(), "/tmp/hadoop/conf");
        // hadoop kerberos config
        //        configs.put(CommonConfig.KERBEROS_PRINCIPAL.key(), "hive/xxxx@xxxx.COM");
        //        configs.put(
        //                CommonConfig.KERBEROS_KEYTAB_PATH.key(),
        // "/tmp/hadoop/conf/hive.service.keytab");
        //        configs.put(CommonConfig.KRB5_PATH.key(), "/tmp/hadoop/conf/krb5.conf");
        icebergCatalog = new IcebergCatalog(CATALOG_NAME, ReadonlyConfig.fromMap(configs));
        icebergCatalog.open();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        icebergCatalog.close();
    }

    @Test
    @Order(1)
    void getDefaultDatabase() {
        Assertions.assertEquals(icebergCatalog.getDefaultDatabase(), databaseName);
    }

    @Test
    @Order(2)
    void createTable() {
        CatalogTable catalogTable = buildAllTypesTable(tableIdentifier);
        icebergCatalog.createTable(tablePath, catalogTable, true);
        Assertions.assertTrue(icebergCatalog.tableExists(tablePath));
    }

    @Test
    @Order(3)
    void databaseExists() {
        Assertions.assertTrue(icebergCatalog.databaseExists(databaseName));
        Assertions.assertFalse(icebergCatalog.databaseExists("sssss"));
    }

    @Test
    @Order(4)
    void listDatabases() {
        icebergCatalog.listDatabases().forEach(System.out::println);
        Assertions.assertTrue(icebergCatalog.listDatabases().contains(databaseName));
    }

    @Test
    @Order(5)
    void listTables() {
        Assertions.assertTrue(icebergCatalog.listTables(databaseName).contains(tableName));
    }

    @Test
    @Order(6)
    void tableExists() {
        Assertions.assertTrue(icebergCatalog.tableExists(tablePath));
        Assertions.assertFalse(icebergCatalog.tableExists(TablePath.of(databaseName, "ssssss")));
    }

    @Test
    @Order(7)
    void getTable() {
        CatalogTable table = icebergCatalog.getTable(tablePath);
        CatalogTable templateTable = buildAllTypesTable(tableIdentifier);
        Assertions.assertEquals(table.toString(), templateTable.toString());
    }

    @Test
    @Order(8)
    void dropTable() {
        icebergCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(icebergCatalog.tableExists(tablePath));
    }

    CatalogTable buildAllTypesTable(TableIdentifier tableIdentifier) {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(
                PhysicalColumn.of(
                        "id", BasicType.INT_TYPE, (Long) null, false, null, "id comment"));
        builder.column(
                PhysicalColumn.of(
                        "boolean_col", BasicType.BOOLEAN_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of(
                        "integer_col", BasicType.INT_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of("long_col", BasicType.LONG_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of(
                        "float_col", BasicType.FLOAT_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of(
                        "double_col", BasicType.DOUBLE_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of("date_col", LOCAL_DATE_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of(
                        "timestamp_col", LOCAL_DATE_TIME_TYPE, (Long) null, true, null, null));
        builder.column(PhysicalColumn.of("string_col", STRING_TYPE, (Long) null, true, null, null));
        builder.column(
                PhysicalColumn.of(
                        "binary_col",
                        PrimitiveByteArrayType.INSTANCE,
                        (Long) null,
                        true,
                        null,
                        null));
        builder.column(
                PhysicalColumn.of(
                        "decimal_col", new DecimalType(38, 18), (Long) null, true, null, null));
        builder.column(PhysicalColumn.of("dt_col", STRING_TYPE, (Long) null, true, null, null));
        builder.primaryKey(
                PrimaryKey.of(
                        tableIdentifier.getTableName() + "_pk", Collections.singletonList("id")));

        TableSchema schema = builder.build();
        HashMap<String, String> options = new HashMap<>();
        options.put("write.parquet.compression-codec", "zstd");
        return CatalogTable.of(
                tableIdentifier, schema, options, Collections.singletonList("dt_col"), "null");
    }
}
