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

package org.apache.seatunnel.connectors.seatunnel.lance.namespace;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.lance.catalog.LanceCatalog;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonOptions;

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
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE;

@DisabledOnOs(OS.WINDOWS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LanceCatalogTest {

    private static final String CATALOG_NAME = "lance";

    private static final String CATALOG_DIR = "/seatunnel/lance/namespace-test/";

    private static final String WAREHOUSE = "file://" + CATALOG_DIR;

    private static LanceCatalog lanceCatalog;

    private static String databaseName = "default";

    private static String tableName = "lance_tb1";

    private TablePath tablePath = TablePath.of(databaseName, null, tableName);

    private TableIdentifier tableIdentifier =
            TableIdentifier.of(CATALOG_NAME, databaseName, null, tableName);

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        // build catalog configs
        configs.put(LanceCommonOptions.KEY_DATASET_PATH.key(), CATALOG_DIR);
        configs.put(LanceCommonOptions.KEY_NAMESPACE_TYPE.key(), "dir");
        configs.put(LanceCommonOptions.KEY_ROOT_NAMESPACE_PATH.key(), "/tmp");

        lanceCatalog = new LanceCatalog(CATALOG_NAME, ReadonlyConfig.fromMap(configs));
        lanceCatalog.open();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        lanceCatalog.close();
    }

    @Test
    @Order(1)
    void createTable() {
        CatalogTable catalogTable = buildAllTypesTable(tableIdentifier);
        lanceCatalog.createTable(tablePath, catalogTable, true);
        Assertions.assertTrue(lanceCatalog.tableExists(tablePath));
    }

    @Test
    @Order(2)
    void listTables() {
        // Directory namespace only supports empty namespace ID
        Assertions.assertTrue(lanceCatalog.listTables("").contains(tableName));
    }

    @Test
    @Order(3)
    void tableExists() {
        Assertions.assertTrue(lanceCatalog.tableExists(tablePath));
        Assertions.assertFalse(lanceCatalog.tableExists(TablePath.of(databaseName, "aaaaaa")));
    }

    @Test
    @Order(4)
    void getTable() {
        CatalogTable table = lanceCatalog.getTable(tablePath);
        CatalogTable templateTable = buildAllTypesTable(tableIdentifier);
        // The getTable() should return the same table structure as created, including primary key
        // and comment
        Assertions.assertEquals(templateTable.toString(), table.toString());
    }

    @Test
    @Order(5)
    void dropTable() {
        lanceCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(lanceCatalog.tableExists(tablePath));
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
        // Note: date type is not fully supported by Lance namespace API, so we skip it
        // builder.column(
        //         PhysicalColumn.of("date_col", LOCAL_DATE_TYPE, (Long) null, true, null, null));
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
        // Note: decimal type is not fully supported by Lance namespace API, so we skip it
        // builder.column(
        //         PhysicalColumn.of(
        //                 "decimal_col", new DecimalType(38, 18), (Long) null, true, null, null));
        builder.column(PhysicalColumn.of("dt_col", STRING_TYPE, (Long) null, true, null, null));
        builder.primaryKey(
                PrimaryKey.of(
                        tableIdentifier.getTableName() + "_pk", Collections.singletonList("id")));

        TableSchema schema = builder.build();
        HashMap<String, String> options = new HashMap<>();
        options.put("comment", "test");
        List<String> partitionsKeys = Lists.newArrayList();
        return CatalogTable.of(tableIdentifier, schema, options, partitionsKeys, "test");
    }
}
