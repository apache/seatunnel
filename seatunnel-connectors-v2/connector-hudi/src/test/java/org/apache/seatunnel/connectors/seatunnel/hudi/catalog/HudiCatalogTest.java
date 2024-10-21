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

package org.apache.seatunnel.connectors.seatunnel.hudi.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TYPE;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled
class HudiCatalogTest {
    private static final String CATALOG_NAME = "seatunnel";
    private static final String CATALOG_DIR = "/tmp/seatunnel/hudi";

    private static HudiCatalog hudicatalog;

    private static final String DATABASE = "st";
    private static final String DEFAULT_DATABASE = "default";
    private static final String TABLE_NAME = "hudi_test";

    private final TablePath tablePath = TablePath.of(DATABASE, null, TABLE_NAME);
    private final TableIdentifier tableIdentifier =
            TableIdentifier.of(CATALOG_NAME, DATABASE, null, TABLE_NAME);

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        hudicatalog = new HudiCatalog(CATALOG_NAME, new Configuration(), CATALOG_DIR);
        hudicatalog.open();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        hudicatalog.close();
    }

    @Test
    @Order(1)
    void getDefaultDatabase() {
        Assertions.assertEquals(hudicatalog.getDefaultDatabase(), DEFAULT_DATABASE);
        Assertions.assertTrue(hudicatalog.databaseExists(DEFAULT_DATABASE));
    }

    @Test
    @Order(2)
    void createTable() {
        CatalogTable catalogTable = buildAllTypesTable(tableIdentifier);
        hudicatalog.createTable(tablePath, catalogTable, true);
        Assertions.assertTrue(hudicatalog.tableExists(tablePath));
    }

    @Test
    @Order(3)
    void databaseExists() {
        Assertions.assertTrue(hudicatalog.databaseExists(DATABASE));
        Assertions.assertFalse(hudicatalog.databaseExists("st_not_exists"));
    }

    @Test
    @Order(4)
    void listDatabases() {
        hudicatalog.listDatabases().forEach(System.out::println);
        Assertions.assertTrue(hudicatalog.listDatabases().contains(DATABASE));
        Assertions.assertTrue(hudicatalog.listDatabases().contains(DEFAULT_DATABASE));
    }

    @Test
    @Order(5)
    void listTables() {
        Assertions.assertTrue(hudicatalog.listTables(DATABASE).contains(TABLE_NAME));
    }

    @Test
    @Order(6)
    void tableExists() {
        Assertions.assertTrue(hudicatalog.tableExists(tablePath));
        Assertions.assertFalse(hudicatalog.tableExists(TablePath.of(DATABASE, "ssssss")));
    }

    @Test
    @Order(7)
    void getTable() {
        CatalogTable table = hudicatalog.getTable(tablePath);
        CatalogTable templateTable = buildAllTypesTable(tableIdentifier);
        Assertions.assertEquals(table.toString(), templateTable.toString());
    }

    @Test
    @Order(8)
    void dropTable() {
        hudicatalog.dropTable(tablePath, false);
        Assertions.assertFalse(hudicatalog.tableExists(tablePath));
    }

    CatalogTable buildAllTypesTable(TableIdentifier tableIdentifier) {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("id", BasicType.INT_TYPE, (Long) null, true, null, null));
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

        TableSchema schema = builder.build();
        HashMap<String, String> options = new HashMap<>();
        options.put("record_key_fields", "id,boolean_col");
        options.put("cdc_enabled", "false");
        options.put("table_type", "MERGE_ON_READ");
        return CatalogTable.of(
                tableIdentifier, schema, options, Collections.singletonList("dt_col"), "null");
    }
}
