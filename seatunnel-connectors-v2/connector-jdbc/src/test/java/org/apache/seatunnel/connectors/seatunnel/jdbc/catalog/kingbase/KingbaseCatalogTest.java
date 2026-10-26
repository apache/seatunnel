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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

@Disabled("Please Test it in your local environment")
class KingbaseCatalogTest {

    private static final String DATABASE = "test";
    private static final String SCHEMA = "public";
    private static final String SOURCE_TABLE = "st_type_converter_source";
    private static final String TARGET_TABLE = "st_type_converter_target";

    private static KingbaseCatalog catalog;

    @BeforeAll
    static void before() {
        catalog =
                new KingbaseCatalog(
                        "kingbase",
                        "kingbase",
                        "kingbase",
                        JdbcUrlUtil.getUrlInfo("jdbc:kingbase8://192.168.102.101:54321/test"),
                        null,
                        null);
        catalog.open();
    }

    @AfterAll
    static void after() {
        TablePath sourcePath = TablePath.of(DATABASE, SCHEMA, SOURCE_TABLE);
        TablePath targetPath = TablePath.of(DATABASE, SCHEMA, TARGET_TABLE);
        dropTableIfExists(targetPath);
        dropTableIfExists(sourcePath);
        catalog.close();
    }

    @Test
    void databaseExists() {
        Assertions.assertTrue(catalog.databaseExists(DATABASE));
    }

    @Test
    void createTableFromSource() {
        TablePath sourcePath = TablePath.of(DATABASE, SCHEMA, SOURCE_TABLE);
        TablePath targetPath = TablePath.of(DATABASE, SCHEMA, TARGET_TABLE);

        dropTableIfExists(targetPath);
        dropTableIfExists(sourcePath);

        catalog.executeSql(sourcePath, buildCreateTableSql(sourcePath));
        Assertions.assertTrue(catalog.tableExists(sourcePath));

        CatalogTable sourceTable = catalog.getTable(sourcePath);
        catalog.createTable(targetPath, sourceTable, true);
        Assertions.assertTrue(catalog.tableExists(targetPath));
    }

    private static void dropTableIfExists(TablePath tablePath) {
        if (catalog.tableExists(tablePath)) {
            catalog.dropTable(tablePath, true);
        }
    }

    private static String buildCreateTableSql(TablePath tablePath) {
        List<String> columns =
                Lists.newArrayList(
                        "\"id\" BIGSERIAL PRIMARY KEY",
                        "\"c_smallserial\" SMALLSERIAL",
                        "\"c_serial\" SERIAL",
                        "\"c_tinyint\" TINYINT",
                        "\"c_bool\" BOOL",
                        "\"c_int2\" INT2",
                        "\"c_int4\" INT4",
                        "\"c_int8\" INT8",
                        "\"c_float4\" FLOAT4",
                        "\"c_float8\" FLOAT8",
                        "\"c_numeric\" NUMERIC(38,18)",
                        "\"c_money\" MONEY",
                        "\"c_bytea\" BYTEA",
                        "\"c_blob\" BLOB",
                        "\"c_clob\" CLOB",
                        "\"c_bit\" BIT(16)",
                        "\"c_char\" CHARACTER(10)",
                        "\"c_bpchar\" BPCHAR(10)",
                        "\"c_varchar\" VARCHAR(255)",
                        "\"c_text\" TEXT",
                        "\"c_date\" DATE",
                        "\"c_time\" TIME",
                        "\"c_timestamp\" TIMESTAMP",
                        "\"c_timestamptz\" TIMESTAMPTZ",
                        "\"c_uuid\" UUID",
                        "\"c_json\" JSON",
                        "\"c_jsonb\" JSONB");

        return "CREATE TABLE "
                + tablePath.getSchemaAndTableName("\"")
                + " (\n"
                + String.join(",\n", columns)
                + "\n);";
    }
}
