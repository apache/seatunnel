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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.yashandb;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class YashanDbCreateTableSqlBuilderTest {

    private static final PrintStream CONSOLE = System.out;

    @Test
    public void testBuild() {
        String dataBaseName = "test_database";
        String tableName = "test_table";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "blob_v",
                                        PrimitiveByteArrayType.INSTANCE,
                                        Long.MAX_VALUE,
                                        true,
                                        null,
                                        "blob_v"))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "createTime"))
                        .column(
                                PhysicalColumn.of(
                                        "lastUpdateTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "lastUpdateTime"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                Arrays.asList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null))),
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "blob_v",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "blob_v", null)))))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        YashanDbCreateTableSqlBuilder builder =
                new YashanDbCreateTableSqlBuilder(catalogTable, true);
        List<String> sqls = builder.build(tablePath);
        String createTableSql = sqls.get(0);

        // YashanDB uses BIGINT for LONG, VARCHAR for STRING, BLOB for bytes, TIMESTAMP for datetime
        String expect =
                "CREATE TABLE \"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR(128) NOT NULL,\n"
                        + "\"age\" INT,\n"
                        + "\"blob_v\" BLOB,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP,\n"
                        + "CONSTRAINT id_9a8b PRIMARY KEY (\"id\")\n"
                        + ")";

        // replace "CONSTRAINT id_xxxx" because it's dynamically generated(random)
        String regex = "id_\\w+";
        String replacedStr1 = createTableSql.replaceAll(regex, "id_");
        String replacedStr2 = expect.replaceAll(regex, "id_");
        CONSOLE.println(replacedStr2);
        Assertions.assertEquals(replacedStr2, replacedStr1);

        Assertions.assertEquals("COMMENT ON TABLE \"test_table\" IS 'User table'", sqls.get(1));

        // skip index
        YashanDbCreateTableSqlBuilder builderSkipIndex =
                new YashanDbCreateTableSqlBuilder(catalogTable, false);
        String createTableSqlSkipIndex = builderSkipIndex.build(tablePath).get(0);
        String expectSkipIndex =
                "CREATE TABLE \"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR(128) NOT NULL,\n"
                        + "\"age\" INT,\n"
                        + "\"blob_v\" BLOB,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP\n"
                        + ")";
        CONSOLE.println(expectSkipIndex);
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }

    @Test
    public void testColumnSinkType() {
        YashanDbCreateTableSqlBuilder sqlBuilder = mock(YashanDbCreateTableSqlBuilder.class);

        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR(10)");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        when(sqlBuilder.buildColumnSql(column)).thenCallRealMethod();

        String result = sqlBuilder.buildColumnSql(column);

        Assertions.assertEquals("\"col1\" VARCHAR(10) NOT NULL", result);
    }

    @Test
    public void testColumnComments() {
        String dataBaseName = "test_db";
        String tableName = "test_table";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, 22, false, null, "primary key"))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        100,
                                        true,
                                        null,
                                        "user name"))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);

        YashanDbCreateTableSqlBuilder builder =
                new YashanDbCreateTableSqlBuilder(catalogTable, false);
        List<String> sqls = builder.build(tablePath);

        // sqls[0] = CREATE TABLE, sqls[1..n] = column comments
        Assertions.assertTrue(sqls.size() >= 3);
        Assertions.assertTrue(sqls.get(1).contains("COMMENT ON COLUMN"));
        Assertions.assertTrue(sqls.get(1).contains("primary key"));
        Assertions.assertTrue(sqls.get(2).contains("COMMENT ON COLUMN"));
        Assertions.assertTrue(sqls.get(2).contains("user name"));
    }

    @Test
    public void testBuildWithBooleanAndFloatColumns() {
        String dataBaseName = "test_db";
        String tableName = "test_types";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "flag",
                                        BasicType.BOOLEAN_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "score",
                                        BasicType.DOUBLE_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "tiny_val",
                                        BasicType.BYTE_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "small_val",
                                        BasicType.SHORT_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);

        YashanDbCreateTableSqlBuilder builder =
                new YashanDbCreateTableSqlBuilder(catalogTable, false);
        List<String> sqls = builder.build(tablePath);
        String createTableSql = sqls.get(0);

        Assertions.assertTrue(createTableSql.contains("\"flag\" BOOLEAN NOT NULL"));
        Assertions.assertTrue(createTableSql.contains("\"score\" DOUBLE"));
        Assertions.assertTrue(createTableSql.contains("\"tiny_val\" TINYINT"));
        Assertions.assertTrue(createTableSql.contains("\"small_val\" SMALLINT"));
    }

    @Test
    public void testBuildWithSameSourceCatalog() {
        String dataBaseName = "test_db";
        String tableName = "test_source";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "col1", BasicType.STRING_TYPE, 100, false, null, null))
                        .build();
        // Simulate same source catalog (YASHANDB)
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("YashanDB", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);

        // Column has sourceType set
        Column columnWithSource =
                PhysicalColumn.builder()
                        .name("col1")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(100L)
                        .nullable(false)
                        .sourceType("VARCHAR2(100)")
                        .build();
        TableSchema schemaWithSource = TableSchema.builder().column(columnWithSource).build();
        CatalogTable catalogTableWithSource =
                CatalogTable.of(
                        TableIdentifier.of("YashanDB", dataBaseName, tableName),
                        schemaWithSource,
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);

        YashanDbCreateTableSqlBuilder builder =
                new YashanDbCreateTableSqlBuilder(catalogTableWithSource, false);
        List<String> sqls = builder.build(tablePath);
        String createTableSql = sqls.get(0);

        // Should use sourceType when sourceCatalogName matches YASHANDB
        Assertions.assertTrue(createTableSql.contains("VARCHAR2(100)"));
    }
}
