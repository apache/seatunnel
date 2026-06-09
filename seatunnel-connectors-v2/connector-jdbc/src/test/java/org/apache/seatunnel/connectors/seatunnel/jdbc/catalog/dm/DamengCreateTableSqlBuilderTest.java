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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DamengCreateTableSqlBuilderTest {

    @Test
    public void testCreateTableSqlsWithComments() {
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        List<String> sqls = new DamengCreateTableSqlBuilder(catalogTable, true).build(tablePath);

        // 1 CREATE TABLE + 3 COMMENT ON COLUMN
        Assertions.assertEquals(4, sqls.size());

        String createTable = sqls.get(0);
        Assertions.assertTrue(
                createTable.startsWith("CREATE TABLE \"test_schema\".\"test_table\" ("),
                "First element should be CREATE TABLE");
        Assertions.assertTrue(
                createTable.contains("\"id\" BIGINT NOT NULL"),
                "CREATE TABLE should contain id column");
        Assertions.assertTrue(
                createTable.contains("\"name\" VARCHAR2(128) NOT NULL"),
                "CREATE TABLE should contain name column");
        Assertions.assertTrue(
                createTable.contains("\"age\" INT"), "CREATE TABLE should contain age column");

        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id'", sqls.get(1));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"name\" IS 'name'", sqls.get(2));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"age\" IS 'age'", sqls.get(3));
    }

    @Test
    public void testCreateTableSqlsSkipIndex() {
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        List<String> sqls = new DamengCreateTableSqlBuilder(catalogTable, false).build(tablePath);

        // 1 CREATE TABLE + 2 COMMENT ON COLUMN
        Assertions.assertEquals(3, sqls.size());
        Assertions.assertTrue(sqls.get(0).startsWith("CREATE TABLE"));

        // With createIndex=false, no CONSTRAINT should appear
        Assertions.assertFalse(sqls.get(0).contains("CONSTRAINT"));
    }

    @Test
    public void testCommentWithSemicolonNotSplit() {
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "col1",
                                        BasicType.STRING_TYPE,
                                        64,
                                        true,
                                        null,
                                        "comment with a;b semicolon"))
                        .column(
                                PhysicalColumn.of(
                                        "col2",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "normal comment"))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "Test table");

        List<String> sqls = new DamengCreateTableSqlBuilder(catalogTable, false).build(tablePath);

        // 1 CREATE TABLE + 2 COMMENT ON COLUMN
        Assertions.assertEquals(
                3, sqls.size(), "Should have exactly 3 statements: 1 CREATE TABLE + 2 COMMENTs");

        // The comment with semicolon must remain intact as a single statement
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"col1\" IS 'comment with a;b semicolon'",
                sqls.get(1),
                "Comment containing ';' must not be split");
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"col2\" IS 'normal comment'",
                sqls.get(2));
    }

    @Test
    public void testNoCommentColumns() {
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, null))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "Table without comments");

        List<String> sqls = new DamengCreateTableSqlBuilder(catalogTable, false).build(tablePath);

        // Only CREATE TABLE, no COMMENT statements
        Assertions.assertEquals(1, sqls.size());
        Assertions.assertTrue(sqls.get(0).startsWith("CREATE TABLE"));
    }

    @Test
    public void testColumnSinkType() {
        DamengCreateTableSqlBuilder sqlBuilder = mock(DamengCreateTableSqlBuilder.class);

        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR(10)");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        when(sqlBuilder.buildColumnSql(column)).thenCallRealMethod();

        String result = sqlBuilder.buildColumnSql(column);

        Assertions.assertEquals("\"col1\" VARCHAR(10) NOT NULL", result);
    }
}
