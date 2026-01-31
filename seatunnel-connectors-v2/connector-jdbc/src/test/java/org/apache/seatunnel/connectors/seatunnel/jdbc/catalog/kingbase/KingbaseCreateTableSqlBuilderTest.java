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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

class KingbaseCreateTableSqlBuilderTest {

    @Test
    void testBuildWithKingbaseCatalog() {
        TablePath tablePath = TablePath.of("test", "public", "test_table");

        CatalogTable catalogTable = kingbaseCatalogTable(tablePath);
        String createTableSql =
                new KingbaseCreateTableSqlBuilder(catalogTable, true).build(tablePath);
        String expectedSql = buildExpectedSql(tablePath, true);

        Assertions.assertEquals(
                expectedSql.replaceAll("pk_id_\\w+", "pk_id_"),
                createTableSql.replaceAll("pk_id_\\w+", "pk_id_"));

        String createTableSqlSkipIndex =
                new KingbaseCreateTableSqlBuilder(catalogTable, false).build(tablePath);
        String expectedSqlSkipIndex = buildExpectedSql(tablePath, false);
        Assertions.assertEquals(expectedSqlSkipIndex, createTableSqlSkipIndex);
    }

    @Test
    void testBuildWithOtherCatalog() {
        TablePath tablePath = TablePath.of("test_database", "public", "st_type_converter_test");

        CatalogTable catalogTable = otherCatalogTable(tablePath);
        String createTableSql =
                new KingbaseCreateTableSqlBuilder(catalogTable, true).build(tablePath);
        String expectedSql = buildExpectedSqlFromOtherCatalog(tablePath, true);

        Assertions.assertEquals(
                expectedSql.replaceAll("pk_id_\\w+", "pk_id_"),
                createTableSql.replaceAll("pk_id_\\w+", "pk_id_"));

        String createTableSqlSkipIndex =
                new KingbaseCreateTableSqlBuilder(catalogTable, false).build(tablePath);
        String expectedSqlSkipIndex = buildExpectedSqlFromOtherCatalog(tablePath, false);
        Assertions.assertEquals(expectedSqlSkipIndex, createTableSqlSkipIndex);
    }

    private CatalogTable kingbaseCatalogTable(TablePath tablePath) {
        List<Column> columns =
                Lists.newArrayList(
                        PhysicalColumn.of(
                                "id",
                                BasicType.LONG_TYPE,
                                null,
                                false,
                                null,
                                "id",
                                "BIGSERIAL",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_smallserial",
                                BasicType.SHORT_TYPE,
                                null,
                                true,
                                null,
                                "c_smallserial",
                                "SMALLSERIAL",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_serial",
                                BasicType.INT_TYPE,
                                null,
                                true,
                                null,
                                "c_serial",
                                "SERIAL",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_tinyint",
                                BasicType.BYTE_TYPE,
                                null,
                                true,
                                null,
                                "c_tinyint",
                                "TINYINT",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_bool",
                                BasicType.BOOLEAN_TYPE,
                                null,
                                true,
                                null,
                                "c_bool",
                                "BOOL",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_int2",
                                BasicType.SHORT_TYPE,
                                null,
                                true,
                                null,
                                "c_int2",
                                "INT2",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_int4",
                                BasicType.INT_TYPE,
                                null,
                                true,
                                null,
                                "c_int4",
                                "INT4",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_int8",
                                BasicType.LONG_TYPE,
                                null,
                                true,
                                null,
                                "c_int8",
                                "INT8",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_float4",
                                BasicType.FLOAT_TYPE,
                                null,
                                true,
                                null,
                                "c_float4",
                                "FLOAT4",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_float8",
                                BasicType.DOUBLE_TYPE,
                                null,
                                true,
                                null,
                                "c_float8",
                                "FLOAT8",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_numeric",
                                new DecimalType(38, 18),
                                38L,
                                18,
                                true,
                                null,
                                "c_numeric",
                                "NUMERIC(38,18)",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_money",
                                new DecimalType(38, 18),
                                38L,
                                18,
                                true,
                                null,
                                "c_money",
                                "MONEY",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_bytea",
                                PrimitiveByteArrayType.INSTANCE,
                                null,
                                true,
                                null,
                                "c_bytea",
                                "BYTEA",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_blob",
                                PrimitiveByteArrayType.INSTANCE,
                                null,
                                true,
                                null,
                                "c_blob",
                                "BLOB",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_clob",
                                BasicType.STRING_TYPE,
                                null,
                                true,
                                null,
                                "c_clob",
                                "CLOB",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_bit",
                                PrimitiveByteArrayType.INSTANCE,
                                16L,
                                true,
                                null,
                                "c_bit",
                                "BIT(16)",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_char",
                                BasicType.STRING_TYPE,
                                10L,
                                true,
                                null,
                                "c_char",
                                "CHARACTER(10)",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_bpchar",
                                BasicType.STRING_TYPE,
                                10L,
                                true,
                                null,
                                "c_bpchar",
                                "BPCHAR(10)",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_varchar",
                                BasicType.STRING_TYPE,
                                255L,
                                true,
                                null,
                                "c_varchar",
                                "VARCHAR(255)",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_text",
                                BasicType.STRING_TYPE,
                                null,
                                true,
                                null,
                                "c_text",
                                "TEXT",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_date",
                                LocalTimeType.LOCAL_DATE_TYPE,
                                null,
                                true,
                                null,
                                "c_date",
                                "DATE",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_time",
                                LocalTimeType.LOCAL_TIME_TYPE,
                                null,
                                true,
                                null,
                                "c_time",
                                "TIME",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_timestamp",
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                null,
                                true,
                                null,
                                "c_timestamp",
                                "TIMESTAMP",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_timestamptz",
                                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                null,
                                true,
                                null,
                                "c_timestamptz",
                                "TIMESTAMPTZ",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_uuid",
                                BasicType.STRING_TYPE,
                                null,
                                true,
                                null,
                                "c_uuid",
                                "UUID",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_json",
                                BasicType.STRING_TYPE,
                                null,
                                true,
                                null,
                                "c_json",
                                "JSON",
                                Collections.emptyMap()),
                        PhysicalColumn.of(
                                "c_jsonb",
                                BasicType.STRING_TYPE,
                                null,
                                true,
                                null,
                                "c_jsonb",
                                "JSONB",
                                Collections.emptyMap()));

        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(PrimaryKey.of("pk_id", Lists.newArrayList("id")))
                        .build();

        return CatalogTable.of(
                TableIdentifier.of(DatabaseIdentifier.KINGBASE, tablePath),
                tableSchema,
                new HashMap<>(),
                Lists.newArrayList(),
                "test table");
    }

    private CatalogTable otherCatalogTable(TablePath tablePath) {
        List<Column> columns =
                Lists.newArrayList(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, "id"),
                        PhysicalColumn.of(
                                "c_bool",
                                BasicType.BOOLEAN_TYPE,
                                (Long) null,
                                false,
                                null,
                                "c_bool"),
                        PhysicalColumn.of(
                                "c_int2", BasicType.SHORT_TYPE, (Long) null, true, null, "c_int2"),
                        PhysicalColumn.of(
                                "c_int4", BasicType.INT_TYPE, (Long) null, true, null, "c_int4"),
                        PhysicalColumn.of(
                                "c_int8", BasicType.LONG_TYPE, (Long) null, true, null, "c_int8"),
                        PhysicalColumn.of(
                                "c_float4",
                                BasicType.FLOAT_TYPE,
                                (Long) null,
                                true,
                                null,
                                "c_float4"),
                        PhysicalColumn.of(
                                "c_float8",
                                BasicType.DOUBLE_TYPE,
                                (Long) null,
                                true,
                                null,
                                "c_float8"),
                        PhysicalColumn.of(
                                "c_numeric",
                                new DecimalType(38, 18),
                                38L,
                                18,
                                true,
                                null,
                                "c_numeric"),
                        PhysicalColumn.of(
                                "c_bytea",
                                PrimitiveByteArrayType.INSTANCE,
                                (Long) null,
                                true,
                                null,
                                "c_bytea"),
                        PhysicalColumn.of(
                                "c_varchar", BasicType.STRING_TYPE, 255L, true, null, "c_varchar"),
                        PhysicalColumn.of(
                                "c_text", BasicType.STRING_TYPE, (Long) null, true, null, "c_text"),
                        PhysicalColumn.of(
                                "c_date",
                                LocalTimeType.LOCAL_DATE_TYPE,
                                (Long) null,
                                true,
                                null,
                                "c_date"),
                        PhysicalColumn.of(
                                "c_time",
                                LocalTimeType.LOCAL_TIME_TYPE,
                                (Long) null,
                                true,
                                null,
                                "c_time"),
                        PhysicalColumn.of(
                                "c_timestamp",
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                (Long) null,
                                true,
                                null,
                                "c_timestamp"),
                        PhysicalColumn.of(
                                "c_timestamptz",
                                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                (Long) null,
                                true,
                                null,
                                "c_timestamptz"));

        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(PrimaryKey.of("pk_id", Lists.newArrayList("id")))
                        .build();

        return CatalogTable.of(
                TableIdentifier.of(DatabaseIdentifier.MYSQL, tablePath),
                tableSchema,
                new HashMap<>(),
                Lists.newArrayList(),
                "test table");
    }

    private String buildExpectedSql(TablePath tablePath, boolean includePrimaryKey) {
        List<String> columnSqls =
                Lists.newArrayList(
                        "\"id\" BIGSERIAL NOT NULL",
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

        if (includePrimaryKey) {
            columnSqls.add("CONSTRAINT pk_id_ PRIMARY KEY (\"id\")");
        }

        List<String> commentSqls =
                Lists.newArrayList(
                        commentSql(tablePath, "id"),
                        commentSql(tablePath, "c_smallserial"),
                        commentSql(tablePath, "c_serial"),
                        commentSql(tablePath, "c_tinyint"),
                        commentSql(tablePath, "c_bool"),
                        commentSql(tablePath, "c_int2"),
                        commentSql(tablePath, "c_int4"),
                        commentSql(tablePath, "c_int8"),
                        commentSql(tablePath, "c_float4"),
                        commentSql(tablePath, "c_float8"),
                        commentSql(tablePath, "c_numeric"),
                        commentSql(tablePath, "c_money"),
                        commentSql(tablePath, "c_bytea"),
                        commentSql(tablePath, "c_blob"),
                        commentSql(tablePath, "c_clob"),
                        commentSql(tablePath, "c_bit"),
                        commentSql(tablePath, "c_char"),
                        commentSql(tablePath, "c_bpchar"),
                        commentSql(tablePath, "c_varchar"),
                        commentSql(tablePath, "c_text"),
                        commentSql(tablePath, "c_date"),
                        commentSql(tablePath, "c_time"),
                        commentSql(tablePath, "c_timestamp"),
                        commentSql(tablePath, "c_timestamptz"),
                        commentSql(tablePath, "c_uuid"),
                        commentSql(tablePath, "c_json"),
                        commentSql(tablePath, "c_jsonb"));

        return "CREATE TABLE "
                + tablePath.getSchemaAndTableName("\"")
                + " (\n"
                + String.join(",\n", columnSqls)
                + "\n);\n"
                + String.join(";\n", commentSqls);
    }

    private String buildExpectedSqlFromOtherCatalog(
            TablePath tablePath, boolean includePrimaryKey) {
        List<String> columnSqls =
                Lists.newArrayList(
                        "\"id\" int8 NOT NULL",
                        "\"c_bool\" bool NOT NULL",
                        "\"c_int2\" int2",
                        "\"c_int4\" int4",
                        "\"c_int8\" int8",
                        "\"c_float4\" float4",
                        "\"c_float8\" float8",
                        "\"c_numeric\" numeric(38,18)",
                        "\"c_bytea\" bytea",
                        "\"c_varchar\" varchar(255)",
                        "\"c_text\" text",
                        "\"c_date\" date",
                        "\"c_time\" time",
                        "\"c_timestamp\" timestamp",
                        "\"c_timestamptz\" timestamptz");

        if (includePrimaryKey) {
            columnSqls.add("CONSTRAINT pk_id_ PRIMARY KEY (\"id\")");
        }

        List<String> commentSqls =
                Lists.newArrayList(
                        commentSql(tablePath, "id"),
                        commentSql(tablePath, "c_bool"),
                        commentSql(tablePath, "c_int2"),
                        commentSql(tablePath, "c_int4"),
                        commentSql(tablePath, "c_int8"),
                        commentSql(tablePath, "c_float4"),
                        commentSql(tablePath, "c_float8"),
                        commentSql(tablePath, "c_numeric"),
                        commentSql(tablePath, "c_bytea"),
                        commentSql(tablePath, "c_varchar"),
                        commentSql(tablePath, "c_text"),
                        commentSql(tablePath, "c_date"),
                        commentSql(tablePath, "c_time"),
                        commentSql(tablePath, "c_timestamp"),
                        commentSql(tablePath, "c_timestamptz"));

        return "CREATE TABLE "
                + tablePath.getSchemaAndTableName("\"")
                + " (\n"
                + String.join(",\n", columnSqls)
                + "\n);\n"
                + String.join(";\n", commentSqls);
    }

    private String commentSql(TablePath tablePath, String columnName) {
        return "COMMENT ON COLUMN "
                + tablePath.getSchemaAndTableName("\"")
                + ".\""
                + columnName
                + "\" IS '"
                + columnName
                + "'";
    }
}
