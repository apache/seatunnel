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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClickhouseCatalogUtilTest {
    @Test
    void returnsReconvertedTypeWhenSinkTypeNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn("String");
        when(column.isNullable()).thenReturn(false);
        when(column.getComment()).thenReturn("");

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` String ", result);
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeIsNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.isNullable()).thenReturn(false);
        when(column.getComment()).thenReturn("");

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` Int32 ", result);
    }

    @Test
    void returnsReconvertedTypeWhenTypesNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getSinkType()).thenReturn("String");
        when(column.isNullable()).thenReturn(false);
        when(column.getComment()).thenReturn("");

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` String ", result);
    }

    @Test
    void wrapsTypeWithNullableWhenColumnIsNullable() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn("String");
        when(column.isNullable()).thenReturn(true);
        when(column.getComment()).thenReturn("");

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` Nullable(String) ", result);
    }

    @Test
    void escapesSingleQuoteAndBackslashInComment() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn("String");
        when(column.isNullable()).thenReturn(false);
        when(column.getComment()).thenReturn("O'Reilly \\ path");

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` String COMMENT 'O''Reilly \\\\ path'", result);
    }

    @Test
    void throwsExceptionWhenColumnIsNull() {
        assertThrows(
                NullPointerException.class,
                () -> ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(null));
    }

    @Test
    void testPrimaryKeyColumnShouldNotBeNullable() {
        // Test that ThreadLocal is properly cleared after getCreateTableSql call
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("pk_column");
        when(column.getSinkType()).thenReturn("String");
        when(column.isNullable()).thenReturn(true);
        when(column.getComment()).thenReturn("");

        List<Column> columns = new ArrayList<>();
        columns.add(column);

        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("", Collections.singletonList("pk_column")))
                        .columns(columns)
                        .build();

        ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                "CREATE TABLE `${database}`.`${table}` (${rowtype_fields})",
                "test_db",
                "test_table",
                tableSchema,
                null,
                ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());

        // After getCreateTableSql call, ThreadLocal should be cleared
        // so columnToConnectorType should treat it as NOT a primary key
        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);
        assertEquals("`pk_column` Nullable(String) ", result);
    }

    @Test
    void testPrimaryKeyColumnWithNullableShouldNotWrapInNullable() {
        // Test the actual scenario: primary key columns should NOT be wrapped in Nullable
        // because ClickHouse doesn't allow nullable columns in ORDER BY / PRIMARY KEY
        String template =
                "CREATE TABLE `${database}`.`${table}` (\n"
                        + "    ${rowtype_primary_key},\n"
                        + "    ${rowtype_fields}\n"
                        + ") ENGINE = MergeTree()\n"
                        + "ORDER BY (${rowtype_primary_key})";

        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, true, null, ""));

        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("", Arrays.asList("id", "age")))
                        .columns(columns)
                        .build();

        String sql =
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        template,
                        "test_db",
                        "test_table",
                        tableSchema,
                        null,
                        ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());

        // Primary key columns (id, age) should NOT be wrapped in Nullable
        assertEquals(true, sql.contains("`id` Int64 "));
        assertEquals(true, sql.contains("`age` Int32 "));
        // Non-primary key column (name) should be wrapped in Nullable
        assertEquals(true, sql.contains("`name` Nullable(String) "));
    }
}
