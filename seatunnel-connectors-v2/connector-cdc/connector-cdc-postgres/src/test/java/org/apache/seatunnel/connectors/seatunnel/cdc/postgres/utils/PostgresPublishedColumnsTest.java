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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresPublishedColumnsTest {

    private static final List<String> COLUMNS =
            Arrays.asList("id", "price", "note", "stored_total", "virtual_total");

    @Test
    public void testGeneratedColumnsAreExcludedWhenPublicationDoesNotCoverTable() {
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("stored_total", "virtual_total")),
                PostgresPublishedColumns.excludedColumns(COLUMNS, null, generated(), false));
    }

    @Test
    public void testStoredColumnsAreKeptWhenPublicationPublishesThem() {
        Assertions.assertEquals(
                Collections.singleton("virtual_total"),
                PostgresPublishedColumns.excludedColumns(COLUMNS, null, generated(), true));
    }

    @Test
    public void testPublicationColumnListDecidesWhenTableIsPublished() {
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("note", "virtual_total")),
                PostgresPublishedColumns.excludedColumns(
                        COLUMNS,
                        new HashSet<>(Arrays.asList("id", "price", "stored_total")),
                        generated(),
                        true));
    }

    @Test
    public void testNothingIsExcludedWithoutGeneratedColumns() {
        Assertions.assertTrue(
                PostgresPublishedColumns.excludedColumns(
                                Arrays.asList("id", "price"), null, Collections.emptyMap(), false)
                        .isEmpty());
    }

    @Test
    public void testRetainColumnsRemovesExcludedColumnsAndTheirKeys() {
        CatalogTable table = table();

        CatalogTable retained =
                PostgresPublishedColumns.retainColumns(
                        table,
                        new LinkedHashSet<>(Arrays.asList("stored_total", "virtual_total")),
                        "dbz_publication");

        Assertions.assertEquals(
                Arrays.asList("id", "price", "note"),
                columnNames(retained.getTableSchema().getColumns()));
        Assertions.assertEquals(
                Collections.singletonList("id"),
                retained.getTableSchema().getPrimaryKey().getColumnNames());
        Assertions.assertTrue(retained.getTableSchema().getConstraintKeys().isEmpty());
        Assertions.assertEquals(table.getTableId(), retained.getTableId());
    }

    @Test
    public void testRetainColumnsNeverRemovesPrimaryKeyColumns() {
        CatalogTable table = table();

        Assertions.assertSame(
                table,
                PostgresPublishedColumns.retainColumns(
                        table, Collections.singleton("id"), "dbz_publication"));
    }

    @Test
    public void testPublicationNameDefaultsToDebeziumPublication() {
        Assertions.assertEquals(
                "dbz_publication", PostgresPublishedColumns.publicationName(new HashMap<>()));
        Assertions.assertEquals(
                "orders_pub",
                PostgresPublishedColumns.publicationName(
                        Collections.singletonMap("publication.name", "orders_pub")));
    }

    private static Map<String, String> generated() {
        Map<String, String> generated = new HashMap<>();
        generated.put("stored_total", "s");
        generated.put("virtual_total", "v");
        return generated;
    }

    private static CatalogTable table() {
        TableSchema.Builder builder = TableSchema.builder();
        COLUMNS.forEach(name -> builder.column(column(name)));
        TableSchema schema =
                builder.primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                        "uk_total",
                                        Collections.singletonList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "stored_total", null))))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("Postgres", "db", "public", "orders"),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private static Column column(String name) {
        return PhysicalColumn.of(name, BasicType.INT_TYPE, (Long) null, true, null, null);
    }

    private static List<String> columnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }
}
