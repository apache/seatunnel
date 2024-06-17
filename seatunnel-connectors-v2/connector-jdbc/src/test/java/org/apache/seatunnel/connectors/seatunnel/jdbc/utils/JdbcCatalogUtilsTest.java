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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcCatalogUtilsTest {
    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("mysql-1", "database-x", null, "table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            false,
                                            null,
                                            null,
                                            "int unsigned",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.STRING_TYPE,
                                            10,
                                            false,
                                            null,
                                            null,
                                            "varchar(10)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.STRING_TYPE,
                                            20,
                                            false,
                                            null,
                                            null,
                                            "varchar(20)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    public void testColumnEqualsMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f2",
                                                BasicType.STRING_TYPE,
                                                10,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);
        Assertions.assertEquals(DEFAULT_TABLE.getTableId(), mergeTable.getTableId());
        Assertions.assertEquals(DEFAULT_TABLE.getOptions(), mergeTable.getOptions());
        Assertions.assertEquals(DEFAULT_TABLE.getComment(), mergeTable.getComment());
        Assertions.assertEquals(DEFAULT_TABLE.getCatalogName(), mergeTable.getCatalogName());
        Assertions.assertNotEquals(DEFAULT_TABLE.getTableSchema(), mergeTable.getTableSchema());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getConstraintKeys(),
                mergeTable.getTableSchema().getConstraintKeys());

        Map<String, Column> columnMap =
                DEFAULT_TABLE.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(e -> e.getName(), e -> e));
        List<Column> sortByQueryColumns =
                tableOfQuery.getTableSchema().getColumns().stream()
                        .map(e -> columnMap.get(e.getName()))
                        .collect(Collectors.toList());
        Assertions.assertEquals(sortByQueryColumns, mergeTable.getTableSchema().getColumns());
    }

    @Test
    public void testColumnIncludeMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);

        Assertions.assertEquals(DEFAULT_TABLE.getTableId(), mergeTable.getTableId());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getColumns().stream()
                        .filter(c -> Arrays.asList("f1", "f3").contains(c.getName()))
                        .collect(Collectors.toList()),
                mergeTable.getTableSchema().getColumns());
        Assertions.assertTrue(mergeTable.getPartitionKeys().isEmpty());
        Assertions.assertTrue(mergeTable.getTableSchema().getConstraintKeys().isEmpty());
    }

    @Test
    public void testColumnNotIncludeMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f2",
                                                BasicType.STRING_TYPE,
                                                10,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f4",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);

        Assertions.assertEquals(
                DEFAULT_TABLE.getTableId().toTablePath(), mergeTable.getTableId().toTablePath());
        Assertions.assertEquals(DEFAULT_TABLE.getPartitionKeys(), mergeTable.getPartitionKeys());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getConstraintKeys(),
                mergeTable.getTableSchema().getConstraintKeys());

        Assertions.assertEquals(
                tableOfQuery.getTableId().getCatalogName(),
                mergeTable.getTableId().getCatalogName());
        Assertions.assertEquals(
                tableOfQuery.getTableSchema().getColumns(),
                mergeTable.getTableSchema().getColumns());
    }
}
