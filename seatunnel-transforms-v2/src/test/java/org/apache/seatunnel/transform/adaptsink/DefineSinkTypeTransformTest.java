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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefineSinkTypeTransformTest {

    @Test
    void transformRowReturnsInputRow() {
        CatalogTable table1 =
                CatalogTableUtil.getCatalogTable(
                        "catalog",
                        "db1",
                        "schema1",
                        "table1",
                        new SeaTunnelRowType(
                                new String[] {"col1", "col2"},
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE, BasicType.INT_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        ImmutableMap.of(
                                "columns",
                                Arrays.asList(
                                        ImmutableMap.of("column", "col1", "type", "varchar(10)"))));
        DefineSinkTypeTransformFactory factory = new DefineSinkTypeTransformFactory();
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Arrays.asList(table1),
                        config,
                        Thread.currentThread().getContextClassLoader());
        SeaTunnelMapTransform<SeaTunnelRow> transform =
                (SeaTunnelMapTransform) factory.createTransform(context).createTransform();

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"value1", "value2"});
        inputRow.setTableId(table1.getTablePath().getFullName());
        SeaTunnelRow resultRow = transform.map(inputRow);
        assertEquals(inputRow, resultRow);
    }

    @Test
    void transformTableSchemaUpdatesColumnTypes() {
        CatalogTable table1 =
                CatalogTableUtil.getCatalogTable(
                        "catalog",
                        "db1",
                        "schema1",
                        "table1",
                        new SeaTunnelRowType(
                                new String[] {"col1", "col2"},
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE, BasicType.INT_TYPE
                                }));
        CatalogTable table2 =
                CatalogTableUtil.getCatalogTable(
                        "catalog",
                        "db1",
                        "schema1",
                        "table2",
                        new SeaTunnelRowType(
                                new String[] {"col1", "col2"},
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE, BasicType.INT_TYPE
                                }));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        ImmutableMap.of(
                                "columns",
                                        Arrays.asList(
                                                ImmutableMap.of(
                                                        "column", "col1", "type", "varchar(10)"),
                                                ImmutableMap.of(
                                                        "column", "col2", "type", "integer")),
                                "table_transform",
                                        Arrays.asList(
                                                ImmutableMap.of(
                                                        "table_path",
                                                        "db1.schema1.table2",
                                                        "columns",
                                                        Arrays.asList(
                                                                ImmutableMap.of(
                                                                        "column",
                                                                        "col1",
                                                                        "type",
                                                                        "varchar(11)"))))));
        DefineSinkTypeTransformFactory factory = new DefineSinkTypeTransformFactory();
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Arrays.asList(table1, table2),
                        config,
                        Thread.currentThread().getContextClassLoader());
        SeaTunnelMapTransform<SeaTunnelRow> transform =
                (SeaTunnelMapTransform) factory.createTransform(context).createTransform();
        List<CatalogTable> resultTables = transform.getProducedCatalogTables();
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"value1", "value2"});
        inputRow.setTableId(table1.getTablePath().getFullName());
        SeaTunnelRow resultRow = transform.map(inputRow);
        assertEquals(inputRow, resultRow);
        inputRow = new SeaTunnelRow(new Object[] {"value1", "value2"});
        inputRow.setTableId(table2.getTablePath().getFullName());
        resultRow = transform.map(inputRow);
        assertEquals(inputRow, resultRow);

        assertEquals(
                "varchar(10)",
                resultTables.get(0).getTableSchema().getColumns().get(0).getSinkType());
        assertEquals(
                "integer", resultTables.get(0).getTableSchema().getColumns().get(1).getSinkType());
        assertEquals(
                "varchar(11)",
                resultTables.get(1).getTableSchema().getColumns().get(0).getSinkType());
        assertNull(resultTables.get(1).getTableSchema().getColumns().get(1).getSinkType());
    }

    @Test
    void constructorThrowsExceptionForInvalidColumn() {
        CatalogTable table1 =
                CatalogTableUtil.getCatalogTable(
                        "catalog",
                        "db1",
                        "schema1",
                        "table1",
                        new SeaTunnelRowType(
                                new String[] {"col1", "col2"},
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE, BasicType.INT_TYPE
                                }));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        ImmutableMap.of(
                                "columns",
                                Arrays.asList(
                                        ImmutableMap.of(
                                                "column", "invalid_col", "type", "varchar(10)"))));
        DefineSinkTypeTransformFactory factory = new DefineSinkTypeTransformFactory();
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Arrays.asList(table1),
                        config,
                        Thread.currentThread().getContextClassLoader());

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.createTransform(context).createTransform());
    }
}
