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

package org.apache.seatunnel.transform.validator;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataValidatorTransformTest {

    private static ReadonlyConfig routeToTableConfig(String errorTableName) {
        return ReadonlyConfig.fromMap(
                ImmutableMap.of(
                        "row_error_handle_way",
                        "ROUTE_TO_TABLE",
                        "row_error_handle_way.error_table",
                        errorTableName,
                        "field_rules",
                        Arrays.asList(
                                ImmutableMap.of("field_name", "name", "rule_type", "NOT_NULL"))));
    }

    @Test
    void routeToTableShouldUseSameDatabaseInErrorRowTableId() {
        SeaTunnelRowType inputRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable inputCatalogTable =
                CatalogTableUtil.getCatalogTable("catalog", "db1", null, "source", inputRowType);

        DataValidatorTransform transform =
                new DataValidatorTransform(routeToTableConfig("ffp"), inputCatalogTable);

        SeaTunnelRow invalidRow = new SeaTunnelRow(new Object[] {1, null});
        SeaTunnelRow routedRow = transform.map(invalidRow);

        assertEquals("db1.ffp", routedRow.getTableId());

        List<CatalogTable> producedTables = transform.getProducedCatalogTables();
        assertEquals(2, producedTables.size());
        assertEquals("db1.source", producedTables.get(0).getTablePath().toString());
        assertEquals("db1.ffp", producedTables.get(1).getTablePath().toString());
    }

    @Test
    void routeToTableShouldPreserveSchemaInErrorRowTableId() {
        SeaTunnelRowType inputRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable inputCatalogTable =
                CatalogTableUtil.getCatalogTable(
                        "catalog", "db1", "schema1", "source", inputRowType);

        DataValidatorTransform transform =
                new DataValidatorTransform(routeToTableConfig("ffp"), inputCatalogTable);

        SeaTunnelRow invalidRow = new SeaTunnelRow(new Object[] {1, null});
        SeaTunnelRow routedRow = transform.map(invalidRow);

        assertEquals("db1.schema1.ffp", routedRow.getTableId());

        List<CatalogTable> producedTables = transform.getProducedCatalogTables();
        assertEquals(2, producedTables.size());
        assertEquals("db1.schema1.source", producedTables.get(0).getTablePath().toString());
        assertEquals("db1.schema1.ffp", producedTables.get(1).getTablePath().toString());
    }

    @Test
    void routeToTableShouldWorkWithoutDatabaseAndSchemaPrefix() {
        SeaTunnelRowType inputRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable inputCatalogTable =
                CatalogTableUtil.getCatalogTable("catalog", null, null, "source", inputRowType);

        DataValidatorTransform transform =
                new DataValidatorTransform(routeToTableConfig("ffp"), inputCatalogTable);

        SeaTunnelRow invalidRow = new SeaTunnelRow(new Object[] {1, null});
        SeaTunnelRow routedRow = transform.map(invalidRow);

        assertEquals("ffp", routedRow.getTableId());

        List<CatalogTable> producedTables = transform.getProducedCatalogTables();
        assertEquals(2, producedTables.size());
        assertEquals("source", producedTables.get(0).getTablePath().toString());
        assertEquals("ffp", producedTables.get(1).getTablePath().toString());
    }

    @Test
    void routeToTableShouldRespectQualifiedErrorTablePath() {
        SeaTunnelRowType inputRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable inputCatalogTable =
                CatalogTableUtil.getCatalogTable("catalog", "db1", null, "source", inputRowType);

        DataValidatorTransform transform =
                new DataValidatorTransform(routeToTableConfig("db2.ffp"), inputCatalogTable);

        SeaTunnelRow invalidRow = new SeaTunnelRow(new Object[] {1, null});
        SeaTunnelRow routedRow = transform.map(invalidRow);

        assertEquals("db2.ffp", routedRow.getTableId());

        List<CatalogTable> producedTables = transform.getProducedCatalogTables();
        assertEquals(2, producedTables.size());
        assertEquals("db1.source", producedTables.get(0).getTablePath().toString());
        assertEquals("db2.ffp", producedTables.get(1).getTablePath().toString());
    }
}
