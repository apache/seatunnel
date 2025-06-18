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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OceanBaseOracleCreateTableSqlBuilderTest {

    @Test
    public void testColumnWithUnSupportedType() {

        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "Oracle",
                        "test_database",
                        "test_schema",
                        "test_table",
                        new SeaTunnelRowType(
                                new String[] {"field"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        OceanBaseOracleCreateTableSqlBuilder sqlBuilder =
                new OceanBaseOracleCreateTableSqlBuilder(catalogTable, false);

        Column column = mock(Column.class);
        when(column.getSourceType()).thenReturn("LONG");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        String result = sqlBuilder.buildColumnSql(column);
        Assertions.assertEquals("\"col1\" CLOB NOT NULL", result);

        when(column.getSourceType()).thenReturn("LONG RAW");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        result = sqlBuilder.buildColumnSql(column);
        Assertions.assertEquals("\"col1\" BLOB NOT NULL", result);

        when(column.getSourceType()).thenReturn("BFILE");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        result = sqlBuilder.buildColumnSql(column);
        Assertions.assertEquals("\"col1\" BLOB NOT NULL", result);

        when(column.getSourceType()).thenReturn("NCLOB");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        result = sqlBuilder.buildColumnSql(column);
        Assertions.assertEquals("\"col1\" NVARCHAR2(32767) NOT NULL", result);

        when(column.getSourceType()).thenReturn("REAL");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        result = sqlBuilder.buildColumnSql(column);
        Assertions.assertEquals("\"col1\" FLOAT NOT NULL", result);

        when(column.getSourceType()).thenReturn("OTHERTYPE");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        result = sqlBuilder.buildColumnSql(column);
        Assertions.assertEquals("\"col1\" OTHERTYPE NOT NULL", result);
    }
}
