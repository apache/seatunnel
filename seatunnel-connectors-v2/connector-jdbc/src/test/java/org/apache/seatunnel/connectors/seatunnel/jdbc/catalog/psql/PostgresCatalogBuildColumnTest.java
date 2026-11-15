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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostgresCatalogBuildColumnTest {

    private final PostgresCatalog catalog =
            new PostgresCatalog(
                    "Postgres",
                    "postgres",
                    "postgres",
                    JdbcUrlUtil.getUrlInfo("jdbc:postgresql://localhost:5432/test"),
                    null,
                    null);

    @Test
    void testBuildEnumColumnAsString() throws SQLException {
        ResultSet resultSet = mockColumn("m", "mood", "inv.mood", "e", true);
        when(resultSet.getObject("default_value")).thenReturn("'happy'::inv.mood");
        when(resultSet.getString("column_comment")).thenReturn("current mood");

        Column column = catalog.buildColumn(resultSet);

        Assertions.assertEquals("m", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals("inv.mood", column.getSourceType());
        Assertions.assertTrue(column.isNullable());
        Assertions.assertEquals("'happy'::inv.mood", column.getDefaultValue());
        Assertions.assertEquals("current mood", column.getComment());
    }

    @Test
    void testBuildEnumNamedLikeBuiltInTypeAsString() throws SQLException {
        Column dateEnum = catalog.buildColumn(mockColumn("d", "date", "inv.date", "e", true));
        Column numericEnum =
                catalog.buildColumn(mockColumn("n", "numeric", "inv.\"numeric\"", "e", true));

        Assertions.assertEquals(BasicType.STRING_TYPE, dateEnum.getDataType());
        Assertions.assertEquals("inv.date", dateEnum.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, numericEnum.getDataType());
        Assertions.assertEquals("inv.\"numeric\"", numericEnum.getSourceType());
    }

    @Test
    void testBuildEnumKeepsQuotedFormatTypeName() throws SQLException {
        Column column =
                catalog.buildColumn(mockColumn("w", "Weird Name", "inv.\"Weird Name\"", "e", true));

        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals("inv.\"Weird Name\"", column.getSourceType());
    }

    @Test
    void testSelectColumnsSqlReadsEnumTypeType() {
        String sql = catalog.getSelectColumnsSql(TablePath.of("test", "inv", "products"));

        Assertions.assertTrue(sql.contains("t.typtype as type_type"));
        Assertions.assertTrue(
                sql.contains("WHEN t.typtype = 'e' THEN format_type(a.atttypid, NULL)"));
    }

    @Test
    void testBuildBaseColumnUnchanged() throws SQLException {
        ResultSet resultSet = mockColumn("id", "int4", "int4", "b", false);

        Column column = catalog.buildColumn(resultSet);

        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals("int4", column.getSourceType());
        Assertions.assertFalse(column.isNullable());
    }

    @Test
    void testBuildUnsupportedNonEnumColumnStillFails() throws SQLException {
        ResultSet resultSet = mockColumn("v", "tsvector", "tsvector", "b", true);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class, () -> catalog.buildColumn(resultSet));
    }

    private static ResultSet mockColumn(
            String columnName,
            String typeName,
            String fullTypeName,
            String typeType,
            boolean nullable)
            throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("column_name")).thenReturn(columnName);
        when(resultSet.getString("type_name")).thenReturn(typeName);
        when(resultSet.getString("full_type_name")).thenReturn(fullTypeName);
        when(resultSet.getString("type_type")).thenReturn(typeType);
        when(resultSet.getString("is_nullable")).thenReturn(nullable ? "YES" : "NO");
        return resultSet;
    }
}
