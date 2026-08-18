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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MySqlTypeMapperTest {
    @Test
    void returnsTinyint1WhenNativeTypeIsTinyintAndPrecisionIs1() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("test_column");
        when(metadata.getColumnTypeName(1)).thenReturn("tinyint");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(1);
        when(metadata.getScale(1)).thenReturn(0);

        MySqlTypeMapper typeMapper = new MySqlTypeMapper();
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals("tinyint(1)", column.getSourceType());
    }

    @Test
    void returnsOriginalTypeWhenNativeTypeIsTinyintAndPrecisionIsNot1() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("test_column");
        when(metadata.getColumnTypeName(1)).thenReturn("tinyint");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(2);
        when(metadata.getScale(1)).thenReturn(0);

        MySqlTypeMapper typeMapper = new MySqlTypeMapper();
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals("tinyint", column.getSourceType());
    }

    @Test
    void testTinyint1ReturnShortType() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("test_column");
        when(metadata.getColumnTypeName(1)).thenReturn("tinyint");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(1);
        when(metadata.getScale(1)).thenReturn(0);

        MySqlTypeMapper typeMapper =
                new MySqlTypeMapper(new MySqlTypeConverter(MySqlVersion.V_8, false));
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals(BasicType.BYTE_TYPE, column.getDataType());

        typeMapper = new MySqlTypeMapper(new MySqlTypeConverter(MySqlVersion.V_8, true));
        column = typeMapper.mappingColumn(metadata, 1);

        assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
    }

    @Test
    void testMappingIntColumn() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("id");
        when(metadata.getColumnTypeName(1)).thenReturn("int");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls);
        when(metadata.getPrecision(1)).thenReturn(11);
        when(metadata.getScale(1)).thenReturn(0);

        MySqlTypeMapper typeMapper = new MySqlTypeMapper();
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals("id", column.getName());
        assertEquals("int", column.getSourceType());
        assertEquals(BasicType.INT_TYPE, column.getDataType());
        assertEquals(false, column.isNullable());
    }

    @Test
    void testMappingVarcharColumn() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("name");
        when(metadata.getColumnTypeName(1)).thenReturn("VARCHAR");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(255);
        when(metadata.getScale(1)).thenReturn(0);

        MySqlTypeMapper typeMapper = new MySqlTypeMapper();
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals("name", column.getName());
        assertEquals("VARCHAR", column.getSourceType());
        assertEquals(BasicType.STRING_TYPE, column.getDataType());
        assertEquals(1020, column.getColumnLength());
        assertEquals(true, column.isNullable());
    }

    @Test
    void testMappingBigintColumn() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("big_id");
        when(metadata.getColumnTypeName(1)).thenReturn("bigint");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(20);
        when(metadata.getScale(1)).thenReturn(0);

        MySqlTypeMapper typeMapper = new MySqlTypeMapper();
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals("big_id", column.getName());
        assertEquals("bigint", column.getSourceType());
        assertEquals(BasicType.LONG_TYPE, column.getDataType());
    }

    @Test
    void testMappingDecimalColumn() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnLabel(1)).thenReturn("amount");
        when(metadata.getColumnTypeName(1)).thenReturn("decimal");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(10);
        when(metadata.getScale(1)).thenReturn(2);

        MySqlTypeMapper typeMapper = new MySqlTypeMapper();
        Column column = typeMapper.mappingColumn(metadata, 1);

        assertEquals("amount", column.getName());
        assertEquals("decimal", column.getSourceType());
        assertEquals(new DecimalType(10, 2), column.getDataType());
        assertEquals(10, column.getColumnLength());
        assertEquals(2, column.getScale());
    }
}
