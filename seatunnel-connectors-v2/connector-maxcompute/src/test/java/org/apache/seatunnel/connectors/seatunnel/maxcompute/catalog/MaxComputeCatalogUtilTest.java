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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.datatype.MaxComputeTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.CreateTableParser;

import org.junit.jupiter.api.Test;

import com.aliyun.odps.type.TypeInfo;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaxComputeCatalogUtilTest {
    @Test
    @SuppressWarnings("deprecation")
    void legacyParserPreservesColumnTypesAndOffsets() {
        String sql = "CREATE TABLE demo (`id` BIGINT, amount DECIMAL(10, 2), PRIMARY KEY (`id`))";
        Map<String, CreateTableParser.ColumnInfo> columns = CreateTableParser.getColumnList(sql);
        assertEquals(2, columns.size());
        CreateTableParser.ColumnInfo id = columns.get("id");
        assertEquals("id", id.getName());
        assertEquals("BIGINT", id.getInfo());
        assertEquals(sql.indexOf("id"), id.getStartIndex());
        assertEquals(sql.indexOf("id") + 2, id.getEndIndex());
        CreateTableParser.ColumnInfo amount = columns.get("amount");
        assertEquals("amount", amount.getName());
        assertEquals("DECIMAL(10, 2)", amount.getInfo());
        assertEquals(sql.indexOf("amount"), amount.getStartIndex());
        assertEquals(sql.indexOf("amount") + 6, amount.getEndIndex());
        // Retain the public nested constructor and mutable map contract as well.
        columns.put("extra", new CreateTableParser.ColumnInfo("extra", "INT", 1, 6));
        assertEquals("INT", columns.get("extra").getInfo());
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn("varchar");

        String result =
                MaxComputeCatalogUtil.columnToMaxComputeType(column, mock(TypeConverter.class));

        assertEquals("`col1` varchar NOT NULL ", result);
    }

    @Test
    void reconvertsColumnTypeWhenSinkTypeIsNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn(null);
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        TypeConverter<BasicTypeDefine<TypeInfo>> typeConverter = MaxComputeTypeConverter.INSTANCE;

        String result = MaxComputeCatalogUtil.columnToMaxComputeType(column, typeConverter);

        assertEquals("`col1` INT NOT NULL ", result);
    }

    @Test
    void returnsReconvertedTypeWhenTypesNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getSinkType()).thenReturn("varchar");

        TypeConverter<BasicTypeDefine<TypeInfo>> typeConverter = MaxComputeTypeConverter.INSTANCE;

        String result = MaxComputeCatalogUtil.columnToMaxComputeType(column, typeConverter);

        assertEquals("`col1` varchar NOT NULL ", result);
    }
}
