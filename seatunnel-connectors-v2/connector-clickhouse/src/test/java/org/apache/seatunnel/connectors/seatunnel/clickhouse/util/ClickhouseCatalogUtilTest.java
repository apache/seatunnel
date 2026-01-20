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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Test;

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
}
