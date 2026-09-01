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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterFactory;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV2;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DorisCatalogUtilTest {

    @Test
    void buildsWholeTableTruncateQueryWithoutPartitions() {
        TablePath tablePath = TablePath.of("test_db", "test_table");

        assertEquals(
                "TRUNCATE TABLE test_db.test_table",
                DorisCatalogUtil.getTruncateTableQuery(tablePath, Collections.emptyList()));
    }

    @Test
    void buildsPartitionTruncateQueryWithQuotedIdentifiers() {
        TablePath tablePath = TablePath.of("test_db", "test_table");

        assertEquals(
                "TRUNCATE TABLE test_db.test_table PARTITION (`p1`, `p``2`)",
                DorisCatalogUtil.getTruncateTableQuery(tablePath, Arrays.asList("p1", "p`2")));
    }

    @Test
    void keepsPartitionContentInsideQuotedIdentifier() {
        TablePath tablePath = TablePath.of("test_db", "test_table");

        assertEquals(
                "TRUNCATE TABLE test_db.test_table PARTITION (`p1``) DROP TABLE test_db.other; --`)",
                DorisCatalogUtil.getTruncateTableQuery(
                        tablePath, Collections.singletonList("p1`) DROP TABLE test_db.other; --")));
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn("VARCHAR");

        String result = DorisCatalogUtil.columnToDorisType(column, mock(TypeConverter.class));

        assertEquals("`col1` VARCHAR NOT NULL ", result);
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeIsNull() {
        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn(null);
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        TypeConverter<BasicTypeDefine> typeConverter =
                DorisTypeConverterFactory.getTypeConverter("Doris version Doris-2.0.0");
        String result = DorisCatalogUtil.columnToDorisType(column, typeConverter);

        assertEquals("`col1` INT NOT NULL ", result);
    }

    @Test
    void returnsReconvertedTypeWhenTypesNotNull() {
        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        when(column.isNullable()).thenReturn(false);
        TypeConverter<BasicTypeDefine> typeConverter =
                DorisTypeConverterFactory.getTypeConverter("Doris version Doris-2.0.0");
        String result = DorisCatalogUtil.columnToDorisType(column, typeConverter);

        assertEquals("`col1` VARCHAR NOT NULL ", result);
    }

    @Test
    void returnsVariantTypeWhenSourceTypeIsVariant() {
        Column column =
                PhysicalColumn.builder()
                        .name("col1")
                        .dataType(BasicType.STRING_TYPE)
                        .sourceType(DorisTypeConverterV2.DORIS_VARIANT)
                        .nullable(true)
                        .build();
        TypeConverter<BasicTypeDefine> typeConverter =
                DorisTypeConverterFactory.getTypeConverter("Doris version Doris-2.0.0");

        String result = DorisCatalogUtil.columnToDorisType(column, typeConverter);

        assertEquals("`col1` VARIANT NULL ", result);
    }
}
