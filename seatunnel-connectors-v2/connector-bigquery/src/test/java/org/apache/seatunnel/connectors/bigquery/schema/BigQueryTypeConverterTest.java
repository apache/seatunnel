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

package org.apache.seatunnel.connectors.bigquery.schema;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryTypeConverterTest {

    @Test
    public void testScalarAndCompositeDdlTypes() {
        assertEquals("INT64", BigQueryTypeConverter.toDdlType(BasicType.LONG_TYPE));
        assertEquals("NUMERIC(30, 8)", BigQueryTypeConverter.toDdlType(new DecimalType(30, 8)));
        assertEquals("BIGNUMERIC(38, 0)", BigQueryTypeConverter.toDdlType(new DecimalType(38, 0)));
        assertEquals(
                "BIGNUMERIC(48, 10)", BigQueryTypeConverter.toDdlType(new DecimalType(48, 10)));
        assertEquals("ARRAY<STRING>", BigQueryTypeConverter.toDdlType(ArrayType.STRING_ARRAY_TYPE));
        assertEquals(
                "STRUCT<`id` INT64, `name` STRING>",
                BigQueryTypeConverter.toDdlType(
                        new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                                    BasicType.LONG_TYPE, BasicType.STRING_TYPE
                                })));
    }

    @Test
    public void testRejectUnsupportedMapType() {
        assertThrows(
                BigQueryConnectorException.class,
                () ->
                        BigQueryTypeConverter.toDdlType(
                                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)));
        assertThrows(
                BigQueryConnectorException.class,
                () -> BigQueryTypeConverter.toDdlType(new DecimalType(76, 0)));
    }

    // ---- BigQuery → SeaTunnel (read direction) ----

    @Test
    public void testToSeaTunnelRowTypeFromSchema() {
        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING),
                        Field.of("price", StandardSQLTypeName.NUMERIC),
                        Field.of("active", StandardSQLTypeName.BOOL),
                        Field.of("score", StandardSQLTypeName.FLOAT64),
                        Field.of("created", StandardSQLTypeName.TIMESTAMP),
                        Field.of("birth_date", StandardSQLTypeName.DATE),
                        Field.of("data", StandardSQLTypeName.BYTES));

        SeaTunnelRowType rowType = BigQueryTypeConverter.toSeaTunnelRowType(schema);

        assertEquals(8, rowType.getTotalFields());
        assertEquals("id", rowType.getFieldName(0));
        assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(1));
        assertEquals(new DecimalType(38, 9), rowType.getFieldType(2));
        assertEquals(BasicType.BOOLEAN_TYPE, rowType.getFieldType(3));
        assertEquals(BasicType.DOUBLE_TYPE, rowType.getFieldType(4));
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, rowType.getFieldType(5));
        assertEquals(LocalTimeType.LOCAL_DATE_TYPE, rowType.getFieldType(6));
        assertEquals(PrimitiveByteArrayType.INSTANCE, rowType.getFieldType(7));
    }

    @Test
    public void testToSeaTunnelRowTypeNullSchemaReturnsEmpty() {
        SeaTunnelRowType emptyType = BigQueryTypeConverter.toSeaTunnelRowType(null);
        assertEquals(0, emptyType.getTotalFields());
    }

    @Test
    public void testToSeaTunnelTypeRepeatedFieldProducesArray() {
        Field repeated =
                Field.newBuilder("tags", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REPEATED)
                        .build();
        SeaTunnelDataType<?> type = BigQueryTypeConverter.toSeaTunnelType(repeated);
        assertTrue(type instanceof ArrayType);
        assertEquals(BasicType.STRING_TYPE, ((ArrayType<?, ?>) type).getElementType());
    }

    @Test
    public void testToSeaTunnelTypeStructFieldProducesRowType() {
        Field structField =
                Field.newBuilder(
                                "address",
                                StandardSQLTypeName.STRUCT,
                                Field.of("street", StandardSQLTypeName.STRING),
                                Field.of("city", StandardSQLTypeName.STRING))
                        .build();
        SeaTunnelDataType<?> type = BigQueryTypeConverter.toSeaTunnelType(structField);
        assertInstanceOf(SeaTunnelRowType.class, type);
        SeaTunnelRowType rowType = (SeaTunnelRowType) type;
        assertEquals(2, rowType.getTotalFields());
        assertEquals("street", rowType.getFieldName(0));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(0));
    }

    @Test
    public void testToSeaTunnelTypeTimeAndDatetime() {
        SeaTunnelDataType<?> timeType =
                BigQueryTypeConverter.toSeaTunnelType(
                        Field.of("start_time", StandardSQLTypeName.TIME));
        assertEquals(LocalTimeType.LOCAL_TIME_TYPE, timeType);

        SeaTunnelDataType<?> datetimeType =
                BigQueryTypeConverter.toSeaTunnelType(
                        Field.of("event_ts", StandardSQLTypeName.DATETIME));
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, datetimeType);
    }

    @Test
    public void testToSeaTunnelTypeBignumericMapsToDecimal() {
        SeaTunnelDataType<?> type =
                BigQueryTypeConverter.toSeaTunnelType(
                        Field.of("huge", StandardSQLTypeName.BIGNUMERIC));
        assertEquals(new DecimalType(76, 38), type);
    }

    @Test
    public void testToSeaTunnelTypeRepeatedStructProducesArrayOfRow() {
        Field repeatedStruct =
                Field.newBuilder(
                                "addresses",
                                StandardSQLTypeName.STRUCT,
                                Field.of("street", StandardSQLTypeName.STRING),
                                Field.of("city", StandardSQLTypeName.STRING))
                        .setMode(Field.Mode.REPEATED)
                        .build();
        SeaTunnelDataType<?> type = BigQueryTypeConverter.toSeaTunnelType(repeatedStruct);
        assertInstanceOf(ArrayType.class, type);
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) type;
        assertInstanceOf(SeaTunnelRowType.class, arrayType.getElementType());
        SeaTunnelRowType rowType = (SeaTunnelRowType) arrayType.getElementType();
        assertEquals(2, rowType.getTotalFields());
        assertEquals("street", rowType.getFieldName(0));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(0));
        assertEquals("city", rowType.getFieldName(1));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(1));
    }

    @Test
    public void testNullFieldTypeThrowsDescriptiveException() {
        Field nullTypeField = mock(Field.class);
        when(nullTypeField.getName()).thenReturn("bad_field");
        when(nullTypeField.getType()).thenReturn(null);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> BigQueryTypeConverter.toSeaTunnelType(nullTypeField));
        assertTrue(ex.getMessage().contains("bad_field"));
        assertTrue(ex.getMessage().contains("field type is null"));
    }
}
