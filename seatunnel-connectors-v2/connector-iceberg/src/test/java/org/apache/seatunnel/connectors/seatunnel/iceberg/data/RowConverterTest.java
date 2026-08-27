/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class RowConverterTest {

    @Mock private Table table;

    @Mock private IcebergSinkConfig config;

    private RowConverter converter;
    private Schema schema;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        // Create a schema with various field types
        schema =
                new Schema(
                        Types.NestedField.required(1, "int_field", Types.IntegerType.get()),
                        Types.NestedField.required(2, "long_field", Types.LongType.get()),
                        Types.NestedField.required(3, "float_field", Types.FloatType.get()),
                        Types.NestedField.required(4, "double_field", Types.DoubleType.get()),
                        Types.NestedField.required(5, "decimal_field", Types.DecimalType.of(10, 2)),
                        Types.NestedField.required(6, "boolean_field", Types.BooleanType.get()),
                        Types.NestedField.required(7, "string_field", Types.StringType.get()),
                        Types.NestedField.required(8, "uuid_field", Types.UUIDType.get()),
                        Types.NestedField.required(9, "binary_field", Types.BinaryType.get()),
                        Types.NestedField.required(10, "date_field", Types.DateType.get()),
                        Types.NestedField.required(11, "time_field", Types.TimeType.get()),
                        Types.NestedField.required(
                                12, "timestamp_field", Types.TimestampType.withoutZone()));

        when(table.schema()).thenReturn(schema);
        when(config.isCaseSensitive()).thenReturn(true);
        when(config.isTableSchemaEvolutionEnabled()).thenReturn(false);

        converter = new RowConverter(table, config);
    }

    @Test
    public void testConvertBasicTypes() {
        // Create test data
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "int_field",
                            "long_field",
                            "float_field",
                            "double_field",
                            "decimal_field",
                            "boolean_field",
                            "string_field",
                            "uuid_field",
                            "binary_field",
                            "date_field",
                            "time_field",
                            "timestamp_field"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(10, 2),
                            BasicType.BOOLEAN_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        UUID testUuid = UUID.randomUUID();
        LocalDateTime now = LocalDateTime.now();
        LocalDate today = LocalDate.now();
        LocalTime time = LocalTime.now();
        byte[] binaryData = "test binary".getBytes();

        Object[] fields =
                new Object[] {
                    42, // int
                    123456789L, // long
                    3.14f, // float
                    2.71828, // double
                    new BigDecimal("123.45"), // decimal
                    true, // boolean
                    "test string", // string
                    testUuid.toString(), // UUID as string
                    binaryData, // binary
                    today, // date
                    time, // time
                    now // timestamp
                };

        SeaTunnelRow row = new SeaTunnelRow(fields);

        // Convert and verify
        org.apache.iceberg.data.Record result = converter.convert(row, rowType);

        assertNotNull(result);
        assertEquals(42, result.getField("int_field"));
        assertEquals(123456789L, result.getField("long_field"));
        assertEquals(3.14f, result.getField("float_field"));
        assertEquals(2.71828, result.getField("double_field"));
        assertEquals(new BigDecimal("123.45"), result.getField("decimal_field"));
        assertEquals(true, result.getField("boolean_field"));
        assertEquals("test string", result.getField("string_field"));
        assertEquals(testUuid, result.getField("uuid_field"));
        assertNotNull(result.getField("binary_field"));
        assertEquals(today, result.getField("date_field"));
        assertEquals(time, result.getField("time_field"));
        assertEquals(now, result.getField("timestamp_field"));
    }

    @Test
    public void testOffsetDateTimeWithZone() {
        // Create a schema with timestamp with timezone
        Schema timestampSchema =
                new Schema(
                        Types.NestedField.required(
                                1, "timestamp_field", Types.TimestampType.withZone()));

        when(table.schema()).thenReturn(timestampSchema);
        converter = new RowConverter(table, config);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"timestamp_field"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        // create local timestamp
        LocalDateTime localDateTime = LocalDateTime.of(2024, 12, 7, 11, 42, 52);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {localDateTime});

        // convert and verify
        org.apache.iceberg.data.Record result = converter.convert(row, rowType);
        OffsetDateTime converted = (OffsetDateTime) result.getField("timestamp_field");

        // Debug print statements removed to keep test output clean and focused.

        // get system offset for the local timestamp
        ZoneOffset systemOffset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
        // convert local timestamp to UTC
        OffsetDateTime expected =
                localDateTime.minusSeconds(systemOffset.getTotalSeconds()).atOffset(ZoneOffset.UTC);

        assertEquals(expected, converted, "Should convert to correct UTC time");
    }

    @Test
    public void testInvalidTypeConversion() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"int_field"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"not an integer"});

        assertThrows(IllegalArgumentException.class, () -> converter.convert(row, rowType));
    }

    @Test
    public void testNullValues() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"int_field", "string_field"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null, null});

        org.apache.iceberg.data.Record result = converter.convert(row, rowType);
        assertNotNull(result);
        assertEquals(null, result.getField("int_field"));
        assertEquals(null, result.getField("string_field"));
    }
}
