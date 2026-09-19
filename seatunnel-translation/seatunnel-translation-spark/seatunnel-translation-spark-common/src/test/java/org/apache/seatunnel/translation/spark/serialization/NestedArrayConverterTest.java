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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NestedArrayConverterTest {

    @Test
    void retainsUnsupportedElementTypeValidation() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        TypeConverterUtils.convert(
                                DataTypes.createArrayType(DataTypes.CalendarIntervalType)));
    }

    @Test
    void roundTripsThroughSparkExecution() throws IOException {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("nested-array-conversion")
                        .config("spark.ui.enabled", "false")
                        .config("spark.driver.host", "127.0.0.1")
                        .getOrCreate();
        try {
            for (Arguments arguments : (Iterable<Arguments>) arrays()::iterator) {
                ArrayType<?, ?> type = (ArrayType<?, ?>) arguments.get()[0];
                Object value = arguments.get()[1];
                SeaTunnelRowType rowType = rowType(type);
                SeaTunnelRowConverter converter = new SeaTunnelRowConverter(rowType);
                Dataset<Row> dataset =
                        spark.createDataFrame(
                                Collections.singletonList(converter.convert(row(value))),
                                (StructType) TypeConverterUtils.parcel(rowType));
                assertEquals(
                        type, TypeConverterUtils.convert(dataset.schema().fields()[2].dataType()));
                Row result = dataset.repartition(1).collectAsList().get(0);
                assertValue(value, converter.reconvert((GenericRow) result).getField(0));
            }
        } finally {
            spark.stop();
        }
    }

    static Stream<Arguments> arrays() {
        SeaTunnelRowType nestedRow =
                new SeaTunnelRowType(
                        new String[] {"values"},
                        new SeaTunnelDataType<?>[] {ArrayType.STRING_ARRAY_TYPE});
        return Stream.of(
                Arguments.of(ArrayType.INT_ARRAY_TYPE, new Integer[] {1, null, 2}),
                Arguments.of(ArrayType.STRING_ARRAY_TYPE, new String[] {"a", null, "b"}),
                Arguments.of(ArrayType.BOOLEAN_ARRAY_TYPE, new Boolean[] {true, null, false}),
                Arguments.of(ArrayType.BYTE_ARRAY_TYPE, new Byte[] {1, null}),
                Arguments.of(ArrayType.SHORT_ARRAY_TYPE, new Short[] {1, null}),
                Arguments.of(ArrayType.LONG_ARRAY_TYPE, new Long[] {1L, null}),
                Arguments.of(ArrayType.FLOAT_ARRAY_TYPE, new Float[] {1.5F, null}),
                Arguments.of(ArrayType.DOUBLE_ARRAY_TYPE, new Double[] {1.5D, null}),
                Arguments.of(
                        ArrayType.of(ArrayType.INT_ARRAY_TYPE),
                        new Integer[][] {{1, null}, {}, null}),
                Arguments.of(
                        ArrayType.of(ArrayType.of(ArrayType.STRING_ARRAY_TYPE)),
                        new String[][][] {{{"one", null}, {}}, {}, null}),
                Arguments.of(
                        ArrayType.of(
                                new MapType<>(BasicType.STRING_TYPE, ArrayType.INT_ARRAY_TYPE)),
                        new Map[] {
                            Collections.singletonMap("values", new Integer[] {1, null}),
                            Collections.singletonMap("null", null),
                            Collections.emptyMap(),
                            null
                        }),
                Arguments.of(
                        ArrayType.of(nestedRow),
                        new SeaTunnelRow[] {
                            new SeaTunnelRow(new Object[] {new String[] {"a", null}}),
                            new SeaTunnelRow(new Object[] {new String[0]}),
                            null
                        }),
                Arguments.of(
                        ArrayType.of(new DecimalType(10, 2)),
                        new BigDecimal[] {new BigDecimal("12.34"), null}),
                Arguments.of(
                        ArrayType.of(PrimitiveByteArrayType.INSTANCE),
                        new byte[][] {new byte[] {1, 2}, new byte[0], null}),
                Arguments.of(
                        ArrayType.of(LocalTimeType.LOCAL_DATE_TYPE),
                        new LocalDate[] {LocalDate.of(2026, 1, 1), null}),
                Arguments.of(ArrayType.of(BasicType.VOID_TYPE), new Void[] {null}));
    }

    @ParameterizedTest
    @MethodSource("arrays")
    void convertsSchemaRecursively(ArrayType<?, ?> type, Object value) {
        assertEquals(type, TypeConverterUtils.convert(TypeConverterUtils.convert(type)));
    }

    @ParameterizedTest
    @MethodSource("arrays")
    void preservesCatalystValues(ArrayType<?, ?> type, Object value) throws IOException {
        SeaTunnelRowType rowType = rowType(type);
        InternalRowConverter converter = new InternalRowConverter(rowType);
        for (Object input :
                new Object[] {
                    value,
                    java.lang.reflect.Array.newInstance(type.getElementType().getTypeClass(), 0),
                    null
                }) {
            SeaTunnelRow row = row(input);
            assertValue(input, converter.reconvert(converter.convert(row)).getField(0));
            UnsafeProjection projection =
                    UnsafeProjection.create((StructType) TypeConverterUtils.parcel(rowType));
            SeaTunnelRow restored = converter.reconvert(projection.apply(converter.convert(row)));
            assertValue(input, restored.getField(0));
            assertEquals(row.getRowKind(), restored.getRowKind());
            assertEquals(row.getTableId(), restored.getTableId());
        }
    }

    @ParameterizedTest
    @MethodSource("arrays")
    void preservesExternalValuesAndInput(ArrayType<?, ?> type, Object value) throws IOException {
        SeaTunnelRowConverter converter = new SeaTunnelRowConverter(rowType(type));
        for (Object input :
                new Object[] {
                    value,
                    java.lang.reflect.Array.newInstance(type.getElementType().getTypeClass(), 0),
                    null
                }) {
            SeaTunnelRow row = row(input);
            SeaTunnelRow restored = converter.reconvert(converter.convert(row));
            assertValue(input, restored.getField(0));
            // Reusing the same input must not encounter a converted Spark value in a Java array.
            assertValue(input, converter.reconvert(converter.convert(row)).getField(0));
            assertEquals(row.getRowKind(), restored.getRowKind());
            assertEquals(row.getTableId(), restored.getTableId());
        }
    }

    private static SeaTunnelRowType rowType(ArrayType<?, ?> type) {
        return new SeaTunnelRowType(new String[] {"items"}, new SeaTunnelDataType<?>[] {type});
    }

    private static SeaTunnelRow row(Object value) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {value});
        row.setTableId("catalog.database.table");
        row.setRowKind(RowKind.UPDATE_AFTER);
        return row;
    }

    private static void assertValue(Object expected, Object actual) {
        if (expected == null) {
            assertNull(actual);
        } else if (expected instanceof byte[]) {
            assertArrayEquals((byte[]) expected, (byte[]) actual);
        } else if (expected instanceof Object[]) {
            assertEquals(expected.getClass(), actual.getClass());
            Object[] expectedArray = (Object[]) expected;
            Object[] actualArray = (Object[]) actual;
            assertEquals(expectedArray.length, actualArray.length);
            for (int i = 0; i < expectedArray.length; i++) {
                assertValue(expectedArray[i], actualArray[i]);
            }
        } else if (expected instanceof Map) {
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            assertEquals(expectedMap.keySet(), actualMap.keySet());
            expectedMap.forEach((key, value) -> assertValue(value, actualMap.get(key)));
        } else if (expected instanceof SeaTunnelRow) {
            assertValue(((SeaTunnelRow) expected).getFields(), ((SeaTunnelRow) actual).getFields());
        } else {
            assertEquals(expected, actual);
        }
    }
}
