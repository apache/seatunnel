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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
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
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedArrayConverterTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("nested-array-conversion")
                        .config("spark.ui.enabled", "false")
                        .config("spark.driver.host", "127.0.0.1")
                        .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void retainsUnsupportedElementTypeValidation() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        TypeConverterUtils.convert(
                                DataTypes.createArrayType(DataTypes.CalendarIntervalType)));
    }

    @ParameterizedTest(name = "{index}: internal={0}")
    @MethodSource("invalidArrayElements")
    void reportsDeclaredAndRuntimeArrayElementTypes(boolean internal, Object invalidElement)
            throws IOException {
        ArrayType<?, ?> type = ArrayType.INT_ARRAY_TYPE;
        SeaTunnelRow input = row(new Integer[] {1, null, 2});
        WrappedArray.ofRef<?> invalid =
                new WrappedArray.ofRef<>(new Object[] {1, null, invalidElement});
        IllegalArgumentException error;
        if (internal) {
            InternalRowConverter converter = new InternalRowConverter(rowType(type));
            InternalRow converted = converter.convert(input);
            InternalRow malformed =
                    new GenericInternalRow(
                            new Object[] {
                                converted.getByte(0), converted.getUTF8String(1), invalid
                            });
            error =
                    assertThrows(
                            IllegalArgumentException.class, () -> converter.reconvert(malformed));
        } else {
            SeaTunnelRowConverter converter = new SeaTunnelRowConverter(rowType(type));
            GenericRow converted = converter.convert(input);
            GenericRow malformed =
                    new GenericRow(new Object[] {converted.get(0), converted.get(1), invalid});
            error =
                    assertThrows(
                            IllegalArgumentException.class, () -> converter.reconvert(malformed));
        }
        assertTrue(error.getMessage().contains(type.toString()), error.getMessage());
        assertTrue(error.getMessage().contains("index 2"), error.getMessage());
        assertTrue(error.getMessage().contains(Integer.class.getName()), error.getMessage());
        assertTrue(
                error.getMessage().contains(invalidElement.getClass().getName()),
                error.getMessage());
        assertFalse(error.getMessage().contains(invalidElement.toString()), error.getMessage());
        assertTrue(error.getCause() instanceof ArrayStoreException);
    }

    static Stream<Arguments> invalidArrayElements() {
        return Stream.of(
                Arguments.of(false, 987654321L),
                Arguments.of(true, 987654321L),
                Arguments.of(false, "private-test-value"),
                Arguments.of(true, "private-test-value"));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("arrays")
    void roundTripsThroughSparkExecution(ArrayType<?, ?> type, Object value) throws IOException {
        SeaTunnelRowType rowType = rowType(type);
        SeaTunnelRowConverter converter = new SeaTunnelRowConverter(rowType);
        SeaTunnelRow input = row(value);
        Dataset<Row> dataset =
                spark.createDataFrame(
                        Collections.singletonList(converter.convert(input)),
                        (StructType) TypeConverterUtils.parcel(rowType));
        assertEquals(TypeConverterUtils.convert(type), dataset.schema().fields()[2].dataType());
        Row result = dataset.repartition(1).collectAsList().get(0);
        SeaTunnelRow restored = converter.reconvert((GenericRow) result);
        assertValue(value, restored.getField(0));
        assertEquals(input.getRowKind(), restored.getRowKind());
        assertEquals(input.getTableId(), restored.getTableId());
    }

    static Stream<Arguments> arrays() {
        // Value conversion uses the declared SeaTunnel type; reverse schema inference is separate.
        return Stream.concat(
                Stream.concat(schemaArrays(), timeArrays()),
                Stream.concat(parsedArrays(), constantArrays()));
    }

    static Stream<Arguments> constantArrays() {
        // API constants carry a non-runtime array class; value conversion must derive it
        // recursively.
        return Stream.of(
                Arguments.of(
                        ArrayType.of(ArrayType.LOCAL_DATE_ARRAY_TYPE),
                        new LocalDate[][] {{LocalDate.of(2026, 1, 1), null}, {}, null}));
    }

    static Stream<Arguments> parsedArrays() {
        Map[] maps = {
            Collections.singletonMap("values", new Integer[] {1, null}),
            Collections.singletonMap("null", null),
            Collections.emptyMap(),
            null
        };
        return Stream.of(
                Arguments.of(
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "items", "array<array<map<string,array<int>>>>"),
                        new Map[][] {maps, new Map[0], null}),
                Arguments.of(
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "items", "array<array<array<map<string,array<int>>>>>"),
                        new Map[][][] {{maps, new Map[0], null}, new Map[0][], null}));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("parsedArrays")
    void preservesInternalWrappedArrayValues(ArrayType<?, ?> type, Object value)
            throws IOException {
        SeaTunnelRowType rowType = rowType(type);
        InternalRowConverter internalConverter = new InternalRowConverter(rowType);
        for (Object input :
                new Object[] {
                    value, Array.newInstance(value.getClass().getComponentType(), 0), null
                }) {
            SeaTunnelRow row = row(input);
            InternalRow converted = internalConverter.convert(row);
            InternalRow internal =
                    new GenericInternalRow(
                            new Object[] {
                                converted.getByte(0),
                                converted.getUTF8String(1),
                                input == null
                                        ? null
                                        : new WrappedArray.ofRef<>(converted.getArray(2).array())
                            });
            SeaTunnelRow restored = internalConverter.reconvert(internal);
            assertValue(input, restored.getField(0));
            assertEquals(row.getRowKind(), restored.getRowKind());
            assertEquals(row.getTableId(), restored.getTableId());
        }
    }

    static Stream<Arguments> timeArrays() {
        // Existing representations retain TIME nanos, TIMESTAMP micros and TIMESTAMP_TZ millis.
        return Stream.of(
                Arguments.of(
                        ArrayType.of(LocalTimeType.LOCAL_TIME_TYPE),
                        new LocalTime[] {LocalTime.of(12, 34, 56, 123456789), null}),
                Arguments.of(
                        ArrayType.of(LocalTimeType.LOCAL_DATE_TIME_TYPE),
                        new LocalDateTime[] {
                            LocalDateTime.of(2026, 1, 1, 12, 34, 56, 123456000), null
                        }),
                Arguments.of(
                        ArrayType.of(LocalTimeType.OFFSET_DATE_TIME_TYPE),
                        new OffsetDateTime[] {
                            OffsetDateTime.parse("2026-01-01T12:34:56.123+05:30"),
                            OffsetDateTime.parse("2026-01-01T12:34:56.123-07:00"),
                            null
                        }));
    }

    static Stream<Arguments> schemaArrays() {
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
                        ArrayType.of(new DecimalType(20, 6)),
                        new BigDecimal[] {new BigDecimal("12.345678"), null}),
                Arguments.of(
                        ArrayType.of(PrimitiveByteArrayType.INSTANCE),
                        new byte[][] {new byte[] {1, 2}, new byte[0], null}),
                Arguments.of(
                        ArrayType.of(LocalTimeType.LOCAL_DATE_TYPE),
                        new LocalDate[] {LocalDate.of(2026, 1, 1), null}),
                Arguments.of(ArrayType.of(BasicType.VOID_TYPE), new Void[] {null}));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("schemaArrays")
    void convertsSchemaRecursively(ArrayType<?, ?> type, Object value) {
        assertEquals(type, TypeConverterUtils.convert(TypeConverterUtils.convert(type)));
    }

    @Test
    void retainsTimeArraySchemaInferenceLimits() {
        assertEquals(
                ArrayType.LONG_ARRAY_TYPE,
                TypeConverterUtils.convert(
                        TypeConverterUtils.convert(ArrayType.of(LocalTimeType.LOCAL_TIME_TYPE))));
        assertEquals(
                ArrayType.of(new DecimalType(20, 6)),
                TypeConverterUtils.convert(
                        TypeConverterUtils.convert(
                                ArrayType.of(LocalTimeType.OFFSET_DATE_TIME_TYPE))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        TypeConverterUtils.convert(
                                TypeConverterUtils.convert(
                                        ArrayType.of(LocalTimeType.LOCAL_DATE_TIME_TYPE))));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("arrays")
    void preservesCatalystValues(ArrayType<?, ?> type, Object value) throws IOException {
        SeaTunnelRowType rowType = rowType(type);
        InternalRowConverter converter = new InternalRowConverter(rowType);
        for (Object input :
                new Object[] {
                    value, Array.newInstance(value.getClass().getComponentType(), 0), null
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

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("arrays")
    void preservesExternalValuesAndInput(ArrayType<?, ?> type, Object value) throws IOException {
        SeaTunnelRowConverter converter = new SeaTunnelRowConverter(rowType(type));
        for (Object input :
                new Object[] {
                    value, Array.newInstance(value.getClass().getComponentType(), 0), null
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

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("arrays")
    void preservesArraysThroughMultiTableIndexes(ArrayType<?, ?> type, Object value)
            throws IOException {
        SeaTunnelRowType firstType =
                new SeaTunnelRowType(
                        new String[] {"label", "first", "second"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, type, type});
        SeaTunnelRowType secondType =
                new SeaTunnelRowType(
                        new String[] {"second", "id", "first"},
                        new SeaTunnelDataType<?>[] {type, BasicType.INT_TYPE, type});
        CatalogTable firstTable =
                CatalogTableUtil.getCatalogTable("spark", "catalog", "database", "a", firstType);
        CatalogTable secondTable =
                CatalogTableUtil.getCatalogTable("spark", "catalog", "database", "b", secondType);
        MultiTableManager manager =
                new MultiTableManager(new CatalogTable[] {secondTable, firstTable});
        InternalMultiRowCollector collector =
                (InternalMultiRowCollector) manager.getInternalRowCollector(null, null, null);
        Object empty = Array.newInstance(value.getClass().getComponentType(), 0);
        SeaTunnelRow first = new SeaTunnelRow(new Object[] {"first table", value, empty});
        first.setTableId(firstTable.getTablePath().toString());
        first.setRowKind(RowKind.UPDATE_BEFORE);
        SeaTunnelRow second = new SeaTunnelRow(new Object[] {empty, 42, value});
        second.setTableId(secondTable.getTablePath().toString());
        second.setRowKind(RowKind.UPDATE_AFTER);
        // The second table maps to [1, 3, 2], with two distinct slots of the same array type.
        assertEquals(6, manager.getTableSchema().fields().length);
        for (SeaTunnelRow input : new SeaTunnelRow[] {first, second}) {
            SeaTunnelRow external = manager.reconvert(manager.convert(input));
            assertValue(input.getFields(), external.getFields());
            assertEquals(input.getTableId(), external.getTableId());
            assertEquals(input.getRowKind(), external.getRowKind());
            InternalRow internal =
                    collector.getRowSerializationMap().get(input.getTableId()).convert(input);
            UnsafeProjection projection = UnsafeProjection.create(manager.getTableSchema());
            SeaTunnelRow restored = manager.reconvert(projection.apply(internal));
            assertValue(input.getFields(), restored.getFields());
            assertEquals(input.getTableId(), restored.getTableId());
            assertEquals(input.getRowKind(), restored.getRowKind());
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
