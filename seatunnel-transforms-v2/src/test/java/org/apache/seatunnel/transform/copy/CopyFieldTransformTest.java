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

package org.apache.seatunnel.transform.copy;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class CopyFieldTransformTest {

    private static final String SOURCE_FIELD = "source_field";
    private static final String COPIED_FIELD = "copied_field";
    private static final String NESTED_TABLE_ID = "nested_table";
    private static final String UNKNOWN_TABLE_ID = "unknown_table";

    @ParameterizedTest
    @MethodSource("basicTypes")
    void testBasicTypeClone(SeaTunnelDataType<?> dataType, Object value) {
        CatalogTable catalogTable = createCatalogTable(dataType);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(dataType, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {value}));

        assertEquals(value, result.getField(1));
    }

    @Test
    void testBytesClone() {
        byte[] sourceBytes = createSampleBytes();

        CatalogTable catalogTable = createCatalogTable(PrimitiveByteArrayType.INSTANCE);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(PrimitiveByteArrayType.INSTANCE, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {sourceBytes}));

        byte[] copiedBytes = (byte[]) result.getField(1);
        assertNotSame(sourceBytes, copiedBytes);
        assertArrayEquals(createSampleBytes(), copiedBytes);

        sourceBytes[0] = 9;
        assertArrayEquals(createSampleBytes(), copiedBytes);
    }

    @ParameterizedTest
    @MethodSource("vectorTypes")
    void testDenseVectorClone(SeaTunnelDataType<?> dataType) {
        ByteBuffer sourceBuffer = createVectorBuffer(false);
        CatalogTable catalogTable = createCatalogTable(dataType);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(dataType, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {sourceBuffer}));

        ByteBuffer copiedBuffer = (ByteBuffer) result.getField(1);
        assertNotSame(sourceBuffer, copiedBuffer);
        assertArrayEquals(createSampleBytes(), toBytes(copiedBuffer));

        sourceBuffer.put(0, (byte) 9);
        assertArrayEquals(createSampleBytes(), toBytes(copiedBuffer));
    }

    @Test
    void testDenseVectorCloneDirectBuffer() {
        ByteBuffer sourceBuffer = createVectorBuffer(true);

        CatalogTable catalogTable = createCatalogTable(VectorType.VECTOR_BINARY_TYPE);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(VectorType.VECTOR_BINARY_TYPE, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {sourceBuffer}));

        ByteBuffer copiedBuffer = (ByteBuffer) result.getField(1);
        assertNotSame(sourceBuffer, copiedBuffer);
        assertTrue(copiedBuffer.isDirect());
        assertEquals(ByteOrder.LITTLE_ENDIAN, copiedBuffer.order());
        assertArrayEquals(createSampleBytes(), toBytes(copiedBuffer));
    }

    @Test
    void testSparseVectorClone() {
        Map<Integer, Float> sourceVector = new HashMap<>();
        sourceVector.put(1, 0.5f);
        sourceVector.put(3, 1.0f);

        CatalogTable catalogTable = createCatalogTable(VectorType.VECTOR_SPARSE_FLOAT_TYPE);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(VectorType.VECTOR_SPARSE_FLOAT_TYPE, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {sourceVector}));

        Map<Integer, Float> copiedVector = (Map<Integer, Float>) result.getField(1);
        assertNotSame(sourceVector, copiedVector);
        assertEquals(2, copiedVector.size());
        assertEquals(0.5f, copiedVector.get(1));
        assertEquals(1.0f, copiedVector.get(3));

        sourceVector.put(1, 9.0f);
        sourceVector.put(5, 2.0f);

        assertEquals(0.5f, copiedVector.get(1));
        assertEquals(1.0f, copiedVector.get(3));
        assertEquals(2, copiedVector.size());
    }

    @Test
    void testArrayClone() {
        Integer[] sourceValues = new Integer[] {1, 2, 3};
        SeaTunnelRow nestedRow = createNestedRow(sourceValues, NESTED_TABLE_ID);
        SeaTunnelRow[] sourceArray = new SeaTunnelRow[] {nestedRow};

        SeaTunnelDataType<?> arrayType = ArrayType.of(createSimpleNestedRowType());
        CatalogTable catalogTable = createCatalogTable(arrayType);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(arrayType, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {sourceArray}));

        SeaTunnelRow[] copiedArray = (SeaTunnelRow[]) result.getField(1);
        assertNotSame(sourceArray, copiedArray);
        assertNotSame(sourceArray[0], copiedArray[0]);
        Integer[] copiedValues = (Integer[]) copiedArray[0].getField(0);
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedValues);

        sourceValues[0] = 9;
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedValues);
    }

    @Test
    void testMapClone() {
        Integer[] sourceValues = new Integer[] {1, 2, 3};
        SeaTunnelRow nestedRow = createNestedRow(sourceValues, NESTED_TABLE_ID);
        Map<String, SeaTunnelRow> sourceMap = new HashMap<>();
        sourceMap.put("key", nestedRow);

        SeaTunnelDataType<?> mapType =
                new MapType<>(BasicType.STRING_TYPE, createSimpleNestedRowType());
        CatalogTable catalogTable = createCatalogTable(mapType);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(mapType, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {sourceMap}));

        Map<String, SeaTunnelRow> copiedMap = (Map<String, SeaTunnelRow>) result.getField(1);
        assertNotSame(sourceMap, copiedMap);
        assertNotSame(sourceMap.get("key"), copiedMap.get("key"));
        Integer[] copiedValues = (Integer[]) copiedMap.get("key").getField(0);
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedValues);

        sourceValues[0] = 7;
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedValues);
    }

    @Test
    void testRowClone() {
        Integer[] sourceArray = new Integer[] {1, 2, 3};
        Map<String, Integer> sourceMap = new HashMap<>();
        sourceMap.put("a", 10);
        SeaTunnelRow nestedRow = new SeaTunnelRow(new Object[] {sourceArray, sourceMap});

        CatalogTable catalogTable = createCatalogTable(createRecursiveRowType());
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(createRecursiveRowType(), outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {nestedRow}));

        SeaTunnelRow copiedRow = (SeaTunnelRow) result.getField(1);
        Integer[] copiedArray = (Integer[]) copiedRow.getField(0);
        Map<String, Integer> copiedMap = (Map<String, Integer>) copiedRow.getField(1);

        assertNotSame(nestedRow, copiedRow);
        assertNotSame(sourceArray, copiedArray);
        assertNotSame(sourceMap, copiedMap);
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedArray);
        assertEquals(10, copiedMap.get("a"));
    }

    @Test
    void testMultipleRowClone() {
        String tableId = NESTED_TABLE_ID;
        Integer[] sourceValues = new Integer[] {1, 2, 3};
        SeaTunnelRow nestedRow = createNestedRow(sourceValues, tableId);

        CatalogTable catalogTable = createCatalogTable(createMultipleRowType(tableId));
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(SqlType.MULTIPLE_ROW, outputColumns[0].getDataType().getSqlType());
        MultipleRowType outputType = (MultipleRowType) outputColumns[0].getDataType();
        assertArrayEquals(new String[] {tableId}, outputType.getTableIds());
        assertEquals(createSimpleNestedRowType(), outputType.getRowType(tableId));

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {nestedRow}));

        SeaTunnelRow copiedNestedRow = (SeaTunnelRow) result.getField(1);
        Integer[] copiedValues = (Integer[]) copiedNestedRow.getField(0);
        assertNotSame(sourceValues, copiedValues);
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedValues);
        assertEquals(tableId, copiedNestedRow.getTableId());
        assertEquals(nestedRow.getRowKind(), copiedNestedRow.getRowKind());

        sourceValues[0] = 8;
        assertArrayEquals(new Integer[] {1, 2, 3}, copiedValues);
    }

    @Test
    void testNullClone() {
        CatalogTable catalogTable = createCatalogTable(BasicType.STRING_TYPE);
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(BasicType.STRING_TYPE, outputColumns[0].getDataType());

        transform.getProducedCatalogTable();

        SeaTunnelRow result = transform.transform(new SeaTunnelRow(new Object[] {null}));

        assertNull(result.getField(1));
    }

    @Test
    void multipleRowCloneShouldFailWithoutTableId() {
        SeaTunnelRow nestedRow = createNestedRow(new Integer[] {1, 2, 3}, "");

        CatalogTable catalogTable = createCatalogTable(createMultipleRowType(NESTED_TABLE_ID));
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(SqlType.MULTIPLE_ROW, outputColumns[0].getDataType().getSqlType());

        MultipleRowType outputType = (MultipleRowType) outputColumns[0].getDataType();
        assertArrayEquals(new String[] {NESTED_TABLE_ID}, outputType.getTableIds());
        assertEquals(createSimpleNestedRowType(), outputType.getRowType(NESTED_TABLE_ID));

        transform.getProducedCatalogTable();

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> transform.transform(new SeaTunnelRow(new Object[] {nestedRow})));

        assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['Copy' unsupported data type 'MULTIPLE_ROW' of 'tableId']",
                exception.getMessage());
    }

    @Test
    void multipleRowCloneShouldFailWithUnknownTableId() {
        SeaTunnelRow nestedRow = createNestedRow(new Integer[] {1, 2, 3}, UNKNOWN_TABLE_ID);

        CatalogTable catalogTable = createCatalogTable(createMultipleRowType(NESTED_TABLE_ID));
        CopyFieldTransform transform = buildTransform(catalogTable);
        Column[] outputColumns = transform.getOutputColumns();
        assertEquals(1, outputColumns.length);
        assertEquals(COPIED_FIELD, outputColumns[0].getName());
        assertEquals(SqlType.MULTIPLE_ROW, outputColumns[0].getDataType().getSqlType());

        MultipleRowType outputType = (MultipleRowType) outputColumns[0].getDataType();
        assertArrayEquals(new String[] {NESTED_TABLE_ID}, outputType.getTableIds());
        assertEquals(createSimpleNestedRowType(), outputType.getRowType(NESTED_TABLE_ID));

        transform.getProducedCatalogTable();

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> transform.transform(new SeaTunnelRow(new Object[] {nestedRow})));

        assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['Copy' unsupported data type 'MULTIPLE_ROW' of 'unknown_table']",
                exception.getMessage());
    }

    private static Stream<Arguments> basicTypes() {
        return Stream.of(
                arguments(BasicType.STRING_TYPE, "copy test"),
                arguments(BasicType.BOOLEAN_TYPE, true),
                arguments(BasicType.BYTE_TYPE, (byte) 7),
                arguments(BasicType.SHORT_TYPE, (short) 11),
                arguments(BasicType.INT_TYPE, 13),
                arguments(BasicType.LONG_TYPE, 17L),
                arguments(BasicType.FLOAT_TYPE, 1.5f),
                arguments(BasicType.DOUBLE_TYPE, 2.5d),
                arguments(new DecimalType(38, 18), new BigDecimal("12345.678901234567890123")),
                arguments(LocalTimeType.LOCAL_DATE_TYPE, LocalDate.of(2024, 1, 1)),
                arguments(LocalTimeType.LOCAL_TIME_TYPE, LocalTime.of(10, 20, 30)),
                arguments(
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        LocalDateTime.of(2024, 1, 1, 10, 20, 30)),
                arguments(
                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                        OffsetDateTime.of(2024, 1, 1, 10, 20, 30, 0, ZoneOffset.UTC)));
    }

    private static Stream<Arguments> vectorTypes() {
        return Stream.of(
                arguments(VectorType.VECTOR_BINARY_TYPE),
                arguments(VectorType.VECTOR_FLOAT_TYPE),
                arguments(VectorType.VECTOR_FLOAT16_TYPE),
                arguments(VectorType.VECTOR_BFLOAT16_TYPE));
    }

    private static byte[] createSampleBytes() {
        return new byte[] {1, 2, 3, 4};
    }

    private static ByteBuffer createVectorBuffer(boolean direct) {
        ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(4) : ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(createSampleBytes());
        buffer.flip();
        return buffer;
    }

    private static SeaTunnelRow createNestedRow(Integer[] value, String tableId) {
        SeaTunnelRow nestedRow = new SeaTunnelRow(new Object[] {value});
        nestedRow.setTableId(tableId);
        return nestedRow;
    }

    private static SeaTunnelRowType createSimpleNestedRowType() {
        return new SeaTunnelRowType(
                new String[] {"nested_values"}, new SeaTunnelDataType[] {ArrayType.INT_ARRAY_TYPE});
    }

    private static MultipleRowType createMultipleRowType(String tableId) {
        SeaTunnelRowType nestedRowType = createSimpleNestedRowType();
        return new MultipleRowType(new String[] {tableId}, new SeaTunnelRowType[] {nestedRowType});
    }

    private static SeaTunnelRowType createRecursiveRowType() {
        return new SeaTunnelRowType(
                new String[] {"nested_array", "nested_map"},
                new SeaTunnelDataType[] {
                    ArrayType.INT_ARRAY_TYPE,
                    new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)
                });
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private static CatalogTable createCatalogTable(SeaTunnelDataType<?> sourceFieldType) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        SOURCE_FIELD, sourceFieldType, 1L, false, null, null))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.DEFAULT),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "copy supported type test");
    }

    private static CopyFieldTransform buildTransform(CatalogTable catalogTable) {
        return new CopyFieldTransform(
                CopyTransformConfig.of(ReadonlyConfig.fromMap(buildCopyConfig())), catalogTable);
    }

    private static Map<String, Object> buildCopyConfig() {
        Map<String, Object> configMap = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put(COPIED_FIELD, SOURCE_FIELD);
        configMap.put(CopyTransformConfig.FIELDS.key(), fields);
        return configMap;
    }
}
