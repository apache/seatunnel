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

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class AvroConverterTest {

    private static final byte[] BYTES_PAYLOAD = new byte[] {10, 11, 12, 13, 14, 15, 16, 17};

    // Reader-reuse tests: the first value is 64 bytes long, the second value is either the same
    // length or shorter, so Avro reuses the previous record's ByteBuffer in both cases.
    private static final byte[] FIRST_PAYLOAD = sequentialPayload(64, 1);
    private static final byte[] SAME_LENGTH_PAYLOAD = sequentialPayload(64, 101);
    private static final byte[] SHORTER_PAYLOAD = new byte[] {101, 102, 103};

    private static byte[] sequentialPayload(int length, int firstValue) {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) (firstValue + i);
        }
        return payload;
    }

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow subSeaTunnelRow = new SeaTunnelRow(14);
        Map<String, String> map = new HashMap<String, String>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        String[] strArray = new String[] {"l1", "l2"};
        byte byteVal = 100;
        LocalDate localDate = LocalDate.of(2023, 1, 1);

        BigDecimal bigDecimal = new BigDecimal("61592600349703735722.724745739637773662");
        LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 1, 6, 30, 40);

        subSeaTunnelRow.setField(0, map);
        subSeaTunnelRow.setField(1, strArray);
        subSeaTunnelRow.setField(2, "strVal");
        subSeaTunnelRow.setField(3, true);
        subSeaTunnelRow.setField(4, 1);
        subSeaTunnelRow.setField(5, 2);
        subSeaTunnelRow.setField(6, 3);
        subSeaTunnelRow.setField(7, Long.MAX_VALUE - 1);
        subSeaTunnelRow.setField(8, 33.333F);
        subSeaTunnelRow.setField(9, 123.456);
        subSeaTunnelRow.setField(10, byteVal);
        subSeaTunnelRow.setField(11, localDate);
        subSeaTunnelRow.setField(12, bigDecimal);
        subSeaTunnelRow.setField(13, localDateTime);

        Map<String, Short> mapData = new HashMap<>();
        mapData.put("k1", Short.valueOf("1"));
        mapData.put("k2", Short.valueOf("2"));
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(15);
        seaTunnelRow.setField(0, mapData);
        seaTunnelRow.setField(1, strArray);
        seaTunnelRow.setField(2, "strVal");
        seaTunnelRow.setField(3, true);
        seaTunnelRow.setField(4, new Byte("1"));
        seaTunnelRow.setField(5, Short.valueOf("2"));
        seaTunnelRow.setField(6, 3);
        seaTunnelRow.setField(7, Long.MAX_VALUE - 1);
        seaTunnelRow.setField(8, 33.333F);
        seaTunnelRow.setField(9, 123.456);
        seaTunnelRow.setField(10, byteVal);
        seaTunnelRow.setField(11, localDate);
        seaTunnelRow.setField(12, bigDecimal);
        seaTunnelRow.setField(13, localDateTime);
        seaTunnelRow.setField(14, subSeaTunnelRow);
        return seaTunnelRow;
    }

    private SeaTunnelRowType buildSeaTunnelRowType() {
        String[] subField = {
            "c_map",
            "c_array",
            "c_string",
            "c_boolean",
            "c_tinyint",
            "c_smallint",
            "c_int",
            "c_bigint",
            "c_float",
            "c_double",
            "c_bytes",
            "c_date",
            "c_decimal",
            "c_timestamp"
        };
        SeaTunnelDataType<?>[] subFieldTypes = {
            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
            ArrayType.STRING_ARRAY_TYPE,
            BasicType.STRING_TYPE,
            BasicType.BOOLEAN_TYPE,
            BasicType.INT_TYPE,
            BasicType.INT_TYPE,
            BasicType.INT_TYPE,
            BasicType.LONG_TYPE,
            BasicType.FLOAT_TYPE,
            BasicType.DOUBLE_TYPE,
            BasicType.BYTE_TYPE,
            LocalTimeType.LOCAL_DATE_TYPE,
            new DecimalType(38, 18),
            LocalTimeType.LOCAL_DATE_TIME_TYPE
        };
        SeaTunnelRowType subRow = new SeaTunnelRowType(subField, subFieldTypes);

        String[] fieldNames = {
            "c_map",
            "c_array",
            "c_string",
            "c_boolean",
            "c_tinyint",
            "c_smallint",
            "c_int",
            "c_bigint",
            "c_float",
            "c_double",
            "c_bytes",
            "c_date",
            "c_decimal",
            "c_timestamp",
            "c_row"
        };
        SeaTunnelDataType<?>[] fieldTypes = {
            new MapType<>(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
            ArrayType.STRING_ARRAY_TYPE,
            BasicType.STRING_TYPE,
            BasicType.BOOLEAN_TYPE,
            BasicType.BYTE_TYPE,
            BasicType.SHORT_TYPE,
            BasicType.INT_TYPE,
            BasicType.LONG_TYPE,
            BasicType.FLOAT_TYPE,
            BasicType.DOUBLE_TYPE,
            BasicType.BYTE_TYPE,
            LocalTimeType.LOCAL_DATE_TYPE,
            new DecimalType(38, 18),
            LocalTimeType.LOCAL_DATE_TIME_TYPE,
            subRow
        };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        return rowType;
    }

    @Test
    public void testConverter() {

        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();
        RowToAvroConverter rowToAvroConverter = new RowToAvroConverter(rowType);
        GenericRecord record = rowToAvroConverter.convertRowToGenericRecord(seaTunnelRow);

        AvroToRowConverter avroToRowConverter = new AvroToRowConverter(rowType);
        SeaTunnelRow converterRow = avroToRowConverter.converter(record, rowType);

        Assertions.assertEquals(converterRow, seaTunnelRow);
    }

    /**
     * Row type with one real {@code BYTES} ({@code SqlType.BYTES}) field plus a scalar field, so
     * the byte conversion can be asserted independently of the rest of the row.
     */
    private static SeaTunnelRowType buildBytesRowType() {
        return new SeaTunnelRowType(
                new String[] {"c_bytes", "c_id"},
                new SeaTunnelDataType<?>[] {PrimitiveByteArrayType.INSTANCE, BasicType.INT_TYPE});
    }

    private static Stream<Arguments> byteBufferCases() {
        return Stream.of(
                Arguments.of(
                        "heap buffer: position 0, limit == capacity",
                        ByteBuffer.wrap(BYTES_PAYLOAD.clone()),
                        BYTES_PAYLOAD),
                Arguments.of(
                        "heap buffer: bounded region with sentinel prefix and suffix",
                        boundedHeapBuffer(),
                        BYTES_PAYLOAD),
                Arguments.of(
                        "heap buffer: slice with non-zero arrayOffset",
                        slicedHeapBuffer(),
                        BYTES_PAYLOAD),
                Arguments.of("direct buffer", directBuffer(), BYTES_PAYLOAD),
                Arguments.of("read-only buffer", readOnlyBuffer(), BYTES_PAYLOAD),
                Arguments.of("empty heap buffer", ByteBuffer.allocate(0), new byte[0]),
                Arguments.of("null value", null, null));
    }

    private static ByteBuffer boundedHeapBuffer() {
        byte[] backing = new byte[BYTES_PAYLOAD.length + 8];
        Arrays.fill(backing, (byte) 0x7F);
        System.arraycopy(BYTES_PAYLOAD, 0, backing, 4, BYTES_PAYLOAD.length);
        ByteBuffer buffer = ByteBuffer.wrap(backing);
        buffer.position(4);
        buffer.limit(4 + BYTES_PAYLOAD.length);
        return buffer;
    }

    private static ByteBuffer slicedHeapBuffer() {
        byte[] backing = new byte[BYTES_PAYLOAD.length + 6];
        Arrays.fill(backing, (byte) 0x7F);
        System.arraycopy(BYTES_PAYLOAD, 0, backing, 3, BYTES_PAYLOAD.length);
        ByteBuffer buffer = ByteBuffer.wrap(backing);
        buffer.position(3);
        buffer.limit(3 + BYTES_PAYLOAD.length);
        return buffer.slice();
    }

    private static ByteBuffer directBuffer() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(BYTES_PAYLOAD.length);
        buffer.put(BYTES_PAYLOAD);
        buffer.flip();
        return buffer;
    }

    private static ByteBuffer readOnlyBuffer() {
        return ByteBuffer.wrap(BYTES_PAYLOAD.clone()).asReadOnlyBuffer();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("byteBufferCases")
    public void testConverterBytesRespectsByteBufferPositionAndLimit(
            String caseName, ByteBuffer buffer, byte[] expectedBytes) {
        SeaTunnelRowType rowType = buildBytesRowType();
        RowToAvroConverter rowToAvroConverter = new RowToAvroConverter(rowType);
        SeaTunnelRow sourceRow = new SeaTunnelRow(new Object[] {BYTES_PAYLOAD.clone(), 42});
        GenericRecord record = rowToAvroConverter.convertRowToGenericRecord(sourceRow);
        record.put("c_bytes", buffer);
        int position = buffer == null ? -1 : buffer.position();
        int limit = buffer == null ? -1 : buffer.limit();

        AvroToRowConverter avroToRowConverter = new AvroToRowConverter(rowType);
        SeaTunnelRow convertedRow = avroToRowConverter.converter(record, rowType);

        Assertions.assertEquals(42, convertedRow.getField(1));
        if (expectedBytes == null) {
            Assertions.assertNull(convertedRow.getField(0), caseName + ": bytes field");
        } else {
            Assertions.assertArrayEquals(
                    expectedBytes, (byte[]) convertedRow.getField(0), caseName + ": bytes field");
        }
        if (buffer != null) {
            Assertions.assertEquals(position, buffer.position(), "source ByteBuffer position");
            Assertions.assertEquals(limit, buffer.limit(), "source ByteBuffer limit");
        }
    }

    @Test
    public void testConverterBytesMustNotAliasWholeHeapBuffer() {
        SeaTunnelRowType rowType = buildBytesRowType();
        byte[] payload = BYTES_PAYLOAD.clone();
        ByteBuffer sourceBuffer = ByteBuffer.wrap(payload.clone());
        RowToAvroConverter rowToAvroConverter = new RowToAvroConverter(rowType);
        SeaTunnelRow sourceRow = new SeaTunnelRow(new Object[] {payload.clone(), 42});
        GenericRecord record = rowToAvroConverter.convertRowToGenericRecord(sourceRow);
        record.put("c_bytes", sourceBuffer);

        AvroToRowConverter avroToRowConverter = new AvroToRowConverter(rowType);
        SeaTunnelRow convertedRow = avroToRowConverter.converter(record, rowType);
        Assertions.assertEquals(42, convertedRow.getField(1));

        // A reader may reuse the buffer it handed out earlier, overwriting it in place.
        sourceBuffer.array()[0] = (byte) 0x7F;

        Assertions.assertArrayEquals(
                payload,
                (byte[]) convertedRow.getField(0),
                "converted bytes must not alias the source ByteBuffer");
    }

    private static Stream<Arguments> readerReuseCases() {
        return Stream.of(
                Arguments.of("equal-length reuse: 64 bytes then 64 bytes", SAME_LENGTH_PAYLOAD),
                Arguments.of("shorter reuse: 64 bytes then 3 bytes", SHORTER_PAYLOAD));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("readerReuseCases")
    public void testConverterBytesAfterAvroReaderRecordReuse(String caseName, byte[] nextPayload)
            throws IOException {
        SeaTunnelRowType rowType = buildBytesRowType();
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        SeaTunnelRow firstSourceRow = new SeaTunnelRow(new Object[] {FIRST_PAYLOAD, 1});
        SeaTunnelRow secondSourceRow = new SeaTunnelRow(new Object[] {nextPayload, 2});
        byte[] firstMessage = serializationSchema.serialize(firstSourceRow);
        byte[] secondMessage = serializationSchema.serialize(secondSourceRow);

        AvroToRowConverter avroToRowConverter = new AvroToRowConverter(rowType);
        DatumReader<GenericRecord> reader = avroToRowConverter.getReader();

        BinaryDecoder firstDecoder = DecoderFactory.get().binaryDecoder(firstMessage, null);
        GenericRecord firstRecord = reader.read(null, firstDecoder);
        ByteBuffer firstBuffer = (ByteBuffer) firstRecord.get("c_bytes");
        SeaTunnelRow firstRow = avroToRowConverter.converter(firstRecord, rowType);
        Assertions.assertArrayEquals(
                FIRST_PAYLOAD, (byte[]) firstRow.getField(0), caseName + ": first row bytes");

        // Avro reuses the previous record's ByteBuffer when the new value fits its capacity.
        BinaryDecoder secondDecoder = DecoderFactory.get().binaryDecoder(secondMessage, null);
        GenericRecord reusedRecord = reader.read(firstRecord, secondDecoder);
        // Precondition, not a value assertion: without record reuse this test proves nothing.
        Assertions.assertSame(
                firstBuffer,
                reusedRecord.get("c_bytes"),
                caseName + ": Avro must reuse the record's ByteBuffer");
        SeaTunnelRow reusedRow = avroToRowConverter.converter(reusedRecord, rowType);

        Assertions.assertEquals(2, reusedRow.getField(1));
        Assertions.assertArrayEquals(
                nextPayload, (byte[]) reusedRow.getField(0), caseName + ": reused row bytes");
        // The retained row was converted before the reuse read, so it must keep its own bytes.
        Assertions.assertArrayEquals(
                FIRST_PAYLOAD,
                (byte[]) firstRow.getField(0),
                caseName + ": retained first row bytes after reuse");
    }

    @Test
    public void testDeserializeRetainsFirstRowBytesAcrossMessages() throws IOException {
        SeaTunnelRowType rowType = buildBytesRowType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        // Both messages carry 64-byte values, the case where a reusing reader aliases buffers.
        SeaTunnelRow firstSourceRow = new SeaTunnelRow(new Object[] {FIRST_PAYLOAD, 1});
        SeaTunnelRow secondSourceRow = new SeaTunnelRow(new Object[] {SAME_LENGTH_PAYLOAD, 2});
        byte[] firstMessage = serializationSchema.serialize(firstSourceRow);
        byte[] secondMessage = serializationSchema.serialize(secondSourceRow);

        AvroDeserializationSchema deserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        SeaTunnelRow firstRow = deserializationSchema.deserialize(firstMessage);
        SeaTunnelRow secondRow = deserializationSchema.deserialize(secondMessage);

        Assertions.assertEquals(2, secondRow.getField(1));
        Assertions.assertArrayEquals(SAME_LENGTH_PAYLOAD, (byte[]) secondRow.getField(0));
        Assertions.assertArrayEquals(
                FIRST_PAYLOAD,
                (byte[]) firstRow.getField(0),
                "retained first row bytes after the second message");
    }

    @Test
    public void testConverterBytesRoundTripThroughSerializationAndDeserialization()
            throws IOException {
        SeaTunnelRowType rowType = buildBytesRowType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow sourceRow = new SeaTunnelRow(new Object[] {BYTES_PAYLOAD.clone(), 7});

        byte[] message = new AvroSerializationSchema(rowType).serialize(sourceRow);
        SeaTunnelRow resultRow = new AvroDeserializationSchema(catalogTable).deserialize(message);

        Assertions.assertEquals(7, resultRow.getField(1));
        Assertions.assertArrayEquals(BYTES_PAYLOAD, (byte[]) resultRow.getField(0));
    }

    // The default mode is the legacy mode, so the GenericRecord shape built for a nested ROW must
    // stay exactly the shape that existed before the nullable ROW option was introduced: the nested
    // record still carries the historic top level record name and the same values. The explicit
    // false mode is the same public mode, so it has to produce that identical shape as well.
    @Test
    public void testLegacyNestedRecordShapeAndValuesUnchanged() {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();
        SeaTunnelRowType nestedRowType = (SeaTunnelRowType) rowType.getFieldType(14);

        GenericRecord defaultRecord =
                new RowToAvroConverter(rowType).convertRowToGenericRecord(seaTunnelRow);
        GenericRecord explicitFalseRecord =
                new RowToAvroConverter(rowType, false).convertRowToGenericRecord(seaTunnelRow);

        Assertions.assertEquals(
                SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(rowType)
                        .toString(),
                defaultRecord.getSchema().toString());

        GenericRecord defaultNested = (GenericRecord) defaultRecord.get("c_row");
        GenericRecord explicitFalseNested = (GenericRecord) explicitFalseRecord.get("c_row");
        String legacyNestedSchema =
                SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(nestedRowType)
                        .toString();
        Assertions.assertEquals("SeaTunnelRecord", defaultNested.getSchema().getName());
        Assertions.assertEquals(legacyNestedSchema, defaultNested.getSchema().toString());
        Assertions.assertEquals("SeaTunnelRecord", explicitFalseNested.getSchema().getName());
        Assertions.assertEquals(legacyNestedSchema, explicitFalseNested.getSchema().toString());
        Assertions.assertEquals(defaultNested.toString(), explicitFalseNested.toString());
        Assertions.assertEquals("strVal", defaultNested.get("c_string"));
        Assertions.assertEquals(Integer.valueOf(3), defaultNested.get("c_int"));
    }

    // With the opt-in nullable layout a ROW field is a [null, record] union, so a present nested
    // row must be built under the name the enclosing schema gave the field, otherwise the write
    // fails with UnresolvedUnionException.
    @Test
    public void testPresentNestedRowSerializesWithNullableRow() throws IOException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();

        RowToAvroConverter converter = new RowToAvroConverter(rowType, true);
        GenericRecord record = converter.convertRowToGenericRecord(seaTunnelRow);
        Assertions.assertEquals(
                "c_row", ((GenericRecord) record.get("c_row")).getSchema().getName());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        converter.getWriter().write(record, encoder);
        encoder.flush();
        Assertions.assertTrue(out.size() > 0);
    }

    // Two ROW fields sit under different parents but share the same leaf ROW field name, and the
    // two nested records differ structurally: one carries a STRING and one an INT. With the opt-in
    // nullable layout the nested record name is derived from the field name, so both nested
    // records are named "detail" while describing different fields. The public binary round trip
    // still has to read every value back through its own parent branch, both for a present and for
    // a null nested row.
    @ParameterizedTest
    @CsvSource({"false, true", "true, true", "true, false"})
    public void testSameLeafRowNameUnderDifferentParentsRoundTrips(
            boolean rowNullable, boolean present) throws IOException {
        SeaTunnelRowType leftDetailType =
                new SeaTunnelRowType(
                        new String[] {"code"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        SeaTunnelRowType rightDetailType =
                new SeaTunnelRowType(
                        new String[] {"amount"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        SeaTunnelRowType leftParentType =
                new SeaTunnelRowType(
                        new String[] {"detail"}, new SeaTunnelDataType<?>[] {leftDetailType});
        SeaTunnelRowType rightParentType =
                new SeaTunnelRowType(
                        new String[] {"detail"}, new SeaTunnelDataType<?>[] {rightDetailType});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"left_parent", "right_parent"},
                        new SeaTunnelDataType<?>[] {leftParentType, rightParentType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);

        SeaTunnelRow leftDetail = new SeaTunnelRow(new Object[] {"left-code"});
        SeaTunnelRow rightDetail = new SeaTunnelRow(new Object[] {7});
        SeaTunnelRow leftParent = new SeaTunnelRow(new Object[] {present ? leftDetail : null});
        SeaTunnelRow rightParent = new SeaTunnelRow(new Object[] {present ? rightDetail : null});
        SeaTunnelRow sourceRow = new SeaTunnelRow(new Object[] {leftParent, rightParent});

        AvroSerializationSchema serializer = new AvroSerializationSchema(rowType, rowNullable);
        byte[] bytes = serializer.serialize(sourceRow);
        Assertions.assertArrayEquals(bytes, serializer.serialize(sourceRow));
        SeaTunnelRow result =
                new AvroDeserializationSchema(catalogTable, null, false, rowNullable)
                        .deserialize(bytes);

        SeaTunnelRow leftResult = (SeaTunnelRow) result.getField(0);
        SeaTunnelRow rightResult = (SeaTunnelRow) result.getField(1);
        if (present) {
            SeaTunnelRow leftDetailResult = (SeaTunnelRow) leftResult.getField(0);
            SeaTunnelRow rightDetailResult = (SeaTunnelRow) rightResult.getField(0);
            Assertions.assertEquals("left-code", leftDetailResult.getField(0));
            Assertions.assertEquals(7, rightDetailResult.getField(0));
        } else {
            Assertions.assertNull(leftResult.getField(0));
            Assertions.assertNull(rightResult.getField(0));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNestedSchemasReusedAcrossRowsAndContainers(boolean rowNullable) {
        SeaTunnelRowType detailType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"direct", "items", "entries", "other"},
                        new SeaTunnelDataType<?>[] {
                            detailType,
                            ArrayType.of(detailType),
                            new MapType<>(BasicType.STRING_TYPE, detailType),
                            detailType
                        });
        SeaTunnelRow detail = new SeaTunnelRow(new Object[] {7});
        Map<String, SeaTunnelRow> entries = new HashMap<>();
        entries.put("first", detail);
        entries.put("second", detail);
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            detail, new SeaTunnelRow[] {detail, detail}, entries, detail
                        });
        RowToAvroConverter converter = new RowToAvroConverter(rowType, rowNullable);
        GenericRecord first = converter.convertRowToGenericRecord(row);
        GenericRecord second = converter.convertRowToGenericRecord(row);
        for (String field : rowType.getFieldNames()) {
            GenericRecord firstNested = nestedRecord(first.get(field));
            GenericRecord secondNested = nestedRecord(second.get(field));
            Assertions.assertSame(firstNested.getSchema(), secondNested.getSchema());
            Assertions.assertNotSame(firstNested, secondNested);
            Assertions.assertEquals(7, secondNested.get("value"));
            Assertions.assertEquals(
                    rowNullable ? field : "SeaTunnelRecord", secondNested.getSchema().getName());
        }
        List<?> items = (List<?>) first.get("items");
        Assertions.assertSame(
                ((GenericRecord) items.get(0)).getSchema(),
                ((GenericRecord) items.get(1)).getSchema());
        Map<?, ?> records = (Map<?, ?>) first.get("entries");
        Assertions.assertSame(
                ((GenericRecord) records.get("first")).getSchema(),
                ((GenericRecord) records.get("second")).getSchema());
        detail.setField(0, 9);
        GenericRecord updated = converter.convertRowToGenericRecord(row);
        Assertions.assertEquals(7, nestedRecord(first.get("direct")).get("value"));
        Assertions.assertEquals(9, nestedRecord(updated.get("direct")).get("value"));
        Assertions.assertEquals(9, nestedRecord(updated.get("items")).get("value"));
        Assertions.assertEquals(9, nestedRecord(updated.get("entries")).get("value"));
        if (rowNullable) {
            GenericRecord nullRecord =
                    converter.convertRowToGenericRecord(
                            new SeaTunnelRow(new Object[] {null, null, null, null}));
            Assertions.assertNull(nullRecord.get("direct"));
            Assertions.assertEquals(updated, converter.convertRowToGenericRecord(row));
        }
    }

    private GenericRecord nestedRecord(Object value) {
        if (value instanceof List) {
            return (GenericRecord) ((List<?>) value).get(0);
        }
        if (value instanceof Map) {
            return (GenericRecord) ((Map<?, ?>) value).values().iterator().next();
        }
        return (GenericRecord) value;
    }
}
