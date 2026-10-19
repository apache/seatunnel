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
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class AvroConverterTest {

    private static final byte[] BYTES_PAYLOAD = new byte[] {10, 11, 12, 13, 14, 15, 16, 17};

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
    public void testConverterBytesAfterAvroReaderRecordReuse() throws IOException {
        SeaTunnelRowType rowType = buildBytesRowType();
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        byte[] longPayload = new byte[64];
        for (int i = 0; i < longPayload.length; i++) {
            longPayload[i] = (byte) (i + 1);
        }
        byte[] shortPayload = new byte[] {101, 102, 103};
        byte[] longMessage =
                serializationSchema.serialize(new SeaTunnelRow(new Object[] {longPayload, 1}));
        byte[] shortMessage =
                serializationSchema.serialize(new SeaTunnelRow(new Object[] {shortPayload, 2}));

        AvroToRowConverter avroToRowConverter = new AvroToRowConverter(rowType);
        DatumReader<GenericRecord> reader = avroToRowConverter.getReader();

        BinaryDecoder firstDecoder = DecoderFactory.get().binaryDecoder(longMessage, null);
        GenericRecord firstRecord = reader.read(null, firstDecoder);
        SeaTunnelRow firstRow = avroToRowConverter.converter(firstRecord, rowType);
        Assertions.assertArrayEquals(longPayload, (byte[]) firstRow.getField(0));

        // Avro reuses the previous record's ByteBuffer when the new value fits its capacity.
        BinaryDecoder reuseDecoder = DecoderFactory.get().binaryDecoder(shortMessage, null);
        GenericRecord reusedRecord = reader.read(firstRecord, reuseDecoder);
        SeaTunnelRow reusedRow = avroToRowConverter.converter(reusedRecord, rowType);

        Assertions.assertEquals(2, reusedRow.getField(1));
        Assertions.assertArrayEquals(shortPayload, (byte[]) reusedRow.getField(0));
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
}
