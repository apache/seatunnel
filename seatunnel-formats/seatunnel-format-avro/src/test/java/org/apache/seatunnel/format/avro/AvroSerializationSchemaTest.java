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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class AvroSerializationSchemaTest {

    private static final LocalDate localDate = LocalDate.of(2023, 1, 1);
    private static final BigDecimal bigDecimal =
            new BigDecimal("61592600349703735722.724745739637773662");
    private static final LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 1, 6, 30, 40);

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow subSeaTunnelRow = new SeaTunnelRow(14);
        Map<String, String> map = new HashMap<>();
        map.put("k1", "1");
        map.put("k2", "2");
        String[] strArray = new String[] {"l1", "l2"};
        byte byteVal = 100;
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
            new SeaTunnelRowType(subField, subFieldTypes)
        };
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Test
    public void testSerialization() throws IOException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        byte[] bytes = serializationSchema.serialize(seaTunnelRow);
        AvroDeserializationSchema deserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        SeaTunnelRow deserialize = deserializationSchema.deserialize(bytes);
        String[] strArray1 = (String[]) seaTunnelRow.getField(1);
        String[] strArray2 = (String[]) deserialize.getField(1);
        Assertions.assertArrayEquals(strArray1, strArray2);
        SeaTunnelRow subRow = (SeaTunnelRow) deserialize.getField(14);
        Assertions.assertEquals((double) subRow.getField(9), 123.456);
        BigDecimal bigDecimal1 = (BigDecimal) subRow.getField(12);
        Assertions.assertEquals(bigDecimal1.compareTo(bigDecimal), 0);
        LocalDateTime localDateTime1 = (LocalDateTime) subRow.getField(13);
        Assertions.assertEquals(localDateTime1.compareTo(localDateTime), 0);
    }

    private SeaTunnelRow buildSeaTunnelRowValueNull() {
        SeaTunnelRow subSeaTunnelRow = new SeaTunnelRow(14);
        subSeaTunnelRow.setField(0, null);
        subSeaTunnelRow.setField(1, null);
        subSeaTunnelRow.setField(2, null);
        subSeaTunnelRow.setField(3, null);
        subSeaTunnelRow.setField(4, null);
        subSeaTunnelRow.setField(5, null);
        subSeaTunnelRow.setField(6, null);
        subSeaTunnelRow.setField(7, null);
        subSeaTunnelRow.setField(8, null);
        subSeaTunnelRow.setField(9, null);
        subSeaTunnelRow.setField(10, null);
        subSeaTunnelRow.setField(11, null);
        subSeaTunnelRow.setField(12, null);
        subSeaTunnelRow.setField(13, null);

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(15);
        seaTunnelRow.setField(0, null);
        seaTunnelRow.setField(1, null);
        seaTunnelRow.setField(2, null);
        seaTunnelRow.setField(3, null);
        seaTunnelRow.setField(4, null);
        seaTunnelRow.setField(5, null);
        seaTunnelRow.setField(6, null);
        seaTunnelRow.setField(7, null);
        seaTunnelRow.setField(8, null);
        seaTunnelRow.setField(9, null);
        seaTunnelRow.setField(10, null);
        seaTunnelRow.setField(11, null);
        seaTunnelRow.setField(12, null);
        seaTunnelRow.setField(13, null);
        seaTunnelRow.setField(14, subSeaTunnelRow);
        return seaTunnelRow;
    }

    @Test
    public void testSerializationValueNull() throws IOException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRowValueNull();
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        byte[] bytes = serializationSchema.serialize(seaTunnelRow);
        AvroDeserializationSchema deserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        SeaTunnelRow deserialize = deserializationSchema.deserialize(bytes);
        String[] strArray1 = (String[]) seaTunnelRow.getField(1);
        String[] strArray2 = (String[]) deserialize.getField(1);
        Assertions.assertArrayEquals(strArray1, strArray2);
        SeaTunnelRow subRow = (SeaTunnelRow) deserialize.getField(14);
        Assertions.assertEquals(subRow.getField(9), null);
        Assertions.assertEquals(subRow.getField(12), null);
        Assertions.assertEquals(subRow.getField(13), null);
    }

    // A null ROW value is legal for the write converter (RowToAvroConverter#resolveObject returns
    // null before the type switch for every SeaTunnelDataType), so it must survive the public
    // binary round trip once the opt-in nullable ROW layout is enabled on both sides. The three
    // tests below cover the single root cause "a ROW schema carries no NULL union branch" through
    // the three containers that reuse that schema: a top-level ROW field, ARRAY<ROW> elements and
    // MAP<STRING, ROW> values. They check round-trip values independently of how the schema is
    // constructed.
    @Test
    public void testSerializationNestedRowFieldValueNullWithNullableRow() throws IOException {
        SeaTunnelRowType nestedType =
                new SeaTunnelRowType(
                        new String[] {"InnerID"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"payload"}, new SeaTunnelDataType<?>[] {nestedType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow sourceRow = new SeaTunnelRow(1);
        sourceRow.setField(0, null);

        byte[] bytes = new AvroSerializationSchema(rowType, true).serialize(sourceRow);
        SeaTunnelRow result = nullableRowDeserializer(catalogTable).deserialize(bytes);

        Assertions.assertNull(result.getField(0));
    }

    @Test
    public void testSerializationArrayOfRowNullElementWithNullableRow() throws IOException {
        SeaTunnelRowType nestedType =
                new SeaTunnelRowType(
                        new String[] {"InnerID"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        ArrayType<SeaTunnelRow[], SeaTunnelRow> arrayType = ArrayType.of(nestedType);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"items"}, new SeaTunnelDataType<?>[] {arrayType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow present = new SeaTunnelRow(1);
        present.setField(0, 42);
        SeaTunnelRow sourceRow = new SeaTunnelRow(1);
        sourceRow.setField(0, new SeaTunnelRow[] {present, null});

        byte[] bytes = new AvroSerializationSchema(rowType, true).serialize(sourceRow);
        SeaTunnelRow result = nullableRowDeserializer(catalogTable).deserialize(bytes);

        SeaTunnelRow[] items = (SeaTunnelRow[]) result.getField(0);
        Assertions.assertEquals(42, items[0].getField(0));
        Assertions.assertNull(items[1]);
    }

    @Test
    public void testSerializationMapOfRowNullValueWithNullableRow() throws IOException {
        SeaTunnelRowType nestedType =
                new SeaTunnelRowType(
                        new String[] {"InnerID"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        MapType<String, SeaTunnelRow> mapType = new MapType<>(BasicType.STRING_TYPE, nestedType);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"payload_map"}, new SeaTunnelDataType<?>[] {mapType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow present = new SeaTunnelRow(1);
        present.setField(0, 42);
        Map<String, SeaTunnelRow> mapValue = new HashMap<>();
        mapValue.put("present", present);
        mapValue.put("absent", null);
        SeaTunnelRow sourceRow = new SeaTunnelRow(1);
        sourceRow.setField(0, mapValue);

        byte[] bytes = new AvroSerializationSchema(rowType, true).serialize(sourceRow);
        SeaTunnelRow result = nullableRowDeserializer(catalogTable).deserialize(bytes);

        Map<?, ?> roundTripped = (Map<?, ?>) result.getField(0);
        Assertions.assertEquals(2, roundTripped.size());
        Assertions.assertTrue(roundTripped.containsKey("absent"));
        Assertions.assertEquals(42, ((SeaTunnelRow) roundTripped.get("present")).getField(0));
        Assertions.assertNull(roundTripped.get("absent"));
    }

    private static AvroDeserializationSchema nullableRowDeserializer(CatalogTable catalogTable) {
        return new AvroDeserializationSchema(catalogTable, null, false, true);
    }

    private static SeaTunnelRow buildNestedRowTypeValue(int innerId) {
        SeaTunnelRow nestedRow = new SeaTunnelRow(1);
        nestedRow.setField(0, innerId);
        return nestedRow;
    }

    private static SeaTunnelRowType buildNestedRowType() {
        return new SeaTunnelRowType(
                new String[] {"InnerID"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
    }

    // Both modes must stay byte identical for a schema without any ROW field: the option only
    // describes the ROW layout, so the binary layout of every other schema is untouched.
    @Test
    public void testRowFreeSchemaBytesUnaffectedByNullableRow() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "amount"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                        });
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "seaTunnel", 42L});

        byte[] defaultBytes = new AvroSerializationSchema(rowType).serialize(row);
        byte[] explicitFalseBytes = new AvroSerializationSchema(rowType, false).serialize(row);
        byte[] nullableRowBytes = new AvroSerializationSchema(rowType, true).serialize(row);

        Assertions.assertArrayEquals(defaultBytes, explicitFalseBytes);
        Assertions.assertArrayEquals(explicitFalseBytes, nullableRowBytes);
    }

    // The default path is what every existing job uses, so it has to stay byte identical to the
    // explicit false path even when the schema contains a ROW field.
    @Test
    public void testDefaultBytesMatchExplicitFalseForRowBearingSchema() {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow row = buildSeaTunnelRow();

        byte[] defaultBytes = new AvroSerializationSchema(rowType).serialize(row);
        byte[] explicitFalseBytes = new AvroSerializationSchema(rowType, false).serialize(row);

        Assertions.assertArrayEquals(defaultBytes, explicitFalseBytes);
    }

    // Enabling the option inserts the union branch index of every ROW value, so a schema that
    // contains a present ROW field cannot produce the same bytes in both modes.
    @Test
    public void testNonNullRowBytesDifferWhenNullableRowEnabled() {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow row = buildSeaTunnelRow();

        byte[] defaultBytes = new AvroSerializationSchema(rowType).serialize(row);
        byte[] nullableRowBytes = new AvroSerializationSchema(rowType, true).serialize(row);

        Assertions.assertFalse(Arrays.equals(defaultBytes, nullableRowBytes));
    }

    // The flag has to be applied recursively: a ROW that is nested inside another ROW, and a ROW
    // nested in an ARRAY of a nested ROW, must follow the same nullable layout as a top level ROW.
    @Test
    public void testNullableRowAppliesBeyondOneNestingLevel() throws IOException {
        SeaTunnelRowType innermostType = buildNestedRowType();
        SeaTunnelRowType middleType =
                new SeaTunnelRowType(
                        new String[] {"inner", "inner_items"},
                        new SeaTunnelDataType<?>[] {innermostType, ArrayType.of(innermostType)});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"outer"}, new SeaTunnelDataType<?>[] {middleType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);

        SeaTunnelRow middleRow = new SeaTunnelRow(2);
        middleRow.setField(0, null);
        middleRow.setField(1, new SeaTunnelRow[] {buildNestedRowTypeValue(7), null});
        SeaTunnelRow sourceRow = new SeaTunnelRow(1);
        sourceRow.setField(0, middleRow);

        byte[] bytes = new AvroSerializationSchema(rowType, true).serialize(sourceRow);
        SeaTunnelRow result = nullableRowDeserializer(catalogTable).deserialize(bytes);

        SeaTunnelRow middleResult = (SeaTunnelRow) result.getField(0);
        Assertions.assertNull(middleResult.getField(0));
        SeaTunnelRow[] innerItems = (SeaTunnelRow[]) middleResult.getField(1);
        Assertions.assertEquals(7, innerItems[0].getField(0));
        Assertions.assertNull(innerItems[1]);
    }

    // Messages written before the option existed keep the legacy layout. They stay readable only
    // through the supplied writer schema, which keeps describing the actual wire layout even when
    // the nullable flag is enabled on the reader.
    @Test
    public void testSuppliedWriterSchemaStaysAuthoritativeForNullableRowReader()
            throws IOException {
        SeaTunnelRowType rowType = buildNestedRowType();
        SeaTunnelRowType tableRowType =
                new SeaTunnelRowType(
                        new String[] {"payload"}, new SeaTunnelDataType<?>[] {rowType});
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("", "", "", "test", tableRowType);
        SeaTunnelRow sourceRow = new SeaTunnelRow(1);
        sourceRow.setField(0, buildNestedRowTypeValue(42));

        byte[] legacyBytes = new AvroSerializationSchema(tableRowType).serialize(sourceRow);
        String legacyWriterSchema =
                SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(
                                tableRowType, false)
                        .toString();

        SeaTunnelRow result =
                new AvroDeserializationSchema(catalogTable, legacyWriterSchema, false, true)
                        .deserialize(legacyBytes);

        Assertions.assertEquals(42, ((SeaTunnelRow) result.getField(0)).getField(0));
    }

    @Test
    public void testDeserializeWithProvidedWriterSchema() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dev_id", "timestamp", "s_port"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.LONG_TYPE, BasicType.INT_TYPE
                        });
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        String writerSchemaText =
                "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"AvroEvent\","
                        + "\"namespace\":\"safe.serialize\","
                        + "\"fields\":["
                        + "{\"name\":\"dev_id\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"]},"
                        + "{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]},"
                        + "{\"name\":\"s_port\",\"type\":[\"int\",\"null\"]}"
                        + "]"
                        + "}";
        Schema writerSchema = new Schema.Parser().parse(writerSchemaText);
        GenericRecord record =
                new GenericRecordBuilder(writerSchema)
                        .set("dev_id", "device-1")
                        .set("timestamp", 1769504419000L)
                        .set("s_port", null)
                        .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(writerSchema).write(record, encoder);
        encoder.flush();

        AvroDeserializationSchema deserializationSchema =
                new AvroDeserializationSchema(catalogTable, writerSchemaText);
        SeaTunnelRow row = deserializationSchema.deserialize(out.toByteArray());

        Assertions.assertEquals("device-1", row.getField(0));
        Assertions.assertEquals(1769504419000L, row.getField(1));
        Assertions.assertNull(row.getField(2));
    }

    @Test
    public void testMixedCaseFieldSerialization() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"CustomerID", "customerid"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.INT_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow sourceRow = new SeaTunnelRow(2);
        sourceRow.setField(0, 42);
        sourceRow.setField(1, 84);

        byte[] bytes = new AvroSerializationSchema(rowType).serialize(sourceRow);
        SeaTunnelRow result = new AvroDeserializationSchema(catalogTable).deserialize(bytes);

        Assertions.assertEquals(42, result.getField(0));
        Assertions.assertEquals(84, result.getField(1));
    }

    @Test
    public void testNestedMixedCaseFieldSerialization() throws IOException {
        SeaTunnelRowType nestedType =
                new SeaTunnelRowType(
                        new String[] {"InnerID"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"payload"}, new SeaTunnelDataType<?>[] {nestedType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow nestedRow = new SeaTunnelRow(1);
        nestedRow.setField(0, 42);
        SeaTunnelRow sourceRow = new SeaTunnelRow(1);
        sourceRow.setField(0, nestedRow);

        byte[] bytes = new AvroSerializationSchema(rowType).serialize(sourceRow);
        SeaTunnelRow result = new AvroDeserializationSchema(catalogTable).deserialize(bytes);

        Assertions.assertEquals(42, ((SeaTunnelRow) result.getField(0)).getField(0));
    }

    @Test
    public void testDeserializeConfluentSchemaRegistryHeader() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"payload"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        String writerSchemaText =
                "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"payload\",\"type\":\"string\"}]}";
        Schema writerSchema = new Schema.Parser().parse(writerSchemaText);
        GenericRecord record =
                new GenericRecordBuilder(writerSchema).set("payload", "seatunnel").build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(writerSchema).write(record, encoder);
        encoder.flush();
        byte[] framed = new byte[out.size() + 5];
        framed[0] = 0;
        framed[1] = 0;
        framed[2] = 0;
        framed[3] = 0;
        framed[4] = 1;
        System.arraycopy(out.toByteArray(), 0, framed, 5, out.size());

        AvroDeserializationSchema schema =
                new AvroDeserializationSchema(catalogTable, writerSchemaText, true);
        Assertions.assertEquals("seatunnel", schema.deserialize(framed).getField(0));
    }

    @Test
    public void testDeserializeTombstoneIsSkippedInsteadOfThrowing() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"payload"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);

        // A Kafka tombstone carries a key and a null value. Every other SeaTunnel format
        // returns null for it rather than failing, so Avro must not raise a
        // NullPointerException from message.length.
        Assertions.assertNull(new AvroDeserializationSchema(catalogTable).deserialize(null));
        Assertions.assertNull(new AvroDeserializationSchema(catalogTable).deserialize(new byte[0]));

        // The same holds when the Confluent header is being stripped.
        AvroDeserializationSchema framedSchema =
                new AvroDeserializationSchema(
                        catalogTable,
                        "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"payload\",\"type\":\"string\"}]}",
                        true);
        Assertions.assertNull(framedSchema.deserialize(null));
        Assertions.assertNull(framedSchema.deserialize(new byte[0]));
    }

    @Test
    public void testDeserializeConfluentSchemaRegistryHeaderRejectsInvalidHeader() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"payload"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        AvroDeserializationSchema schema =
                new AvroDeserializationSchema(
                        catalogTable,
                        "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"payload\",\"type\":\"string\"}]}",
                        true);
        Assertions.assertThrows(
                RuntimeException.class, () -> schema.deserialize(new byte[] {1, 0, 0, 0, 1}));
    }
}
