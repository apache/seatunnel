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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SeaTunnelRowDebeziumDeserializationConvertersTest {

    @Test
    void testDefaultValueNotUsed() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);
        Schema schema =
                SchemaBuilder.struct()
                        .field("id", SchemaBuilder.int32().build())
                        .field("name", SchemaBuilder.string().defaultValue("UL"))
                        .build();
        Struct value = new Struct(schema);
        // the value of `name` is null, so do not put value for it
        value.put("id", 1);
        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        schema,
                        value,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, value, schema);
        Assertions.assertEquals(row.getField(0), 1);
        Assertions.assertNull(row.getField(1));
    }

    @Test
    void testArrayConverter() throws Exception {
        DebeziumDeserializationConverter converter;
        // bool array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.BOOLEAN_ARRAY_TYPE);
        Boolean[] booleans = new Boolean[] {false, true};
        Assertions.assertTrue(
                Arrays.equals(
                        booleans, (Boolean[]) (converter.convert(Arrays.asList(booleans), null))));
        // smallInt array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.SHORT_ARRAY_TYPE);
        Short[] shorts = new Short[] {(short) 1, (short) 2};
        Assertions.assertTrue(
                Arrays.equals(shorts, (Short[]) (converter.convert(Arrays.asList(shorts), null))));
        // int array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.INT_ARRAY_TYPE);
        Integer[] ints = new Integer[] {1, 2};
        Assertions.assertTrue(
                Arrays.equals(ints, (Integer[]) (converter.convert(Arrays.asList(ints), null))));
        // long array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.LONG_ARRAY_TYPE);
        Long[] longs = new Long[] {1L, 2L};
        Assertions.assertTrue(
                Arrays.equals(longs, (Long[]) (converter.convert(Arrays.asList(longs), null))));
        // float array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.FLOAT_ARRAY_TYPE);
        Float[] floats = new Float[] {1.0f, 2.0f};
        Assertions.assertTrue(
                Arrays.equals(floats, (Float[]) (converter.convert(Arrays.asList(floats), null))));
        // double array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.DOUBLE_ARRAY_TYPE);
        Double[] doubles = new Double[] {1.0, 2.0};
        Assertions.assertTrue(
                Arrays.equals(
                        doubles, (Double[]) (converter.convert(Arrays.asList(doubles), null))));
    }

    @Test
    void testGeometryStringConversion() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"geo"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);

        byte[] wkb = new byte[] {0x01, 0x02, (byte) 0xFF};
        Schema geometrySchema = Geometry.builder().optional().build();
        Schema recordSchema = SchemaBuilder.struct().field("geo", geometrySchema).build();

        Struct geometryValue = Geometry.createValue(geometrySchema, wkb, 4549);
        Struct recordValue = new Struct(recordSchema);
        recordValue.put("geo", geometryValue);

        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        recordSchema,
                        recordValue,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, recordValue, recordSchema);
        Object fieldValue = row.getField(0);
        Assertions.assertTrue(fieldValue instanceof String);
        Assertions.assertEquals("0102FF", fieldValue);
    }

    @Test
    void testGeographyStringConversion() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"geo"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);

        byte[] wkb = new byte[] {0x01, 0x02, (byte) 0xFF};
        Schema geographySchema = Geography.builder().optional().build();
        Schema recordSchema = SchemaBuilder.struct().field("geo", geographySchema).build();

        Struct geographyValue = Geometry.createValue(geographySchema, wkb, 4549);
        Struct recordValue = new Struct(recordSchema);
        recordValue.put("geo", geographyValue);

        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        recordSchema,
                        recordValue,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, recordValue, recordSchema);
        Object fieldValue = row.getField(0);
        Assertions.assertTrue(fieldValue instanceof String);
        Assertions.assertEquals("0102FF", fieldValue);
    }

    /**
     * Verifies the fallback chain in {@code parseOffsetDateTimeFromString} against the four
     * Debezium timestamp formats called out in the review:
     *
     * <ol>
     *   <li>Standard ISO-8601 with numeric offset (baseline)
     *   <li>Oracle TIMESTAMP WITH LOCAL TIME ZONE — IANA zone-region id
     *   <li>MySQL TIMESTAMP in certain schema-history modes — space date/time separator
     *   <li>PostgreSQL timestamptz — short-form hour-only offset
     * </ol>
     *
     * All four variants must parse to the same UTC instant.
     */
    @Test
    void testParseOffsetDateTimeFromStringFallbackChain() {
        ZoneId serverTz = ZoneId.of("Asia/Shanghai");
        // Expected UTC instant: 2024-01-01 04:00:00Z  (2024-01-01 12:00:00+08:00)
        OffsetDateTime expected = Instant.parse("2024-01-01T04:00:00Z").atOffset(ZoneOffset.UTC);

        // 1. ISO-8601 with numeric offset — handled by OffsetDateTime.parse
        OffsetDateTime r1 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01T12:00:00+08:00", serverTz);
        Assertions.assertEquals(expected, r1, "ISO-8601 numeric offset failed");

        // 2. IANA zone-region id — Oracle TIMESTAMP WITH LOCAL TIME ZONE
        OffsetDateTime r2 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01T12:00:00 Asia/Shanghai", serverTz);
        Assertions.assertEquals(expected, r2, "IANA zone-region id failed");

        // 3. Space separator — MySQL in certain schema-history modes
        OffsetDateTime r3 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01 12:00:00+08:00", serverTz);
        Assertions.assertEquals(expected, r3, "Space date/time separator failed");

        // 4. Short-form hour-only offset — PostgreSQL timestamptz
        OffsetDateTime r4 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01T12:00:00+08", serverTz);
        Assertions.assertEquals(expected, r4, "Short-form hour-only offset failed");
    }

    @Test
    void testParseOffsetDateTimeFromStringThrowsOnUnknownFormat() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                                "not-a-timestamp", ZoneId.systemDefault()));
    }
}
