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

package org.apache.seatunnel.format.compatible.debezium.json;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestDebeziumJsonConverter {

    /** A nullable DECIMAL(20,4) column with DEFAULT '0.0000'. */
    private static Schema decimalWithDefaultSchema() {
        return SchemaBuilder.struct()
                .field(
                        "reg_capital",
                        Decimal.builder(4)
                                .optional()
                                .defaultValue(new BigDecimal("0.0000"))
                                .build())
                .build();
    }

    private static SourceRecord record(Schema schema, Struct value) {
        return new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), null, null, null, schema, value);
    }

    @Test
    public void testSerializeDecimalToNumber() throws Exception {
        String key = "k";
        String value = "v";
        Struct keyStruct =
                new Struct(SchemaBuilder.struct().field(key, Decimal.builder(2).build()).build());
        keyStruct.put(key, BigDecimal.valueOf(1101, 2));
        Struct valueStruct =
                new Struct(SchemaBuilder.struct().field(value, Decimal.builder(2).build()).build());
        valueStruct.put(value, BigDecimal.valueOf(1101, 2));

        SourceRecord sourceRecord =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        keyStruct.schema(),
                        keyStruct,
                        valueStruct.schema(),
                        valueStruct);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals("{\"k\":11.01}", converter.serializeKey(sourceRecord));
        Assertions.assertEquals("{\"v\":11.01}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testNullWithSchemaDefaultIsSerializedAsJsonNull() throws Exception {
        // Raw NULL must stay JSON null even though the schema has a default, because the
        // underlying JsonConverter is configured with replace.null.with.default=false.
        Schema schema = decimalWithDefaultSchema();
        Struct value = new Struct(schema); // reg_capital is not set -> null
        SourceRecord sourceRecord = record(schema, value);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals("{\"reg_capital\":null}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testZeroDecimalIsPreservedAsProvided() throws Exception {
        // A real zero must stay zero and keep its scale in the raw output.
        Schema schema = decimalWithDefaultSchema();
        Struct value = new Struct(schema);
        value.put("reg_capital", new BigDecimal("0.0000"));
        SourceRecord sourceRecord = record(schema, value);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals("{\"reg_capital\":0.0000}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testNonZeroDecimalIsPreserved() throws Exception {
        Schema schema = decimalWithDefaultSchema();
        Struct value = new Struct(schema);
        value.put("reg_capital", new BigDecimal("116161.5000"));
        SourceRecord sourceRecord = record(schema, value);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"reg_capital\":116161.5000}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testNullWithSchemaDefaultInKeyIsSerializedAsJsonNull() throws Exception {
        Schema schema = decimalWithDefaultSchema();
        Struct key = new Struct(schema);
        SourceRecord sourceRecord =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        schema,
                        key,
                        schema,
                        new Struct(schema));

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals("{\"reg_capital\":null}", converter.serializeKey(sourceRecord));
    }

    @Test
    public void testWithEnvelopeKeepsSchemaDefaultAndNullPayload() throws Exception {
        Schema schema = decimalWithDefaultSchema();
        Struct value = new Struct(schema);
        SourceRecord sourceRecord = record(schema, value);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(true, true);
        String serialized = converter.serializeValue(sourceRecord);

        JsonNode json = new ObjectMapper().readTree(serialized);
        Assertions.assertTrue(json.has("schema"));
        Assertions.assertTrue(json.has("payload"));

        // payload keeps the explicit NULL
        Assertions.assertTrue(json.get("payload").get("reg_capital").isNull());

        // schema metadata (including the default) is preserved
        JsonNode fieldSchema = json.at("/schema/fields/0");
        Assertions.assertEquals("reg_capital", fieldSchema.get("field").asText());
        Assertions.assertEquals(
                0, fieldSchema.get("default").decimalValue().compareTo(new BigDecimal("0.0000")));
        Assertions.assertTrue(serialized.contains("\"default\":0.0000"));
    }

    @Test
    public void testWithEnvelopeAndNonNullValue() throws Exception {
        Schema schema = decimalWithDefaultSchema();
        Struct value = new Struct(schema);
        value.put("reg_capital", new BigDecimal("123456.7800"));
        SourceRecord sourceRecord = record(schema, value);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(true, true);
        JsonNode json = new ObjectMapper().readTree(converter.serializeValue(sourceRecord));
        Assertions.assertEquals(
                0,
                json.get("payload")
                        .get("reg_capital")
                        .decimalValue()
                        .compareTo(new BigDecimal("123456.7800")));
        Assertions.assertEquals(
                0,
                json.at("/schema/fields/0/default")
                        .decimalValue()
                        .compareTo(new BigDecimal("0.0000")));
    }

    @Test
    public void testReplaceNullWithDefaultTrueReproducesUpstreamDefault() throws Exception {
        // With replaceNullWithDefault=true the upstream default behavior (Kafka Connect 3.9.0
        // default) is reproduced: NULL is substituted with the schema default.
        Schema schema = decimalWithDefaultSchema();
        Struct value = new Struct(schema);
        SourceRecord sourceRecord = record(schema, value);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false, true);
        Assertions.assertEquals("{\"reg_capital\":0.0000}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testSerializeValueWithNullSchemaAndNullValueReturnsNull() throws Exception {
        // Mirrors JsonConverter#fromConnectData: schema == null && value == null yields null
        // instead of a NPE.
        SourceRecord sourceRecord =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        null,
                        null,
                        null,
                        null);
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertNull(converter.serializeValue(sourceRecord));
    }

    @Test
    public void testMixedNullAndValueFields() throws Exception {
        Schema schema =
                SchemaBuilder.struct()
                        .field(
                                "reg_capital",
                                Decimal.builder(4)
                                        .optional()
                                        .defaultValue(new BigDecimal("0.0000"))
                                        .build())
                        .field("cus_id", SchemaBuilder.int64().optional().defaultValue(0L).build())
                        .build();
        Struct value = new Struct(schema);
        value.put("cus_id", 116161L);

        SourceRecord sourceRecord = record(schema, value);
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"reg_capital\":null,\"cus_id\":116161}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testPrimitives() throws Exception {
        Schema schema =
                SchemaBuilder.struct()
                        .field("i8", SchemaBuilder.int8().build())
                        .field("i16", SchemaBuilder.int16().build())
                        .field("i32", SchemaBuilder.int32().build())
                        .field("i64", SchemaBuilder.int64().build())
                        .field("f32", SchemaBuilder.float32().build())
                        .field("f64", SchemaBuilder.float64().build())
                        .field("b", SchemaBuilder.bool().build())
                        .field("s", SchemaBuilder.string().build())
                        .build();
        Struct value = new Struct(schema);
        value.put("i8", (byte) 1);
        value.put("i16", (short) 2);
        value.put("i32", 3);
        value.put("i64", 4L);
        value.put("f32", 1.5f);
        value.put("f64", 2.25d);
        value.put("b", true);
        value.put("s", "hello");
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"i8\":1,\"i16\":2,\"i32\":3,\"i64\":4,\"f32\":1.5,\"f64\":2.25,\"b\":true,\"s\":\"hello\"}",
                converter.serializeValue(record(schema, value)));
    }

    @Test
    public void testBytes() throws Exception {
        Schema schema =
                SchemaBuilder.struct()
                        .field("b1", SchemaBuilder.bytes().optional().build())
                        .field("b2", SchemaBuilder.bytes().optional().build())
                        .build();
        Struct value = new Struct(schema);
        value.put("b1", new byte[] {0, 1, -1, 42});
        value.put("b2", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"b1\":\"AAH/Kg==\",\"b2\":\"AQIDBA==\"}",
                converter.serializeValue(record(schema, value)));
    }

    @Test
    public void testArray() throws Exception {
        Schema schema =
                SchemaBuilder.struct()
                        .field("arr", SchemaBuilder.array(SchemaBuilder.string().build()).build())
                        .build();
        Struct value = new Struct(schema);
        value.put("arr", Arrays.asList("a", "b", "c"));
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"arr\":[\"a\",\"b\",\"c\"]}", converter.serializeValue(record(schema, value)));
    }

    @Test
    public void testMapBothModes() throws Exception {
        Schema schema =
                SchemaBuilder.struct()
                        .field(
                                "m1",
                                SchemaBuilder.map(
                                                SchemaBuilder.string().build(),
                                                SchemaBuilder.int64().build())
                                        .build())
                        .field(
                                "m2",
                                SchemaBuilder.map(
                                                SchemaBuilder.int32().build(),
                                                SchemaBuilder.string().build())
                                        .build())
                        .build();
        Struct value = new Struct(schema);
        Map<String, Long> m1 = new LinkedHashMap<>();
        m1.put("a", 1L);
        m1.put("b", 2L);
        value.put("m1", m1);
        Map<Integer, String> m2 = new LinkedHashMap<>();
        m2.put(1, "x");
        m2.put(2, "y");
        value.put("m2", m2);
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"m1\":{\"a\":1,\"b\":2},\"m2\":[[1,\"x\"],[2,\"y\"]]}",
                converter.serializeValue(record(schema, value)));
    }

    @Test
    public void testNestedStruct() throws Exception {
        Schema inner =
                SchemaBuilder.struct().field("n", SchemaBuilder.int64().optional().build()).build();
        Schema schema = SchemaBuilder.struct().field("outer", inner).build();
        Struct value = new Struct(schema);
        Struct innerValue = new Struct(inner);
        innerValue.put("n", 7L);
        value.put("outer", innerValue);
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"outer\":{\"n\":7}}", converter.serializeValue(record(schema, value)));
    }

    @Test
    public void testLogicalTypes() throws Exception {
        Schema schema =
                SchemaBuilder.struct()
                        .field("d", Date.builder().optional().build())
                        .field("t", Time.builder().optional().build())
                        .field("ts", Timestamp.builder().optional().build())
                        .build();
        Struct value = new Struct(schema);
        value.put("d", new java.util.Date(1173312000000L));
        value.put("t", new java.util.Date(44700000L));
        value.put("ts", new java.util.Date(1788436000000L));
        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(
                "{\"d\":13580,\"t\":44700000,\"ts\":1788436000000}",
                converter.serializeValue(record(schema, value)));
    }

    @Test
    public void testDebeziumSerializeKeyIsNull() throws Exception {
        String value = "v";
        Struct valueStruct = new Struct(SchemaBuilder.struct().field(value, Schema.STRING_SCHEMA));
        valueStruct.put(value, "DebeziumTest");

        SourceRecord sourceRecord =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        null,
                        null,
                        valueStruct.schema(),
                        valueStruct);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(null, converter.serializeKey(sourceRecord));
        Assertions.assertEquals("{\"v\":\"DebeziumTest\"}", converter.serializeValue(sourceRecord));
    }
}
