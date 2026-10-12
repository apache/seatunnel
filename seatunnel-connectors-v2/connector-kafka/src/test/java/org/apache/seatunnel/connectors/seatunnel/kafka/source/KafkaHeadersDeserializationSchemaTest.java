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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KafkaHeadersDeserializationSchemaTest {

    private static final SeaTunnelRowType BASE_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"user_id", "name"},
                    new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

    private static final SeaTunnelRowType EXTENDED_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"user_id", "name", "correlation_id"},
                    new SeaTunnelDataType[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                    });

    private static final List<String> HEADER_FIELDS = Arrays.asList("correlation_id");

    @Test
    void testHeaderValueAppendedToRow() throws IOException {
        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(BASE_ROW_TYPE, new Object[] {42, "Alice"}),
                        HEADER_FIELDS,
                        EXTENDED_ROW_TYPE);

        RecordHeaders headers = new RecordHeaders();
        headers.add(
                new RecordHeader("correlation_id", "corr-001".getBytes(StandardCharsets.UTF_8)));
        schema.setCurrentRecordHeaders(headers);

        SeaTunnelRow row = schema.deserialize(new byte[0]);

        Assertions.assertEquals(3, row.getFields().length);
        Assertions.assertEquals(42, row.getField(0));
        Assertions.assertEquals("Alice", row.getField(1));
        Assertions.assertEquals("corr-001", row.getField(2));
    }

    @Test
    void testNullHeaderValueBecomesNull() throws IOException {
        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(BASE_ROW_TYPE, new Object[] {1, "Bob"}),
                        HEADER_FIELDS,
                        EXTENDED_ROW_TYPE);

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("correlation_id", null));
        schema.setCurrentRecordHeaders(headers);

        SeaTunnelRow row = schema.deserialize(new byte[0]);

        Assertions.assertNull(row.getField(2));
    }

    @Test
    void testMissingHeaderKeyBecomesNull() throws IOException {
        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(BASE_ROW_TYPE, new Object[] {1, "Charlie"}),
                        HEADER_FIELDS,
                        EXTENDED_ROW_TYPE);

        schema.setCurrentRecordHeaders(new RecordHeaders());

        SeaTunnelRow row = schema.deserialize(new byte[0]);

        Assertions.assertNull(row.getField(2));
    }

    @Test
    void testRowOptionsPreserved() throws IOException {
        Map<String, Object> options = new HashMap<>();
        options.put("EVENT_TIME", 12345L);
        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(BASE_ROW_TYPE, new Object[] {1, "Dave"}, options),
                        HEADER_FIELDS,
                        EXTENDED_ROW_TYPE);

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("correlation_id", "val".getBytes(StandardCharsets.UTF_8)));
        schema.setCurrentRecordHeaders(headers);

        SeaTunnelRow row = schema.deserialize(new byte[0]);

        Assertions.assertEquals(12345L, row.getOptions().get("EVENT_TIME"));
    }

    @Test
    void testGetProducedTypeReturnsExtended() {
        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(BASE_ROW_TYPE, new Object[] {1, "Eve"}),
                        HEADER_FIELDS,
                        EXTENDED_ROW_TYPE);

        SeaTunnelRowType producedType = (SeaTunnelRowType) schema.getProducedType();

        Assertions.assertEquals(3, producedType.getTotalFields());
        Assertions.assertEquals("user_id", producedType.getFieldName(0));
        Assertions.assertEquals("name", producedType.getFieldName(1));
        Assertions.assertEquals("correlation_id", producedType.getFieldName(2));
        Assertions.assertEquals(BasicType.STRING_TYPE, producedType.getFieldType(2));
    }

    @Test
    void testDeserializeViaCollector() throws IOException {
        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(BASE_ROW_TYPE, new Object[] {5, "Frank"}),
                        HEADER_FIELDS,
                        EXTENDED_ROW_TYPE);

        RecordHeaders headers = new RecordHeaders();
        headers.add(
                new RecordHeader("correlation_id", "corr-abc".getBytes(StandardCharsets.UTF_8)));
        schema.setCurrentRecordHeaders(headers);

        List<SeaTunnelRow> out = new ArrayList<>();
        schema.deserialize(new byte[0], new TestCollector(out));

        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("corr-abc", out.get(0).getField(2));
    }

    @Test
    void testMultipleHeaderFields() throws IOException {
        SeaTunnelRowType multiExtendedType =
                new SeaTunnelRowType(
                        new String[] {"user_id", "correlation_id", "trace_id"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        List<String> multiHeaders = Arrays.asList("correlation_id", "trace_id");

        SeaTunnelRowType singleBase =
                new SeaTunnelRowType(
                        new String[] {"user_id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        KafkaHeadersDeserializationSchema schema =
                new KafkaHeadersDeserializationSchema(
                        new FixedRowSchema(singleBase, new Object[] {7}),
                        multiHeaders,
                        multiExtendedType);

        RecordHeaders headers = new RecordHeaders();
        headers.add(
                new RecordHeader("correlation_id", "corr-xyz".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("trace_id", "trace-123".getBytes(StandardCharsets.UTF_8)));
        schema.setCurrentRecordHeaders(headers);

        SeaTunnelRow row = schema.deserialize(new byte[0]);

        Assertions.assertEquals(3, row.getFields().length);
        Assertions.assertEquals(7, row.getField(0));
        Assertions.assertEquals("corr-xyz", row.getField(1));
        Assertions.assertEquals("trace-123", row.getField(2));
    }

    private static class FixedRowSchema implements DeserializationSchema<SeaTunnelRow> {
        private final SeaTunnelRowType rowType;
        private final Object[] fields;
        private final Map<String, Object> options;

        FixedRowSchema(SeaTunnelRowType rowType, Object[] fields) {
            this(rowType, fields, new HashMap<>());
        }

        FixedRowSchema(SeaTunnelRowType rowType, Object[] fields, Map<String, Object> options) {
            this.rowType = rowType;
            this.fields = fields;
            this.options = options;
        }

        @Override
        public SeaTunnelRow deserialize(byte[] message) {
            SeaTunnelRow row = new SeaTunnelRow(fields.clone());
            row.setOptions(new HashMap<>(options));
            return row;
        }

        @Override
        public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
            out.collect(deserialize(message));
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return rowType;
        }
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> out;

        TestCollector(List<SeaTunnelRow> out) {
            this.out = out;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            out.add(record);
        }

        @Override
        public void collect(SchemaChangeEvent event) {}

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
