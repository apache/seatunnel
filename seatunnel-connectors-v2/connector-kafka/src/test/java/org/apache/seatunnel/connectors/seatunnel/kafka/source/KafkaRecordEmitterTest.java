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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KafkaRecordEmitterTest {

    @Test
    void emitRecordShouldAttachKafkaTimestampAsEventTime() throws Exception {
        long kafkaTimestamp = 1690000000000L;

        // Prepare a simple deserialization schema that creates a single-field row from bytes
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f0"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        DeserializationSchema<SeaTunnelRow> schema =
                new KafkaEventTimeDeserializationSchema(new SimpleStringRowSchema(rowType));

        // Build ConsumerMetadata map for the table
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setDeserializationSchema(schema);
        Map<TablePath, ConsumerMetadata> map = new HashMap<>();
        TablePath tablePath = TablePath.DEFAULT;
        map.put(tablePath, metadata);

        KafkaRecordEmitter emitter = new KafkaRecordEmitter(map, MessageFormatErrorHandleWay.FAIL);

        // Mock ConsumerRecord<byte[], byte[]>
        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record =
                Mockito.mock(org.apache.kafka.clients.consumer.ConsumerRecord.class);
        Mockito.when(record.timestamp()).thenReturn(kafkaTimestamp);
        Mockito.when(record.value()).thenReturn("hello".getBytes(StandardCharsets.UTF_8));
        Mockito.when(record.offset()).thenReturn(100L);

        // Prepare split state
        KafkaSourceSplit split = new KafkaSourceSplit(tablePath, new TopicPartition("t", 0));
        KafkaSourceSplitState splitState = new KafkaSourceSplitState(split);

        // Capture outputs
        List<SeaTunnelRow> out = new ArrayList<>();
        Collector<SeaTunnelRow> collector = new TestCollector(out);

        emitter.emitRecord(record, collector, splitState);

        Assertions.assertEquals(1, out.size());
        SeaTunnelRow row = out.get(0);
        Object eventTime = row.getOptions().get(CommonOptions.EVENT_TIME.getName());
        Assertions.assertEquals(kafkaTimestamp, eventTime);

        // Also verify split state offset advanced
        Assertions.assertEquals(101L, splitState.getCurrentOffset());
    }

    @Test
    void emitRecordShouldNotAttachEventTimeWhenTimestampNegative() throws Exception {
        long kafkaTimestamp = -1L; // invalid timestamp

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f0"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        DeserializationSchema<SeaTunnelRow> schema =
                new KafkaEventTimeDeserializationSchema(new SimpleStringRowSchema(rowType));

        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setDeserializationSchema(schema);
        Map<TablePath, ConsumerMetadata> map = new HashMap<>();
        TablePath tablePath = TablePath.DEFAULT;
        map.put(tablePath, metadata);

        KafkaRecordEmitter emitter = new KafkaRecordEmitter(map, MessageFormatErrorHandleWay.FAIL);

        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record =
                Mockito.mock(org.apache.kafka.clients.consumer.ConsumerRecord.class);
        Mockito.when(record.timestamp()).thenReturn(kafkaTimestamp);
        Mockito.when(record.value()).thenReturn("world".getBytes(StandardCharsets.UTF_8));
        Mockito.when(record.offset()).thenReturn(5L);

        KafkaSourceSplit split = new KafkaSourceSplit(tablePath, new TopicPartition("t2", 1));
        KafkaSourceSplitState splitState = new KafkaSourceSplitState(split);

        List<SeaTunnelRow> out = new ArrayList<>();
        Collector<SeaTunnelRow> collector = new TestCollector(out);

        emitter.emitRecord(record, collector, splitState);

        Assertions.assertEquals(1, out.size());
        SeaTunnelRow row = out.get(0);
        Assertions.assertFalse(row.getOptions().containsKey(CommonOptions.EVENT_TIME.getName()));
        Assertions.assertEquals(6L, splitState.getCurrentOffset());
    }

    @Test
    void emitRecordShouldAttachHeaderFields() throws Exception {
        SeaTunnelRowType baseRowType =
                new SeaTunnelRowType(
                        new String[] {"f0"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        SeaTunnelRowType extendedRowType =
                new SeaTunnelRowType(
                        new String[] {"f0", "correlation_id"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        List<String> headerFields = Arrays.asList("correlation_id");

        KafkaHeadersDeserializationSchema headersSchema =
                new KafkaHeadersDeserializationSchema(
                        new SimpleStringRowSchema(baseRowType), headerFields, extendedRowType);
        DeserializationSchema<SeaTunnelRow> schema =
                new KafkaEventTimeDeserializationSchema(headersSchema);

        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setDeserializationSchema(schema);
        Map<TablePath, ConsumerMetadata> map = new HashMap<>();
        TablePath tablePath = TablePath.DEFAULT;
        map.put(tablePath, metadata);

        KafkaRecordEmitter emitter = new KafkaRecordEmitter(map, MessageFormatErrorHandleWay.FAIL);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(
                new RecordHeader("correlation_id", "corr-test".getBytes(StandardCharsets.UTF_8)));

        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record =
                Mockito.mock(org.apache.kafka.clients.consumer.ConsumerRecord.class);
        Mockito.when(record.timestamp()).thenReturn(1000L);
        Mockito.when(record.value()).thenReturn("hello".getBytes(StandardCharsets.UTF_8));
        Mockito.when(record.offset()).thenReturn(0L);
        Mockito.when(record.headers()).thenReturn(recordHeaders);

        KafkaSourceSplit split = new KafkaSourceSplit(tablePath, new TopicPartition("t", 0));
        KafkaSourceSplitState splitState = new KafkaSourceSplitState(split);

        List<SeaTunnelRow> out = new ArrayList<>();
        emitter.emitRecord(record, new TestCollector(out), splitState);

        Assertions.assertEquals(1, out.size());
        SeaTunnelRow row = out.get(0);
        Assertions.assertEquals(2, row.getFields().length);
        Assertions.assertEquals("hello", row.getField(0));
        Assertions.assertEquals("corr-test", row.getField(1));
    }

    @Test
    void emitRecordShouldPreserveEventTimeWithHeaders() throws Exception {
        long kafkaTimestamp = 9876543210L;
        SeaTunnelRowType baseRowType =
                new SeaTunnelRowType(
                        new String[] {"f0"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        SeaTunnelRowType extendedRowType =
                new SeaTunnelRowType(
                        new String[] {"f0", "trace_id"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        KafkaHeadersDeserializationSchema headersSchema =
                new KafkaHeadersDeserializationSchema(
                        new SimpleStringRowSchema(baseRowType),
                        Arrays.asList("trace_id"),
                        extendedRowType);
        DeserializationSchema<SeaTunnelRow> schema =
                new KafkaEventTimeDeserializationSchema(headersSchema);

        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setDeserializationSchema(schema);
        Map<TablePath, ConsumerMetadata> map = new HashMap<>();
        TablePath tablePath = TablePath.DEFAULT;
        map.put(tablePath, metadata);

        KafkaRecordEmitter emitter = new KafkaRecordEmitter(map, MessageFormatErrorHandleWay.FAIL);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(
                new RecordHeader("trace_id", "trace-abc".getBytes(StandardCharsets.UTF_8)));

        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record =
                Mockito.mock(org.apache.kafka.clients.consumer.ConsumerRecord.class);
        Mockito.when(record.timestamp()).thenReturn(kafkaTimestamp);
        Mockito.when(record.value()).thenReturn("data".getBytes(StandardCharsets.UTF_8));
        Mockito.when(record.offset()).thenReturn(0L);
        Mockito.when(record.headers()).thenReturn(recordHeaders);

        KafkaSourceSplit split = new KafkaSourceSplit(tablePath, new TopicPartition("t3", 0));
        KafkaSourceSplitState splitState = new KafkaSourceSplitState(split);

        List<SeaTunnelRow> out = new ArrayList<>();
        emitter.emitRecord(record, new TestCollector(out), splitState);

        Assertions.assertEquals(1, out.size());
        SeaTunnelRow row = out.get(0);
        // Header field populated
        Assertions.assertEquals("trace-abc", row.getField(1));
        // Event time also set
        Assertions.assertEquals(
                kafkaTimestamp, row.getOptions().get(CommonOptions.EVENT_TIME.getName()));
    }

    @Test
    void emitRecordShouldSkipAnAvroTombstoneAndAdvanceTheOffset() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f0"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);

        // A tombstone must not fail the job under the default handling, and the offset must
        // move past it. Leaving the offset behind makes the record be replayed after every
        // restart, which the reader can never get past.
        for (MessageFormatErrorHandleWay handleWay : MessageFormatErrorHandleWay.values()) {
            List<SeaTunnelRow> out = new ArrayList<>();
            KafkaSourceSplitState splitState =
                    emitTombstone(new AvroDeserializationSchema(catalogTable), handleWay, out);

            Assertions.assertEquals(0, out.size(), "handleWay=" + handleWay);
            Assertions.assertEquals(43L, splitState.getCurrentOffset(), "handleWay=" + handleWay);
        }
    }

    @Test
    void emitRecordShouldNotFailInsideTheSkipHandlerWhenTheValueIsNull() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f0"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // Independently of any one format: once the skip handler is reached for a record with
        // no value, logging the body would raise a NullPointerException from inside the
        // handler and so defeat the skip that was configured.
        List<SeaTunnelRow> out = new ArrayList<>();
        KafkaSourceSplitState splitState =
                emitTombstone(
                        new AlwaysFailingRowSchema(rowType), MessageFormatErrorHandleWay.SKIP, out);

        Assertions.assertEquals(0, out.size());
        Assertions.assertEquals(43L, splitState.getCurrentOffset());
    }

    private static KafkaSourceSplitState emitTombstone(
            DeserializationSchema<SeaTunnelRow> schema,
            MessageFormatErrorHandleWay handleWay,
            List<SeaTunnelRow> out)
            throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setDeserializationSchema(schema);
        Map<TablePath, ConsumerMetadata> map = new HashMap<>();
        map.put(TablePath.DEFAULT, metadata);
        KafkaRecordEmitter emitter = new KafkaRecordEmitter(map, handleWay);

        // A real tombstone: a key, and a null value.
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "tombstone-topic",
                        3,
                        42L,
                        123L,
                        TimestampType.CREATE_TIME,
                        0L,
                        1,
                        -1,
                        "k".getBytes(StandardCharsets.UTF_8),
                        null);

        KafkaSourceSplit split =
                new KafkaSourceSplit(TablePath.DEFAULT, new TopicPartition("tombstone-topic", 3));
        KafkaSourceSplitState splitState = new KafkaSourceSplitState(split);
        splitState.setCurrentOffset(42L);

        emitter.emitRecord(record, new TestCollector(out), splitState);
        return splitState;
    }

    private static class AlwaysFailingRowSchema implements DeserializationSchema<SeaTunnelRow> {
        private final SeaTunnelRowType producedType;

        private AlwaysFailingRowSchema(SeaTunnelRowType producedType) {
            this.producedType = producedType;
        }

        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            throw new IOException("cannot deserialize");
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return producedType;
        }
    }

    private static class SimpleStringRowSchema implements DeserializationSchema<SeaTunnelRow> {
        private final SeaTunnelRowType producedType;

        private SimpleStringRowSchema(SeaTunnelRowType producedType) {
            this.producedType = producedType;
        }

        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            String v = new String(message, StandardCharsets.UTF_8);
            return new SeaTunnelRow(new Object[] {v});
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return producedType;
        }
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> out;

        private TestCollector(List<SeaTunnelRow> out) {
            this.out = out;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            out.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
