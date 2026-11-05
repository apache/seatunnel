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

package org.apache.seatunnel.format.compatible.kafka.connect.json;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Tombstone handling for the {@code compatible_kafka_connect_json} source format. */
class CompatibleKafkaConnectDeserializationSchemaTest {

    /**
     * The converters are configured with {@code schemas.enable}, whose option default is true, so
     * both the key and the value of a well formed record carry a schema/payload envelope.
     */
    private static final String KEY_ENVELOPE =
            "{\"schema\":{\"type\":\"string\",\"optional\":true},\"payload\":\"k1\"}";

    private static final String VALUE_ENVELOPE =
            "{\"schema\":{\"type\":\"struct\",\"fields\":["
                    + "{\"type\":\"int64\",\"optional\":true,\"field\":\"id\"},"
                    + "{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}]},"
                    + "\"payload\":{\"id\":1,\"name\":\"a\"}}";

    /** A tombstone carries a null value, so it must produce no row rather than failing. */
    @Test
    void deserializeProducesNoRowForATombstone() throws Exception {
        RecordingCollector collector = new RecordingCollector();
        schema().deserialize(record(null), collector);

        Assertions.assertTrue(
                collector.rows.isEmpty(), "a tombstone must not produce a row of its own");
    }

    /** The guard must not swallow ordinary records. */
    @Test
    void deserializeStillReadsAWellFormedRecord() throws Exception {
        RecordingCollector collector = new RecordingCollector();
        schema().deserialize(record(VALUE_ENVELOPE.getBytes(StandardCharsets.UTF_8)), collector);

        Assertions.assertEquals(1, collector.rows.size(), "a valid record must still be read");
        SeaTunnelRow row = collector.rows.get(0);
        Assertions.assertEquals(1L, row.getField(0));
        Assertions.assertEquals("a", row.getField(1));
    }

    /**
     * A zero length value is a decode error rather than a deletion marker, so the guard must not
     * pass it over. Asserted against the contract rather than against a specific exception type,
     * which comes from the Kafka converter.
     */
    @Test
    void deserializeDoesNotTreatAnEmptyValueAsATombstone() {
        RecordingCollector collector = new RecordingCollector();

        Assertions.assertThrows(
                Exception.class,
                () -> schema().deserialize(record(new byte[0]), collector),
                "a zero length value must stay a decode error, not be read as a tombstone");
        Assertions.assertTrue(
                collector.rows.isEmpty(), "a rejected record must not have produced a row");
    }

    private static CompatibleKafkaConnectDeserializationSchema schema() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("kafka_connect_json", rowType);
        // The same flags KafkaSourceConfig passes, both of which default to true.
        return new CompatibleKafkaConnectDeserializationSchema(
                catalogTable, true, true, false, false);
    }

    private static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        return new ConsumerRecord<>(
                "topic",
                0,
                42L,
                1234L,
                TimestampType.CREATE_TIME,
                0,
                0,
                KEY_ENVELOPE.getBytes(StandardCharsets.UTF_8),
                value,
                new RecordHeaders(),
                Optional.empty());
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public void collect(SchemaChangeEvent event) {}

        @Override
        public void markSchemaChangeBeforeCheckpoint() {}

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
