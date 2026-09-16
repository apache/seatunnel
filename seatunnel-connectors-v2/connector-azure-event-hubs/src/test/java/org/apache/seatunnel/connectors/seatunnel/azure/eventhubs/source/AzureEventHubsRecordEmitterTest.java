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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class AzureEventHubsRecordEmitterTest {

    @Test
    void emitsThenAdvancesCheckpointPosition() {
        RecordingCollector collector = new RecordingCollector();
        AzureEventHubsSourceSplitState state = stateAt(10L);
        AzureEventHubsRecordEmitter emitter = new AzureEventHubsRecordEmitter(new StringSchema());

        emitter.emitRecord(
                new EventHubsRecord("value".getBytes(StandardCharsets.UTF_8), 10L),
                collector,
                state);

        Assertions.assertEquals("value", collector.rows.get(0).getField(0));
        Assertions.assertEquals(11L, state.toSourceSplit().getNextSequenceNumber());
    }

    @Test
    void failedDeserializationDoesNotAdvanceOrEmit() {
        RecordingCollector collector = new RecordingCollector();
        AzureEventHubsSourceSplitState state = stateAt(10L);
        AzureEventHubsRecordEmitter emitter = new AzureEventHubsRecordEmitter(new FailingSchema());

        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () ->
                                emitter.emitRecord(
                                        new EventHubsRecord(new byte[] {1}, 10L),
                                        collector,
                                        state));

        Assertions.assertTrue(exception.getMessage().contains("sequence number 10"));
        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
    }

    @Test
    void filteredNullRowStillAdvancesConsumedPosition() {
        RecordingCollector collector = new RecordingCollector();
        AzureEventHubsSourceSplitState state = stateAt(10L);
        AzureEventHubsRecordEmitter emitter = new AzureEventHubsRecordEmitter(new NullSchema());

        emitter.emitRecord(new EventHubsRecord(new byte[] {1}, 10L), collector, state);

        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(11L, state.toSourceSplit().getNextSequenceNumber());
    }

    @Test
    void sequenceOverflowIsRejectedBeforeOutput() {
        RecordingCollector collector = new RecordingCollector();
        AzureEventHubsSourceSplitState state = stateAt(Long.MAX_VALUE);
        AzureEventHubsRecordEmitter emitter = new AzureEventHubsRecordEmitter(new StringSchema());

        Assertions.assertThrows(
                AzureEventHubsConnectorException.class,
                () ->
                        emitter.emitRecord(
                                new EventHubsRecord(new byte[] {1}, Long.MAX_VALUE),
                                collector,
                                state));

        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(Long.MAX_VALUE, state.toSourceSplit().getNextSequenceNumber());
    }

    private AzureEventHubsSourceSplitState stateAt(long sequenceNumber) {
        return new AzureEventHubsSourceSplitState(
                new AzureEventHubsSourceSplit("events", "0", sequenceNumber));
    }

    private static class StringSchema implements DeserializationSchema<SeaTunnelRow> {
        private static final SeaTunnelRowType ROW_TYPE =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            return new SeaTunnelRow(new Object[] {new String(message, StandardCharsets.UTF_8)});
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return ROW_TYPE;
        }
    }

    private static class FailingSchema extends StringSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            throw new IOException("invalid body");
        }
    }

    private static class NullSchema extends StringSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) {
            return null;
        }
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object checkpointLock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
