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
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class AzureEventHubsRecordEmitterTest {

    private static final String PRIVATE_PAYLOAD = "SYNTHETIC_PRIVATE_EVENT_123";
    private static final String PRIVATE_CONNECTION_STRING =
            "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=listen;"
                    + "SharedAccessKey=c3ludGhldGljLXNlY3JldA==";

    @Test
    void emitsThenAdvancesCheckpointPosition() {
        AzureEventHubsSourceSplitState state = stateAt(10L);
        RecordingCollector collector =
                new RecordingCollector() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
                        super.collect(record);
                    }
                };
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

        assertPayloadIsNotExposed(exception, "I/O failure");
        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
    }

    @Test
    void malformedJsonDoesNotExposePayloadOrAdvancePosition() {
        assertJsonFailureIsSafe("{\"count\":1,\"private\":\"" + PRIVATE_PAYLOAD + "\",broken}");
    }

    @Test
    void jsonConversionFailureDoesNotExposePayloadOrAdvancePosition() {
        assertJsonFailureIsSafe("{\"count\":\"" + PRIVATE_PAYLOAD + "\"}");
    }

    @Test
    void collectorFailureDoesNotExposePayloadOrAdvancePosition() {
        RecordingCollector collector =
                new RecordingCollector() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        IllegalStateException failure =
                                new IllegalStateException(
                                        PRIVATE_PAYLOAD,
                                        new InterruptedException(PRIVATE_CONNECTION_STRING));
                        failure.addSuppressed(new IllegalArgumentException(PRIVATE_PAYLOAD));
                        throw failure;
                    }
                };
        AzureEventHubsSourceSplitState state = stateAt(10L);
        AzureEventHubsRecordEmitter emitter = new AzureEventHubsRecordEmitter(new StringSchema());

        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () ->
                                emitter.emitRecord(
                                        new EventHubsRecord(
                                                PRIVATE_PAYLOAD.getBytes(StandardCharsets.UTF_8),
                                                10L),
                                        collector,
                                        state));

        assertPayloadIsNotExposed(exception, "runtime failure");
        Assertions.assertTrue(
                exception.getMessage().contains("IllegalStateException <- InterruptedException"));
        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
    }

    @Test
    void cyclicCollectorCauseChainIsReportedOnceWithoutPayload() {
        IllegalStateException first = new IllegalStateException(PRIVATE_PAYLOAD);
        IllegalArgumentException second = new IllegalArgumentException(PRIVATE_CONNECTION_STRING);
        first.initCause(second);
        second.initCause(first);

        AzureEventHubsConnectorException exception = collectorFailure(first);

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .endsWith(
                                "(runtime failure: IllegalStateException <- IllegalArgumentException <- ...)"));
    }

    @Test
    void deepCollectorCauseChainIsBoundedWithoutPayload() {
        RuntimeException failure = new RuntimeException(PRIVATE_PAYLOAD);
        for (int i = 0; i < 20; i++) {
            failure = new RuntimeException(PRIVATE_CONNECTION_STRING, failure);
        }

        AzureEventHubsConnectorException exception = collectorFailure(failure);

        Assertions.assertEquals(8, exception.getMessage().split("RuntimeException", -1).length - 1);
        Assertions.assertTrue(exception.getMessage().endsWith(" <- ...)"));
    }

    private AzureEventHubsConnectorException collectorFailure(RuntimeException failure) {
        AzureEventHubsSourceSplitState state = stateAt(10L);
        RecordingCollector collector =
                new RecordingCollector() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        throw failure;
                    }
                };
        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () ->
                                new AzureEventHubsRecordEmitter(new StringSchema())
                                        .emitRecord(
                                                new EventHubsRecord(
                                                        PRIVATE_PAYLOAD.getBytes(
                                                                StandardCharsets.UTF_8),
                                                        10L),
                                                collector,
                                                state));
        assertPayloadIsNotExposed(exception, "runtime failure");
        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
        return exception;
    }

    private void assertJsonFailureIsSafe(String payload) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"count"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        AzureEventHubsRecordEmitter emitter =
                new AzureEventHubsRecordEmitter(
                        new JsonDeserializationSchema(false, false, rowType));
        RecordingCollector collector = new RecordingCollector();
        AzureEventHubsSourceSplitState state = stateAt(10L);

        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () ->
                                emitter.emitRecord(
                                        new EventHubsRecord(
                                                payload.getBytes(StandardCharsets.UTF_8), 10L),
                                        collector,
                                        state));

        assertPayloadIsNotExposed(exception, "runtime failure");
        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
    }

    private void assertPayloadIsNotExposed(
            AzureEventHubsConnectorException exception, String category) {
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "Could not deserialize or emit Event Hubs event in partition '3'"
                                        + " at sequence number 10 ("
                                        + category));
        String trace = ExceptionUtils.getMessage(exception);
        Assertions.assertFalse(trace.contains(PRIVATE_PAYLOAD));
        Assertions.assertFalse(trace.contains(PRIVATE_CONNECTION_STRING));
        Assertions.assertFalse(trace.contains("c3ludGhldGljLXNlY3JldA=="));
        Assertions.assertNull(exception.getCause());
        Assertions.assertEquals(0, exception.getSuppressed().length);
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
        AzureEventHubsRecordEmitter emitter =
                new AzureEventHubsRecordEmitter(
                        new StringSchema() {
                            @Override
                            public SeaTunnelRow deserialize(byte[] message) {
                                throw new AssertionError("Overflow must be checked before parsing");
                            }
                        });

        Assertions.assertThrows(
                ArithmeticException.class,
                () ->
                        emitter.emitRecord(
                                new EventHubsRecord(new byte[] {1}, Long.MAX_VALUE),
                                collector,
                                state));

        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(Long.MAX_VALUE, state.toSourceSplit().getNextSequenceNumber());
    }

    @Test
    void stateUpdateFailureIsNotRelabeledAsDeserializationFailure() {
        IllegalStateException failure = new IllegalStateException("state update failed");
        AzureEventHubsSourceSplitState state =
                new AzureEventHubsSourceSplitState(
                        new AzureEventHubsSourceSplit("events", "3", 10L)) {
                    @Override
                    public void setCurrentSequenceNumber(long sequenceNumber) {
                        throw failure;
                    }
                };
        RecordingCollector collector = new RecordingCollector();
        AzureEventHubsRecordEmitter emitter = new AzureEventHubsRecordEmitter(new StringSchema());

        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                emitter.emitRecord(
                                        new EventHubsRecord(
                                                "value".getBytes(StandardCharsets.UTF_8), 10L),
                                        collector,
                                        state)));

        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(10L, state.toSourceSplit().getNextSequenceNumber());
    }

    private AzureEventHubsSourceSplitState stateAt(long sequenceNumber) {
        return new AzureEventHubsSourceSplitState(
                new AzureEventHubsSourceSplit("events", "3", sequenceNumber));
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
            IOException failure =
                    new IOException(
                            "invalid body: " + PRIVATE_PAYLOAD,
                            new IllegalArgumentException(PRIVATE_CONNECTION_STRING));
            failure.addSuppressed(new IllegalStateException(PRIVATE_CONNECTION_STRING));
            throw failure;
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
