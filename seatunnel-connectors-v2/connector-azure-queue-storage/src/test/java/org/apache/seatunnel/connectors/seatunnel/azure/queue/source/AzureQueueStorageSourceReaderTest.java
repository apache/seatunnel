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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageEncoding;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class AzureQueueStorageSourceReaderTest {

    @Test
    void shouldWaitForSplitBeforePolling() throws Exception {
        TestReceiver receiver = new TestReceiver();
        receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        reader.pollNext(new TestCollector());
        Assertions.assertEquals(0, receiver.receiveCount.get());

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        Assertions.assertEquals(1, receiver.receiveCount.get());
        reader.close();
    }

    @Test
    void shouldDeleteOnlyAfterCheckpointCompletes() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage message = receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);

        Assertions.assertTrue(receiver.deleted.isEmpty());
        reader.notifyCheckpointComplete(1L);
        Assertions.assertEquals(Collections.singletonList(message), receiver.deleted);
        reader.close();
    }

    @Test
    void shouldRetainMessageAfterCheckpointIsAborted() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage message = receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);
        reader.notifyCheckpointAborted(1L);
        reader.snapshotState(2L);
        reader.notifyCheckpointComplete(2L);

        Assertions.assertEquals(Collections.singletonList(message), receiver.deleted);
        reader.close();
    }

    @Test
    void shouldDeleteMessagesFromOverlappingCheckpointsOnce() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage first = receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);

        AzureQueueMessage second = receiver.enqueue("second");
        reader.pollNext(new TestCollector());
        reader.snapshotState(2L);
        reader.notifyCheckpointComplete(2L);
        reader.notifyCheckpointComplete(1L);

        Assertions.assertEquals(Arrays.asList(first, second), receiver.deleted);
        reader.close();
    }

    @Test
    void shouldRetryOnlyMessagesNotDeletedByPartialCheckpointFailure() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage first = receiver.enqueue("first");
        AzureQueueMessage second = receiver.enqueue("second");
        receiver.failDeleteFor = second;
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);

        Assertions.assertThrows(
                AzureQueueConnectorException.class, () -> reader.notifyCheckpointComplete(1L));
        Assertions.assertTrue(first.isDeleted());
        Assertions.assertFalse(second.isDeleted());

        receiver.failDeleteFor = null;
        reader.notifyCheckpointComplete(1L);
        Assertions.assertEquals(Arrays.asList(first, second), receiver.deleted);
        reader.close();
    }

    @Test
    void shouldUseRenewedPopReceiptForCheckpointDelete() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage message = receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.renewVisibilityNow();
        reader.snapshotState(1L);
        reader.notifyCheckpointComplete(1L);

        Assertions.assertEquals(1, receiver.renewCount.get());
        Assertions.assertEquals("renewed-receipt", receiver.receiptUsedForDelete);
        Assertions.assertTrue(message.isDeleted());
        reader.close();
    }

    @Test
    void shouldSurfaceBackgroundVisibilityRenewalFailure() throws Exception {
        TestReceiver receiver = new TestReceiver();
        receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        receiver.failRenewal = true;
        reader.renewVisibilitySafely();

        AzureQueueConnectorException exception =
                Assertions.assertThrows(
                        AzureQueueConnectorException.class,
                        () -> reader.pollNext(new TestCollector()));
        Assertions.assertTrue(exception.getMessage().contains("visibility renewal failed"));
        reader.close();
    }

    @Test
    void shouldReleaseMalformedMessageAndRestOfReceivedBatch() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage first = receiver.enqueue("invalid");
        AzureQueueMessage second = receiver.enqueue("not-processed");
        AzureQueueStorageSourceReader reader =
                createReader(receiver, new FailingDeserializationSchema());

        assignSplit(reader);
        Assertions.assertThrows(
                AzureQueueConnectorException.class, () -> reader.pollNext(new TestCollector()));

        Assertions.assertEquals(Arrays.asList(first, second), receiver.released);
        reader.close();
    }

    @Test
    void shouldUseCollectorBasedDeserialization() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage message = receiver.enqueue("first");
        CollectorDeserializationSchema schema = new CollectorDeserializationSchema();
        AzureQueueStorageSourceReader reader = createReader(receiver, schema);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);
        reader.notifyCheckpointComplete(1L);

        Assertions.assertTrue(schema.collectorMethodCalled);
        Assertions.assertEquals(Collections.singletonList(message), receiver.deleted);
        reader.close();
    }

    @Test
    void shouldBoundReceivedMessagesUntilCheckpointCompletes() throws Exception {
        TestReceiver receiver = new TestReceiver();
        receiver.enqueue("first");
        receiver.enqueue("second");
        AzureQueueStorageSourceReader reader = createReader(receiver, schema(), config(1, 1));

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        Assertions.assertEquals(1, receiver.receiveCount.get());

        reader.pollNext(new TestCollector());
        Assertions.assertEquals(1, receiver.receiveCount.get());

        reader.snapshotState(1L);
        reader.notifyCheckpointComplete(1L);
        reader.pollNext(new TestCollector());
        Assertions.assertEquals(2, receiver.receiveCount.get());
        reader.close();
    }

    @Test
    void shouldReleaseOutstandingMessagesWhenReaderCloses() throws Exception {
        TestReceiver receiver = new TestReceiver();
        AzureQueueMessage message = receiver.enqueue("first");
        AzureQueueStorageSourceReader reader = createReader(receiver);

        assignSplit(reader);
        reader.pollNext(new TestCollector());
        reader.close();

        Assertions.assertEquals(Collections.singletonList(message), receiver.released);
    }

    private AzureQueueStorageSourceReader createReader(TestReceiver receiver) {
        return createReader(receiver, schema(), config(32, 1_000));
    }

    private AzureQueueStorageSourceReader createReader(
            TestReceiver receiver, DeserializationSchema<SeaTunnelRow> schema) {
        return createReader(receiver, schema, config(32, 1_000));
    }

    private AzureQueueStorageSourceReader createReader(
            TestReceiver receiver,
            DeserializationSchema<SeaTunnelRow> schema,
            AzureQueueSourceConfig config) {
        return new AzureQueueStorageSourceReader(config, schema, () -> receiver);
    }

    private AzureQueueSourceConfig config(int batchSize, int maxInFlightMessages) {
        return AzureQueueSourceConfig.builder()
                .queueName("events")
                .authenticationType(AuthenticationType.CONNECTION_STRING)
                .connectionString("UseDevelopmentStorage=true")
                .format(MessageFormat.JSON)
                .fieldDelimiter(",")
                .messageEncoding(MessageEncoding.NONE)
                .batchSize(batchSize)
                .visibilityTimeoutSeconds(300)
                .pollIntervalMillis(1)
                .maxInFlightMessages(maxInFlightMessages)
                .operationTimeoutMillis(60_000)
                .build();
    }

    private DeserializationSchema<SeaTunnelRow> schema() {
        return new TestDeserializationSchema();
    }

    private void assignSplit(AzureQueueStorageSourceReader reader) {
        reader.addSplits(Collections.singletonList(new SingleSplit(null)));
    }

    private static class TestReceiver implements AzureQueueReceiver {
        private final Deque<AzureQueueMessage> messages = new ArrayDeque<>();
        private final List<AzureQueueMessage> deleted = new ArrayList<>();
        private final List<AzureQueueMessage> released = new ArrayList<>();
        private final AtomicInteger receiveCount = new AtomicInteger();
        private final AtomicInteger renewCount = new AtomicInteger();
        private AzureQueueMessage failDeleteFor;
        private boolean failRenewal;
        private String receiptUsedForDelete;

        AzureQueueMessage enqueue(String value) {
            byte[] body = value.getBytes(StandardCharsets.UTF_8);
            AzureQueueMessage message =
                    new AzureQueueMessage(
                            "message-" + messages.size(), "initial-receipt", value, body);
            messages.add(message);
            return message;
        }

        @Override
        public List<AzureQueueMessage> receive(int maxMessages) {
            receiveCount.incrementAndGet();
            List<AzureQueueMessage> result = new ArrayList<>();
            while (result.size() < maxMessages && !messages.isEmpty()) {
                result.add(messages.removeFirst());
            }
            return result;
        }

        @Override
        public void renewVisibility(AzureQueueMessage message) {
            if (failRenewal) {
                throw new IllegalStateException("renewal failed");
            }
            renewCount.incrementAndGet();
            message.updatePopReceipt("renewed-receipt");
        }

        @Override
        public void delete(AzureQueueMessage message) {
            if (message == failDeleteFor) {
                throw new IllegalStateException("delete failed");
            }
            receiptUsedForDelete = message.getPopReceipt();
            message.markDeleted();
            deleted.add(message);
        }

        @Override
        public void release(AzureQueueMessage message) {
            released.add(message);
        }
    }

    private static class TestDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
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

    private static final class FailingDeserializationSchema extends TestDeserializationSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            throw new IOException("invalid payload");
        }
    }

    private static final class CollectorDeserializationSchema extends TestDeserializationSchema {
        private boolean collectorMethodCalled;

        @Override
        public SeaTunnelRow deserialize(byte[] message) {
            throw new AssertionError("Single-row deserialization must not be used");
        }

        @Override
        public void deserialize(byte[] message, Collector<SeaTunnelRow> output) {
            collectorMethodCalled = true;
            output.collect(
                    new SeaTunnelRow(new Object[] {new String(message, StandardCharsets.UTF_8)}));
        }
    }

    private static final class TestCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {}

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
