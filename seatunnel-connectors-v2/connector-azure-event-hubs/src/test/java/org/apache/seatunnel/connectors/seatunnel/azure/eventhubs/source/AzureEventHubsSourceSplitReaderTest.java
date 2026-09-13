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

import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsStartMode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class AzureEventHubsSourceSplitReaderTest {

    @Test
    void emptyReaderDoesNotCreateClient() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        int[] creations = new int[1];
        AzureEventHubsSourceSplitReader reader =
                new AzureEventHubsSourceSplitReader(
                        config(),
                        ignored -> {
                            creations[0]++;
                            return consumer;
                        });

        RecordsWithSplitIds<EventHubsRecord> records = reader.fetch();
        reader.close();

        Assertions.assertNull(records.nextSplit());
        Assertions.assertEquals(0, creations[0]);
        Assertions.assertFalse(consumer.closed);
    }

    @Test
    void pollsAssignedPartitionsFairlyAndAdvancesFetchCursor() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        AzureEventHubsSourceSplitReader reader = reader(consumer);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Arrays.asList(
                                new AzureEventHubsSourceSplit("events", "0", 10L),
                                new AzureEventHubsSourceSplit("events", "1", 20L))));

        assertSingleRecord(reader.fetch(), "events-0", 10L);
        assertSingleRecord(reader.fetch(), "events-1", 20L);
        assertSingleRecord(reader.fetch(), "events-0", 11L);

        Assertions.assertEquals(Arrays.asList("0:10", "1:20", "0:11"), consumer.requests);
        Assertions.assertEquals(100, consumer.lastMaxEvents);
        Assertions.assertEquals(Duration.ofSeconds(1), consumer.lastMaximumWaitTime);
    }

    @Test
    void wakeupReturnsWithoutPollingAndNextFetchResumes() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        AzureEventHubsSourceSplitReader reader = reader(consumer);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new AzureEventHubsSourceSplit("events", "0", 10L))));

        reader.wakeUp();
        Assertions.assertNull(reader.fetch().nextSplit());
        Assertions.assertTrue(consumer.requests.isEmpty());

        assertSingleRecord(reader.fetch(), "events-0", 10L);
    }

    @Test
    void duplicateAssignmentFailsWithoutReplacingReadPosition() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        AzureEventHubsSourceSplitReader reader = reader(consumer);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new AzureEventHubsSourceSplit("events", "0", 10L))));

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                reader.handleSplitsChanges(
                                        new SplitsAddition<>(
                                                Collections.singletonList(
                                                        new AzureEventHubsSourceSplit(
                                                                "events", "0", 99L)))));
        Assertions.assertTrue(exception.getMessage().contains("events-0"));

        assertSingleRecord(reader.fetch(), "events-0", 10L);
    }

    @Test
    void restoredCheckpointPositionIsUsedDirectly() throws Exception {
        AzureEventHubsSourceSplitState checkpointState =
                new AzureEventHubsSourceSplitState(
                        new AzureEventHubsSourceSplit("events", "0", 10L));
        checkpointState.setCurrentSequenceNumber(42L);
        FakeConsumer consumer = new FakeConsumer();
        AzureEventHubsSourceSplitReader reader = reader(consumer);

        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(checkpointState.toSourceSplit())));

        assertSingleRecord(reader.fetch(), "events-0", 42L);
        Assertions.assertEquals(Collections.singletonList("0:42"), consumer.requests);
    }

    @Test
    void sequenceGapFailsWithoutSilentlySkippingCheckpointPosition() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        consumer.sequenceNumberDelta = 5L;
        AzureEventHubsSourceSplitReader reader = reader(consumer);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new AzureEventHubsSourceSplit("events", "0", 10L))));

        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(AzureEventHubsConnectorException.class, reader::fetch);

        Assertions.assertTrue(
                exception.getMessage().contains("sequence number 10 but received 15"));
        Assertions.assertTrue(exception.getMessage().contains("may no longer be retained"));
    }

    @Test
    void closesLazilyCreatedClientExactlyOnce() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        AzureEventHubsSourceSplitReader reader = reader(consumer);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new AzureEventHubsSourceSplit("events", "0", 10L))));
        reader.fetch();

        reader.close();
        reader.close();

        Assertions.assertEquals(1, consumer.closeCount);
    }

    private AzureEventHubsSourceSplitReader reader(FakeConsumer consumer) {
        return new AzureEventHubsSourceSplitReader(config(), ignored -> consumer);
    }

    private AzureEventHubsSourceConfig config() {
        return AzureEventHubsSourceConfig.builder()
                .connectionString("secret")
                .eventHubName("events")
                .consumerGroup("$Default")
                .startMode(AzureEventHubsStartMode.EARLIEST)
                .format(AzureEventHubsMessageFormat.JSON)
                .fieldDelimiter(",")
                .maxBatchSize(100)
                .pollTimeoutMs(1_000L)
                .prefetchCount(300)
                .build();
    }

    private void assertSingleRecord(
            RecordsWithSplitIds<EventHubsRecord> records,
            String expectedSplit,
            long expectedSequence) {
        Assertions.assertEquals(expectedSplit, records.nextSplit());
        Assertions.assertEquals(
                expectedSequence, records.nextRecordFromSplit().getSequenceNumber());
        Assertions.assertNull(records.nextRecordFromSplit());
        Assertions.assertNull(records.nextSplit());
    }

    private static class FakeConsumer implements EventHubsConsumer {
        private final List<String> requests = new ArrayList<>();
        private boolean closed;
        private int closeCount;
        private int lastMaxEvents;
        private Duration lastMaximumWaitTime;
        private long sequenceNumberDelta;

        @Override
        public List<String> partitionIds() {
            return Collections.emptyList();
        }

        @Override
        public long initialSequenceNumber(String partitionId, AzureEventHubsStartMode startMode) {
            return 0;
        }

        @Override
        public List<EventHubsRecord> receive(
                String partitionId,
                long nextSequenceNumber,
                int maxEvents,
                Duration maximumWaitTime) {
            requests.add(partitionId + ":" + nextSequenceNumber);
            lastMaxEvents = maxEvents;
            lastMaximumWaitTime = maximumWaitTime;
            return Collections.singletonList(
                    new EventHubsRecord(new byte[] {1}, nextSequenceNumber + sequenceNumberDelta));
        }

        @Override
        public void close() {
            closed = true;
            closeCount++;
        }
    }
}
