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

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsStartMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class AzureEventHubsSourceSplitEnumeratorTest {

    @Test
    void freshDiscoveryResolvesConcretePositionsAndClosesClient() {
        FakeConsumer consumer = new FakeConsumer();
        consumer.partitionIds.addAll(Arrays.asList("1", "0"));
        consumer.initialPositions.put("0", 10L);
        consumer.initialPositions.put("1", 20L);
        TestingContext context = new TestingContext(1, Collections.singleton(0));
        AzureEventHubsSourceSplitEnumerator enumerator =
                new AzureEventHubsSourceSplitEnumerator(
                        config(AzureEventHubsStartMode.LATEST), context, ignored -> consumer, null);

        enumerator.run();

        List<AzureEventHubsSourceSplit> splits = context.allAssignedSplits();
        Assertions.assertEquals(Arrays.asList("0", "1"), partitionIds(splits));
        Assertions.assertEquals(10L, splits.get(0).getNextSequenceNumber());
        Assertions.assertEquals(20L, splits.get(1).getNextSequenceNumber());
        Assertions.assertEquals(
                Arrays.asList(AzureEventHubsStartMode.LATEST, AzureEventHubsStartMode.LATEST),
                consumer.requestedStartModes);
        Assertions.assertTrue(consumer.closed);
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void restoreUsesPendingStateWithoutRediscovery() {
        int[] creations = new int[1];
        AzureEventHubsSourceSplit restored = new AzureEventHubsSourceSplit("events", "0", 500L);
        AzureEventHubsSourceState state =
                new AzureEventHubsSourceState(new HashSet<>(Collections.singletonList(restored)));
        TestingContext context = new TestingContext(1, Collections.singleton(0));
        AzureEventHubsSourceSplitEnumerator enumerator =
                new AzureEventHubsSourceSplitEnumerator(
                        config(AzureEventHubsStartMode.EARLIEST),
                        context,
                        ignored -> {
                            creations[0]++;
                            return new FakeConsumer();
                        },
                        state);

        enumerator.run();

        Assertions.assertEquals(0, creations[0]);
        Assertions.assertEquals(500L, context.allAssignedSplits().get(0).getNextSequenceNumber());
    }

    @Test
    void returnedSplitIsReassignedAtLiveCheckpointPosition() {
        AzureEventHubsSourceSplit initial = new AzureEventHubsSourceSplit("events", "0", 10L);
        TestingContext context = new TestingContext(1, Collections.singleton(0));
        AzureEventHubsSourceSplitEnumerator enumerator =
                new AzureEventHubsSourceSplitEnumerator(
                        config(AzureEventHubsStartMode.EARLIEST),
                        context,
                        ignored -> new FakeConsumer(),
                        new AzureEventHubsSourceState(
                                new HashSet<>(Collections.singletonList(initial))));
        enumerator.run();
        context.assignments.clear();

        enumerator.addSplitsBack(
                Collections.singletonList(new AzureEventHubsSourceSplit("events", "0", 42L)), 0);

        Assertions.assertEquals(42L, context.allAssignedSplits().get(0).getNextSequenceNumber());
    }

    @Test
    void unregisteredOwnersRemainInEnumeratorCheckpoint() {
        AzureEventHubsSourceSplit partition0 = new AzureEventHubsSourceSplit("events", "0", 10L);
        AzureEventHubsSourceSplit partition1 = new AzureEventHubsSourceSplit("events", "1", 20L);
        Set<AzureEventHubsSourceSplit> pending =
                new HashSet<>(Arrays.asList(partition0, partition1));
        int owner0 = AzureEventHubsSourceSplitEnumerator.splitOwner(partition0, 2);
        int owner1 = AzureEventHubsSourceSplitEnumerator.splitOwner(partition1, 2);
        Assertions.assertNotEquals(
                owner0, owner1, "Test requires partitions with different owners");
        TestingContext context = new TestingContext(2, Collections.singleton(owner0));
        AzureEventHubsSourceSplitEnumerator enumerator =
                new AzureEventHubsSourceSplitEnumerator(
                        config(AzureEventHubsStartMode.EARLIEST),
                        context,
                        ignored -> new FakeConsumer(),
                        new AzureEventHubsSourceState(pending));

        enumerator.run();

        Assertions.assertEquals(1, context.allAssignedSplits().size());
        Assertions.assertEquals(1, enumerator.snapshotState(1L).getPendingSplits().size());
    }

    @Test
    void discoveryRejectsEventHubWithoutPartitions() {
        FakeConsumer consumer = new FakeConsumer();
        TestingContext context = new TestingContext(1, Collections.singleton(0));
        AzureEventHubsSourceSplitEnumerator enumerator =
                new AzureEventHubsSourceSplitEnumerator(
                        config(AzureEventHubsStartMode.EARLIEST),
                        context,
                        ignored -> consumer,
                        null);

        IllegalStateException exception =
                Assertions.assertThrows(IllegalStateException.class, enumerator::run);

        Assertions.assertTrue(exception.getMessage().contains("has no partitions"));
        Assertions.assertTrue(consumer.closed);
    }

    private AzureEventHubsSourceConfig config(AzureEventHubsStartMode startMode) {
        return AzureEventHubsSourceConfig.builder()
                .connectionString("secret")
                .eventHubName("events")
                .consumerGroup("$Default")
                .startMode(startMode)
                .format(AzureEventHubsMessageFormat.JSON)
                .fieldDelimiter(",")
                .maxBatchSize(100)
                .pollTimeoutMs(1_000L)
                .prefetchCount(300)
                .build();
    }

    private List<String> partitionIds(List<AzureEventHubsSourceSplit> splits) {
        return splits.stream()
                .map(AzureEventHubsSourceSplit::getPartitionId)
                .collect(Collectors.toList());
    }

    private static class FakeConsumer implements EventHubsConsumer {
        private final List<String> partitionIds = new ArrayList<>();
        private final Map<String, Long> initialPositions = new HashMap<>();
        private final List<AzureEventHubsStartMode> requestedStartModes = new ArrayList<>();
        private boolean closed;

        @Override
        public List<String> partitionIds() {
            return new ArrayList<>(partitionIds);
        }

        @Override
        public long initialSequenceNumber(String partitionId, AzureEventHubsStartMode startMode) {
            requestedStartModes.add(startMode);
            return initialPositions.get(partitionId);
        }

        @Override
        public List<EventHubsRecord> receive(
                String partitionId,
                long nextSequenceNumber,
                int maxEvents,
                Duration maximumWaitTime) {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<AzureEventHubsSourceSplit> {
        private final int parallelism;
        private final Set<Integer> readers;
        private final Map<Integer, List<AzureEventHubsSourceSplit>> assignments = new HashMap<>();
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};
        private final EventListener eventListener =
                new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        // no-op
                    }
                };

        private TestingContext(int parallelism, Set<Integer> readers) {
            this.parallelism = parallelism;
            this.readers = new HashSet<>(readers);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplit(int subtaskId, List<AzureEventHubsSourceSplit> splits) {
            assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            throw new AssertionError("Streaming source must not signal no more splits");
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            // no-op
        }

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return eventListener;
        }

        private List<AzureEventHubsSourceSplit> allAssignedSplits() {
            return assignments.values().stream().flatMap(List::stream).collect(Collectors.toList());
        }
    }
}
