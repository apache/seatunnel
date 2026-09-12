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

package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class FlussSourceSplitEnumeratorTest {

    private static final TablePath TABLE_PATH = TablePath.of("fluss_db", "fluss_tbl");
    private static final long TABLE_ID = 1L;
    private static final int BUCKET_ID = 0;

    @Test
    void runtimeFailoverReassignsReturnedSplitWithLiveOffset() throws Exception {
        FlussSourceSplit restored =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 500L, Long.MAX_VALUE);
        FlussSourceState restoreState =
                new FlussSourceState(new HashSet<>(Collections.singletonList(restored)));
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(null, context, restoreState, true);

        enumerator.run();
        context.clearAssignments();

        // Reader 0 fails after making progress; its split is returned carrying the live position.
        FlussSourceSplit returned =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 800L, Long.MAX_VALUE);
        enumerator.addSplitsBack(Collections.singletonList(returned), 0);

        List<FlussSourceSplit> reassigned = context.getAllAssignedSplits();
        Assertions.assertEquals(1, reassigned.size(), "Returned split must be reassigned once");
        Assertions.assertEquals(
                800L,
                reassigned.get(0).getStartOffset(),
                "Runtime failover must reassign the returned split at its live offset");
    }

    @Test
    void batchRunSignalsNoMoreSplitsOncePerReader() throws Exception {
        FlussSourceSplit split = new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 0L, 100L);
        FlussSourceState restoreState =
                new FlussSourceState(new HashSet<>(Collections.singletonList(split)));
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(null, context, restoreState, false); // batch

        enumerator.run();
        enumerator.run(); // idempotent: must not signal twice

        Assertions.assertEquals(
                Collections.singletonList(0),
                context.getNoMoreSplitsSignals(),
                "Batch reader must be signalled no-more-splits exactly once");
    }

    @Test
    void streamingRunDoesNotSignalNoMoreSplits() throws Exception {
        FlussSourceSplit split =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 0L, Long.MAX_VALUE);
        FlussSourceState restoreState =
                new FlussSourceState(new HashSet<>(Collections.singletonList(split)));
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(null, context, restoreState, true); // streaming

        enumerator.run();

        Assertions.assertTrue(
                context.getNoMoreSplitsSignals().isEmpty(),
                "Streaming reader must never be signalled no-more-splits");
    }

    @Test
    void lateRegisterReaderTriggersAssignment() throws Exception {
        FlussSourceSplit split =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 0L, Long.MAX_VALUE);
        FlussSourceState restoreState =
                new FlussSourceState(new HashSet<>(Collections.singletonList(split)));
        // No readers registered yet.
        TestingEnumeratorContext context = new TestingEnumeratorContext(1, new HashSet<>());
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(null, context, restoreState, true);

        enumerator.run();
        Assertions.assertTrue(
                context.getAllAssignedSplits().isEmpty(),
                "Nothing can be assigned before any reader registers");

        context.registeredReaders().add(0);
        enumerator.registerReader(0);

        Assertions.assertEquals(
                1,
                context.getAllAssignedSplits().size(),
                "Registering a reader after run() must assign the pending split");
    }

    @Test
    void snapshotPersistsOnlyUnassignedSplits() throws Exception {
        FlussSourceSplit bucket0 =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, 0, 0L, Long.MAX_VALUE);
        FlussSourceSplit bucket1 =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, 1, 0L, Long.MAX_VALUE);
        FlussSourceState restoreState =
                new FlussSourceState(new HashSet<>(Arrays.asList(bucket0, bucket1)));
        // parallelism 2 with only reader 0 registered: consecutive bucket ids hash to different
        // owners, so exactly one split is assigned and the other stays pending.
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(null, context, restoreState, true);

        enumerator.run();

        List<FlussSourceSplit> assigned = context.getAllAssignedSplits();
        Assertions.assertEquals(
                1,
                assigned.size(),
                "Exactly one of the two buckets should be assigned to reader 0");

        Set<FlussSourceSplit> persisted = enumerator.snapshotState(1L).getPendingSplits();
        Assertions.assertEquals(
                1, persisted.size(), "Snapshot must persist only the unassigned split");
        Assertions.assertFalse(
                persisted.contains(assigned.get(0)),
                "The reader-owned (assigned) split must be absent from the snapshot");
    }

    @Test
    void restoredEnumeratorNeitherRediscoversNorRedispatches() {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(
                        null, context, new FlussSourceState(new HashSet<>()), true);

        Assertions.assertDoesNotThrow(enumerator::run, "Restored enumerator must skip discovery");

        Assertions.assertTrue(
                context.getAllAssignedSplits().isEmpty(),
                "Restored enumerator with an empty backlog must dispatch nothing to readers");
        Assertions.assertEquals(
                0,
                enumerator.snapshotState(1L).getPendingSplits().size(),
                "Nothing to persist when the restored backlog is empty");
    }

    @Test
    void handleSplitRequestThrows() {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(null, context, null, true);

        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> enumerator.handleSplitRequest(0));
    }

    @Test
    void addSplitsBackIgnoresNullAndEmpty() {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, new HashSet<>(Collections.singletonList(0)));
        FlussSourceSplitEnumerator enumerator =
                new FlussSourceSplitEnumerator(
                        null, context, new FlussSourceState(new HashSet<>()), true);

        enumerator.addSplitsBack(null, 0);
        enumerator.addSplitsBack(Collections.emptyList(), 0);

        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        Assertions.assertTrue(context.getAllAssignedSplits().isEmpty());
    }

    private static final class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<FlussSourceSplit> {
        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<FlussSourceSplit>> assignedSplitsByReader = new HashMap<>();
        private final List<Integer> noMoreSplitsSignals = new ArrayList<>();
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};
        private final EventListener eventListener =
                new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        // no-op
                    }
                };

        private TestingEnumeratorContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = registeredReaders;
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<FlussSourceSplit> splits) {
            assignedSplitsByReader
                    .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                    .addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsSignals.add(subtask);
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

        private List<FlussSourceSplit> getAllAssignedSplits() {
            return assignedSplitsByReader.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }

        private List<Integer> getNoMoreSplitsSignals() {
            return noMoreSplitsSignals;
        }

        private void clearAssignments() {
            assignedSplitsByReader.clear();
        }
    }
}
