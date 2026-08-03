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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BigtableSourceSplitEnumeratorTest {

    @Test
    void testSingleSplitFullRange() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");
        assertEquals("bigtable_source_split_0", split.splitId());
        assertEquals("", split.getStartRowKey());
        assertEquals("", split.getEndRowKey());
    }

    @Test
    void testSplitWithRowKeyRange() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "aaa", "zzz");
        assertEquals("aaa", split.getStartRowKey());
        assertEquals("zzz", split.getEndRowKey());
    }

    /**
     * Regression for the returned-split recovery window: a split requeued via {@link
     * BigtableSourceSplitEnumerator#addSplitsBack} but not yet reassigned must survive enumerator
     * checkpoint / restore and be reassigned to the reader.
     */
    @Test
    void testReturnedSplitSurvivesCheckpointRestoreAndReassign() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableParameters parameters =
                BigtableParameters.builder()
                        .projectId("test-project")
                        .instanceId("test-instance")
                        .table("test-table")
                        .build();

        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, parameters);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit split = context.getLastAssignedSplit(0);
        assertEquals("bigtable_source_split_0", split.splitId());

        // Simulate failover: reader returns its split before the enumerator can reassign it.
        context.unregisterReaderForTest(0);
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        BigtableSourceState checkpoint = enumerator.snapshotState(1L);
        assertTrue(checkpoint.getPendingSplits().contains(split));
        assertTrue(checkpoint.getAssignedSplits().contains(split));

        BigtableSourceSplitEnumerator restored =
                new BigtableSourceSplitEnumerator(context, parameters, checkpoint);
        restored.open();

        context.clearAssignments();
        context.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(1, context.getAssignedSplitCount(0));
        assertEquals(split.splitId(), context.getLastAssignedSplit(0).splitId());
        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    @Test
    void testSnapshotStateDefensivelyCopiesPendingSplits() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableParameters parameters =
                BigtableParameters.builder()
                        .projectId("test-project")
                        .instanceId("test-instance")
                        .table("test-table")
                        .build();

        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, parameters);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit split = context.getLastAssignedSplit(0);

        context.unregisterReaderForTest(0);
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        BigtableSourceState checkpoint = enumerator.snapshotState(1L);
        assertEquals(1, checkpoint.getPendingSplits().size());
        checkpoint.getPendingSplits().clear();

        assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<BigtableSourceSplit> {

        private final int parallelism;
        private final Set<Integer> registeredReaders = new HashSet<>();
        private final Map<Integer, List<BigtableSourceSplit>> assignments = new HashMap<>();

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
        }

        void registerReaderForTest(int subtaskId) {
            registeredReaders.add(subtaskId);
        }

        void unregisterReaderForTest(int subtaskId) {
            registeredReaders.remove(subtaskId);
        }

        void clearAssignments() {
            assignments.clear();
        }

        int getAssignedSplitCount(int subtaskId) {
            return assignments.getOrDefault(subtaskId, Collections.emptyList()).size();
        }

        BigtableSourceSplit getLastAssignedSplit(int subtaskId) {
            List<BigtableSourceSplit> splits = assignments.get(subtaskId);
            return splits.get(splits.size() - 1);
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
        public void assignSplit(int subtaskId, List<BigtableSourceSplit> splits) {
            assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
