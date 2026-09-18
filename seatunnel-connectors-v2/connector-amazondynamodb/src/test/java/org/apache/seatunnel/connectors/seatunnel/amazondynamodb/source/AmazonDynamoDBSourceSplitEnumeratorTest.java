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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AmazonDynamoDBSourceSplitEnumeratorTest {

    @Test
    void addPendingSplitShouldAssignSegmentsRoundRobin() throws Exception {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        AmazonDynamoDBSourceSplitEnumerator enumerator =
                new AmazonDynamoDBSourceSplitEnumerator(context, null);

        invokeAddPendingSplit(
                enumerator, Arrays.asList(newSplit(0), newSplit(1), newSplit(2), newSplit(3)));
        enumerator.registerReader(0);

        Assertions.assertEquals(2, context.getAssignedSplits(0).size());
        Assertions.assertEquals(2, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void addSplitsBackShouldReturnSplitsToOriginalReader() {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        AmazonDynamoDBSourceSplitEnumerator enumerator =
                new AmazonDynamoDBSourceSplitEnumerator(context, null);

        enumerator.addSplitsBack(
                Arrays.asList(newSplit(0), newSplit(1), newSplit(2), newSplit(3)), 0);

        Assertions.assertEquals(4, context.getAssignedSplits(0).size());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void snapshotStateShouldKeepAssignCountAcrossRestore() throws Exception {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        AmazonDynamoDBSourceSplitEnumerator enumerator =
                new AmazonDynamoDBSourceSplitEnumerator(context, null);

        invokeAddPendingSplit(enumerator, Arrays.asList(newSplit(0), newSplit(1), newSplit(2)));
        AmazonDynamoDBSourceState snapshot = enumerator.snapshotState(1L);

        Assertions.assertEquals(3, snapshot.getAssignCount());

        AmazonDynamoDBSourceSplitEnumerator restored =
                new AmazonDynamoDBSourceSplitEnumerator(context, null, snapshot);
        invokeAddPendingSplit(restored, Collections.singletonList(newSplit(3)));
        AmazonDynamoDBSourceState restoredSnapshot = restored.snapshotState(2L);

        Assertions.assertEquals(4, restoredSnapshot.getAssignCount());
    }

    private static AmazonDynamoDBSourceSplit newSplit(int splitId) {
        return new AmazonDynamoDBSourceSplit(splitId, 4, 100);
    }

    private static void invokeAddPendingSplit(
            AmazonDynamoDBSourceSplitEnumerator enumerator,
            Collection<AmazonDynamoDBSourceSplit> splits)
            throws Exception {
        Method addPendingSplit =
                AmazonDynamoDBSourceSplitEnumerator.class.getDeclaredMethod(
                        "addPendingSplit", Collection.class);
        addPendingSplit.setAccessible(true);
        addPendingSplit.invoke(enumerator, splits);
    }

    private static class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<AmazonDynamoDBSourceSplit> {
        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<AmazonDynamoDBSourceSplit>> assignedSplitsByReader =
                new HashMap<>();
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
        public void assignSplit(int subtaskId, List<AmazonDynamoDBSourceSplit> splits) {
            assignedSplitsByReader
                    .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                    .addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return eventListener;
        }

        private List<AmazonDynamoDBSourceSplit> getAssignedSplits(int subtaskId) {
            return assignedSplitsByReader.getOrDefault(subtaskId, Collections.emptyList());
        }
    }
}
