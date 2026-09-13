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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Verifies that restored CDC splits are reassigned correctly when the enumerator receives
 * add-splits-back callbacks before or after the engine enters the running state.
 */
class IncrementalSourceEnumeratorTest {

    /**
     * Covers the failover path where the reader is already waiting and restored splits arrive after
     * the enumerator has started scheduling work.
     */
    @Test
    void shouldAssignRestoredSplitsToWaitingReaderWhenEnumeratorIsRunning() throws Exception {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, Collections.singleton(0));
        SplitAssigner splitAssigner = Mockito.mock(SplitAssigner.class);
        SourceSplitBase restoredSplit = newIncrementalSplit("restored-running");
        Mockito.when(splitAssigner.getNext())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(restoredSplit));
        Mockito.when(splitAssigner.waitingForCompletedSplits()).thenReturn(true);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);
        enumerator.handleSplitRequest(0);
        enumerator.run();

        Assertions.assertTrue(context.getAssignedSplits(0).isEmpty());

        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);

        Mockito.verify(splitAssigner).addSplits(Collections.singletonList(restoredSplit));
        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), context.getAssignedSplits(0));
    }

    /**
     * Verifies that restored splits are kept pending when they arrive before {@code run()}, and are
     * assigned immediately once the enumerator starts.
     */
    @Test
    void shouldQueueRestoredSplitsUntilEnumeratorStarts() throws Exception {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, Collections.singleton(0));
        SplitAssigner splitAssigner = Mockito.mock(SplitAssigner.class);
        SourceSplitBase restoredSplit = newIncrementalSplit("restored-before-run");
        Mockito.when(splitAssigner.getNext()).thenReturn(Optional.of(restoredSplit));

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);
        enumerator.handleSplitRequest(0);
        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);

        Assertions.assertTrue(context.getAssignedSplits(0).isEmpty());

        enumerator.run();

        Mockito.verify(splitAssigner).addSplits(Collections.singletonList(restoredSplit));
        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), context.getAssignedSplits(0));
    }

    /** Creates a minimal incremental split that exercises the enumerator reassignment path. */
    private static SourceSplitBase newIncrementalSplit(String splitId) {
        return new IncrementalSplit(
                splitId,
                Collections.singletonList(TableId.parse("inventory.products")),
                Mockito.mock(Offset.class),
                Mockito.mock(Offset.class),
                Collections.emptyList());
    }

    /** Minimal enumerator context used to assert split assignment behavior deterministically. */
    private static final class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {
        /** Fixed parallelism exposed to the enumerator under test. */
        private final int parallelism;
        /** Registered readers that are allowed to request or receive splits. */
        private final Set<Integer> registeredReaders;
        /** Split assignment history keyed by reader id. */
        private final Map<Integer, List<SourceSplitBase>> assignedSplitsByReader = new HashMap<>();
        /** Lightweight metrics context required by the enumerator contract. */
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};
        /** No-op event listener used to satisfy the enumerator context contract. */
        private final EventListener eventListener =
                new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        // no-op
                    }
                };

        /** Creates a test context with deterministic reader registration. */
        private TestingEnumeratorContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = new HashSet<>(registeredReaders);
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
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
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

        /** Returns every split that has been assigned to the target reader so far. */
        private List<SourceSplitBase> getAssignedSplits(int subtaskId) {
            return assignedSplitsByReader.getOrDefault(subtaskId, Collections.emptyList());
        }
    }
}
