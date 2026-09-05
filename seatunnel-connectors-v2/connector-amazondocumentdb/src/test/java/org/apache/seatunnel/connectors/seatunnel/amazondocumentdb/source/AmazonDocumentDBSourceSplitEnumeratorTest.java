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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AmazonDocumentDBSourceSplitEnumeratorTest {

    @Test
    public void testAssignsOneSplitAndSignalsAllReaders() throws Exception {
        Set<Integer> readers = new HashSet<>(Arrays.asList(0, 1));
        RecordingContext context = new RecordingContext(2, readers);
        AmazonDocumentDBSourceSplitEnumerator enumerator =
                new AmazonDocumentDBSourceSplitEnumerator(
                        context, null, "{\"status\": \"OPEN\"}", "{\"status\": 1}");

        try {
            enumerator.run();
        } finally {
            enumerator.close();
        }

        Assertions.assertEquals(1, context.assignedSplits.get(0).size());
        Assertions.assertEquals("0", context.assignedSplits.get(0).get(0).splitId());
        Assertions.assertEquals(
                "{\"status\": \"OPEN\"}", context.assignedSplits.get(0).get(0).getMatchQuery());
        Assertions.assertTrue(
                context.assignedSplits.getOrDefault(1, Collections.emptyList()).isEmpty());
        Assertions.assertEquals(readers, context.noMoreSplitReaders);
    }

    @Test
    public void testRestoresWithoutCreatingAnotherSplit() {
        Map<Integer, List<AmazonDocumentDBSourceSplit>> pendingSplits = new HashMap<>();
        pendingSplits.put(
                0,
                Collections.singletonList(
                        new AmazonDocumentDBSourceSplit(0, "{\"restored\": true}", null)));
        AmazonDocumentDBSourceState state = new AmazonDocumentDBSourceState(false, pendingSplits);
        RecordingContext context = new RecordingContext(1, Collections.singleton(0));
        AmazonDocumentDBSourceSplitEnumerator enumerator =
                new AmazonDocumentDBSourceSplitEnumerator(context, state, "{}", null);

        enumerator.run();

        Assertions.assertEquals(1, context.assignedSplits.get(0).size());
        Assertions.assertEquals(
                "{\"restored\": true}", context.assignedSplits.get(0).get(0).getMatchQuery());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    public void testReaderRegisteredBeforeRunIsNotSignaledEarly() {
        RecordingContext context = new RecordingContext(1, Collections.singleton(0));
        AmazonDocumentDBSourceSplitEnumerator enumerator =
                new AmazonDocumentDBSourceSplitEnumerator(context, null, "{}", null);

        enumerator.registerReader(0);

        Assertions.assertTrue(context.noMoreSplitReaders.isEmpty());
        Assertions.assertTrue(context.assignedSplits.isEmpty());

        enumerator.run();

        Assertions.assertEquals(1, context.assignedSplits.get(0).size());
        Assertions.assertTrue(context.noMoreSplitReaders.contains(0));
    }

    @Test
    public void testReaderRegisteredAfterRunIsSignaledImmediately() {
        RecordingContext context = new RecordingContext(2, Collections.singleton(0));
        AmazonDocumentDBSourceSplitEnumerator enumerator =
                new AmazonDocumentDBSourceSplitEnumerator(context, null, "{}", null);

        enumerator.run();
        context.registerReader(1);
        enumerator.registerReader(1);

        Assertions.assertTrue(context.noMoreSplitReaders.contains(1));
        Assertions.assertTrue(
                context.assignedSplits.getOrDefault(1, Collections.emptyList()).isEmpty());
    }

    @Test
    public void testReturnedSplitWaitsForReaderRegistration() {
        AmazonDocumentDBSourceState state =
                new AmazonDocumentDBSourceState(false, Collections.emptyMap());
        RecordingContext context = new RecordingContext(1, Collections.emptySet());
        AmazonDocumentDBSourceSplitEnumerator enumerator =
                new AmazonDocumentDBSourceSplitEnumerator(context, state, "{}", null);
        AmazonDocumentDBSourceSplit returnedSplit =
                new AmazonDocumentDBSourceSplit(0, "{\"retry\": true}", null);

        enumerator.addSplitsBack(Collections.singletonList(returnedSplit), 0);

        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
        Assertions.assertTrue(context.assignedSplits.isEmpty());
        Assertions.assertTrue(context.noMoreSplitReaders.isEmpty());

        context.registerReader(0);
        enumerator.registerReader(0);

        Assertions.assertEquals(
                "{\"retry\": true}", context.assignedSplits.get(0).get(0).getMatchQuery());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        Assertions.assertTrue(context.noMoreSplitReaders.contains(0));
    }

    private static class RecordingContext
            implements SourceSplitEnumerator.Context<AmazonDocumentDBSourceSplit> {

        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<AmazonDocumentDBSourceSplit>> assignedSplits =
                new HashMap<>();
        private final Set<Integer> noMoreSplitReaders = new HashSet<>();

        private RecordingContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = new HashSet<>(registeredReaders);
        }

        private void registerReader(int subtaskId) {
            registeredReaders.add(subtaskId);
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
        public void assignSplit(int subtaskId, List<AmazonDocumentDBSourceSplit> splits) {
            assignedSplits.put(subtaskId, splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitReaders.add(subtask);
        }

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
