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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ElasticsearchSourceSplitEnumeratorTest {

    @Test
    public void shouldBalanceSplitsEvenlyAcrossReaders() throws Exception {
        TestingContext context = new TestingContext(4);
        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(context, null, Collections.emptyList());

        Method addPendingSplit =
                ElasticsearchSourceSplitEnumerator.class.getDeclaredMethod(
                        "addPendingSplit", java.util.Collection.class);
        addPendingSplit.setAccessible(true);
        addPendingSplit.invoke(enumerator, buildSplits(10));

        ElasticsearchSourceState state = enumerator.snapshotState(1L);
        Map<Integer, List<ElasticsearchSourceSplit>> pendingSplits = state.getPendingSplit();

        Assertions.assertEquals(4, pendingSplits.size());
        Assertions.assertEquals(3, pendingSplits.get(0).size());
        Assertions.assertEquals(3, pendingSplits.get(1).size());
        Assertions.assertEquals(2, pendingSplits.get(2).size());
        Assertions.assertEquals(2, pendingSplits.get(3).size());
    }

    @Test
    public void shouldReassignReturnedSplitsToOriginalReader() throws Exception {
        TestingContext context = new TestingContext(3);
        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(context, null, Collections.emptyList());

        enumerator.addSplitsBack(buildSplits(3), 2);

        Assertions.assertEquals(0, context.getAssignedSplitCount(0));
        Assertions.assertEquals(0, context.getAssignedSplitCount(1));
        Assertions.assertEquals(3, context.getAssignedSplitCount(2));

        ElasticsearchSourceState state = enumerator.snapshotState(1L);
        Assertions.assertTrue(state.getPendingSplit().isEmpty());
    }

    private List<ElasticsearchSourceSplit> buildSplits(int size) {
        List<ElasticsearchSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            splits.add(new ElasticsearchSourceSplit("split-" + i, null));
        }
        return splits;
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<ElasticsearchSourceSplit> {
        private final int parallelism;
        private final Set<Integer> readers;
        private final Map<Integer, List<ElasticsearchSourceSplit>> assignments = new HashMap<>();

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
            this.readers = new LinkedHashSet<>();
            for (int i = 0; i < parallelism; i++) {
                readers.add(i);
            }
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
        public void assignSplit(int subtaskId, List<ElasticsearchSourceSplit> splits) {
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

        private int getAssignedSplitCount(int subtaskId) {
            return assignments.getOrDefault(subtaskId, Collections.emptyList()).size();
        }
    }
}
