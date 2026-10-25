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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClickhouseSourceSplitEnumeratorTest {

    @Test
    public void shouldBalanceSplitsEvenlyAcrossReaders() throws Exception {
        TestingContext context = new TestingContext(4);

        ClickhouseSourceSplitEnumerator enumerator =
                new ClickhouseSourceSplitEnumerator(
                        context, null, Collections.emptyMap(), Collections.emptyMap());

        Method addPendingSplit =
                ClickhouseSourceSplitEnumerator.class.getDeclaredMethod(
                        "addPendingSplit", java.util.Collection.class);
        addPendingSplit.setAccessible(true);
        addPendingSplit.invoke(enumerator, buildSplits(10));

        ClickhouseSourceState state = enumerator.snapshotState(1L);
        Map<Integer, List<ClickhouseSourceSplit>> pendingSplit = state.getPendingSplit();

        Assertions.assertEquals(4, pendingSplit.size());
        Assertions.assertEquals(3, pendingSplit.get(0).size());
        Assertions.assertEquals(3, pendingSplit.get(1).size());
        Assertions.assertEquals(2, pendingSplit.get(2).size());
        Assertions.assertEquals(2, pendingSplit.get(3).size());
    }

    private List<ClickhouseSourceSplit> buildSplits(int size) {
        List<ClickhouseSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            splits.add(
                    new ClickhouseSourceSplit(
                            null, null, Collections.emptyList(), null, null, "split-" + i));
        }
        return splits;
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<ClickhouseSourceSplit> {
        private final int parallelism;

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.emptySet();
        }

        @Override
        public void assignSplit(int subtaskId, List<ClickhouseSourceSplit> splits) {}

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
