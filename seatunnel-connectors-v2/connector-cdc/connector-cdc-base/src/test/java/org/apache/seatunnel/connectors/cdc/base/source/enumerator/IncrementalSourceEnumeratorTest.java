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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class IncrementalSourceEnumeratorTest {

    @Test
    public void shouldAssignRestoredSplitsToWaitingReader() throws Exception {
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        TestingSplitAssigner splitAssigner = new TestingSplitAssigner();
        SourceSplitBase restoredSplit = new SnapshotSplit("restored", null, null, null, null);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);
        enumerator.open();
        enumerator.run();
        enumerator.handleSplitRequest(0);
        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);

        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), splitAssigner.restoredSplits);
        Assertions.assertEquals(Collections.singletonList(restoredSplit), context.assignedSplits);
    }

    private static final class TestingSplitAssigner implements SplitAssigner {
        private List<SourceSplitBase> addedSplits = Collections.emptyList();
        private List<SourceSplitBase> restoredSplits = Collections.emptyList();

        @Override
        public void open() {}

        @Override
        public Optional<SourceSplitBase> getNext() {
            if (addedSplits.isEmpty()) {
                return Optional.empty();
            }
            SourceSplitBase split = addedSplits.get(0);
            addedSplits = Collections.emptyList();
            return Optional.of(split);
        }

        @Override
        public boolean waitingForCompletedSplits() {
            return true;
        }

        @Override
        public void onCompletedSplits(
                List<SnapshotSplitWatermark> completedSnapshotSplitWatermarks) {}

        @Override
        public void addSplits(Collection<SourceSplitBase> splits) {
            restoredSplits = Collections.unmodifiableList(new ArrayList<>(splits));
            addedSplits = restoredSplits;
        }

        @Override
        public PendingSplitsState snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    private static final class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {
        private List<SourceSplitBase> assignedSplits = Collections.emptyList();

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
            assignedSplits = splits;
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
