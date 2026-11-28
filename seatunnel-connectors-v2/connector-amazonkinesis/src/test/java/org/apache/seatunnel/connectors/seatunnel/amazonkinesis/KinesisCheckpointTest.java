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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KinesisCheckpointTest {
    @Test
    void restoresBeforeAssignmentAndAfterTerminalCompletion() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("stream_name", "test");
        options.put("start_position", "LATEST");
        AmazonKinesisSource source = new AmazonKinesisSource(ReadonlyConfig.fromMap(options));
        EnumeratorContext earlyContext = new EnumeratorContext();
        SourceSplitEnumerator<KinesisSplit, KinesisSplit> early =
                source.createEnumerator(earlyContext);
        ReaderContext readerContext = new ReaderContext();
        try (SourceReader<SeaTunnelRow, KinesisSplit> reader = source.createReader(readerContext)) {
            KinesisSplit checkpoint = early.snapshotState(1);
            List<KinesisSplit> notYetAssigned = reader.snapshotState(1);
            assertEquals(Collections.emptyList(), notYetAssigned);

            SourceSplitEnumerator<KinesisSplit, KinesisSplit> restored =
                    source.restoreEnumerator(earlyContext, checkpoint);
            restored.addSplitsBack(notYetAssigned, 0);
            restored.run();
            assertEquals(1, earlyContext.assigned.size());
            assertEquals(checkpoint.anchor, earlyContext.assigned.get(0).anchor);

            KinesisSplit terminal = earlyContext.assigned.get(0).copy();
            KinesisSplit.ShardState shard =
                    new KinesisSplit.ShardState("shard-1", Collections.emptyList(), true, true);
            shard.done = true;
            shard.committed = "9007199254740993";
            terminal.shards.put(shard.id, shard);
            terminal.complete = true;

            EnumeratorContext lastContext = new EnumeratorContext();
            SourceSplitEnumerator<KinesisSplit, KinesisSplit> last =
                    source.restoreEnumerator(lastContext, checkpoint);
            last.addSplitsBack(Collections.singletonList(terminal), 0);
            last.run();
            reader.addSplits(lastContext.assigned);
            reader.pollNext(
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow row) {
                            throw new AssertionError("Terminal checkpoint must not emit");
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return this;
                        }
                    });
            assertEquals(1, readerContext.terminalSignals);
            KinesisSplit after = reader.snapshotState(2).get(0);
            assertTrue(after.complete);
            assertEquals(checkpoint.anchor, after.anchor);
            assertEquals("9007199254740993", after.shards.get(shard.id).committed);
        }
    }

    private static final class ReaderContext implements SourceReader.Context {
        private int terminalSignals;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            terminalSignals++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    private static final class EnumeratorContext
            implements SourceSplitEnumerator.Context<KinesisSplit> {
        private final List<KinesisSplit> assigned = new ArrayList<>();

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtask, List<KinesisSplit> splits) {
            assigned.addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtask, SourceEvent event) {}

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
