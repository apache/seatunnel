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

package org.apache.seatunnel.translation.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ParallelSourceTest {

    @Test
    void shouldDistributeSplitsAcrossParallelReaders() throws Exception {
        int splitCount = 15;
        int parallelism = 4;
        List<MockSplit> splits =
                IntStream.range(0, splitCount)
                        .mapToObj(i -> new MockSplit("split-" + i))
                        .collect(Collectors.toList());

        MockSource source = new MockSource(splits);
        Map<Integer, List<MockSplit>> assignedSplits = new LinkedHashMap<>();

        for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
            ParallelSource<String, MockSplit, Integer> parallelSource =
                    new ParallelSource<>(
                            source, null, parallelism, "parallel-source-test", subtaskId);
            parallelSource.open();
            parallelSource.splitEnumerator.run();

            assignedSplits.put(subtaskId, parallelSource.reader.snapshotState(0L));
            parallelSource.close();
        }

        int totalAssigned = assignedSplits.values().stream().mapToInt(List::size).sum();
        Assertions.assertEquals(splitCount, totalAssigned);

        List<String> splitIds =
                assignedSplits.values().stream()
                        .flatMap(List::stream)
                        .map(MockSplit::splitId)
                        .sorted()
                        .collect(Collectors.toList());
        Assertions.assertEquals(
                splits.stream().map(MockSplit::splitId).sorted().collect(Collectors.toList()),
                splitIds);

        for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
            Assertions.assertEquals(
                    expectedSplitCount(subtaskId, parallelism, splitCount),
                    assignedSplits.get(subtaskId).size());
        }
    }

    @Test
    void shouldRestoreReaderSplitsFromCheckpointState() throws Exception {
        MockSource source = new MockSource(Collections.emptyList());
        Map<Integer, List<byte[]>> restoredState = new HashMap<>();
        restoredState.put(-1, Collections.singletonList(toBytes(Integer.valueOf(7))));
        restoredState.put(0, Collections.singletonList(toBytes(new MockSplit("restored-split-0"))));

        ParallelSource<String, MockSplit, Integer> parallelSource =
                new ParallelSource<>(source, restoredState, 4, "parallel-source-restore", 0);
        parallelSource.open();
        parallelSource.splitEnumerator.run();

        Assertions.assertEquals(
                Collections.singletonList("restored-split-0"),
                parallelSource.reader.snapshotState(0L).stream()
                        .map(MockSplit::splitId)
                        .sorted()
                        .collect(Collectors.toList()));

        MockSplitEnumerator restoredEnumerator = source.lastRestoredEnumerator;
        Assertions.assertNotNull(restoredEnumerator);
        Assertions.assertEquals(Integer.valueOf(7), restoredEnumerator.restoredState);

        parallelSource.close();
    }

    private static int expectedSplitCount(int subtaskId, int parallelism, int splitCount) {
        int splitsPerReader = splitCount / parallelism;
        int remainder = splitCount % parallelism;
        return subtaskId < remainder ? splitsPerReader + 1 : splitsPerReader;
    }

    private static byte[] toBytes(Serializable value) throws IOException {
        return new org.apache.seatunnel.api.serialization.DefaultSerializer<Serializable>()
                .serialize(value);
    }

    private static final class MockSource implements SeaTunnelSource<String, MockSplit, Integer> {

        private final List<MockSplit> splits;
        private MockSplitEnumerator lastRestoredEnumerator;

        private MockSource(List<MockSplit> splits) {
            this.splits = splits;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, MockSplit> createReader(SourceReader.Context readerContext) {
            return new MockReader();
        }

        @Override
        public SourceSplitEnumerator<MockSplit, Integer> createEnumerator(
                SourceSplitEnumerator.Context<MockSplit> enumeratorContext) {
            return new MockSplitEnumerator(enumeratorContext, splits, null);
        }

        @Override
        public SourceSplitEnumerator<MockSplit, Integer> restoreEnumerator(
                SourceSplitEnumerator.Context<MockSplit> enumeratorContext,
                Integer checkpointState) {
            lastRestoredEnumerator =
                    new MockSplitEnumerator(
                            enumeratorContext, Collections.emptyList(), checkpointState);
            return lastRestoredEnumerator;
        }

        @Override
        public String getPluginName() {
            return "mock-source";
        }
    }

    private static final class MockSplitEnumerator
            implements SourceSplitEnumerator<MockSplit, Integer> {

        private final SourceSplitEnumerator.Context<MockSplit> context;
        private final Map<Integer, List<MockSplit>> pendingSplits = new HashMap<>();
        private final Integer restoredState;
        private final AtomicInteger assignCount = new AtomicInteger(0);

        private MockSplitEnumerator(
                SourceSplitEnumerator.Context<MockSplit> context,
                List<MockSplit> splits,
                Integer restoredState) {
            this.context = context;
            this.restoredState = restoredState;
            addPendingSplits(splits);
        }

        @Override
        public void open() {}

        @Override
        public void run() {
            assignSplits(context.registeredReaders());
            context.registeredReaders().forEach(context::signalNoMoreSplits);
        }

        private static int getSplitOwner(int assignCount, int parallelism) {
            return assignCount % parallelism;
        }

        private void assignSplits(Iterable<Integer> readers) {
            for (Integer reader : readers) {
                List<MockSplit> assigned = pendingSplits.remove(reader);
                if (assigned != null && !assigned.isEmpty()) {
                    context.assignSplit(reader, assigned);
                }
            }
        }

        private void addPendingSplits(List<MockSplit> splits) {
            int parallelism = context.currentParallelism();
            List<MockSplit> orderedSplits =
                    splits.stream()
                            .sorted((left, right) -> left.splitId().compareTo(right.splitId()))
                            .collect(Collectors.toList());
            for (MockSplit split : orderedSplits) {
                int ownerReader = getSplitOwner(assignCount.getAndIncrement(), parallelism);
                pendingSplits.computeIfAbsent(ownerReader, key -> new ArrayList<>()).add(split);
            }
        }

        @Override
        public void close() {}

        @Override
        public void addSplitsBack(List<MockSplit> splits, int subtaskId) {
            pendingSplits.computeIfAbsent(subtaskId, key -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public int currentUnassignedSplitSize() {
            return pendingSplits.values().stream().mapToInt(List::size).sum();
        }

        @Override
        public void handleSplitRequest(int subtaskId) {}

        @Override
        public void registerReader(int subtaskId) {}

        @Override
        public Integer snapshotState(long checkpointId) {
            return restoredState;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static final class MockReader implements SourceReader<String, MockSplit> {

        private final List<MockSplit> splits = new ArrayList<>();

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<String> output) {}

        @Override
        public List<MockSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(splits);
        }

        @Override
        public void addSplits(List<MockSplit> splits) {
            this.splits.addAll(splits);
        }

        @Override
        public void handleNoMoreSplits() {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static final class MockSplit implements SourceSplit {

        private final String splitId;

        private MockSplit(String splitId) {
            this.splitId = splitId;
        }

        @Override
        public String splitId() {
            return splitId;
        }
    }
}
