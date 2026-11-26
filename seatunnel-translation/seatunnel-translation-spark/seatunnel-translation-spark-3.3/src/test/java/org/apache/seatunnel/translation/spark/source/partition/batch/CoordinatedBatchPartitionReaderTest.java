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

package org.apache.seatunnel.translation.spark.source.partition.batch;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.scan.SeaTunnelScan;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class CoordinatedBatchPartitionReaderTest {

    @Test
    void shouldReadSourceThatExchangesEventsBetweenReadersAndEnumerator() throws Exception {
        int parallelism = 2;
        List<String> splitIds =
                IntStream.range(0, 4).mapToObj(i -> "split-" + i).collect(Collectors.toList());
        EventSource source = new EventSource(splitIds, false);

        InputPartition[] partitions =
                new SeaTunnelBatch(
                                source,
                                parallelism,
                                "coordinated-batch-events",
                                Collections.emptyMap(),
                                multiTableManager())
                        .planInputPartitions();
        Assertions.assertEquals(1, partitions.length);

        List<String> rows = new ArrayList<>();
        MultiTableManager multiTableManager = multiTableManager();
        CoordinatedBatchPartitionReader partitionReader =
                new CoordinatedBatchPartitionReader(
                        source,
                        parallelism,
                        "coordinated-batch-events",
                        0,
                        Collections.emptyMap(),
                        multiTableManager);
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30),
                () -> {
                    while (partitionReader.next()) {
                        InternalRow row = partitionReader.get();
                        if (row != null) {
                            rows.add((String) multiTableManager.reconvert(row).getField(0));
                        }
                    }
                });
        partitionReader.close();

        Assertions.assertEquals(splitIds, rows.stream().sorted().collect(Collectors.toList()));
        Assertions.assertEquals(
                splitIds, source.finishedSplits.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void shouldFailPartitionWhenReaderFails() throws Exception {
        EventSource source = new EventSource(Collections.singletonList("split-0"), true);
        CoordinatedBatchPartitionReader partitionReader =
                new CoordinatedBatchPartitionReader(
                        source,
                        1,
                        "coordinated-batch-failure",
                        0,
                        Collections.emptyMap(),
                        multiTableManager());

        Exception exception =
                Assertions.assertThrows(
                        Exception.class,
                        () ->
                                Assertions.assertTimeoutPreemptively(
                                        Duration.ofSeconds(30),
                                        () -> {
                                            while (partitionReader.next()) {
                                                partitionReader.get();
                                            }
                                        }));
        partitionReader.close();

        boolean causedByReader = false;
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            causedByReader |= EventReader.FAILURE_MESSAGE.equals(cause.getMessage());
        }
        Assertions.assertTrue(causedByReader, "The reader failure must fail the partition");
    }

    @Test
    void shouldMoveCheckpointGatedEnumeratorOnWithoutEngineCheckpoints() throws Exception {
        int parallelism = 2;
        List<String> splitIds =
                IntStream.range(0, 4).mapToObj(i -> "split-" + i).collect(Collectors.toList());
        EventSource source = new EventSource(splitIds, false);
        source.checkpointGated = true;

        MultiTableManager multiTableManager = multiTableManager();
        CoordinatedBatchPartitionReader partitionReader =
                new CoordinatedBatchPartitionReader(
                        source,
                        parallelism,
                        "coordinated-batch-gated",
                        0,
                        Collections.emptyMap(),
                        multiTableManager);
        List<String> rows = new ArrayList<>();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(60),
                () -> {
                    while (partitionReader.next()) {
                        InternalRow row = partitionReader.get();
                        if (row != null) {
                            rows.add((String) multiTableManager.reconvert(row).getField(0));
                        }
                    }
                });
        partitionReader.close();

        List<String> expected = new ArrayList<>(splitIds);
        expected.add(EventEnumerator.FOLLOW_UP_SPLIT);
        Assertions.assertEquals(expected, rows.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void shouldRejectCoordinatedSourceForMicroBatchReading() {
        SeaTunnelScan scan =
                new SeaTunnelScan(
                        new EventSource(Collections.emptyList(), false),
                        1,
                        "coordinated-micro-batch",
                        new CaseInsensitiveStringMap(Collections.emptyMap()),
                        multiTableManager());

        UnsupportedOperationException exception =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> scan.toMicroBatchStream("/tmp/unused-checkpoint"));
        Assertions.assertTrue(exception.getMessage().contains("coordinated-event-source"));
    }

    private static MultiTableManager multiTableManager() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        return new MultiTableManager(
                new CatalogTable[] {CatalogTableUtil.getCatalogTable("events", rowType)});
    }

    /** A source whose readers report every finished split to the enumerator, like CDC sources. */
    private static final class EventSource
            implements SeaTunnelSource<SeaTunnelRow, EventSplit, Integer>, SupportCoordinate {

        private final List<String> splitIds;
        private final boolean failReaders;
        private final Queue<String> finishedSplits = new ConcurrentLinkedQueue<>();
        private boolean checkpointGated;

        private EventSource(List<String> splitIds, boolean failReaders) {
            this.splitIds = splitIds;
            this.failReaders = failReaders;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<SeaTunnelRow, EventSplit> createReader(
                SourceReader.Context readerContext) {
            return new EventReader(readerContext, failReaders);
        }

        @Override
        public SourceSplitEnumerator<EventSplit, Integer> createEnumerator(
                SourceSplitEnumerator.Context<EventSplit> enumeratorContext) {
            return new EventEnumerator(
                    enumeratorContext, splitIds, finishedSplits, checkpointGated);
        }

        @Override
        public SourceSplitEnumerator<EventSplit, Integer> restoreEnumerator(
                SourceSplitEnumerator.Context<EventSplit> enumeratorContext,
                Integer checkpointState) {
            return createEnumerator(enumeratorContext);
        }

        @Override
        public String getPluginName() {
            return "coordinated-event-source";
        }
    }

    /**
     * Assigns one split per request. When checkpoint gated it hands out a follow-up split only
     * after every split has finished and a checkpoint taken after that has completed, like the CDC
     * snapshot split assigner at a parallelism above one.
     */
    private static final class EventEnumerator
            implements SourceSplitEnumerator<EventSplit, Integer> {

        private static final String FOLLOW_UP_SPLIT = "split-follow-up";

        private final SourceSplitEnumerator.Context<EventSplit> context;
        private final Queue<String> pendingSplits;
        private final Queue<String> finishedSplits;
        private final int splitCount;
        private final boolean checkpointGated;
        private final Set<Integer> awaitingReaders = new TreeSet<>();
        private long checkpointToComplete = -1L;
        private boolean gateOpen;
        private boolean followUpAssigned;

        private EventEnumerator(
                SourceSplitEnumerator.Context<EventSplit> context,
                List<String> splitIds,
                Queue<String> finishedSplits,
                boolean checkpointGated) {
            this.context = context;
            this.pendingSplits = new ConcurrentLinkedQueue<>(splitIds);
            this.finishedSplits = finishedSplits;
            this.splitCount = splitIds.size();
            this.checkpointGated = checkpointGated;
        }

        @Override
        public void open() {}

        @Override
        public void run() {}

        @Override
        public void close() {}

        @Override
        public void addSplitsBack(List<EventSplit> splits, int subtaskId) {
            splits.forEach(split -> pendingSplits.add(split.splitId()));
        }

        @Override
        public int currentUnassignedSplitSize() {
            return pendingSplits.size();
        }

        @Override
        public synchronized void handleSplitRequest(int subtaskId) {
            awaitingReaders.add(subtaskId);
            assignSplits();
        }

        private void assignSplits() {
            Iterator<Integer> awaiting = awaitingReaders.iterator();
            while (awaiting.hasNext()) {
                int subtaskId = awaiting.next();
                String splitId = pendingSplits.poll();
                if (splitId == null && checkpointGated && !gateOpen) {
                    return;
                }
                if (splitId == null && checkpointGated && !followUpAssigned) {
                    followUpAssigned = true;
                    splitId = FOLLOW_UP_SPLIT;
                }
                if (splitId == null) {
                    context.signalNoMoreSplits(subtaskId);
                } else {
                    context.assignSplit(subtaskId, new EventSplit(splitId));
                }
                awaiting.remove();
            }
        }

        @Override
        public void registerReader(int subtaskId) {}

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
            String splitId = ((SplitFinishedEvent) sourceEvent).splitId;
            finishedSplits.add(splitId);
            context.sendEventToSourceReader(subtaskId, new SplitAckEvent());
        }

        @Override
        public synchronized Integer snapshotState(long checkpointId) {
            if (checkpointToComplete < 0 && finishedSplits.size() == splitCount) {
                checkpointToComplete = checkpointId;
            }
            return null;
        }

        @Override
        public synchronized void notifyCheckpointComplete(long checkpointId) {
            if (checkpointToComplete >= 0 && checkpointId >= checkpointToComplete) {
                gateOpen = true;
                assignSplits();
            }
        }
    }

    private static final class EventReader implements SourceReader<SeaTunnelRow, EventSplit> {

        private static final String FAILURE_MESSAGE = "reader failed";

        private final SourceReader.Context context;
        private final boolean fail;
        private final Queue<EventSplit> splits = new ConcurrentLinkedQueue<>();
        private volatile boolean splitRequested;
        private volatile boolean noMoreSplits;

        private EventReader(SourceReader.Context context, boolean fail) {
            this.context = context;
            this.fail = fail;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<SeaTunnelRow> output) {
            if (fail) {
                throw new IllegalStateException(FAILURE_MESSAGE);
            }
            EventSplit split = splits.poll();
            if (split != null) {
                output.collect(new SeaTunnelRow(new Object[] {split.splitId()}));
                // the enumerator answers with an ack, which requests the next split
                context.sendSourceEventToEnumerator(new SplitFinishedEvent(split.splitId()));
                return;
            }
            if (noMoreSplits) {
                context.signalNoMoreElement();
            } else if (!splitRequested) {
                splitRequested = true;
                context.sendSplitRequest();
            }
        }

        @Override
        public List<EventSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(splits);
        }

        @Override
        public void addSplits(List<EventSplit> splits) {
            this.splits.addAll(splits);
        }

        @Override
        public void handleNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void handleSourceEvent(SourceEvent sourceEvent) {
            splitRequested = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    private static final class EventSplit implements SourceSplit {

        private final String splitId;

        private EventSplit(String splitId) {
            this.splitId = splitId;
        }

        @Override
        public String splitId() {
            return splitId;
        }
    }

    private static final class SplitFinishedEvent implements SourceEvent {

        private final String splitId;

        private SplitFinishedEvent(String splitId) {
            this.splitId = splitId;
        }
    }

    private static final class SplitAckEvent implements SourceEvent {}
}
