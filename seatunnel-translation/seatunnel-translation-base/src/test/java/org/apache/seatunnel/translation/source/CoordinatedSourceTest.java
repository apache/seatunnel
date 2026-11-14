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
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class CoordinatedSourceTest {

    @Test
    void shouldExchangeSourceEventsBetweenReadersAndEnumerator() throws Exception {
        int parallelism = 2;
        List<String> splitIds =
                IntStream.range(0, 5).mapToObj(i -> "split-" + i).collect(Collectors.toList());
        EventSource source = new EventSource(splitIds, false);
        CoordinatedSource<String, EventSplit, Integer> coordinatedSource =
                new CoordinatedSource<>(source, null, parallelism, "coordinated-source-events");

        ListCollector collector = new ListCollector();
        coordinatedSource.open();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30), () -> coordinatedSource.run(collector));
        coordinatedSource.close();

        Assertions.assertEquals(
                splitIds, collector.records.stream().sorted().collect(Collectors.toList()));
        Assertions.assertEquals(
                splitIds,
                source.enumerator.finishedSplits.stream().sorted().collect(Collectors.toList()));
        Assertions.assertEquals(
                splitIds,
                source.readers.values().stream()
                        .flatMap(reader -> reader.ackedSplits.stream())
                        .sorted()
                        .collect(Collectors.toList()));
    }

    @Test
    void shouldExposeConfiguredParallelismToEnumerator() throws Exception {
        int parallelism = 3;
        EventSource source = new EventSource(Collections.emptyList(), false);
        CoordinatedSource<String, EventSplit, Integer> coordinatedSource =
                new CoordinatedSource<>(source, null, parallelism, "coordinated-source-parallel");

        Assertions.assertEquals(parallelism, source.enumerator.parallelismAtCreation);
        Assertions.assertEquals(
                parallelism, coordinatedSource.coordinatedEnumeratorContext.currentParallelism());
        coordinatedSource.close();
    }

    @Test
    void shouldFailRunWhenReaderFails() throws Exception {
        EventSource source = new EventSource(Collections.singletonList("split-0"), true);
        CoordinatedSource<String, EventSplit, Integer> coordinatedSource =
                new CoordinatedSource<>(source, null, 1, "coordinated-source-failure");

        coordinatedSource.open();
        Exception exception =
                Assertions.assertThrows(
                        Exception.class,
                        () ->
                                Assertions.assertTimeoutPreemptively(
                                        Duration.ofSeconds(30),
                                        () -> coordinatedSource.run(new ListCollector())));
        coordinatedSource.close();

        Assertions.assertTrue(
                hasCause(exception, EventReader.FAILURE_MESSAGE),
                "The reader failure must be the cause of the run failure");
    }

    @Test
    void shouldFailFastWhenEnumeratorCannotBeCreated() {
        EventSource source = new EventSource(Collections.emptyList(), false);
        source.failEnumeratorCreation = true;

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                new CoordinatedSource<>(
                                        source, null, 1, "coordinated-source-create-failure"));
        Assertions.assertTrue(hasCause(exception, EventSource.ENUMERATOR_FAILURE_MESSAGE));
    }

    @Test
    void shouldMoveCheckpointGatedEnumeratorOnWithEnumeratorCheckpoints() throws Exception {
        int parallelism = 2;
        List<String> splitIds =
                IntStream.range(0, 4).mapToObj(i -> "split-" + i).collect(Collectors.toList());
        EventSource source = new EventSource(splitIds, false);
        source.checkpointGated = true;
        CoordinatedSource<String, EventSplit, Integer> coordinatedSource =
                new CoordinatedSource<String, EventSplit, Integer>(
                        source, null, parallelism, "coordinated-source-gated") {
                    @Override
                    protected long getEnumeratorCheckpointInterval() {
                        return 10L;
                    }
                };

        ListCollector collector = new ListCollector();
        coordinatedSource.open();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30), () -> coordinatedSource.run(collector));
        coordinatedSource.close();

        List<String> expected = new ArrayList<>(splitIds);
        expected.add(CheckpointGatedEnumerator.FOLLOW_UP_SPLIT);
        Assertions.assertEquals(
                expected, collector.records.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void shouldNotTakeEnumeratorCheckpointsByDefault() throws Exception {
        EventSource source = new EventSource(Collections.singletonList("split-0"), false);
        CoordinatedSource<String, EventSplit, Integer> coordinatedSource =
                new CoordinatedSource<>(source, null, 1, "coordinated-source-default");

        coordinatedSource.open();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30), () -> coordinatedSource.run(new ListCollector()));
        coordinatedSource.close();

        Assertions.assertEquals(0, source.enumerator.snapshotCount);
    }

    @Test
    void shouldIgnoreReaderFailuresCausedByClose() throws Exception {
        EventSource source = new EventSource(Collections.emptyList(), false);
        source.failReadersOnceClosed = true;
        CoordinatedSource<String, EventSplit, Integer> coordinatedSource =
                new CoordinatedSource<>(source, null, 1, "coordinated-source-close");

        coordinatedSource.open();
        Thread runner =
                new Thread(
                        () -> {
                            try {
                                coordinatedSource.run(new ListCollector());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        runner.start();
        Assertions.assertTrue(source.readers.get(0).polling.await(30, TimeUnit.SECONDS));
        coordinatedSource.close();
        runner.join(TimeUnit.SECONDS.toMillis(30));

        Assertions.assertFalse(runner.isAlive());
        Assertions.assertTrue(source.readers.get(0).failedAfterClose.get());
        Assertions.assertNull(coordinatedSource.readerFailure.get());
    }

    @Test
    void shouldCloseCreatedReadersAndEnumeratorWhenConstructionFails() {
        EventSource source = new EventSource(Collections.emptyList(), false);
        source.failReaderCreationForSubtask = 1;

        Assertions.assertThrows(
                RuntimeException.class,
                () -> new CoordinatedSource<>(source, null, 2, "coordinated-source-cleanup"));
        Assertions.assertTrue(source.enumerator.closed);
        Assertions.assertTrue(source.readers.get(0).closed);
    }

    private static boolean hasCause(Throwable throwable, String message) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (message.equals(current.getMessage())) {
                return true;
            }
        }
        return false;
    }

    private static final class ListCollector implements Collector<String> {

        private final Object lock = new Object();
        private final Queue<String> records = new ConcurrentLinkedQueue<>();

        @Override
        public void collect(String record) {
            records.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /** A source whose readers report every finished split to the enumerator, like CDC sources. */
    private static final class EventSource
            implements SeaTunnelSource<String, EventSplit, Integer>, SupportCoordinate {

        private static final String ENUMERATOR_FAILURE_MESSAGE = "cannot create enumerator";

        private final List<String> splitIds;
        private final boolean failReaders;
        private final Map<Integer, EventReader> readers = new ConcurrentHashMap<>();
        private boolean failEnumeratorCreation;
        private boolean checkpointGated;
        private boolean failReadersOnceClosed;
        private int failReaderCreationForSubtask = -1;
        private EventEnumerator enumerator;

        private EventSource(List<String> splitIds, boolean failReaders) {
            this.splitIds = splitIds;
            this.failReaders = failReaders;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, EventSplit> createReader(SourceReader.Context readerContext) {
            if (readerContext.getIndexOfSubtask() == failReaderCreationForSubtask) {
                throw new IllegalStateException("cannot create reader");
            }
            EventReader reader = new EventReader(readerContext, failReaders);
            reader.failOnceClosed = failReadersOnceClosed;
            readers.put(readerContext.getIndexOfSubtask(), reader);
            return reader;
        }

        @Override
        public SourceSplitEnumerator<EventSplit, Integer> createEnumerator(
                SourceSplitEnumerator.Context<EventSplit> enumeratorContext) {
            if (failEnumeratorCreation) {
                throw new IllegalStateException(ENUMERATOR_FAILURE_MESSAGE);
            }
            enumerator =
                    checkpointGated
                            ? new CheckpointGatedEnumerator(enumeratorContext, splitIds)
                            : new EventEnumerator(enumeratorContext, splitIds);
            return enumerator;
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

    private static class EventEnumerator implements SourceSplitEnumerator<EventSplit, Integer> {

        protected final SourceSplitEnumerator.Context<EventSplit> context;
        private final int parallelismAtCreation;
        protected final Queue<String> pendingSplits;
        protected final Queue<String> finishedSplits = new ConcurrentLinkedQueue<>();
        protected volatile int snapshotCount;
        private volatile boolean closed;

        private EventEnumerator(
                SourceSplitEnumerator.Context<EventSplit> context, List<String> splitIds) {
            this.context = context;
            this.parallelismAtCreation = context.currentParallelism();
            this.pendingSplits = new ConcurrentLinkedQueue<>(splitIds);
        }

        @Override
        public void open() {}

        @Override
        public void run() {}

        @Override
        public void close() {
            closed = true;
        }

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
            String splitId = pendingSplits.poll();
            if (splitId == null) {
                context.signalNoMoreSplits(subtaskId);
            } else {
                context.assignSplit(subtaskId, new EventSplit(splitId));
            }
        }

        @Override
        public void registerReader(int subtaskId) {}

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
            String splitId = ((SplitFinishedEvent) sourceEvent).splitId;
            finishedSplits.add(splitId);
            context.sendEventToSourceReader(subtaskId, new SplitAckEvent(splitId));
        }

        @Override
        public Integer snapshotState(long checkpointId) {
            snapshotCount++;
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    /**
     * Hands out a follow-up split only after every split has finished and a checkpoint taken after
     * that has completed, like the CDC snapshot split assigner at a parallelism above one.
     */
    private static final class CheckpointGatedEnumerator extends EventEnumerator {

        private static final String FOLLOW_UP_SPLIT = "split-follow-up";

        private final int splitCount;
        private final Set<Integer> awaitingReaders = new TreeSet<>();
        private long checkpointToComplete = -1L;
        private boolean gateOpen;
        private boolean followUpAssigned;

        private CheckpointGatedEnumerator(
                SourceSplitEnumerator.Context<EventSplit> context, List<String> splitIds) {
            super(context, splitIds);
            this.splitCount = splitIds.size();
        }

        @Override
        public synchronized void handleSplitRequest(int subtaskId) {
            awaitingReaders.add(subtaskId);
            assignSplits();
        }

        @Override
        public synchronized Integer snapshotState(long checkpointId) {
            super.snapshotState(checkpointId);
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

        private void assignSplits() {
            Iterator<Integer> awaiting = awaitingReaders.iterator();
            while (awaiting.hasNext()) {
                int subtaskId = awaiting.next();
                String splitId = pendingSplits.poll();
                if (splitId == null && !gateOpen) {
                    return;
                }
                if (splitId == null && !followUpAssigned) {
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
    }

    private static final class EventReader implements SourceReader<String, EventSplit> {

        private static final String FAILURE_MESSAGE = "reader failed";

        private final SourceReader.Context context;
        private final boolean fail;
        private final Queue<EventSplit> splits = new ConcurrentLinkedQueue<>();
        private final List<String> ackedSplits = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch polling = new CountDownLatch(1);
        private final AtomicBoolean failedAfterClose = new AtomicBoolean();
        private volatile boolean splitRequested;
        private volatile boolean noMoreSplits;
        private volatile boolean failOnceClosed;
        private volatile boolean closed;

        private EventReader(SourceReader.Context context, boolean fail) {
            this.context = context;
            this.fail = fail;
        }

        @Override
        public void open() {}

        @Override
        public void close() throws IOException {
            closed = true;
            if (failOnceClosed) {
                // wait until the polling thread has failed because of this close
                long deadline = System.currentTimeMillis() + 30_000L;
                while (!failedAfterClose.get() && System.currentTimeMillis() < deadline) {
                    sleep(5L);
                }
                // let the polling thread finish handling its failure
                sleep(200L);
            }
        }

        @Override
        public void pollNext(Collector<String> output) throws Exception {
            polling.countDown();
            if (fail) {
                throw new IllegalStateException(FAILURE_MESSAGE);
            }
            if (failOnceClosed) {
                // like a reader blocked in I/O that fails once its connection is closed
                while (!closed) {
                    sleep(5L);
                }
                failedAfterClose.set(true);
                throw new IllegalStateException("reader is closed");
            }
            EventSplit split = splits.poll();
            if (split != null) {
                output.collect(split.splitId());
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
            ackedSplits.add(((SplitAckEvent) sourceEvent).splitId);
            splitRequested = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    private static void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
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

    private static final class SplitAckEvent implements SourceEvent {

        private final String splitId;

        private SplitAckEvent(String splitId) {
            this.splitId = splitId;
        }
    }
}
