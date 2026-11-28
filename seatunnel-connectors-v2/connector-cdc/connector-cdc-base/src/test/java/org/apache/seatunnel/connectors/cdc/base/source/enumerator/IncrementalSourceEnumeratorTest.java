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
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.translation.source.CoordinatedSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IncrementalSourceEnumeratorTest {

    private static final TableId TABLE_ID = TableId.parse("db1.table1");

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

    @Test
    public void shouldAssignSplitsAddedBackBeforeRunExactlyOnce() throws Exception {
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        TestingSplitAssigner splitAssigner = new TestingSplitAssigner();
        SourceSplitBase restoredSplit = new SnapshotSplit("restored", null, null, null, null);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);
        enumerator.open();
        // Mirror the engine's restore ordering: the reader's split request and the restored
        // splits are delivered before the first run() invocation. Nothing is dispatched yet
        // because the enumerator is not running.
        enumerator.handleSplitRequest(0);
        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);
        Assertions.assertEquals(Collections.emptyList(), context.allAssignedSplits);

        enumerator.run();

        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), splitAssigner.restoredSplits);
        Assertions.assertEquals(Collections.singletonList(restoredSplit), context.assignedSplits);
        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), context.allAssignedSplits);
    }

    @Test
    void shouldPreserveInFlightSplitAndNotDeadlockWhenZetaCheckpointsDuringHandoff()
            throws Exception {
        CountDownLatch assignmentStarted = new CountDownLatch(1);
        CountDownLatch releaseAssignment = new CountDownLatch(1);
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(
                        Collections.singleton(0), assignmentStarted, releaseAssignment, false);
        Set<TableId> capturedTables = Collections.singleton(TABLE_ID);
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createAssignerContext(capturedTables), 1, new TestOffsetFactory());
        IncrementalSourceEnumerator enumerator = new IncrementalSourceEnumerator(context, assigner);
        enumerator.open();
        enumerator.handleSplitRequest(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> runFuture =
                executor.submit(
                        () -> {
                            enumerator.run();
                            return null;
                        });
        try {
            Assertions.assertTrue(assignmentStarted.await(5, TimeUnit.SECONDS));

            // Mirror Zeta's SourceSplitEnumeratorTask.triggerBarrier: it holds synchronized
            // (enumeratorContext) while invoking enumerator.snapshotState(checkpointId).
            Future<IncrementalPhaseState> checkpointFuture =
                    executor.submit(
                            () -> {
                                synchronized (context) {
                                    return (IncrementalPhaseState) enumerator.snapshotState(1L);
                                }
                            });

            IncrementalPhaseState inFlightCheckpoint = checkpointFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(inFlightCheckpoint.isSplitAssigned());
            Assertions.assertEquals(1, inFlightCheckpoint.getRemainingSplits().size());
            Assertions.assertEquals(
                    Collections.singletonList(TABLE_ID),
                    inFlightCheckpoint.getRemainingSplits().get(0).getTableIds());

            releaseAssignment.countDown();
            runFuture.get(5, TimeUnit.SECONDS);

            // Once context.assignSplit completes, the next checkpoint no longer keeps the split.
            IncrementalPhaseState postHandoffCheckpoint;
            synchronized (context) {
                postHandoffCheckpoint = (IncrementalPhaseState) enumerator.snapshotState(2L);
            }
            Assertions.assertTrue(postHandoffCheckpoint.isSplitAssigned());
            Assertions.assertTrue(postHandoffCheckpoint.getRemainingSplits().isEmpty());

            // Restore from the mid-handoff checkpoint where the reader had zero splits.
            IncrementalPhaseState restoredState = roundTrip(inFlightCheckpoint);
            TestingEnumeratorContext restoredContext =
                    new TestingEnumeratorContext(Collections.singleton(0));
            IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                    new IncrementalSplitAssigner<>(
                            createAssignerContext(capturedTables),
                            1,
                            new TestOffsetFactory(),
                            restoredState);
            IncrementalSourceEnumerator restoredEnumerator =
                    new IncrementalSourceEnumerator(restoredContext, restoredAssigner);
            restoredEnumerator.open();
            restoredEnumerator.run();
            restoredEnumerator.handleSplitRequest(0);

            Assertions.assertEquals(1, restoredContext.allAssignedSplits.size());
            Assertions.assertEquals(
                    Collections.singletonList(TABLE_ID),
                    restoredContext.allAssignedSplits.get(0).asIncrementalSplit().getTableIds());

            restoredEnumerator.handleSplitRequest(0);
            Assertions.assertEquals(
                    Collections.singletonList(0), restoredContext.noMoreSplitsSubtasks);
        } finally {
            releaseAssignment.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldNotDeadlockWhenZetaCheckpointsDuringMultiReaderAssignmentLoop() throws Exception {
        CountDownLatch firstAssignmentStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstAssignment = new CountDownLatch(1);
        Set<Integer> readers = new LinkedHashSet<>(Arrays.asList(0, 1));
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(
                        readers, firstAssignmentStarted, releaseFirstAssignment, true);
        TableId customers = TableId.parse("db1.customers");
        TableId orders = TableId.parse("db1.orders");
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(customers, orders));
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createAssignerContext(capturedTables), 2, new TestOffsetFactory());
        IncrementalSourceEnumerator enumerator = new IncrementalSourceEnumerator(context, assigner);
        enumerator.open();
        enumerator.handleSplitRequest(0);
        enumerator.handleSplitRequest(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> runFuture =
                executor.submit(
                        () -> {
                            enumerator.run();
                            return null;
                        });
        try {
            Assertions.assertTrue(firstAssignmentStarted.await(5, TimeUnit.SECONDS));

            // While reader 0's assignSplit is in progress and reader 1 is still queued for the
            // next loop iteration, Zeta's triggerBarrier acquires synchronized (context) and calls
            // snapshotState(). This must complete without AB-BA deadlock and capture both splits.
            Future<IncrementalPhaseState> checkpointFuture =
                    executor.submit(
                            () -> {
                                synchronized (context) {
                                    return (IncrementalPhaseState) enumerator.snapshotState(1L);
                                }
                            });

            IncrementalPhaseState checkpoint = checkpointFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(checkpoint.isSplitAssigned());
            Assertions.assertEquals(2, checkpoint.getRemainingSplits().size());

            releaseFirstAssignment.countDown();
            runFuture.get(5, TimeUnit.SECONDS);

            Assertions.assertEquals(2, context.allAssignedSplits.size());
            Set<TableId> assignedTables = new HashSet<>();
            context.allAssignedSplits.forEach(
                    split -> assignedTables.addAll(split.asIncrementalSplit().getTableIds()));
            Assertions.assertEquals(capturedTables, assignedTables);
        } finally {
            releaseFirstAssignment.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldRecoverFlinkMultiTableCheckpointWithUnassignedSplitWithoutDuplicates()
            throws Exception {
        TableId customers = TableId.parse("db1.customers");
        TableId orders = TableId.parse("db1.orders");
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(customers, orders));
        Set<Integer> readers = new LinkedHashSet<>(Arrays.asList(0, 1, 2));

        TestingEnumeratorContext initialContext = new TestingEnumeratorContext(readers);
        IncrementalSplitAssigner<SourceConfig> initialAssigner =
                new IncrementalSplitAssigner<>(
                        createAssignerContext(capturedTables), 2, new TestOffsetFactory());
        IncrementalSourceEnumerator initialEnumerator =
                new IncrementalSourceEnumerator(initialContext, initialAssigner);
        initialEnumerator.open();
        initialEnumerator.run();

        // Reader 0 receives one table split; the second table split stays in remainingSplits.
        initialEnumerator.handleSplitRequest(0);
        Assertions.assertEquals(1, initialContext.assignedSplitsBySubtask.get(0).size());
        IncrementalSplit reader0OwnedSplit =
                initialContext.assignedSplitsBySubtask.get(0).get(0).asIncrementalSplit();

        IncrementalPhaseState checkpoint =
                roundTrip((IncrementalPhaseState) initialEnumerator.snapshotState(1L));
        Assertions.assertTrue(checkpoint.isSplitAssigned());
        Assertions.assertEquals(1, checkpoint.getRemainingSplits().size());

        // Flink restore: reader 0 keeps its split locally without addSplitsBack; readers 1 and 2
        // request splits from the restored enumerator.
        TestingEnumeratorContext restoredContext = new TestingEnumeratorContext(readers);
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createAssignerContext(capturedTables),
                        2,
                        new TestOffsetFactory(),
                        checkpoint);
        IncrementalSourceEnumerator restoredEnumerator =
                new IncrementalSourceEnumerator(restoredContext, restoredAssigner);
        restoredEnumerator.open();
        restoredEnumerator.run();
        restoredEnumerator.handleSplitRequest(1);
        restoredEnumerator.handleSplitRequest(2);

        Assertions.assertEquals(1, restoredContext.assignedSplitsBySubtask.get(1).size());
        Assertions.assertNull(restoredContext.assignedSplitsBySubtask.get(2));
        Assertions.assertEquals(Collections.singletonList(2), restoredContext.noMoreSplitsSubtasks);

        IncrementalSplit reader1Split =
                restoredContext.assignedSplitsBySubtask.get(1).get(0).asIncrementalSplit();
        List<TableId> allActiveTables = new ArrayList<>();
        allActiveTables.addAll(reader0OwnedSplit.getTableIds());
        allActiveTables.addAll(reader1Split.getTableIds());
        Assertions.assertEquals(2, allActiveTables.size());
        Assertions.assertEquals(capturedTables, new HashSet<>(allActiveTables));
    }

    @Test
    void shouldDeduplicateInFlightSplitWhenZetaRestoresBothReaderAndEnumeratorState()
            throws Exception {
        CountDownLatch assignmentStarted = new CountDownLatch(1);
        CountDownLatch releaseAssignment = new CountDownLatch(1);
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(
                        Collections.singleton(0), assignmentStarted, releaseAssignment, false);
        Set<TableId> capturedTables = Collections.singleton(TABLE_ID);
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createAssignerContext(capturedTables), 1, new TestOffsetFactory());
        IncrementalSourceEnumerator enumerator = new IncrementalSourceEnumerator(context, assigner);
        enumerator.open();
        enumerator.handleSplitRequest(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> runFuture =
                executor.submit(
                        () -> {
                            enumerator.run();
                            return null;
                        });
        try {
            Assertions.assertTrue(assignmentStarted.await(5, TimeUnit.SECONDS));
            IncrementalPhaseState inFlightCheckpoint;
            synchronized (context) {
                inFlightCheckpoint = (IncrementalPhaseState) enumerator.snapshotState(1L);
            }
            releaseAssignment.countDown();
            runFuture.get(5, TimeUnit.SECONDS);

            // Simulate the opposite Zeta handoff race: the reader had already received the split
            // and advanced to offset 200 before snapshotting, while the enumerator still included
            // the split in remainingSplits at offset 10.
            IncrementalSplit readerCheckpointedSplit =
                    new IncrementalSplit(
                            inFlightCheckpoint.getRemainingSplits().get(0).splitId(),
                            Collections.singletonList(TABLE_ID),
                            new TestOffset(200L),
                            new TestOffset(Long.MAX_VALUE),
                            Collections.emptyList());

            IncrementalPhaseState restoredState = roundTrip(inFlightCheckpoint);
            TestingEnumeratorContext restoredContext =
                    new TestingEnumeratorContext(Collections.singleton(0));
            IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                    new IncrementalSplitAssigner<>(
                            createAssignerContext(capturedTables),
                            1,
                            new TestOffsetFactory(),
                            restoredState);
            IncrementalSourceEnumerator restoredEnumerator =
                    new IncrementalSourceEnumerator(restoredContext, restoredAssigner);
            restoredEnumerator.open();
            restoredEnumerator.addSplitsBack(Collections.singletonList(readerCheckpointedSplit), 0);
            restoredEnumerator.handleSplitRequest(0);
            restoredEnumerator.run();

            Assertions.assertEquals(1, restoredContext.allAssignedSplits.size());
            IncrementalSplit reassigned =
                    restoredContext.allAssignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals(Collections.singletonList(TABLE_ID), reassigned.getTableIds());
            Assertions.assertEquals(new TestOffset(200L), reassigned.getStartupOffset());

            restoredEnumerator.handleSplitRequest(0);
            Assertions.assertEquals(
                    Collections.singletonList(0), restoredContext.noMoreSplitsSubtasks);
        } finally {
            releaseAssignment.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldRequeueIncrementalSplitWhenAssignSplitThrows() throws Exception {
        AtomicBoolean failFirstAssign = new AtomicBoolean(true);
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(Collections.singleton(0)) {
                    @Override
                    public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
                        if (failFirstAssign.compareAndSet(true, false)) {
                            throw new RuntimeException("simulated transport failure");
                        }
                        super.assignSplit(subtaskId, splits);
                    }
                };
        Set<TableId> capturedTables = Collections.singleton(TABLE_ID);
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createAssignerContext(capturedTables), 1, new TestOffsetFactory());
        IncrementalSourceEnumerator enumerator = new IncrementalSourceEnumerator(context, assigner);
        enumerator.open();
        enumerator.handleSplitRequest(0);

        Assertions.assertThrows(RuntimeException.class, enumerator::run);

        IncrementalPhaseState checkpoint = (IncrementalPhaseState) enumerator.snapshotState(1L);
        Assertions.assertEquals(1, checkpoint.getRemainingSplits().size());

        enumerator.handleSplitRequest(0);
        Assertions.assertEquals(1, context.allAssignedSplits.size());
        Assertions.assertEquals(
                Collections.singletonList(TABLE_ID),
                context.allAssignedSplits.get(0).asIncrementalSplit().getTableIds());
    }

    private static SplitAssigner.Context<SourceConfig> createAssignerContext(
            Set<TableId> capturedTables) {
        return new SplitAssigner.Context<>(
                new IncrementalTestSourceConfig(),
                new LinkedHashSet<>(capturedTables),
                new HashMap<>(),
                new HashMap<>());
    }

    private static IncrementalPhaseState roundTrip(IncrementalPhaseState state) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(state);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            return (IncrementalPhaseState) input.readObject();
        }
    }

    /**
     * Runs the real enumerator and hybrid split assigner through the coordinated source that the
     * Spark engine uses, where all readers share one enumerator and no engine checkpoint is ever
     * taken.
     */
    @Test
    void shouldEnterIncrementalPhaseWithMultipleReadersAndEnumeratorCheckpoints() throws Exception {
        int parallelism = 2;
        List<String> snapshotSplitIds =
                IntStream.range(0, 4)
                        .mapToObj(i -> TABLE_ID + ":" + i)
                        .collect(Collectors.toList());
        ReportingSource source = new ReportingSource(snapshotSplitIds);
        CoordinatedSource<String, SourceSplitBase, PendingSplitsState> coordinatedSource =
                new CoordinatedSource<String, SourceSplitBase, PendingSplitsState>(
                        source, null, parallelism, "incremental-source-enumerator") {
                    @Override
                    protected long getEnumeratorCheckpointInterval() {
                        return 10L;
                    }
                };

        Assertions.assertEquals(parallelism, source.enumeratorParallelism);

        RecordCollector collector = new RecordCollector();
        coordinatedSource.open();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30), () -> coordinatedSource.run(collector));
        coordinatedSource.close();

        List<String> expected = new ArrayList<>(snapshotSplitIds);
        expected.add("incremental-split-0");
        Assertions.assertEquals(
                expected, collector.records.stream().sorted().collect(Collectors.toList()));
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

    private static class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {
        private List<SourceSplitBase> assignedSplits = Collections.emptyList();
        private final List<SourceSplitBase> allAssignedSplits = new ArrayList<>();
        private final Map<Integer, List<SourceSplitBase>> assignedSplitsBySubtask =
                new ConcurrentHashMap<>();
        private final List<Integer> noMoreSplitsSubtasks = new ArrayList<>();
        private final Set<Integer> registeredReaders;
        private final CountDownLatch assignmentStarted;
        private final CountDownLatch releaseAssignment;
        private final boolean pauseFirstAssignmentOnly;
        private final AtomicBoolean pausedOnce = new AtomicBoolean(false);

        private TestingEnumeratorContext() {
            this(Collections.singleton(0), null, null, false);
        }

        private TestingEnumeratorContext(Set<Integer> registeredReaders) {
            this(registeredReaders, null, null, false);
        }

        private TestingEnumeratorContext(
                Set<Integer> registeredReaders,
                CountDownLatch assignmentStarted,
                CountDownLatch releaseAssignment,
                boolean pauseFirstAssignmentOnly) {
            this.registeredReaders = registeredReaders;
            this.assignmentStarted = assignmentStarted;
            this.releaseAssignment = releaseAssignment;
            this.pauseFirstAssignmentOnly = pauseFirstAssignmentOnly;
        }

        @Override
        public int currentParallelism() {
            return registeredReaders.size();
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
            if (assignmentStarted != null
                    && (!pauseFirstAssignmentOnly || pausedOnce.compareAndSet(false, true))) {
                assignmentStarted.countDown();
                try {
                    releaseAssignment.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            assignedSplits = splits;
            allAssignedSplits.addAll(splits);
            assignedSplitsBySubtask
                    .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                    .addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsSubtasks.add(subtask);
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

    private static final class RecordCollector implements Collector<String> {

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

    /** Builds the real enumerator over a fixed set of snapshot splits of one table. */
    private static final class ReportingSource
            implements SeaTunnelSource<String, SourceSplitBase, PendingSplitsState>,
                    SupportCoordinate {

        private final List<String> snapshotSplitIds;
        private int enumeratorParallelism = -1;

        private ReportingSource(List<String> snapshotSplitIds) {
            this.snapshotSplitIds = snapshotSplitIds;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, SourceSplitBase> createReader(
                SourceReader.Context readerContext) {
            return new ReportingReader(readerContext);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) {
            enumeratorParallelism = enumeratorContext.currentParallelism();
            List<SnapshotSplit> remainingSplits =
                    snapshotSplitIds.stream()
                            .map(id -> new SnapshotSplit(id, TABLE_ID, null, null, null))
                            .collect(Collectors.toList());
            SnapshotPhaseState snapshotPhaseState =
                    new SnapshotPhaseState(
                            Collections.singletonList(TABLE_ID),
                            remainingSplits,
                            new HashMap<>(),
                            new HashMap<>(),
                            false,
                            Collections.emptyList(),
                            false,
                            true);
            SplitAssigner.Context<SourceConfig> assignerContext =
                    new SplitAssigner.Context<>(
                            new TestSourceConfig(),
                            Collections.singleton(TABLE_ID),
                            new HashMap<>(),
                            new HashMap<>());
            HybridSplitAssigner<SourceConfig> splitAssigner =
                    new HybridSplitAssigner<>(
                            assignerContext,
                            enumeratorParallelism,
                            1,
                            new HybridPendingSplitsState(snapshotPhaseState, null),
                            testDialect(),
                            new TestOffsetFactory());
            return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext,
                PendingSplitsState checkpointState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPluginName() {
            return "reporting-cdc-source";
        }
    }

    /**
     * Follows the protocol of the CDC reader: it reports every finished snapshot split to the
     * enumerator and then asks for the next split.
     */
    private static final class ReportingReader implements SourceReader<String, SourceSplitBase> {

        private final SourceReader.Context context;
        private final Queue<SourceSplitBase> splits = new ConcurrentLinkedQueue<>();
        private volatile boolean started;
        private volatile boolean noMoreSplits;

        private ReportingReader(SourceReader.Context context) {
            this.context = context;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<String> output) {
            if (!started) {
                started = true;
                context.sendSplitRequest();
                return;
            }
            SourceSplitBase split = splits.poll();
            if (split == null) {
                if (noMoreSplits) {
                    context.signalNoMoreElement();
                }
                return;
            }
            output.collect(split.splitId());
            if (split.isSnapshotSplit()) {
                CompletedSnapshotSplitsReportEvent reportEvent =
                        new CompletedSnapshotSplitsReportEvent();
                reportEvent.setCompletedSnapshotSplitWatermarks(
                        Collections.singletonList(
                                new SnapshotSplitWatermark(
                                        split.splitId(), new TestOffset(1L), new TestOffset(2L))));
                context.sendSourceEventToEnumerator(reportEvent);
            } else {
                IncrementalSplit incrementalSplit = split.asIncrementalSplit();
                Assertions.assertEquals(
                        Collections.singletonList(TABLE_ID), incrementalSplit.getTableIds());
            }
            context.sendSplitRequest();
        }

        @Override
        public List<SourceSplitBase> snapshotState(long checkpointId) {
            return new ArrayList<>(splits);
        }

        @Override
        public void addSplits(List<SourceSplitBase> splits) {
            this.splits.addAll(splits);
        }

        @Override
        public void handleNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    @SuppressWarnings("unchecked")
    private static DataSourceDialect<SourceConfig> testDialect() {
        ChunkSplitter chunkSplitter =
                tableId -> {
                    throw new UnsupportedOperationException();
                };
        return (DataSourceDialect<SourceConfig>)
                Proxy.newProxyInstance(
                        IncrementalSourceEnumeratorTest.class.getClassLoader(),
                        new Class<?>[] {DataSourceDialect.class},
                        (proxy, method, args) -> {
                            if ("createChunkSplitter".equals(method.getName())) {
                                return chunkSplitter;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static class TestSourceConfig implements SourceConfig {

        private static final long serialVersionUID = 1L;

        @Override
        public StartupConfig getStartupConfig() {
            return null;
        }

        @Override
        public StopConfig getStopConfig() {
            return new StopConfig(StopMode.NEVER, null, null, null);
        }

        @Override
        public int getSplitSize() {
            return 1;
        }

        @Override
        public boolean isExactlyOnce() {
            return false;
        }
    }

    private static final class IncrementalTestSourceConfig extends TestSourceConfig {

        private static final long serialVersionUID = 1L;

        @Override
        public StartupConfig getStartupConfig() {
            return new StartupConfig(StartupMode.EARLIEST, null, null, null);
        }
    }

    private static final class TestOffsetFactory extends OffsetFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public Offset earliest() {
            return new TestOffset(10L);
        }

        @Override
        public Offset neverStop() {
            return new TestOffset(Long.MAX_VALUE);
        }

        @Override
        public Offset latest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset specific(String filename, Long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset timestamp(long timestamp) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestOffset extends Offset {

        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = Collections.singletonMap("pos", String.valueOf(value));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
