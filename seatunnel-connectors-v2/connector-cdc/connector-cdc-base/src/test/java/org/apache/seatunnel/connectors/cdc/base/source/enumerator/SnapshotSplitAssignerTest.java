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

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.api.cdc.CdcSnapshotSplitProgress;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.progress.CdcEnumeratorProgressSource;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SnapshotSplitAssignerTest {

    @Test
    @SuppressWarnings("unchecked")
    void testProgressFollowsReassignmentCheckpointAndRetirement() {
        SourceConfig config = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        ChunkSplitter splitter = Mockito.mock(ChunkSplitter.class);
        TableId table = TableId.parse("db1.table1");
        SnapshotSplit first = new SnapshotSplit("first", table, null, null, null);
        SnapshotSplit second = new SnapshotSplit("second", table, null, null, null);
        Mockito.when(dialect.createChunkSplitter(config)).thenReturn(splitter);
        Mockito.when(splitter.generateSplits(table)).thenReturn(Arrays.asList(first, second));
        SnapshotSplitAssigner<SourceConfig> assigner =
                new SnapshotSplitAssigner<>(
                        new SplitAssigner.Context<>(
                                config,
                                Collections.singleton(table),
                                new HashMap<>(),
                                new HashMap<>()),
                        1,
                        Collections.singletonList(table),
                        false,
                        dialect);
        assigner.open();
        Assertions.assertSame(first, assigner.getNext().get());
        CdcEnumeratorProgressReport assigned =
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
        Assertions.assertEquals(1, assigned.getAssignedSplitCount().getValue());
        Assertions.assertEquals(1, assigned.getPreparedRemainingSplitCount().getValue());
        assigner.addSplits(Collections.singletonList(first));
        Assertions.assertEquals(
                0,
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getAssignedSplitCount()
                        .getValue());
        Assertions.assertSame(second, assigner.getNext().get());
        Assertions.assertSame(first, assigner.getNext().get());
        SnapshotPhaseState checkpoint = assigner.snapshotState(1);
        assigner.onCompletedSplits(Arrays.asList(createWatermark(first), createWatermark(second)));
        CdcEnumeratorProgressReport completed =
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
        Assertions.assertEquals(2, completed.getAssignedSplitCount().getValue());
        Assertions.assertEquals(2, completed.getCompletedSplitCount().getValue());
        Assertions.assertEquals(0, completed.getRunningSplitCount().getValue());
        Assertions.assertTrue(checkpoint.getSplitCompletedOffsets().isEmpty());
        Assertions.assertTrue(assigner.completedSnapshotPhase(Collections.singletonList(table)));
        Assertions.assertEquals(
                0,
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getAssignedSplitCount()
                        .getValue());
        Assertions.assertEquals(2, checkpoint.getAssignedSplits().size());
        Assertions.assertEquals(1, assigned.getAssignedSplitCount().getValue());
        Assertions.assertEquals(2, completed.getCompletedSplitCount().getValue());
    }

    @Test
    void testPublishedDetailsAreBoundedSortedAndDetachedFromOffsets() {
        Map<String, SnapshotSplit> assigned = new HashMap<>();
        for (int i = 0; i < CdcEnumeratorProgressReport.MAX_ACTIVE_SPLITS + 5; i++) {
            SnapshotSplit split =
                    createFinishedSnapshotSplit(String.format("split-%03d", i), 10, 20);
            assigned.put(split.splitId(), split);
        }
        SnapshotSplitAssigner<?> assigner =
                createRestoredSnapshotSplitAssigner(assigned, new HashMap<>());
        CdcEnumeratorProgressReport report =
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
        Assertions.assertEquals(105, report.getRunningSplitCount().getValue());
        Assertions.assertEquals(100, report.getActiveSplits().size());
        Assertions.assertTrue(report.isActiveSplitsTruncated());
        Assertions.assertEquals("split-000", report.getActiveSplits().get(0).getSplitId());
        assigned.get("split-000").getLowWatermark().getOffset().put("pos", "999");
        Assertions.assertEquals(
                "10",
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getActiveSplits()
                        .get(0)
                        .getLowWatermark()
                        .getValue()
                        .getValues()
                        .get("pos"));
        Assertions.assertEquals(
                "MYSQL_BINLOG",
                report.getActiveSplits().get(0).getLowWatermark().getValue().getType());
    }

    @RepeatedTest(3)
    void testAddBackPublishesAtomicallyAndIgnoresLateCompletion() throws Exception {
        SnapshotSplit first =
                new SnapshotSplit(
                        "first", TableId.parse("db1.table1"), null, null, null, null, null);
        SnapshotSplit second =
                new SnapshotSplit(
                        "second", TableId.parse("db1.table1"), null, null, null, null, null);
        Map<String, SnapshotSplit> assigned = new HashMap<>();
        assigned.put(first.splitId(), first);
        assigned.put(second.splitId(), second);
        SnapshotSplitAssigner<?> assigner =
                createRestoredSnapshotSplitAssigner(assigned, new HashMap<>());
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Collection<SourceSplitBase> returned =
                new AbstractCollection<SourceSplitBase>() {
                    @Override
                    public Iterator<SourceSplitBase> iterator() {
                        return new Iterator<SourceSplitBase>() {
                            private int index;

                            @Override
                            public boolean hasNext() {
                                return index < 2;
                            }

                            @Override
                            public SourceSplitBase next() {
                                if (index++ == 0) {
                                    return first;
                                }
                                returning.countDown();
                                try {
                                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new AssertionError(e);
                                }
                                return second;
                            }
                        };
                    }

                    @Override
                    public int size() {
                        return 2;
                    }
                };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> addBack = executor.submit(() -> assigner.addSplits(returned));
            Assertions.assertTrue(returning.await(5, TimeUnit.SECONDS));
            CdcEnumeratorProgressReport during =
                    executor.submit(
                                    () ->
                                            assigner.getCdcEnumeratorProgress(
                                                    "MySQL-CDC", "MYSQL_BINLOG"))
                            .get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(2, during.getAssignedSplitCount().getValue());
            Assertions.assertEquals(0, during.getPreparedRemainingSplitCount().getValue());
            release.countDown();
            addBack.get(5, TimeUnit.SECONDS);
            CdcEnumeratorProgressReport after =
                    assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
            Assertions.assertEquals(0, after.getAssignedSplitCount().getValue());
            Assertions.assertEquals(2, after.getPreparedRemainingSplitCount().getValue());
            assigner.onCompletedSplits(Collections.singletonList(createWatermark(first)));
            Assertions.assertEquals(
                    0,
                    assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                            .getCompletedSplitCount()
                            .getValue());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void testEnumeratorPollingDoesNotWaitForChunkGeneration(boolean snapshotOnly) throws Exception {
        CountDownLatch chunking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        SourceConfig config = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        ChunkSplitter splitter = Mockito.mock(ChunkSplitter.class);
        TableId table = TableId.parse("db1.table1");
        Mockito.when(dialect.createChunkSplitter(config)).thenReturn(splitter);
        Mockito.when(splitter.generateSplits(table))
                .thenAnswer(
                        invocation -> {
                            chunking.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            return Collections.singletonList(createFinishedSnapshotSplit("chunk"));
                        });
        SplitAssigner.Context<SourceConfig> assignerContext =
                new SplitAssigner.Context<>(
                        config, Collections.singleton(table), new HashMap<>(), new HashMap<>());
        SplitAssigner assigner =
                snapshotOnly
                        ? new SnapshotOnlySplitAssigner<>(
                                assignerContext,
                                1,
                                Collections.singletonList(table),
                                false,
                                dialect)
                        : new SnapshotSplitAssigner<>(
                                assignerContext,
                                1,
                                Collections.singletonList(table),
                                false,
                                dialect);
        SourceSplitEnumerator.Context<SourceSplitBase> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, assigner, "MySQL-CDC", "MYSQL_BINLOG");
        enumerator.open();
        enumerator.handleSplitRequest(0);
        try {
            Future<?> assignment =
                    executor.submit(
                            () -> {
                                enumerator.run();
                                return null;
                            });
            Assertions.assertTrue(chunking.await(5, TimeUnit.SECONDS));
            executor.submit(enumerator::getCdcProgress).get(1, TimeUnit.SECONDS);
            CdcEnumeratorProgressReport direct =
                    ((CdcEnumeratorProgressSource) assigner)
                            .getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
            Assertions.assertEquals(
                    CdcSnapshotAssignmentStatus.DISCOVERING, direct.getSnapshotAssignmentStatus());
            Assertions.assertEquals(1, direct.getRemainingUnchunkedTableCount().getValue());
            for (int i = 0; i < 20; i++) {
                CdcEnumeratorProgressReport report =
                        executor.submit(enumerator::getCdcProgress).get(1, TimeUnit.SECONDS);
                Assertions.assertEquals(0, report.getAssignedSplitCount().getValue());
                Assertions.assertEquals(
                        CdcSnapshotAssignmentStatus.DISCOVERING,
                        report.getSnapshotAssignmentStatus());
            }
            SnapshotSplit returned = new SnapshotSplit("returned", table, null, null, null);
            executor.submit(() -> enumerator.addSplitsBack(Collections.singletonList(returned), 0))
                    .get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    1, enumerator.getCdcProgress().getPreparedRemainingSplitCount().getValue());
            release.countDown();
            assignment.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    1, enumerator.getCdcProgress().getAssignedSplitCount().getValue());
            Assertions.assertEquals(
                    1, enumerator.getCdcProgress().getPreparedRemainingSplitCount().getValue());
            Assertions.assertEquals(0, direct.getAssignedSplitCount().getValue());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            enumerator.close();
        }
    }

    @RepeatedTest(3)
    void testPollingPublishesOnlyCompletedCountTransitions() throws Exception {
        SnapshotSplit split = createFinishedSnapshotSplit("active");
        SnapshotSplitAssigner<?> assigner =
                createRestoredSnapshotSplitAssigner(
                        new HashMap<>(Collections.singletonMap(split.splitId(), split)),
                        new HashMap<>());
        CountDownLatch updating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        SnapshotSplitWatermark watermark = Mockito.mock(SnapshotSplitWatermark.class);
        Mockito.when(watermark.getSplitId())
                .thenAnswer(
                        invocation -> {
                            if (calls.incrementAndGet() == 2) {
                                updating.countDown();
                                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            }
                            return split.splitId();
                        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> completion =
                    executor.submit(
                            () -> assigner.onCompletedSplits(Collections.singletonList(watermark)));
            Assertions.assertTrue(updating.await(5, TimeUnit.SECONDS));
            CdcEnumeratorProgressReport during =
                    executor.submit(
                                    () ->
                                            assigner.getCdcEnumeratorProgress(
                                                    "MySQL-CDC", "MYSQL_BINLOG"))
                            .get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(0, during.getCompletedSplitCount().getValue());
            Assertions.assertEquals(1, during.getRunningSplitCount().getValue());
            release.countDown();
            completion.get(5, TimeUnit.SECONDS);
            CdcEnumeratorProgressReport after =
                    assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
            Assertions.assertEquals(1, after.getCompletedSplitCount().getValue());
            Assertions.assertEquals(0, after.getRunningSplitCount().getValue());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotOnlyEnumeratorDelegatesRestoredCountsAndCompletion() {
        SnapshotSplit split = createFinishedSnapshotSplit("snapshot-only");
        SnapshotPhaseState checkpoint =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(Collections.singletonMap(split.splitId(), split)),
                        new HashMap<>(),
                        false,
                        Collections.emptyList(),
                        false,
                        true);
        SnapshotOnlySplitAssigner<?> assigner =
                new SnapshotOnlySplitAssigner<>(
                        new SplitAssigner.Context<>(
                                null,
                                Collections.singleton(split.getTableId()),
                                checkpoint.getAssignedSplits(),
                                checkpoint.getSplitCompletedOffsets()),
                        1,
                        checkpoint,
                        null);
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(
                        Mockito.mock(SourceSplitEnumerator.Context.class),
                        assigner,
                        "MySQL-CDC",
                        "MYSQL_BINLOG");
        CdcEnumeratorProgressReport active = enumerator.getCdcProgress();
        Assertions.assertNotNull(active);
        Assertions.assertEquals(1, active.getRunningSplitCount().getValue());
        CompletedSnapshotSplitsReportEvent event = new CompletedSnapshotSplitsReportEvent();
        event.setCompletedSnapshotSplitWatermarks(
                Collections.singletonList(createWatermark(split)));
        enumerator.handleSourceEvent(0, event);
        Assertions.assertEquals(1, enumerator.getCdcProgress().getCompletedSplitCount().getValue());
        Assertions.assertEquals(0, enumerator.getCdcProgress().getRunningSplitCount().getValue());
        Assertions.assertEquals(1, active.getRunningSplitCount().getValue());
    }

    @Test
    public void testAddSplitsShouldKeepCompletedFinishedSplitOutOfRemainingQueue() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        completedOffsets.put(
                finishedSplit.splitId(),
                new SnapshotSplitWatermark(
                        finishedSplit.splitId(),
                        finishedSplit.getLowWatermark(),
                        finishedSplit.getHighWatermark()));

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, completedOffsets);

        splitAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState state = splitAssigner.snapshotState(11L);
        Assertions.assertTrue(state.getRemainingSplits().isEmpty());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()), state.getAssignedSplits().keySet());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                state.getSplitCompletedOffsets().keySet());
        Assertions.assertFalse(splitAssigner.waitingForCompletedSplits());
        Assertions.assertEquals(
                CdcSnapshotAssignmentStatus.COMPLETED,
                splitAssigner
                        .getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getSnapshotAssignmentStatus());

        splitAssigner.notifyCheckpointComplete(11L);
        Assertions.assertTrue(splitAssigner.isCompleted());
    }

    @Test
    public void testAddSplitsShouldRestoreFinishedSplitWithoutCompletedWatermark() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>());

        splitAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState state = splitAssigner.snapshotState(12L);
        // The split was already finished in the reader before the failover but its
        // completed-watermark was never checkpointed. The assigner must reconstruct the
        // watermark from the split itself and skip add-back, otherwise the snapshot phase
        // would never finish.
        Assertions.assertTrue(state.getRemainingSplits().isEmpty());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()), state.getAssignedSplits().keySet());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                state.getSplitCompletedOffsets().keySet());
        Assertions.assertFalse(splitAssigner.waitingForCompletedSplits());
        CdcEnumeratorProgressReport progress =
                splitAssigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
        Assertions.assertEquals(1, progress.getCompletedSplitCount().getValue());
        Assertions.assertEquals(0, progress.getRunningSplitCount().getValue());
        Assertions.assertTrue(progress.getActiveSplits().isEmpty());
    }

    @Test
    public void testRestoreAfterCheckpointedCompletionShouldKeepFinishedSplitOutOfReplayQueue() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.2");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);
        SnapshotSplitAssigner<?> runningAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>());

        runningAssigner.onCompletedSplits(
                Collections.singletonList(createWatermark(finishedSplit)));
        SnapshotPhaseState checkpointState = runningAssigner.snapshotState(13L);

        SnapshotSplitAssigner<?> restoredAssigner =
                createRestoredSnapshotSplitAssigner(
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());

        restoredAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState restoredState = restoredAssigner.snapshotState(14L);
        Assertions.assertTrue(restoredState.getRemainingSplits().isEmpty());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                restoredState.getAssignedSplits().keySet());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                restoredState.getSplitCompletedOffsets().keySet());
        Assertions.assertFalse(restoredAssigner.waitingForCompletedSplits());
    }

    @Test
    public void testProgressUsesWatermarksFromTheSameActiveSplit() {
        SnapshotSplit completedSplit = createFinishedSnapshotSplit("db1.table1.1", 1L, 2L);
        SnapshotSplit activeSplit = createFinishedSnapshotSplit("db1.table1.2", 10L, 20L);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(activeSplit.splitId(), activeSplit);
        assignedSplits.put(completedSplit.splitId(), completedSplit);
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        completedOffsets.put(completedSplit.splitId(), createWatermark(completedSplit));
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.singletonList(
                                createFinishedSnapshotSplit("db1.table1.3", 30L, 40L)),
                        assignedSplits,
                        completedOffsets,
                        false,
                        Arrays.asList(TableId.parse("db1.table2"), TableId.parse("db1.table3")),
                        false,
                        true);
        SplitAssigner.Context<?> context =
                new SplitAssigner.Context<>(
                        null,
                        Collections.singleton(TableId.parse("db1.table1")),
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());
        SnapshotSplitAssigner<?> splitAssigner =
                new SnapshotSplitAssigner<>(context, 10, checkpointState, null);

        CdcEnumeratorProgressReport report =
                splitAssigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");

        Assertions.assertEquals(2, report.getAssignedSplitCount().getValue());
        Assertions.assertEquals(1, report.getCompletedSplitCount().getValue());
        Assertions.assertEquals(1, report.getRunningSplitCount().getValue());
        Assertions.assertEquals(1, report.getPreparedRemainingSplitCount().getValue());
        Assertions.assertEquals(2, report.getRemainingUnchunkedTableCount().getValue());
        Assertions.assertEquals(
                CdcSnapshotAssignmentStatus.DISCOVERING, report.getSnapshotAssignmentStatus());
        Assertions.assertEquals(1, report.getActiveSplits().size());
        CdcSnapshotSplitProgress activeProgress = report.getActiveSplits().get(0);
        Assertions.assertEquals(activeSplit.splitId(), activeProgress.getSplitId());
        Assertions.assertEquals(
                "10", activeProgress.getLowWatermark().getValue().getValues().get("pos"));
        Assertions.assertEquals(
                "20", activeProgress.getHighWatermark().getValue().getValues().get("pos"));
    }

    @Test
    public void testCompletedSplitIsRemovedFromActiveProgressAfterRestore() {
        SnapshotSplit activeSplit = createFinishedSnapshotSplit("db1.table1.active", 10L, 20L);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(activeSplit.splitId(), activeSplit);
        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>());

        Assertions.assertEquals(
                1,
                splitAssigner
                        .getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getActiveSplits()
                        .size());

        splitAssigner.onCompletedSplits(Collections.singletonList(createWatermark(activeSplit)));

        CdcEnumeratorProgressReport completedReport =
                splitAssigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");
        Assertions.assertEquals(0, completedReport.getRunningSplitCount().getValue());
        Assertions.assertTrue(completedReport.getActiveSplits().isEmpty());

        SnapshotPhaseState checkpointState = splitAssigner.snapshotState(15L);
        SnapshotSplitAssigner<?> restoredAssigner =
                createRestoredSnapshotSplitAssigner(
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());
        Assertions.assertTrue(
                restoredAssigner
                        .getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getActiveSplits()
                        .isEmpty());
    }

    @Test
    public void testProgressKeepsOnlyActiveSplitsForLargeCompletedHistory() {
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            SnapshotSplit completedSplit = createFinishedSnapshotSplit("db1.table1.completed-" + i);
            assignedSplits.put(completedSplit.splitId(), completedSplit);
            completedOffsets.put(completedSplit.splitId(), createWatermark(completedSplit));
        }
        SnapshotSplit activeSplit = createFinishedSnapshotSplit("db1.table1.active");
        assignedSplits.put(activeSplit.splitId(), activeSplit);
        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, completedOffsets);

        CdcEnumeratorProgressReport report =
                splitAssigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG");

        Assertions.assertEquals(10_001, report.getAssignedSplitCount().getValue());
        Assertions.assertEquals(10_000, report.getCompletedSplitCount().getValue());
        Assertions.assertEquals(1, report.getRunningSplitCount().getValue());
        Assertions.assertEquals(1, report.getActiveSplits().size());
        Assertions.assertEquals(
                activeSplit.splitId(), report.getActiveSplits().get(0).getSplitId());
    }

    @Test
    public void testSingleParallelismShouldCompleteImmediatelyWhenAllSplitsCompleted() {
        // A single-reader batch job may run with checkpointing disabled (no
        // 'checkpoint.interval' in env), so the assigner can never rely on
        // notifyCheckpointComplete to flip the completed flag. It must complete immediately
        // once all snapshot splits have reported their watermarks, otherwise such jobs hang
        // forever between the snapshot and incremental phases.
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>(), 1);

        Assertions.assertFalse(splitAssigner.isCompleted());

        splitAssigner.onCompletedSplits(Collections.singletonList(createWatermark(finishedSplit)));

        Assertions.assertTrue(splitAssigner.isCompleted());
        Assertions.assertFalse(splitAssigner.waitingForCompletedSplits());
    }

    @Test
    public void testMultiParallelismShouldWaitForCheckpointBeforeCompleted() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>(), 10);

        splitAssigner.onCompletedSplits(Collections.singletonList(createWatermark(finishedSplit)));

        // multi-parallelism jobs must not complete before a checkpoint pinned the completion
        // state, otherwise incremental splits could overtake snapshot records of the same key
        Assertions.assertFalse(splitAssigner.isCompleted());

        splitAssigner.snapshotState(21L);
        Assertions.assertFalse(splitAssigner.isCompleted());

        splitAssigner.notifyCheckpointComplete(20L);
        Assertions.assertFalse(splitAssigner.isCompleted());

        splitAssigner.notifyCheckpointComplete(21L);
        Assertions.assertTrue(splitAssigner.isCompleted());
    }

    private SnapshotSplitAssigner<?> createRestoredSnapshotSplitAssigner(
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, SnapshotSplitWatermark> completedOffsets) {
        return createRestoredSnapshotSplitAssigner(assignedSplits, completedOffsets, 10);
    }

    private SnapshotSplitAssigner<?> createRestoredSnapshotSplitAssigner(
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, SnapshotSplitWatermark> completedOffsets,
            int currentParallelism) {
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        assignedSplits,
                        completedOffsets,
                        false,
                        Collections.emptyList(),
                        false,
                        true);
        SplitAssigner.Context<?> context =
                new SplitAssigner.Context<>(
                        null,
                        Collections.singleton(TableId.parse("db1.table1")),
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());
        return new SnapshotSplitAssigner<>(context, currentParallelism, checkpointState, null);
    }

    private SnapshotSplit createFinishedSnapshotSplit(String splitId) {
        return createFinishedSnapshotSplit(splitId, 1L, 2L);
    }

    private SnapshotSplit createFinishedSnapshotSplit(
            String splitId, long lowWatermark, long highWatermark) {
        return new SnapshotSplit(
                splitId,
                TableId.parse("db1.table1"),
                null,
                null,
                null,
                new TestOffset(lowWatermark),
                new TestOffset(highWatermark));
    }

    private SnapshotSplitWatermark createWatermark(SnapshotSplit finishedSplit) {
        return new SnapshotSplitWatermark(
                finishedSplit.splitId(),
                finishedSplit.getLowWatermark(),
                finishedSplit.getHighWatermark());
    }

    private static final class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = new HashMap<>(Collections.singletonMap("pos", String.valueOf(value)));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
