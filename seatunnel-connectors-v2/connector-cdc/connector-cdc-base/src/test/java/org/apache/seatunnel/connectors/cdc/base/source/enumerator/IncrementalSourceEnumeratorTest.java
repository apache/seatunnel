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
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.translation.source.CoordinatedSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    @Test
    public void
            shouldTransitionToIncrementalPhaseAfterRestoringFinishedLastSnapshotSplitWhenNotExactlyOnce()
                    throws Exception {
        InMemoryCdcRestoreTestFixture.SerializedCheckpointSnapshot snapshot =
                InMemoryCdcRestoreTestFixture.captureUnackedTailSplitCheckpoint(false, 2);

        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> restoreSource =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, false, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer =
                restoreSource.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = restoreSource.getSplitSerializer();

        PendingSplitsState restoredEnumState =
                enumSerializer.deserialize(snapshot.getEnumeratorStateBytes());
        Assertions.assertInstanceOf(HybridPendingSplitsState.class, restoredEnumState);
        SnapshotPhaseState beforeRestore =
                ((HybridPendingSplitsState) restoredEnumState).getSnapshotPhaseState();
        Assertions.assertFalse(beforeRestore.isAssignerCompleted());
        Assertions.assertTrue(
                beforeRestore
                        .getAssignedSplits()
                        .containsKey(InMemoryCdcRestoreTestFixture.SPLIT_1_ID));
        Assertions.assertFalse(
                beforeRestore
                        .getSplitCompletedOffsets()
                        .containsKey(InMemoryCdcRestoreTestFixture.SPLIT_1_ID));

        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                restoreSource.restoreEnumerator(context, restoredEnumState);
        enumerator.open();
        try {
            for (int subtask = 0; subtask < 2; subtask++) {
                List<SourceSplitBase> splits = new ArrayList<>();
                for (byte[] raw : snapshot.getReaderSplitBytes().get(subtask)) {
                    splits.add(splitSerializer.deserialize(raw));
                }
                if (!splits.isEmpty()) {
                    enumerator.addSplitsBack(splits, subtask);
                }
            }

            enumerator.run();

            // Multi-parallelism restore must wait for an aligned checkpoint completion before
            // marking the snapshot phase completed or dispatching the incremental split.
            SnapshotPhaseState afterAddBack =
                    ((HybridPendingSplitsState) enumerator.snapshotState(2L))
                            .getSnapshotPhaseState();
            Assertions.assertFalse(afterAddBack.isAssignerCompleted());
            Assertions.assertTrue(context.assignedSplits.isEmpty());

            enumerator.notifyCheckpointComplete(2L);
            enumerator.handleSplitRequest(0);

            Assertions.assertEquals(1, context.assignedSplits.size());
            Assertions.assertTrue(context.assignedSplits.get(0).isIncrementalSplit());
            IncrementalSplit incrementalSplit = context.assignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals(
                    InMemoryCdcRestoreTestFixture.SPLIT_1_LOW_WATERMARK,
                    InMemoryCdcRestoreTestFixture.offsetPos(incrementalSplit.getStartupOffset()));
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void
            shouldTransitionToIncrementalPhaseAfterRestoringFinishedLastSnapshotSplitWhenExactlyOnce()
                    throws Exception {
        InMemoryCdcRestoreTestFixture.SerializedCheckpointSnapshot snapshot =
                InMemoryCdcRestoreTestFixture.captureUnackedTailSplitCheckpoint(true, 2);

        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> restoreSource =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, true, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer =
                restoreSource.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = restoreSource.getSplitSerializer();

        PendingSplitsState restoredEnumState =
                enumSerializer.deserialize(snapshot.getEnumeratorStateBytes());
        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                restoreSource.restoreEnumerator(context, restoredEnumState);
        enumerator.open();
        try {
            for (int subtask = 0; subtask < 2; subtask++) {
                List<SourceSplitBase> splits = new ArrayList<>();
                for (byte[] raw : snapshot.getReaderSplitBytes().get(subtask)) {
                    splits.add(splitSerializer.deserialize(raw));
                }
                if (!splits.isEmpty()) {
                    enumerator.addSplitsBack(splits, subtask);
                }
            }

            enumerator.run();
            SnapshotPhaseState afterAddBack =
                    ((HybridPendingSplitsState) enumerator.snapshotState(2L))
                            .getSnapshotPhaseState();
            Assertions.assertFalse(afterAddBack.isAssignerCompleted());
            Assertions.assertTrue(context.assignedSplits.isEmpty());

            enumerator.notifyCheckpointComplete(2L);
            enumerator.handleSplitRequest(0);

            Assertions.assertEquals(1, context.assignedSplits.size());
            IncrementalSplit incrementalSplit = context.assignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals(
                    InMemoryCdcRestoreTestFixture.SPLIT_1_HIGH_WATERMARK,
                    InMemoryCdcRestoreTestFixture.offsetPos(incrementalSplit.getStartupOffset()));
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void shouldSelectMinimumHighWatermarkAfterRestoringFinishedSnapshotSplitWhenExactlyOnce()
            throws Exception {
        InMemoryCdcRestoreTestFixture.SerializedCheckpointSnapshot snapshot =
                InMemoryCdcRestoreTestFixture.captureUnackedTailSplitCheckpoint(true, 1);

        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> restoreSource =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, true, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer =
                restoreSource.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = restoreSource.getSplitSerializer();

        PendingSplitsState restoredEnumState =
                enumSerializer.deserialize(snapshot.getEnumeratorStateBytes());
        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                restoreSource.restoreEnumerator(context, restoredEnumState);
        enumerator.open();
        try {
            List<SourceSplitBase> splits = new ArrayList<>();
            for (byte[] raw : snapshot.getReaderSplitBytes().get(0)) {
                splits.add(splitSerializer.deserialize(raw));
            }
            enumerator.addSplitsBack(splits, 0);
            enumerator.run();
            enumerator.handleSplitRequest(0);

            // Parallelism == 1 transitions immediately without waiting for another checkpoint.
            Assertions.assertEquals(1, context.assignedSplits.size());
            IncrementalSplit incrementalSplit = context.assignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals(
                    InMemoryCdcRestoreTestFixture.SPLIT_1_HIGH_WATERMARK,
                    InMemoryCdcRestoreTestFixture.offsetPos(incrementalSplit.getStartupOffset()));
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void shouldReassignUnfinishedRestoredSnapshotSplitWhenRunningAndTransitionCleanly()
            throws Exception {
        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> source =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, false, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        SnapshotSplit split0 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_0_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {1L},
                        new Object[] {100L},
                        InMemoryCdcRestoreTestFixture.offset(100L),
                        InMemoryCdcRestoreTestFixture.offset(120L));
        SnapshotSplit unfinishedSplit1 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {100L},
                        null);

        Map<String, SnapshotSplit> assigned = new HashMap<>();
        assigned.put(split0.splitId(), split0);
        assigned.put(unfinishedSplit1.splitId(), unfinishedSplit1);
        Map<String, SnapshotSplitWatermark> completed = new HashMap<>();
        completed.put(
                split0.splitId(),
                new SnapshotSplitWatermark(
                        split0.splitId(),
                        InMemoryCdcRestoreTestFixture.offset(100L),
                        InMemoryCdcRestoreTestFixture.offset(120L)));

        HybridPendingSplitsState state =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                assigned,
                                completed,
                                false,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());

        PendingSplitsState restoredEnumState =
                enumSerializer.deserialize(enumSerializer.serialize(state));
        SourceSplitBase restoredUnfinishedSplit =
                splitSerializer.deserialize(splitSerializer.serialize(unfinishedSplit1));

        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.restoreEnumerator(context, restoredEnumState);
        enumerator.open();
        try {
            enumerator.run();
            // Reader 1 requests a split before late addSplitsBack arrives (#11677 scenario)
            enumerator.handleSplitRequest(1);
            Assertions.assertTrue(context.assignedSplits.isEmpty());

            enumerator.addSplitsBack(Collections.singletonList(restoredUnfinishedSplit), 1);
            Assertions.assertEquals(1, context.assignedSplits.size());
            Assertions.assertEquals(
                    unfinishedSplit1.splitId(), context.assignedSplits.get(0).splitId());

            CompletedSnapshotSplitsReportEvent report = new CompletedSnapshotSplitsReportEvent();
            report.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    unfinishedSplit1.splitId(),
                                    InMemoryCdcRestoreTestFixture.offset(92L),
                                    InMemoryCdcRestoreTestFixture.offset(108L))));
            enumerator.handleSourceEvent(1, report);

            enumerator.snapshotState(2L);
            enumerator.notifyCheckpointComplete(2L);
            enumerator.handleSplitRequest(0);

            Assertions.assertEquals(2, context.assignedSplits.size());
            IncrementalSplit incrementalSplit = context.assignedSplits.get(1).asIncrementalSplit();
            Assertions.assertEquals(
                    92L,
                    InMemoryCdcRestoreTestFixture.offsetPos(incrementalSplit.getStartupOffset()));
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void shouldWaitBeforeSnapshotCompletionWhenRestoredWatermarkNotYetReported()
            throws Exception {
        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> source =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, false, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();

        SnapshotSplit split0 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_0_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {1L},
                        new Object[] {100L});
        Map<String, SnapshotSplit> assigned = Collections.singletonMap(split0.splitId(), split0);

        HybridPendingSplitsState state =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                assigned,
                                Collections.emptyMap(),
                                false,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());

        PendingSplitsState restored = enumSerializer.deserialize(enumSerializer.serialize(state));
        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.restoreEnumerator(context, restored);
        enumerator.open();
        try {
            enumerator.run();
            enumerator.handleSplitRequest(0);
            Assertions.assertTrue(context.assignedSplits.isEmpty());
            Assertions.assertTrue(context.noMoreSplitsSignaled.isEmpty());
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void shouldFailWithActionableErrorWhenCompletedSnapshotStateMissesSplitWatermark()
            throws Exception {
        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> source =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, false, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();

        SnapshotSplit split0 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_0_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {1L},
                        new Object[] {100L});
        SnapshotSplit split1 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {100L},
                        null);
        Map<String, SnapshotSplit> assigned = new HashMap<>();
        assigned.put(split0.splitId(), split0);
        assigned.put(split1.splitId(), split1);
        Map<String, SnapshotSplitWatermark> completed = new HashMap<>();
        completed.put(
                split0.splitId(),
                new SnapshotSplitWatermark(
                        split0.splitId(),
                        InMemoryCdcRestoreTestFixture.offset(100L),
                        InMemoryCdcRestoreTestFixture.offset(120L)));

        HybridPendingSplitsState malformedCompletedState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                assigned,
                                completed,
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());

        PendingSplitsState restored =
                enumSerializer.deserialize(enumSerializer.serialize(malformedCompletedState));
        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.restoreEnumerator(context, restored);
        enumerator.open();
        try {
            enumerator.run();
            IllegalStateException ex =
                    Assertions.assertThrows(
                            IllegalStateException.class, () -> enumerator.handleSplitRequest(0));
            Assertions.assertTrue(ex.getMessage().contains(TABLE_ID.toString()), ex.getMessage());
            Assertions.assertTrue(ex.getMessage().contains(split1.splitId()), ex.getMessage());
            Assertions.assertTrue(
                    ex.getMessage()
                            .contains(
                                    "submit a new job or restore from another complete checkpoint"),
                    ex.getMessage());
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void
            shouldFailWithActionableErrorInsteadOfFallingBackToStartupOffsetWhenWatermarkOffsetIsNull()
                    throws Exception {
        for (boolean exactlyOnce : new boolean[] {false, true}) {
            CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
            IncrementalSource<SeaTunnelRow, SourceConfig> source =
                    InMemoryCdcRestoreTestFixture.createSource(
                            null, table, exactlyOnce, StopMode.NEVER, false);
            Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();

            SnapshotSplit split1 =
                    new SnapshotSplit(
                            InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                            TABLE_ID,
                            InMemoryCdcRestoreTestFixture.ROW_TYPE,
                            new Object[] {100L},
                            null);
            Map<String, SnapshotSplit> assigned =
                    Collections.singletonMap(split1.splitId(), split1);
            SnapshotSplitWatermark brokenWatermark =
                    exactlyOnce
                            ? new SnapshotSplitWatermark(
                                    split1.splitId(),
                                    InMemoryCdcRestoreTestFixture.offset(95L),
                                    null)
                            : new SnapshotSplitWatermark(
                                    split1.splitId(),
                                    null,
                                    InMemoryCdcRestoreTestFixture.offset(108L));
            Map<String, SnapshotSplitWatermark> completed =
                    Collections.singletonMap(split1.splitId(), brokenWatermark);

            HybridPendingSplitsState malformedNullOffsetState =
                    new HybridPendingSplitsState(
                            new SnapshotPhaseState(
                                    Collections.singletonList(TABLE_ID),
                                    Collections.emptyList(),
                                    assigned,
                                    completed,
                                    true,
                                    Collections.emptyList(),
                                    false,
                                    true),
                            new IncrementalPhaseState());

            PendingSplitsState restored =
                    enumSerializer.deserialize(enumSerializer.serialize(malformedNullOffsetState));
            MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(1);
            SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                    source.restoreEnumerator(context, restored);
            enumerator.open();
            try {
                enumerator.run();
                IllegalStateException ex =
                        Assertions.assertThrows(
                                IllegalStateException.class,
                                () -> enumerator.handleSplitRequest(0));
                Assertions.assertTrue(
                        ex.getMessage().contains(TABLE_ID.toString()), ex.getMessage());
                Assertions.assertTrue(ex.getMessage().contains(split1.splitId()), ex.getMessage());
                Assertions.assertTrue(
                        ex.getMessage()
                                .contains(
                                        "submit a new job or restore from another complete checkpoint"),
                        ex.getMessage());
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    public void
            shouldFailWithActionableErrorWhenRestoredIncrementalSplitMissesCompletedSnapshotWatermark()
                    throws Exception {
        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> source =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, false, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        HybridPendingSplitsState completedState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                new ConcurrentHashMap<>(),
                                new ConcurrentHashMap<>(),
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());

        CompletedSnapshotSplitInfo brokenInfo =
                new CompletedSnapshotSplitInfo(
                        InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {100L},
                        null,
                        null);
        IncrementalSplit malformedIncrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(TABLE_ID),
                        InMemoryCdcRestoreTestFixture.offset(95L),
                        InMemoryCdcRestoreTestFixture.offset(125L),
                        Collections.singletonList(brokenInfo));

        PendingSplitsState restoredState =
                enumSerializer.deserialize(enumSerializer.serialize(completedState));
        SourceSplitBase restoredSplit =
                splitSerializer.deserialize(splitSerializer.serialize(malformedIncrementalSplit));

        MultiSubtaskEnumeratorContext context = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.restoreEnumerator(context, restoredState);
        enumerator.open();
        try {
            IllegalStateException ex =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    enumerator.addSplitsBack(
                                            Collections.singletonList(restoredSplit), 0));
            Assertions.assertTrue(ex.getMessage().contains(TABLE_ID.toString()), ex.getMessage());
            Assertions.assertTrue(
                    ex.getMessage().contains(InMemoryCdcRestoreTestFixture.SPLIT_1_ID),
                    ex.getMessage());
            Assertions.assertTrue(
                    ex.getMessage()
                            .contains(
                                    "submit a new job or restore from another complete checkpoint"),
                    ex.getMessage());
        } finally {
            enumerator.close();
        }
    }

    @Test
    public void shouldRejectNullHighWatermarkInCompletedSnapshotStateWhenSelectingLowWatermark()
            throws Exception {
        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> source =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, false, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();

        SnapshotSplit split0 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_0_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {1L},
                        new Object[] {100L});
        SnapshotSplit split1 =
                new SnapshotSplit(
                        InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {100L},
                        null);
        Map<String, SnapshotSplit> assigned = new HashMap<>();
        assigned.put(split0.splitId(), split0);
        assigned.put(split1.splitId(), split1);

        Map<String, SnapshotSplitWatermark> malformedCompleted = new HashMap<>();
        malformedCompleted.put(
                split0.splitId(),
                new SnapshotSplitWatermark(
                        split0.splitId(),
                        InMemoryCdcRestoreTestFixture.offset(100L),
                        InMemoryCdcRestoreTestFixture.offset(120L)));
        malformedCompleted.put(
                split1.splitId(),
                new SnapshotSplitWatermark(
                        split1.splitId(), InMemoryCdcRestoreTestFixture.offset(95L), null));

        HybridPendingSplitsState malformedState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                assigned,
                                malformedCompleted,
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());

        PendingSplitsState restoredMalformed =
                enumSerializer.deserialize(enumSerializer.serialize(malformedState));
        MultiSubtaskEnumeratorContext brokenContext = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> brokenEnumerator =
                source.restoreEnumerator(brokenContext, restoredMalformed);
        brokenEnumerator.open();
        try {
            brokenEnumerator.run();
            IllegalStateException ex =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> {
                                brokenEnumerator.handleSplitRequest(0);
                                Assertions.assertFalse(brokenContext.assignedSplits.isEmpty());
                                InMemoryCdcRestoreTestFixture.submitToStreamFetcherAndClose(
                                        brokenContext.assignedSplits.get(0), false);
                            });
            Assertions.assertTrue(ex.getMessage().contains(TABLE_ID.toString()), ex.getMessage());
            Assertions.assertTrue(ex.getMessage().contains(split1.splitId()), ex.getMessage());
            Assertions.assertTrue(
                    ex.getMessage()
                            .contains(
                                    "submit a new job or restore from another complete checkpoint"),
                    ex.getMessage());
            Assertions.assertTrue(brokenContext.assignedSplits.isEmpty());
        } finally {
            brokenEnumerator.close();
        }

        // Positive control: replacing only split1's null high watermark with 108 succeeds
        // end-to-end.
        Map<String, SnapshotSplitWatermark> validCompleted = new HashMap<>(malformedCompleted);
        validCompleted.put(
                split1.splitId(),
                new SnapshotSplitWatermark(
                        split1.splitId(),
                        InMemoryCdcRestoreTestFixture.offset(95L),
                        InMemoryCdcRestoreTestFixture.offset(108L)));
        HybridPendingSplitsState validState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                assigned,
                                validCompleted,
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());
        PendingSplitsState restoredValid =
                enumSerializer.deserialize(enumSerializer.serialize(validState));
        MultiSubtaskEnumeratorContext validContext = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> validEnumerator =
                source.restoreEnumerator(validContext, restoredValid);
        validEnumerator.open();
        try {
            validEnumerator.run();
            validEnumerator.handleSplitRequest(0);
            Assertions.assertEquals(1, validContext.assignedSplits.size());
            IncrementalSplit dispatched = validContext.assignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals(
                    95L, InMemoryCdcRestoreTestFixture.offsetPos(dispatched.getStartupOffset()));
            InMemoryCdcRestoreTestFixture.submitToStreamFetcherAndClose(dispatched, false);
        } finally {
            validEnumerator.close();
        }
    }

    @Test
    public void shouldRejectNullHighWatermarkWhenRestoringSerializedIncrementalSplit()
            throws Exception {
        CatalogTable table = InMemoryCdcRestoreTestFixture.createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> source =
                InMemoryCdcRestoreTestFixture.createSource(
                        null, table, true, StopMode.NEVER, false);
        Serializer<PendingSplitsState> enumSerializer = source.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        HybridPendingSplitsState completedState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                new ConcurrentHashMap<>(),
                                new ConcurrentHashMap<>(),
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        new IncrementalPhaseState());

        CompletedSnapshotSplitInfo info0 =
                new CompletedSnapshotSplitInfo(
                        InMemoryCdcRestoreTestFixture.SPLIT_0_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {1L},
                        new Object[] {100L},
                        new SnapshotSplitWatermark(
                                InMemoryCdcRestoreTestFixture.SPLIT_0_ID,
                                InMemoryCdcRestoreTestFixture.offset(100L),
                                InMemoryCdcRestoreTestFixture.offset(120L)));
        CompletedSnapshotSplitInfo brokenInfo1 =
                new CompletedSnapshotSplitInfo(
                        InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {100L},
                        null,
                        new SnapshotSplitWatermark(
                                InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                                InMemoryCdcRestoreTestFixture.offset(95L),
                                null));

        IncrementalSplit brokenIncrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(TABLE_ID),
                        InMemoryCdcRestoreTestFixture.offset(108L),
                        null,
                        Arrays.asList(info0, brokenInfo1));

        PendingSplitsState restoredBrokenState =
                enumSerializer.deserialize(enumSerializer.serialize(completedState));
        SourceSplitBase restoredBrokenSplit =
                splitSerializer.deserialize(splitSerializer.serialize(brokenIncrementalSplit));

        MultiSubtaskEnumeratorContext brokenContext = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> brokenEnumerator =
                source.restoreEnumerator(brokenContext, restoredBrokenState);
        brokenEnumerator.open();
        try {
            IllegalStateException ex =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> {
                                brokenEnumerator.addSplitsBack(
                                        Collections.singletonList(restoredBrokenSplit), 0);
                                brokenEnumerator.run();
                                brokenEnumerator.handleSplitRequest(0);
                                Assertions.assertFalse(brokenContext.assignedSplits.isEmpty());
                                InMemoryCdcRestoreTestFixture.submitToStreamFetcherAndClose(
                                        brokenContext.assignedSplits.get(0), true);
                            });
            Assertions.assertTrue(ex.getMessage().contains(TABLE_ID.toString()), ex.getMessage());
            Assertions.assertTrue(
                    ex.getMessage().contains(InMemoryCdcRestoreTestFixture.SPLIT_1_ID),
                    ex.getMessage());
            Assertions.assertTrue(
                    ex.getMessage()
                            .contains(
                                    "submit a new job or restore from another complete checkpoint"),
                    ex.getMessage());
            Assertions.assertTrue(brokenContext.assignedSplits.isEmpty());
        } finally {
            brokenEnumerator.close();
        }

        // Positive control: replacing only split1's null high watermark with 108 succeeds
        // end-to-end.
        CompletedSnapshotSplitInfo validInfo1 =
                new CompletedSnapshotSplitInfo(
                        InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                        TABLE_ID,
                        InMemoryCdcRestoreTestFixture.ROW_TYPE,
                        new Object[] {100L},
                        null,
                        new SnapshotSplitWatermark(
                                InMemoryCdcRestoreTestFixture.SPLIT_1_ID,
                                InMemoryCdcRestoreTestFixture.offset(95L),
                                InMemoryCdcRestoreTestFixture.offset(108L)));
        IncrementalSplit validIncrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(TABLE_ID),
                        InMemoryCdcRestoreTestFixture.offset(108L),
                        null,
                        Arrays.asList(info0, validInfo1));

        PendingSplitsState restoredValidState =
                enumSerializer.deserialize(enumSerializer.serialize(completedState));
        SourceSplitBase restoredValidSplit =
                splitSerializer.deserialize(splitSerializer.serialize(validIncrementalSplit));

        MultiSubtaskEnumeratorContext validContext = new MultiSubtaskEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> validEnumerator =
                source.restoreEnumerator(validContext, restoredValidState);
        validEnumerator.open();
        try {
            validEnumerator.addSplitsBack(Collections.singletonList(restoredValidSplit), 0);
            validEnumerator.run();
            validEnumerator.handleSplitRequest(0);
            Assertions.assertEquals(1, validContext.assignedSplits.size());
            IncrementalSplit dispatched = validContext.assignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals("incremental-split-0", dispatched.splitId());
            Assertions.assertEquals(
                    108L, InMemoryCdcRestoreTestFixture.offsetPos(dispatched.getStartupOffset()));
            InMemoryCdcRestoreTestFixture.submitToStreamFetcherAndClose(dispatched, true);
        } finally {
            validEnumerator.close();
        }
    }

    private static final class MultiSubtaskEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {

        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final List<SourceSplitBase> assignedSplits = new ArrayList<>();
        private final List<Integer> noMoreSplitsSignaled = new ArrayList<>();

        private MultiSubtaskEnumeratorContext(int parallelism) {
            this.parallelism = parallelism;
            this.registeredReaders =
                    IntStream.range(0, parallelism)
                            .boxed()
                            .collect(Collectors.toCollection(LinkedHashSet::new));
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
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
            assignedSplits.addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsSignaled.add(subtask);
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
        private final List<SourceSplitBase> allAssignedSplits = new ArrayList<>();

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
            allAssignedSplits.addAll(splits);
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

    private static final class TestSourceConfig implements SourceConfig {

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

    private static final class TestOffsetFactory extends OffsetFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public Offset earliest() {
            throw new UnsupportedOperationException();
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
