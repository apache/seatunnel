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

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IncrementalSplitAssignerTest {

    @Test
    void shouldReuseCheckpointedStartupOffsetAfterRestore() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset committedOffset = mock(Offset.class);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.committedOffset()).thenReturn(committedOffset);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        SplitAssigner.Context<SourceConfig> context = createContext(sourceConfig);
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(context, 1, offsetFactory);
        assertSame(
                committedOffset, assigner.getNext().get().asIncrementalSplit().getStartupOffset());

        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig), 1, offsetFactory, checkpoint);
        assertSame(
                committedOffset,
                restoredAssigner.getNext().get().asIncrementalSplit().getStartupOffset());

        verify(offsetFactory).committedOffset();
    }

    @Test
    void shouldUseTableSpecificOffsetWhenCreatingFirstIncrementalSplit() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset stopOffset = mock(Offset.class);
        Offset tableSpecificOffset = new TestOffset(100);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.neverStop()).thenReturn(stopOffset);

        TableId snapshotTable = TableId.parse("database.snapshot_table");
        TableId specificTable = TableId.parse("database.specific_table");
        SplitAssigner.Context<SourceConfig> context =
                new SplitAssigner.Context<>(
                        sourceConfig,
                        new java.util.HashSet<>(
                                java.util.Arrays.asList(snapshotTable, specificTable)),
                        new HashMap<>(),
                        new HashMap<>());
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        context,
                        1,
                        offsetFactory,
                        Collections.singletonMap(specificTable, tableSpecificOffset));

        org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit split =
                assigner.getNext().get().asIncrementalSplit();

        assertSame(tableSpecificOffset, split.getStartupOffset());
        assertSame(tableSpecificOffset, split.getTableStartOffsets().get(specificTable));
    }

    @Test
    void shouldStartTheFirstIncrementalSplitFromTheMinimumMixedBoundary() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset stopOffset = mock(Offset.class);
        TableId snapshotTable = TableId.parse("database.snapshot_table");
        TableId specificTable = TableId.parse("database.specific_table");
        Offset snapshotHighWatermark = new TestOffset(200);
        Offset specificStartOffset = new TestOffset(100);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(sourceConfig.isExactlyOnce()).thenReturn(true);
        when(offsetFactory.neverStop()).thenReturn(stopOffset);

        SnapshotSplit snapshotSplit =
                new SnapshotSplit("snapshot-0", snapshotTable, null, null, null);
        Map<String, SnapshotSplit> assignedSnapshotSplits = new HashMap<>();
        assignedSnapshotSplits.put(snapshotSplit.splitId(), snapshotSplit);
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        completedOffsets.put(
                snapshotSplit.splitId(),
                new SnapshotSplitWatermark(snapshotSplit.splitId(), null, snapshotHighWatermark));
        SplitAssigner.Context<SourceConfig> context =
                new SplitAssigner.Context<>(
                        sourceConfig,
                        new java.util.HashSet<>(
                                java.util.Arrays.asList(snapshotTable, specificTable)),
                        assignedSnapshotSplits,
                        completedOffsets);

        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        context,
                        1,
                        offsetFactory,
                        Collections.singletonMap(specificTable, specificStartOffset));

        assertSame(
                specificStartOffset,
                assigner.createIncrementalSplits(true).get(0).getStartupOffset());

        IncrementalSplitAssigner<SourceConfig> snapshotBoundaryAssigner =
                new IncrementalSplitAssigner<>(
                        context,
                        1,
                        offsetFactory,
                        Collections.singletonMap(specificTable, new TestOffset(300)));

        assertSame(
                snapshotHighWatermark,
                snapshotBoundaryAssigner.createIncrementalSplits(true).get(0).getStartupOffset());
    }

    @Test
    void shouldSkipEmptyBucketsWhenCapturedTablesAreLessThanParallelism() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.neverStop()).thenReturn(mock(Offset.class));

        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(
                                sourceConfig,
                                Collections.singleton(TableId.parse("database.schema.table"))),
                        2,
                        offsetFactory);

        Optional<IncrementalSplit> firstSplit =
                assigner.getNext().map(split -> split.asIncrementalSplit());

        assertTrue(firstSplit.isPresent());
        assertEquals(1, firstSplit.get().getTableIds().size());
        assertEquals("incremental-split-0", firstSplit.get().splitId());
        assertFalse(assigner.getNext().isPresent());
    }

    @Test
    void shouldReturnEmptyWhenNoCapturedTablesRemain() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));

        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, Collections.emptySet()), 2, offsetFactory);

        assertFalse(assigner.getNext().isPresent());
        assertTrue(assigner.noMoreSplits());
    }

    @Test
    void shouldReassignTablesRestoredFromCheckpointWithTheirWatermark() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset startupOffset = mock(Offset.class);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));

        TableId tableId = TableId.parse("database.schema.table");
        IncrementalSplit restoredSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(tableId),
                        startupOffset,
                        stoppingOffset,
                        Collections.emptyList());
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(createContext(sourceConfig), 1, offsetFactory);
        assigner.addSplits(Collections.singletonList(restoredSplit));

        Optional<IncrementalSplit> reassignedSplit =
                assigner.getNext().map(split -> split.asIncrementalSplit());

        assertTrue(reassignedSplit.isPresent());
        assertEquals(Collections.singletonList(tableId), reassignedSplit.get().getTableIds());
        assertSame(startupOffset, reassignedSplit.get().getStartupOffset());
    }

    @Test
    void shouldResolveLatestStopOffsetOnceAtSplitCreationAndReuseAfterRestore() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset committedOffset = mock(Offset.class);
        Offset resolvedOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.LATEST, null, null, null));
        when(offsetFactory.committedOffset()).thenReturn(committedOffset);
        when(offsetFactory.latest()).thenReturn(resolvedOffset);

        SplitAssigner.Context<SourceConfig> context = createContext(sourceConfig);
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(context, 1, offsetFactory);

        // The single authoritative resolution happens exactly once, when the first
        // incremental split is created; the same value is used for the split handed to
        // the reader and stored in the checkpoint, so they can never diverge.
        assertSame(resolvedOffset, assigner.getNext().get().asIncrementalSplit().getStopOffset());
        verify(offsetFactory, times(1)).latest();

        // Snapshot phase completes: no second resolution, the resolved value is reused.
        assertTrue(assigner.completedSnapshotPhase(Collections.emptyList()));
        verify(offsetFactory, times(1)).latest();

        // The checkpoint stores the resolved stop offset...
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);

        // ...and a restored assigner reuses it instead of re-resolving (no drift, and no
        // boundary change on restore: the checkpointed value is the same one the running
        // reader was already stopping at).
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig), 1, offsetFactory, checkpoint);
        assertSame(
                resolvedOffset,
                restoredAssigner.getNext().get().asIncrementalSplit().getStopOffset());
        verify(offsetFactory, times(1)).latest();
    }

    @Test
    void shouldRestoreResolvedStopOffsetFromInFlightSplitOnLegacyCheckpointRestore() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset committedOffset = mock(Offset.class);
        Offset resolvedOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.LATEST, null, null, null));
        when(offsetFactory.committedOffset()).thenReturn(committedOffset);

        // A legacy checkpoint (written before the stopOffset field existed) restores the
        // assigner with resolvedStopOffset == null.
        IncrementalPhaseState legacyCheckpoint = new IncrementalPhaseState(committedOffset);
        SplitAssigner.Context<SourceConfig> context = createContext(sourceConfig);
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(context, 1, offsetFactory, legacyCheckpoint);

        // An already-assigned incremental split is handed back to the enumerator via
        // addSplits() (e.g. a reader task failure/retry within the same job run); it still
        // carries the previously resolved stop offset.
        IncrementalSplit inFlightSplit =
                new IncrementalSplit(
                        "incremental-1",
                        Collections.singletonList(TableId.parse("database.schema.table")),
                        committedOffset,
                        resolvedOffset,
                        Collections.emptyList());
        restoredAssigner.addSplits(Collections.singletonList(inFlightSplit));

        // The assigner adopts the in-flight split's stop offset as its resolvedStopOffset,
        // so the next split created reuses it instead of re-resolving latest() (and drifting
        // the stop boundary after a restore).
        IncrementalSplit nextSplit = restoredAssigner.getNext().get().asIncrementalSplit();
        assertSame(resolvedOffset, nextSplit.getStopOffset());
        verify(offsetFactory, never()).latest();
    }

    @Test
    void shouldStartRegroupedSplitAtEarliestRestoredOffsetAndKeepLaterTableOffsets() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        TableId table1 = TableId.parse("database.schema.table1");
        TableId table2 = TableId.parse("database.schema.table2");
        Offset stoppingOffset = mock(Offset.class);

        // incremental.parallelism was 2: each table was read by its own split, and the split of
        // table2 had read further than the split of table1 when the checkpoint was taken.
        IncrementalSplit restoredSplit1 =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(table1),
                        new TestOffset(100),
                        stoppingOffset,
                        Collections.emptyList());
        IncrementalSplit restoredSplit2 =
                new IncrementalSplit(
                        "incremental-split-1",
                        Collections.singletonList(table2),
                        new TestOffset(200),
                        stoppingOffset,
                        Collections.emptyList());
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, new HashSet<>(Arrays.asList(table1, table2))),
                        1,
                        mock(OffsetFactory.class));
        assigner.addSplits(Arrays.asList(restoredSplit1, restoredSplit2));

        IncrementalSplit split = assigner.getNext().get().asIncrementalSplit();

        assertEquals(
                new HashSet<>(Arrays.asList(table1, table2)), new HashSet<>(split.getTableIds()));
        assertEquals(new TestOffset(100), split.getStartupOffset());
        assertEquals(
                Collections.singletonMap(table2, new TestOffset(200)),
                split.getTableStartupOffsets());
    }

    @Test
    void shouldKeepTableOffsetCarriedByRestoredSplit() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        TableId table1 = TableId.parse("database.schema.table1");
        TableId table2 = TableId.parse("database.schema.table2");

        // The split was checkpointed at 150 while it was still replaying up to table2's
        // restored offset 200, so it carries that offset for table2.
        IncrementalSplit restoredSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Arrays.asList(table1, table2),
                        new TestOffset(150),
                        mock(Offset.class),
                        Collections.emptyList(),
                        null,
                        Collections.emptyMap(),
                        Collections.singletonMap(table2, new TestOffset(200)));
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, new HashSet<>(Arrays.asList(table1, table2))),
                        1,
                        mock(OffsetFactory.class));
        assigner.addSplits(Collections.singletonList(restoredSplit));

        IncrementalSplit split = assigner.getNext().get().asIncrementalSplit();

        assertEquals(new TestOffset(150), split.getStartupOffset());
        assertEquals(
                Collections.singletonMap(table2, new TestOffset(200)),
                split.getTableStartupOffsets());
    }

    private SplitAssigner.Context<SourceConfig> createContext(SourceConfig sourceConfig) {
        return createContext(
                sourceConfig, Collections.singleton(TableId.parse("database.schema.table")));
    }

    private SplitAssigner.Context<SourceConfig> createContext(
            SourceConfig sourceConfig, Set<TableId> capturedTables) {
        return new SplitAssigner.Context<>(
                sourceConfig,
                new LinkedHashSet<>(capturedTables),
                new HashMap<>(),
                new HashMap<>());
    }

    private static class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private final long position;

        TestOffset(long position) {
            this.position = position;
            this.offset = Collections.singletonMap("pos", String.valueOf(position));
        }

        @Override
        public int compareTo(Offset o) {
            return Long.compare(position, ((TestOffset) o).position);
        }
    }
}
