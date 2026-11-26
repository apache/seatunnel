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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
        IncrementalSplit restoredSplit = assigner.getNext().get().asIncrementalSplit();
        assertSame(committedOffset, restoredSplit.getStartupOffset());
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig), 1, offsetFactory, checkpoint);
        restoredAssigner.addSplits(Collections.singletonList(restoredSplit));
        assertSame(
                committedOffset,
                restoredAssigner.getNext().get().asIncrementalSplit().getStartupOffset());

        verify(offsetFactory).committedOffset();
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
        IncrementalSplit restoredSplit =
                assigner.getNext().map(split -> split.asIncrementalSplit()).get();
        assertSame(resolvedOffset, restoredSplit.getStopOffset());
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
        restoredAssigner.addSplits(Collections.singletonList(restoredSplit));
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

    @Test
    void shouldNotRecreateIncrementalSplitsWhenRestoredReadersHoldThem() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));

        TableId tableId = TableId.parse("database.schema.table");
        SplitAssigner.Context<SourceConfig> context =
                createContext(sourceConfig, Collections.singleton(tableId));
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(context, 1, offsetFactory);
        assigner.getNext();

        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, Collections.singleton(tableId)),
                        1,
                        offsetFactory,
                        checkpoint);

        assertFalse(restoredAssigner.getNext().isPresent());
        assertTrue(restoredAssigner.noMoreSplits());
        assertFalse(restoredAssigner.waitingForAssignedSplits());
    }

    @Test
    void shouldRestoreGeneratedSplitsThatWereNotAssignedBeforeCheckpoint() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset startupOffset = mock(Offset.class);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.committedOffset()).thenReturn(startupOffset);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        TableId table1 = TableId.parse("database.schema.table1");
        TableId table2 = TableId.parse("database.schema.table2");
        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(table1, table2));
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory);

        IncrementalSplit assignedSplit = assigner.getNext().get().asIncrementalSplit();
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);

        assertTrue(checkpoint.isSplitAssigned());
        assertEquals(1, checkpoint.getRemainingSplits().size());

        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory, checkpoint);
        IncrementalSplit restoredRemainingSplit =
                restoredAssigner.getNext().get().asIncrementalSplit();
        Set<TableId> restoredTables = new HashSet<>();
        restoredTables.addAll(assignedSplit.getTableIds());
        restoredTables.addAll(restoredRemainingSplit.getTableIds());

        assertEquals(capturedTables, restoredTables);
        assertFalse(restoredAssigner.getNext().isPresent());
    }

    @Test
    void shouldOnlyReassignReturnedTableOnPartialFailoverAfterRestore() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset startupOffset = new TestOffset(10L);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.committedOffset()).thenReturn(startupOffset);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        TableId customers = TableId.parse("database.schema.customers");
        TableId orders = TableId.parse("database.schema.orders");
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(customers, orders));

        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory);
        IncrementalSplit split0 = assigner.getNext().get().asIncrementalSplit();
        IncrementalSplit split1 = assigner.getNext().get().asIncrementalSplit();
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);

        // After Flink restore, both readers hold their own splits locally and enumerator has
        // splitAssigned=true with empty remainingSplits/assignedSplits. If only reader 0 fails
        // and returns its split via addSplitsBack, only that split's table may be reassigned.
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory, checkpoint);
        IncrementalSplit failedSplitWithNewerOffset =
                new IncrementalSplit(
                        split0.splitId(),
                        split0.getTableIds(),
                        new TestOffset(250L),
                        stoppingOffset,
                        Collections.emptyList());
        restoredAssigner.addSplits(Collections.singletonList(failedSplitWithNewerOffset));

        IncrementalSplit reassigned = restoredAssigner.getNext().get().asIncrementalSplit();
        assertEquals(split0.getTableIds(), reassigned.getTableIds());
        assertEquals(new TestOffset(250L), reassigned.getStartupOffset());
        assertFalse(restoredAssigner.getNext().isPresent());
        assertFalse(reassigned.getTableIds().containsAll(split1.getTableIds()));
    }

    @Test
    void shouldRestoreBothRemainingAndReturnedSplitsWithoutCollisionOrLoss() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset initialOffset = new TestOffset(10L);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.committedOffset()).thenReturn(initialOffset);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        TableId customers = TableId.parse("database.schema.customers");
        TableId orders = TableId.parse("database.schema.orders");
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(customers, orders));

        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory);
        IncrementalSplit firstAssigned = assigner.getNext().get().asIncrementalSplit();
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);

        // Zeta restore: reader 0 returns its advanced split while the unassigned split stays in
        // checkpoint.getRemainingSplits().
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory, checkpoint);
        IncrementalSplit returnedAdvanced =
                new IncrementalSplit(
                        firstAssigned.splitId(),
                        firstAssigned.getTableIds(),
                        new TestOffset(500L),
                        stoppingOffset,
                        Collections.emptyList());
        restoredAssigner.addSplits(Collections.singletonList(returnedAdvanced));

        IncrementalSplit splitA = restoredAssigner.getNext().get().asIncrementalSplit();
        // Even a checkpoint taken after assigning only one of the two splits must keep the other.
        IncrementalPhaseState midRestoreCheckpoint = restoredAssigner.snapshotState(2L);
        assertEquals(1, midRestoreCheckpoint.getRemainingSplits().size());

        IncrementalSplit splitB = restoredAssigner.getNext().get().asIncrementalSplit();
        assertFalse(restoredAssigner.getNext().isPresent());
        assertFalse(splitA.splitId().equals(splitB.splitId()));

        Set<TableId> allRestoredTables = new HashSet<>();
        allRestoredTables.addAll(splitA.getTableIds());
        allRestoredTables.addAll(splitB.getTableIds());
        assertEquals(capturedTables, allRestoredTables);

        IncrementalSplit advancedRecreated =
                splitA.getTableIds().equals(firstAssigned.getTableIds()) ? splitA : splitB;
        IncrementalSplit untouchedRemaining =
                splitA.getTableIds().equals(firstAssigned.getTableIds()) ? splitB : splitA;
        assertEquals(new TestOffset(500L), advancedRecreated.getStartupOffset());
        assertEquals(initialOffset, untouchedRemaining.getStartupOffset());
    }

    @Test
    void shouldAssignAddedTableOnRestoreAfterReturnedSplit() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(sourceConfig.isExactlyOnce()).thenReturn(true);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        TableId existingTable = TableId.parse("database.schema.table1");
        TableId addedTable = TableId.parse("database.schema.table2");

        // Initial run with only existingTable assigned.
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, Collections.singleton(existingTable)),
                        1,
                        offsetFactory);
        assigner.getNext();
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);
        assertTrue(checkpoint.isSplitAssigned());
        assertEquals(Collections.singleton(existingTable), checkpoint.getAssignedTables());

        // Restore with addedTable configured and snapshotted at offset 750, while Zeta returns
        // existingTable's split at offset 500.
        Set<TableId> restoredTables = new LinkedHashSet<>(Arrays.asList(existingTable, addedTable));
        SplitAssigner.Context<SourceConfig> restoredContext =
                createContext(sourceConfig, restoredTables);
        SnapshotSplit addedSnapshotSplit =
                new SnapshotSplit("database.schema.table2:0", addedTable, null, null, null);
        restoredContext
                .getAssignedSnapshotSplit()
                .put(addedSnapshotSplit.splitId(), addedSnapshotSplit);
        restoredContext
                .getSplitCompletedOffsets()
                .put(
                        addedSnapshotSplit.splitId(),
                        new SnapshotSplitWatermark(
                                addedSnapshotSplit.splitId(),
                                new TestOffset(700L),
                                new TestOffset(750L)));

        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(restoredContext, 2, offsetFactory, checkpoint);
        IncrementalSplit returnedExistingSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(existingTable),
                        new TestOffset(500L),
                        stoppingOffset,
                        Collections.emptyList());
        restoredAssigner.addSplits(Collections.singletonList(returnedExistingSplit));

        assertFalse(restoredAssigner.noMoreSplits());
        IncrementalSplit split0 = restoredAssigner.getNext().get().asIncrementalSplit();
        IncrementalSplit split1 = restoredAssigner.getNext().get().asIncrementalSplit();
        assertFalse(restoredAssigner.getNext().isPresent());
        assertTrue(restoredAssigner.noMoreSplits());

        Set<TableId> assignedAfterRestore = new HashSet<>();
        assignedAfterRestore.addAll(split0.getTableIds());
        assignedAfterRestore.addAll(split1.getTableIds());
        assertEquals(restoredTables, assignedAfterRestore);

        IncrementalSplit existingAfterRestore =
                split0.getTableIds().contains(existingTable) ? split0 : split1;
        IncrementalSplit addedAfterRestore =
                split0.getTableIds().contains(addedTable) ? split0 : split1;
        assertEquals(new TestOffset(500L), existingAfterRestore.getStartupOffset());
        assertEquals(new TestOffset(750L), addedAfterRestore.getStartupOffset());
        assertEquals(1, addedAfterRestore.getCompletedSnapshotSplitInfos().size());
        assertEquals(
                addedTable, addedAfterRestore.getCompletedSnapshotSplitInfos().get(0).getTableId());
    }

    @Test
    void shouldAssignOnlyAddedTableWhenExistingReadersKeepTheirSplitsOnRestore() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset startupOffset = new TestOffset(100L);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.LATEST, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(offsetFactory.latest()).thenReturn(startupOffset);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        TableId existingTable = TableId.parse("database.schema.table1");
        TableId addedTable = TableId.parse("database.schema.table2");

        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, Collections.singleton(existingTable)),
                        1,
                        offsetFactory);
        assigner.getNext();
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);

        // Restore with existingTable still held by its reader (no addSplitsBack) and addedTable
        // added to configuration. Only addedTable should get a new incremental split.
        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(
                                sourceConfig,
                                new LinkedHashSet<>(Arrays.asList(existingTable, addedTable))),
                        1,
                        offsetFactory,
                        checkpoint);

        assertFalse(restoredAssigner.noMoreSplits());
        IncrementalSplit addedSplit = restoredAssigner.getNext().get().asIncrementalSplit();
        assertEquals(Collections.singletonList(addedTable), addedSplit.getTableIds());
        assertFalse(restoredAssigner.getNext().isPresent());
        assertTrue(restoredAssigner.noMoreSplits());
    }

    @Test
    void shouldPruneRemovedTablesAndDropEmptySplitsFromRestoredRemainingSplits() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));

        TableId keptTable = TableId.parse("database.schema.kept");
        TableId removedTable = TableId.parse("database.schema.removed");
        TableId removedOnlyTable = TableId.parse("database.schema.removed_only");

        Map<TableId, byte[]> mixedHistory = new HashMap<>();
        mixedHistory.put(keptTable, new byte[] {1});
        mixedHistory.put(removedTable, new byte[] {2});
        Map<TableId, Offset> mixedTableOffsets = new HashMap<>();
        mixedTableOffsets.put(keptTable, new TestOffset(200L));
        mixedTableOffsets.put(removedTable, new TestOffset(300L));
        IncrementalSplit mixedSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Arrays.asList(keptTable, removedTable),
                        new TestOffset(100L),
                        stoppingOffset,
                        Arrays.asList(
                                new CompletedSnapshotSplitInfo(
                                        "kept:0",
                                        keptTable,
                                        null,
                                        null,
                                        null,
                                        new SnapshotSplitWatermark(
                                                "kept:0",
                                                new TestOffset(50L),
                                                new TestOffset(60L))),
                                new CompletedSnapshotSplitInfo(
                                        "removed:0",
                                        removedTable,
                                        null,
                                        null,
                                        null,
                                        new SnapshotSplitWatermark(
                                                "removed:0",
                                                new TestOffset(50L),
                                                new TestOffset(60L)))),
                        null,
                        mixedHistory,
                        mixedTableOffsets);
        IncrementalSplit removedOnlySplit =
                new IncrementalSplit(
                        "incremental-split-1",
                        Collections.singletonList(removedOnlyTable),
                        new TestOffset(100L),
                        stoppingOffset,
                        Collections.emptyList());

        IncrementalPhaseState checkpoint =
                new IncrementalPhaseState(
                        new TestOffset(100L),
                        null,
                        true,
                        Arrays.asList(mixedSplit, removedOnlySplit),
                        new LinkedHashSet<>(
                                Arrays.asList(keptTable, removedTable, removedOnlyTable)));

        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, Collections.singleton(keptTable)),
                        2,
                        offsetFactory,
                        checkpoint);

        IncrementalSplit assigned = restoredAssigner.getNext().get().asIncrementalSplit();
        assertEquals("incremental-split-0", assigned.splitId());
        assertEquals(Collections.singletonList(keptTable), assigned.getTableIds());
        assertEquals(1, assigned.getCompletedSnapshotSplitInfos().size());
        assertEquals(keptTable, assigned.getCompletedSnapshotSplitInfos().get(0).getTableId());
        assertEquals(Collections.singleton(keptTable), assigned.getHistoryTableChanges().keySet());
        assertEquals(
                Collections.singletonMap(keptTable, new TestOffset(200L)),
                assigned.getTableStartupOffsets());
        assertFalse(restoredAssigner.getNext().isPresent());
        assertTrue(restoredAssigner.noMoreSplits());
    }

    @Test
    void shouldFitReturnedAndAddedTablesWithinIncrementalParallelismWhenRemainingSplitExists() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset initialOffset = new TestOffset(10L);
        Offset stoppingOffset = mock(Offset.class);
        when(sourceConfig.getStartupConfig())
                .thenReturn(new StartupConfig(StartupMode.INITIAL, null, null, null));
        when(sourceConfig.getStopConfig())
                .thenReturn(new StopConfig(StopMode.NEVER, null, null, null));
        when(sourceConfig.isExactlyOnce()).thenReturn(true);
        when(offsetFactory.neverStop()).thenReturn(stoppingOffset);

        TableId t1 = TableId.parse("database.schema.t1");
        TableId t2 = TableId.parse("database.schema.t2");
        TableId t3 = TableId.parse("database.schema.t3");
        TableId t4 = TableId.parse("database.schema.t4");
        TableId t5 = TableId.parse("database.schema.t5");
        Set<TableId> initialTables = new LinkedHashSet<>(Arrays.asList(t1, t2, t3, t4));

        // Initial run with incremental.parallelism=2 and 4 tables; only 1 split is handed out
        // before savepoint, leaving the 2nd split in checkpoint.getRemainingSplits().
        SplitAssigner.Context<SourceConfig> initialContext =
                createContext(sourceConfig, initialTables);
        for (TableId table : initialTables) {
            SnapshotSplit snapshot = new SnapshotSplit(table + ":0", table, null, null, null);
            initialContext.getAssignedSnapshotSplit().put(snapshot.splitId(), snapshot);
            initialContext
                    .getSplitCompletedOffsets()
                    .put(
                            snapshot.splitId(),
                            new SnapshotSplitWatermark(
                                    snapshot.splitId(), initialOffset, initialOffset));
        }
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(initialContext, 2, offsetFactory);
        IncrementalSplit deliveredSplit = assigner.getNext().get().asIncrementalSplit();
        IncrementalPhaseState checkpoint = assigner.snapshotState(1L);
        assertEquals(1, checkpoint.getRemainingSplits().size());

        // Restore with t5 added and snapshotted at offset 200, while reader 0 returns its split at
        // offset 100. Total splits handed out must still be 2 (= incremental.parallelism), covering
        // all 5 tables with no orphaned 3rd split.
        Set<TableId> restoredTables = new LinkedHashSet<>(Arrays.asList(t1, t2, t3, t4, t5));
        SplitAssigner.Context<SourceConfig> restoredContext =
                createContext(sourceConfig, restoredTables);
        SnapshotSplit addedSnapshot = new SnapshotSplit(t5 + ":0", t5, null, null, null);
        restoredContext.getAssignedSnapshotSplit().put(addedSnapshot.splitId(), addedSnapshot);
        restoredContext
                .getSplitCompletedOffsets()
                .put(
                        addedSnapshot.splitId(),
                        new SnapshotSplitWatermark(
                                addedSnapshot.splitId(),
                                new TestOffset(190L),
                                new TestOffset(200L)));

        IncrementalSplitAssigner<SourceConfig> restoredAssigner =
                new IncrementalSplitAssigner<>(restoredContext, 2, offsetFactory, checkpoint);
        IncrementalSplit returnedSplit =
                new IncrementalSplit(
                        deliveredSplit.splitId(),
                        deliveredSplit.getTableIds(),
                        new TestOffset(100L),
                        stoppingOffset,
                        Collections.emptyList());
        restoredAssigner.addSplits(Collections.singletonList(returnedSplit));

        IncrementalSplit restoredSplit0 = restoredAssigner.getNext().get().asIncrementalSplit();
        IncrementalSplit restoredSplit1 = restoredAssigner.getNext().get().asIncrementalSplit();
        assertFalse(restoredAssigner.getNext().isPresent());
        assertTrue(restoredAssigner.noMoreSplits());
        assertFalse(restoredSplit0.splitId().equals(restoredSplit1.splitId()));

        Set<TableId> allCovered = new HashSet<>();
        allCovered.addAll(restoredSplit0.getTableIds());
        allCovered.addAll(restoredSplit1.getTableIds());
        assertEquals(restoredTables, allCovered);
    }

    @Test
    void shouldMergeCheckpointTablesAndHistoryPreferringSplitOwnerOnMultiSplitAddSplits() {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        StartupConfig startupConfig = mock(StartupConfig.class);
        StopConfig stopConfig = mock(StopConfig.class);
        OffsetFactory offsetFactory = mock(OffsetFactory.class);
        Offset stoppingOffset = new TestOffset(Long.MAX_VALUE);

        when(sourceConfig.getStartupConfig()).thenReturn(startupConfig);
        when(sourceConfig.getStopConfig()).thenReturn(stopConfig);
        when(startupConfig.getStartupMode()).thenReturn(StartupMode.INITIAL);
        when(stopConfig.getStopMode()).thenReturn(StopMode.NEVER);
        when(stopConfig.getStopOffset(offsetFactory)).thenReturn(stoppingOffset);

        TableId t1 = new TableId("mysql_cdc", null, "t1");
        TableId t2 = new TableId("mysql_cdc", null, "t2");
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(t1, t2));

        IncrementalPhaseState checkpoint =
                new IncrementalPhaseState(
                        new TestOffset(10L), null, true, Collections.emptyList(), capturedTables);
        IncrementalSplitAssigner<SourceConfig> assigner =
                new IncrementalSplitAssigner<>(
                        createContext(sourceConfig, capturedTables), 2, offsetFactory, checkpoint);

        CatalogTable t1Old =
                CatalogTable.of(
                        TableIdentifier.of("MySQL", TablePath.of("mysql_cdc", "t1")),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "old");
        CatalogTable t1New =
                CatalogTable.of(
                        TableIdentifier.of("MySQL", TablePath.of("mysql_cdc", "t1")),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "new");
        CatalogTable t2Old =
                CatalogTable.of(
                        TableIdentifier.of("MySQL", TablePath.of("mysql_cdc", "t2")),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "old");
        CatalogTable t2New =
                CatalogTable.of(
                        TableIdentifier.of("MySQL", TablePath.of("mysql_cdc", "t2")),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "new");

        Map<TableId, byte[]> split0History = new HashMap<>();
        split0History.put(t1, new byte[] {1, 2});
        split0History.put(t2, new byte[] {9});

        Map<TableId, byte[]> split1History = new HashMap<>();
        split1History.put(t1, new byte[] {8});
        split1History.put(t2, new byte[] {3, 4});

        IncrementalSplit split0 =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(t1),
                        new TestOffset(100L),
                        stoppingOffset,
                        Collections.emptyList(),
                        Arrays.asList(t1New, t2Old),
                        split0History);
        IncrementalSplit split1 =
                new IncrementalSplit(
                        "incremental-split-1",
                        Collections.singletonList(t2),
                        new TestOffset(120L),
                        stoppingOffset,
                        Collections.emptyList(),
                        Arrays.asList(t1Old, t2New),
                        split1History);

        assigner.addSplits(Arrays.asList(split0, split1));

        IncrementalSplit rebuilt0 = assigner.getNext().get().asIncrementalSplit();
        IncrementalSplit rebuilt1 = assigner.getNext().get().asIncrementalSplit();
        assertFalse(assigner.getNext().isPresent());

        Map<TablePath, String> comments = new HashMap<>();
        rebuilt0.getCheckpointTables()
                .forEach(table -> comments.put(table.getTablePath(), table.getComment()));
        assertEquals("new", comments.get(TablePath.of("mysql_cdc", "t1")));
        assertEquals("new", comments.get(TablePath.of("mysql_cdc", "t2")));

        Map<TableId, byte[]> mergedHistory = new HashMap<>();
        mergedHistory.putAll(rebuilt0.getHistoryTableChanges());
        mergedHistory.putAll(rebuilt1.getHistoryTableChanges());
        assertArrayEquals(new byte[] {1, 2}, mergedHistory.get(t1));
        assertArrayEquals(new byte[] {3, 4}, mergedHistory.get(t2));
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
