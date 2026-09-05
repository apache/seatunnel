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

import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
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
        assertEquals(
                CdcSnapshotAssignmentStatus.NOT_APPLICABLE,
                assigner.getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getSnapshotAssignmentStatus());

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
}
