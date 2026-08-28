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
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;

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

    private SplitAssigner.Context<SourceConfig> createContext(SourceConfig sourceConfig) {
        return new SplitAssigner.Context<>(
                sourceConfig,
                Collections.singleton(TableId.parse("database.schema.table")),
                new HashMap<>(),
                new HashMap<>());
    }
}
