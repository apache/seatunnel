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
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
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

    private SplitAssigner.Context<SourceConfig> createContext(SourceConfig sourceConfig) {
        return new SplitAssigner.Context<>(
                sourceConfig,
                Collections.singleton(TableId.parse("database.schema.table")),
                new HashMap<>(),
                new HashMap<>());
    }

    private static class TestOffset extends Offset {
        private final int value;

        private TestOffset(int value) {
            this.value = value;
            this.offset = Collections.singletonMap("position", String.valueOf(value));
        }

        @Override
        public int compareTo(Offset offset) {
            return Integer.compare(value, ((TestOffset) offset).value);
        }
    }
}
