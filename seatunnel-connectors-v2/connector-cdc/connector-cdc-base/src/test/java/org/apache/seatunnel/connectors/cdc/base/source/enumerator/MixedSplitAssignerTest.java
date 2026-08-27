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
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.MixedPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class MixedSplitAssignerTest {

    @Test
    void shouldPreserveMixedPolicyInCheckpointAndRestore() {
        TableId snapshotTable = TableId.parse("database.snapshot_table");
        TableId specificTable = TableId.parse("database.specific_table");
        Set<TableId> snapshotTables = new HashSet<>(Collections.singleton(snapshotTable));
        Offset specificStartOffset = new TestOffset(100);
        Map<TableId, Offset> tableStartOffsets = new HashMap<>();
        tableStartOffsets.put(specificTable, specificStartOffset);
        SplitAssigner.Context<SourceConfig> context =
                new SplitAssigner.Context<>(
                        null,
                        new HashSet<>(Arrays.asList(snapshotTable, specificTable)),
                        new HashMap<>(),
                        new HashMap<>());

        MixedSplitAssigner<SourceConfig> assigner =
                new MixedSplitAssigner<>(
                        context, 1, 1, snapshotTables, tableStartOffsets, false, null, null);
        MixedPendingSplitsState checkpoint = (MixedPendingSplitsState) assigner.snapshotState(1L);

        snapshotTables.clear();
        tableStartOffsets.clear();
        Assertions.assertEquals(
                Collections.singleton(snapshotTable), checkpoint.getSnapshotTables());
        Assertions.assertEquals(
                Collections.singletonMap(specificTable, specificStartOffset),
                checkpoint.getTableStartOffsets());

        MixedSplitAssigner<SourceConfig> restoredAssigner =
                new MixedSplitAssigner<>(context, 1, 1, checkpoint, null, null);
        MixedPendingSplitsState restoredCheckpoint =
                (MixedPendingSplitsState) restoredAssigner.snapshotState(2L);

        Assertions.assertEquals(
                checkpoint.getSnapshotTables(), restoredCheckpoint.getSnapshotTables());
        Assertions.assertEquals(
                checkpoint.getTableStartOffsets(), restoredCheckpoint.getTableStartOffsets());
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
