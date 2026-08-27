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

import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HybridSplitAssignerTest {
    @Test
    public void testCompletedSnapshotPhase() {
        Map<String, SnapshotSplit> assignedSplits = createAssignedSplits();
        Map<String, SnapshotSplitWatermark> splitCompletedOffsets = createSplitCompletedOffsets();
        SnapshotPhaseState snapshotPhaseState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        assignedSplits,
                        splitCompletedOffsets,
                        true,
                        Collections.emptyList(),
                        false,
                        false);
        HybridPendingSplitsState checkpointState =
                new HybridPendingSplitsState(snapshotPhaseState, null);
        SplitAssigner.Context context =
                new SplitAssigner.Context<>(
                        null,
                        Collections.emptySet(),
                        checkpointState.getSnapshotPhaseState().getAssignedSplits(),
                        checkpointState.getSnapshotPhaseState().getSplitCompletedOffsets());
        HybridSplitAssigner splitAssigner =
                new HybridSplitAssigner<>(context, 1, 1, checkpointState, null, null);
        splitAssigner.getIncrementalSplitAssigner().setSplitAssigned(true);

        Assertions.assertFalse(
                splitAssigner.completedSnapshotPhase(Arrays.asList(TableId.parse("db1.table1"))));
        Assertions.assertFalse(
                splitAssigner.getSnapshotSplitAssigner().getAssignedSplits().isEmpty());
        Assertions.assertFalse(
                splitAssigner.getSnapshotSplitAssigner().getSplitCompletedOffsets().isEmpty());
        Assertions.assertFalse(context.getAssignedSnapshotSplit().isEmpty());
        Assertions.assertFalse(context.getSplitCompletedOffsets().isEmpty());

        Assertions.assertTrue(
                splitAssigner.completedSnapshotPhase(Arrays.asList(TableId.parse("db1.table2"))));
        Assertions.assertTrue(
                splitAssigner.getSnapshotSplitAssigner().getAssignedSplits().isEmpty());
        Assertions.assertTrue(
                splitAssigner.getSnapshotSplitAssigner().getSplitCompletedOffsets().isEmpty());
        Assertions.assertTrue(context.getAssignedSnapshotSplit().isEmpty());
        Assertions.assertTrue(context.getSplitCompletedOffsets().isEmpty());
    }

    private static Map<String, SnapshotSplit> createAssignedSplits() {
        return Stream.of(
                        new AbstractMap.SimpleEntry<>(
                                "db1.table1.1",
                                new SnapshotSplit(
                                        "db1.table1.1",
                                        TableId.parse("db1.table1"),
                                        null,
                                        null,
                                        null)),
                        new AbstractMap.SimpleEntry<>(
                                "db1.table1.2",
                                new SnapshotSplit(
                                        "db1.table1.2",
                                        TableId.parse("db1.table1"),
                                        null,
                                        null,
                                        null)),
                        new AbstractMap.SimpleEntry<>(
                                "db1.table2.1",
                                new SnapshotSplit(
                                        "db1.table2.1",
                                        TableId.parse("db1.table2"),
                                        null,
                                        null,
                                        null)),
                        new AbstractMap.SimpleEntry<>(
                                "db1.table2.2",
                                new SnapshotSplit(
                                        "db1.table2.2",
                                        TableId.parse("db1.table2"),
                                        null,
                                        null,
                                        null)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, SnapshotSplitWatermark> createSplitCompletedOffsets() {
        return Stream.of(
                        new AbstractMap.SimpleEntry<>(
                                "db1.table1.1", new SnapshotSplitWatermark(null, null, null)),
                        new AbstractMap.SimpleEntry<>(
                                "db1.table1.2", new SnapshotSplitWatermark(null, null, null)),
                        new AbstractMap.SimpleEntry<>(
                                "db1.table2.1", new SnapshotSplitWatermark(null, null, null)),
                        new AbstractMap.SimpleEntry<>(
                                "db1.table2.2", new SnapshotSplitWatermark(null, null, null)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
