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

import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SnapshotSplitAssignerTest {

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
        return new SnapshotSplit(
                splitId,
                TableId.parse("db1.table1"),
                null,
                null,
                null,
                new TestOffset(1L),
                new TestOffset(2L));
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
