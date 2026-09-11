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
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
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
        Assertions.assertEquals(
                CdcSnapshotAssignmentStatus.COMPLETED,
                splitAssigner
                        .getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getSnapshotAssignmentStatus());

        splitAssigner.notifyCheckpointComplete(11L);
        Assertions.assertTrue(splitAssigner.isCompleted());
    }

    @Test
    public void testAddSplitsShouldReplayFinishedSplitWithoutCompletedWatermark() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>());

        splitAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState state = splitAssigner.snapshotState(12L);
        Assertions.assertEquals(1, state.getRemainingSplits().size());
        Assertions.assertEquals(
                finishedSplit.splitId(), state.getRemainingSplits().get(0).splitId());
        Assertions.assertTrue(state.getAssignedSplits().isEmpty());
        Assertions.assertTrue(state.getSplitCompletedOffsets().isEmpty());
        Assertions.assertTrue(splitAssigner.waitingForCompletedSplits());
        Assertions.assertEquals(
                CdcSnapshotAssignmentStatus.ASSIGNING,
                splitAssigner
                        .getCdcEnumeratorProgress("MySQL-CDC", "MYSQL_BINLOG")
                        .getSnapshotAssignmentStatus());
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

    private SnapshotSplitAssigner<?> createRestoredSnapshotSplitAssigner(
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, SnapshotSplitWatermark> completedOffsets) {
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
        return new SnapshotSplitAssigner<>(context, 10, checkpointState, null);
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
