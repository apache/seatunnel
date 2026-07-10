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

package org.apache.seatunnel.connectors.cdc.base.source.progress;

import org.apache.seatunnel.api.cdc.CdcProgressPhase;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressSnapshot;
import org.apache.seatunnel.api.cdc.CdcProgressSupportGroup;
import org.apache.seatunnel.api.cdc.CdcProgressSupportLevel;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class CdcProgressSnapshotsTest {

    private static final long LAST_PROGRESS_TIME = 1234L;

    @Test
    public void shouldBuildSnapshotProgressFromSnapshotPhaseState() {
        SnapshotSplit assignedSplit =
                new SnapshotSplit(
                        "assigned-1",
                        new TableId("inventory", null, "orders"),
                        null,
                        null,
                        null,
                        offset("file", "mysql-bin.000001"),
                        offset("file", "mysql-bin.000002"));
        SnapshotSplit remainingSplit =
                new SnapshotSplit(
                        "remaining-1",
                        new TableId("inventory", null, "customers"),
                        null,
                        null,
                        null);
        SnapshotSplit runningSplit =
                new SnapshotSplit(
                        "running-1", new TableId("inventory", null, "payments"), null, null, null);
        Map<String, SnapshotSplit> assignedSplits = new LinkedHashMap<>();
        assignedSplits.put(assignedSplit.splitId(), assignedSplit);
        assignedSplits.put(runningSplit.splitId(), runningSplit);
        Map<String, SnapshotSplitWatermark> completedOffsets = new LinkedHashMap<>();
        completedOffsets.put(
                "completed-1",
                new SnapshotSplitWatermark(
                        "completed-1", offset("pos", "100"), offset("pos", "200")));
        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.singletonList(remainingSplit),
                        assignedSplits,
                        completedOffsets,
                        false,
                        Collections.emptyList(),
                        false,
                        true);

        CdcProgressSnapshot snapshot =
                CdcProgressSnapshots.forSnapshotPhase("Test CDC", state, LAST_PROGRESS_TIME);

        Assertions.assertEquals("Test CDC", snapshot.getConnectorType());
        Assertions.assertEquals(CdcProgressPhase.SNAPSHOT, snapshot.getPhase());
        Assertions.assertEquals("assigned-1", snapshot.getSplitId());
        Assertions.assertEquals(2, snapshot.getSnapshotProgress().getAssignedSplitCount());
        Assertions.assertEquals(1, snapshot.getSnapshotProgress().getCompletedSplitCount());
        Assertions.assertEquals(1, snapshot.getSnapshotProgress().getRunningSplitCount());
        Assertions.assertEquals(1, snapshot.getSnapshotProgress().getRemainingSplitCount());
        Assertions.assertEquals(
                "inventory.orders", snapshot.getSnapshotProgress().getCurrentTable());
        Assertions.assertEquals(
                "mysql-bin.000001",
                snapshot.getSnapshotProgress().getLowWatermark().getValues().get("file"));
        Assertions.assertEquals(
                CdcProgressSupportLevel.EXACT,
                snapshot.getSupportLevels().get(CdcProgressSupportGroup.SNAPSHOT_PROGRESS));
        Assertions.assertEquals(LAST_PROGRESS_TIME, snapshot.getLastProgressTime());
    }

    @Test
    public void shouldBuildIncrementalProgressFromIncrementalSplit() {
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental",
                        Arrays.asList(new TableId("inventory", null, "orders")),
                        offset("pos", "456"),
                        null,
                        Collections.emptyList());

        CdcProgressSnapshot snapshot =
                CdcProgressSnapshots.forIncrementalSplit("Test CDC", split, LAST_PROGRESS_TIME);

        Assertions.assertEquals(CdcProgressPhase.INCREMENTAL, snapshot.getPhase());
        Assertions.assertEquals("incremental", snapshot.getSplitId());
        Assertions.assertEquals(
                "456",
                snapshot.getIncrementalProgress()
                        .getCurrentConsumedPosition()
                        .getValues()
                        .get("pos"));
        Assertions.assertEquals(
                "456",
                snapshot.getCheckpointProgress()
                        .getLastCheckpointedPosition()
                        .getValues()
                        .get("pos"));
        Assertions.assertEquals(
                CdcProgressSupportLevel.EXACT,
                snapshot.getSupportLevels().get(CdcProgressSupportGroup.INCREMENTAL_PROGRESS));
        Assertions.assertEquals(
                CdcProgressSupportLevel.EXACT,
                snapshot.getSupportLevels().get(CdcProgressSupportGroup.CHECKPOINT_PROGRESS));
        Assertions.assertEquals(
                CdcProgressSupportLevel.EXACT,
                snapshot.getSupportLevels().get(CdcProgressSupportGroup.RAW_POSITION));
    }

    @Test
    public void shouldCopyRawOffsetValues() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("file", "mysql-bin.000001");
        Offset offset = new TestOffset(values);

        CdcProgressPosition position = CdcProgressSnapshots.toPosition(offset);
        values.put("file", "mysql-bin.000002");

        Assertions.assertEquals("mysql-bin.000001", position.getValues().get("file"));
    }

    private static Offset offset(String key, String value) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(key, value);
        return new TestOffset(values);
    }

    private static class TestOffset extends Offset {

        TestOffset(Map<String, String> values) {
            this.offset = values;
        }

        @Override
        public int compareTo(Offset offset) {
            return 0;
        }
    }
}
