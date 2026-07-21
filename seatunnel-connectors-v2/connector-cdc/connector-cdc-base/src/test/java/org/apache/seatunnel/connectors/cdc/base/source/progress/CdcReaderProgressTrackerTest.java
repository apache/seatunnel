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

import org.apache.seatunnel.api.cdc.CdcProgressAccuracy;
import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collections;

class CdcReaderProgressTrackerTest {

    @Test
    void testTracksCurrentOffsetWithoutClaimingCheckpointOrRestoreProgress() {
        CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MySQL-CDC", "MYSQL_BINLOG");
        IncrementalSplitState splitState = createIncrementalSplitState(new TestOffset(10L));

        tracker.recordSplitState(splitState);
        Assertions.assertEquals(0L, tracker.current().getLastPositionChangeAt());

        tracker.recordEmission(splitState, 90L, 100L);
        CdcReaderProgressReport first = tracker.current();

        Assertions.assertEquals(CdcProgressLifecycle.INCREMENTAL, first.getLifecycle());
        Assertions.assertEquals("incremental-split", first.getActiveSplitId());
        Assertions.assertEquals(
                "10", first.getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(100L, first.getLastPositionChangeAt());
        Assertions.assertEquals(90L, first.getLastSourceEventAt());
        Assertions.assertEquals(
                CdcProgressAccuracy.UNSUPPORTED,
                first.getLastCompletedCheckpointPosition().getAccuracy());
        Assertions.assertEquals(
                CdcProgressAccuracy.UNSUPPORTED, first.getRestoredPosition().getAccuracy());

        tracker.recordEmission(splitState, null, 200L);
        Assertions.assertEquals(100L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(90L, tracker.current().getLastSourceEventAt());

        splitState.setStartupOffset(new TestOffset(11L));
        tracker.recordEmission(splitState, 190L, 300L);
        Assertions.assertEquals(300L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(
                "11",
                tracker.current().getCurrentConsumedPosition().getValue().getValues().get("pos"));
    }

    @Test
    void testTracksCatchUpToIncrementalTransition() {
        CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MySQL-CDC", "MYSQL_BINLOG");
        TableId tableId = TableId.parse("inventory.orders");
        CompletedSnapshotSplitInfo completedSplit =
                new CompletedSnapshotSplitInfo(
                        "snapshot-split",
                        tableId,
                        null,
                        null,
                        null,
                        new SnapshotSplitWatermark(
                                "snapshot-split", new TestOffset(5L), new TestOffset(15L)));
        IncrementalSplitState splitState =
                new IncrementalSplitState(
                        new IncrementalSplit(
                                "incremental-split",
                                Collections.singletonList(tableId),
                                new TestOffset(10L),
                                null,
                                new ArrayList<>(Collections.singletonList(completedSplit))));

        tracker.recordSplitState(splitState);
        Assertions.assertEquals(CdcProgressLifecycle.CATCH_UP, tracker.current().getLifecycle());

        Assertions.assertTrue(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(15L)));
        splitState.setStartupOffset(new TestOffset(15L));
        tracker.recordEmission(splitState, null, 100L);

        Assertions.assertEquals(CdcProgressLifecycle.INCREMENTAL, tracker.current().getLifecycle());
    }

    private IncrementalSplitState createIncrementalSplitState(Offset offset) {
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("inventory.orders")),
                        offset,
                        null,
                        Collections.emptyList());
        return new IncrementalSplitState(split);
    }

    private static final class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = Collections.singletonMap("pos", String.valueOf(value));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
