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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;

import java.util.HashMap;
import java.util.Map;

class LsnOffsetTest {

    private static final String COMMIT_LSN = "00000027:00000a80:0003";

    private static final String NEXT_COMMIT_LSN = "00000027:00000a80:0004";

    private static final String CHANGE_LSN = "00000027:00000a80:0005";

    private static final String NEXT_CHANGE_LSN = "00000027:00000a80:0006";

    private static LsnOffset completeOffset(String commitLsn, String changeLsn, long serialNo) {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.COMMIT_LSN_KEY, commitLsn);
        offset.put(SourceInfo.CHANGE_LSN_KEY, changeLsn);
        offset.put(SourceInfo.EVENT_SERIAL_NO_KEY, serialNo);
        return LsnOffset.valueOf(offset);
    }

    @Test
    void testInitialOffsetRepresentsNoLsn() {
        LsnOffset initial = LsnOffset.INITIAL_OFFSET;

        // no LSN keys should be present in the offset map
        Assertions.assertTrue(initial.getOffset().isEmpty());

        // commit LSN resolved from the empty map should be Debezium's NULL LSN
        Lsn commitLsn = initial.getCommitLsn();
        Assertions.assertFalse(commitLsn.isAvailable());
    }

    @Test
    void testCompleteOffsetsCompareChangeLsnAndEventSerialNo() {
        LsnOffset first = completeOffset(COMMIT_LSN, CHANGE_LSN, 2L);
        LsnOffset second = completeOffset(COMMIT_LSN, CHANGE_LSN, 3L);

        Assertions.assertTrue(second.isAfter(first));
        Assertions.assertFalse(first.isAfter(second));

        LsnOffset laterChange = completeOffset(COMMIT_LSN, NEXT_CHANGE_LSN, 1L);
        Assertions.assertTrue(laterChange.isAfter(second));
        Assertions.assertFalse(second.isAfter(laterChange));
    }

    @Test
    void testCommitOnlyBoundaryDoesNotReplayInCommitEvents() {
        // startup.mode=latest records the current max commit as a commit-only boundary; the
        // streaming query then re-reads that commit with an inclusive lower bound. Events of
        // the boundary commit must be ordered BEFORE the boundary, otherwise records that
        // already existed before startup would be replayed. This applies to both the
        // non-exactly-once (`isAfter`) and the exactly-once (`isAtOrAfter` via
        // IncrementalSourceStreamFetcher.hasEnterPureBinlogPhase) streaming paths.
        LsnOffset latestBoundary = LsnOffset.valueOf(COMMIT_LSN);
        LsnOffset inCommitEvent = completeOffset(COMMIT_LSN, CHANGE_LSN, 1L);

        Assertions.assertFalse(inCommitEvent.isAfter(latestBoundary));
        Assertions.assertTrue(latestBoundary.isAfter(inCommitEvent));
        Assertions.assertFalse(inCommitEvent.isAtOrAfter(latestBoundary));
        Assertions.assertTrue(inCommitEvent.isAtOrBefore(latestBoundary));
        Assertions.assertTrue(latestBoundary.isAtOrAfter(inCommitEvent));
        Assertions.assertFalse(latestBoundary.isAtOrBefore(inCommitEvent));
    }

    @Test
    void testEventAfterBoundaryCommitIsEmitted() {
        LsnOffset latestBoundary = LsnOffset.valueOf(COMMIT_LSN);
        LsnOffset laterEvent = completeOffset(NEXT_COMMIT_LSN, CHANGE_LSN, 1L);

        Assertions.assertTrue(laterEvent.isAfter(latestBoundary));
        Assertions.assertFalse(latestBoundary.isAfter(laterEvent));
    }

    @Test
    void testCompleteEventIsAfterInitialOffset() {
        LsnOffset event = completeOffset(COMMIT_LSN, CHANGE_LSN, 1L);

        Assertions.assertTrue(event.isAfter(LsnOffset.INITIAL_OFFSET));
        Assertions.assertFalse(LsnOffset.INITIAL_OFFSET.isAfter(event));
    }

    @Test
    void testRestoredCheckpointOffsetKeepsInCommitPrecision() {
        // The #10571 recovery path: a complete event position serialized into a checkpoint is
        // restored through the offset map and used as the incremental startup offset. Events
        // at or before the restored position must be skipped, later in-commit events emitted.
        LsnOffset checkpointed = completeOffset(COMMIT_LSN, CHANGE_LSN, 2L);
        Map<String, String> serialized = new HashMap<>(checkpointed.getOffset());

        LsnOffset restoredStartupOffset = LsnOffset.valueOf(serialized);
        LsnOffset replayedEvent = completeOffset(COMMIT_LSN, CHANGE_LSN, 2L);
        LsnOffset nextEvent = completeOffset(COMMIT_LSN, CHANGE_LSN, 3L);

        Assertions.assertFalse(replayedEvent.isAfter(restoredStartupOffset));
        Assertions.assertTrue(nextEvent.isAfter(restoredStartupOffset));
    }

    @Test
    void testTimestampBoundaryEmitsSameCommitEvents() {
        // startup.mode=timestamp resolves the boundary via fn_cdc_map_time_to_lsn('smallest
        // greater than or equal', ts); the returned LSN is the COMMIT log record of the
        // matching transaction. Rows belonging to that transaction must be emitted, so every
        // real in-commit event must compare as isAfter(timestampBoundary).
        LsnOffset boundary = LsnOffset.timestampBoundary(COMMIT_LSN);
        LsnOffset firstInCommitEvent = completeOffset(COMMIT_LSN, CHANGE_LSN, 1L);
        LsnOffset laterInCommitEvent = completeOffset(COMMIT_LSN, NEXT_CHANGE_LSN, 1L);
        LsnOffset earlierCommitEvent =
                completeOffset("00000027:00000a80:0002", "00000027:00000a80:0002", 1L);
        LsnOffset laterCommitEvent = completeOffset(NEXT_COMMIT_LSN, CHANGE_LSN, 1L);

        Assertions.assertTrue(firstInCommitEvent.isAfter(boundary));
        Assertions.assertTrue(laterInCommitEvent.isAfter(boundary));
        Assertions.assertFalse(earlierCommitEvent.isAfter(boundary));
        Assertions.assertTrue(laterCommitEvent.isAfter(boundary));
        // The boundary itself carries a complete (commit, change, serial) position so that
        // #10571 resume-precision semantic still applies once a checkpoint with a real
        // change-LSN position is loaded.
        Assertions.assertTrue(boundary.getChangeLsn().isAvailable());
        Assertions.assertNotNull(boundary.getEventSerialNo());
    }

    @Test
    void testNoStoppingOffsetIsNeverStop() {
        Assertions.assertTrue(LsnOffset.NO_STOPPING_OFFSET.isNeverStop());
        Assertions.assertFalse(LsnOffset.INITIAL_OFFSET.isNeverStop());
    }
}
