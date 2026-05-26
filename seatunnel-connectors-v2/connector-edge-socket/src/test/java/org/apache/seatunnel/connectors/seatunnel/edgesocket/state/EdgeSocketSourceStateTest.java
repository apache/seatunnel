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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.state;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketCompressionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

class EdgeSocketSourceStateTest {

    @Test
    void resolveCommitResponseReturnsRetryForUnknownBatch() {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        Assertions.assertEquals("RETRY", state.resolveCommitResponse(1));
    }

    @Test
    void resolveCommitResponseReturnsPendingForReceivedBatch() {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        state.markRecordReceived(1);
        Assertions.assertEquals("PENDING", state.resolveCommitResponse(1));
    }

    @Test
    void resolveCommitResponseReturnsAckAfterCheckpoint() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        state.markRecordReceived(1);
        state.markRecordEmitted(1);

        state.snapshotState(100L, new EdgeSocketQueuedRecord[0]);
        state.notifyCheckpointComplete(100L);

        Assertions.assertTrue(state.resolveCommitResponse(1).startsWith("ACK:"));
    }

    @Test
    void watermarkAdvancesContiguously() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        state.markRecordReceived(1);
        state.markRecordReceived(2);
        state.markRecordReceived(3);
        state.markRecordEmitted(1);
        state.markRecordEmitted(2);
        // batch 3 still pending — watermark stops at 2
        state.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        state.notifyCheckpointComplete(1L);

        Assertions.assertEquals("ACK:2", state.resolveCommitResponse(1));
        Assertions.assertEquals("PENDING", state.resolveCommitResponse(3));
    }

    @Test
    void multipleRecordsPerBatchTrackedCorrectly() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        // batch 1 has 3 records; emit only 2 — watermark must not advance
        state.markRecordReceived(1);
        state.markRecordReceived(1);
        state.markRecordReceived(1);
        state.markRecordEmitted(1);
        state.markRecordEmitted(1);
        state.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        state.notifyCheckpointComplete(1L);

        Assertions.assertEquals("ACK:0", state.resolveCommitResponse(0));
        Assertions.assertEquals("PENDING", state.resolveCommitResponse(1));

        // emit the last record — now batch 1 is fully drained
        state.markRecordEmitted(1);
        state.snapshotState(2L, new EdgeSocketQueuedRecord[0]);
        state.notifyCheckpointComplete(2L);

        Assertions.assertEquals("ACK:1", state.resolveCommitResponse(1));
    }

    @Test
    void snapshotAndRestoreRoundTrip() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        state.markRecordReceived(1);
        state.markRecordReceived(2);
        state.markRecordEmitted(1); // batch 1 drained; batch 2 still in queue

        EdgeSocketQueuedRecord[] queueSnapshot = {
            new EdgeSocketQueuedRecord(
                    2, "hello".getBytes(StandardCharsets.UTF_8), EdgeSocketCompressionType.NONE)
        };
        byte[] serialized = state.snapshotState(1L, queueSnapshot);

        EdgeSocketSourceState restored = new EdgeSocketSourceState();
        List<EdgeSocketQueuedRecord> records = restored.restoreState(serialized);

        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals(2, records.get(0).getBatchId());
        Assertions.assertEquals(
                "hello", new String(records.get(0).getPayloadBytes(), StandardCharsets.UTF_8));
        Assertions.assertEquals(
                EdgeSocketCompressionType.NONE, records.get(0).getCompressionType());
        // batch 1 committed (snapshotWatermark=1); batch 2 restored as pending
        Assertions.assertEquals("ACK:1", restored.resolveCommitResponse(1));
        Assertions.assertEquals("PENDING", restored.resolveCommitResponse(2));
    }

    @Test
    void restoreEmptyStateWithNoQueueRecords() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        byte[] serialized = state.snapshotState(1L, new EdgeSocketQueuedRecord[0]);

        EdgeSocketSourceState restored = new EdgeSocketSourceState();
        List<EdgeSocketQueuedRecord> records = restored.restoreState(serialized);

        Assertions.assertTrue(records.isEmpty());
        Assertions.assertEquals("RETRY", restored.resolveCommitResponse(1));
    }

    @Test
    void checkpointAbortedDoesNotAdvanceWatermark() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        state.markRecordReceived(1);
        state.markRecordEmitted(1);
        state.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        state.notifyCheckpointAborted(1L);

        Assertions.assertEquals("PENDING", state.resolveCommitResponse(1));
    }

    @Test
    void restoredWatermarkAcksBatchIdAlreadyCovered() throws Exception {
        // After restore with watermark=100, __COMMIT__:1 is already covered — source returns ACK.
        EdgeSocketSourceState original = new EdgeSocketSourceState();
        for (int i = 1; i <= 100; i++) {
            original.markRecordReceived(i);
            original.markRecordEmitted(i);
        }
        original.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        original.notifyCheckpointComplete(1L);
        byte[] snapshot = original.snapshotState(2L, new EdgeSocketQueuedRecord[0]);
        original.notifyCheckpointComplete(2L);

        EdgeSocketSourceState restored = new EdgeSocketSourceState();
        restored.restoreState(snapshot);
        restored.markRecordReceived(1); // batchId=1 <= watermark=100; ACK regardless
        Assertions.assertEquals("ACK:100", restored.resolveCommitResponse(1));
    }

    @Test
    void restoredWatermarkAcksUnseenBatchIdBelowFloor() throws Exception {
        // __COMMIT__ for a batchId already covered by the restored watermark returns ACK,
        // even if that batchId was never re-received in this session.
        EdgeSocketSourceState original = new EdgeSocketSourceState();
        for (int i = 1; i <= 50; i++) {
            original.markRecordReceived(i);
            original.markRecordEmitted(i);
        }
        original.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        original.notifyCheckpointComplete(1L);
        byte[] snapshot = original.snapshotState(2L, new EdgeSocketQueuedRecord[0]);
        original.notifyCheckpointComplete(2L);

        EdgeSocketSourceState restored = new EdgeSocketSourceState();
        restored.restoreState(snapshot);

        Assertions.assertEquals("ACK:50", restored.resolveCommitResponse(1));
    }

    @Test
    void resolveCommitResponseReturnsResendForGappedBatchAfterRestore() throws Exception {
        EdgeSocketSourceState state = new EdgeSocketSourceState();
        state.markRecordReceived(1);
        state.markRecordReceived(2);
        state.markRecordReceived(3);
        state.markRecordReceived(5);
        state.markRecordEmitted(1);
        state.markRecordEmitted(2);
        state.markRecordEmitted(3);
        state.markRecordEmitted(5);

        state.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        state.notifyCheckpointComplete(1L);

        byte[] snapshot = state.snapshotState(2L, new EdgeSocketQueuedRecord[0]);

        EdgeSocketSourceState restored = new EdgeSocketSourceState();
        restored.restoreState(snapshot);

        Assertions.assertEquals("RESEND", restored.resolveCommitResponse(4));
        Assertions.assertEquals("PENDING", restored.resolveCommitResponse(5));
        Assertions.assertTrue(restored.resolveCommitResponse(3).startsWith("ACK:"));
    }

    @Test
    void restoredStateAcksAfterCurrentSessionCheckpoint() throws Exception {
        // Batches received and checkpointed after restore must be ACK-able.
        EdgeSocketSourceState original = new EdgeSocketSourceState();
        original.markRecordReceived(50);
        original.markRecordEmitted(50);
        original.snapshotState(1L, new EdgeSocketQueuedRecord[0]);
        original.notifyCheckpointComplete(1L);

        byte[] snapshot = original.snapshotState(2L, new EdgeSocketQueuedRecord[0]);
        original.notifyCheckpointComplete(2L);

        EdgeSocketSourceState restored = new EdgeSocketSourceState();
        restored.restoreState(snapshot);
        restored.markRecordReceived(51);
        restored.markRecordEmitted(51);
        restored.snapshotState(10L, new EdgeSocketQueuedRecord[0]);
        restored.notifyCheckpointComplete(10L);

        Assertions.assertTrue(restored.resolveCommitResponse(51).startsWith("ACK:"));
    }
}
