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

import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.EdgeSocketResponseCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketCompressionType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public class EdgeSocketSourceState {

    private static final byte STATE_VERSION = 1;

    private long lastReceivedBatchId;
    private long lastCommittedBatchId;
    private final Map<Long, Integer> pendingBatchRecordCounts = new HashMap<>();
    private final Set<Long> drainedBatchIds = new HashSet<>();
    private final Map<Long, Long> checkpointBatchWatermarks = new TreeMap<>();

    /**
     * Highest batchId received before the last {@link #restoreState(byte[])}. Used by {@link
     * #resolveCommitResponse(long)} to distinguish batches that were in flight at restore but have
     * not been re-received yet in this session (e.g. {@code RESEND} vs {@code PENDING}).
     */
    private long sessionFloorWatermark;

    /**
     * Records an accepted ingress batch (increments pending count for {@code batchId}).
     *
     * <p>{@link #lastCommittedBatchId} is {@code 0} only on a fresh instance before {@link
     * #restoreState} and before any checkpoint completes; {@link #restoreState} replaces it with
     * the serialized watermark. While still {@code 0}, this method sets {@code lastCommittedBatchId
     * = batchId - 1} so the first batch stays strictly above the committed floor until {@link
     * #notifyCheckpointComplete} advances it. For example, when the first {@code batchId} is {@code
     * 1}, the floor becomes {@code 0}.
     *
     * <p>Removes {@code batchId} from {@link #drainedBatchIds} when new records arrive for a batch
     * that was previously fully drained, so it is no longer treated as complete until drained
     * again.
     */
    public void markRecordReceived(long batchId) {
        lastReceivedBatchId = Math.max(lastReceivedBatchId, batchId);
        lastCommittedBatchId = lastCommittedBatchId == 0 ? batchId - 1 : lastCommittedBatchId;
        pendingBatchRecordCounts.merge(batchId, 1, Integer::sum);
        drainedBatchIds.remove(batchId);
    }

    /**
     * Records that one queued record for {@code batchId} was emitted downstream (for example after
     * {@code pollNext} collects a row). A single logical batch may carry multiple records; {@link
     * #markRecordReceived} increments the pending count and this method decrements it. When the
     * count reaches zero, the batch is considered fully drained and is added to {@link
     * #drainedBatchIds}, which feeds contiguous watermark computation in {@link #snapshotState} and
     * eventually {@link #notifyCheckpointComplete}.
     *
     * <p>No-op if {@code batchId <= 0}, or if there is no pending count for {@code batchId} (never
     * received or already fully drained).
     */
    public void markRecordEmitted(long batchId) {
        if (batchId <= 0) {
            return;
        }
        Integer count = pendingBatchRecordCounts.get(batchId);
        if (count == null) {
            return;
        }
        if (count <= 1) {
            pendingBatchRecordCounts.remove(batchId);
            drainedBatchIds.add(batchId);
        } else {
            pendingBatchRecordCounts.put(batchId, count - 1);
        }
    }

    /**
     * Resolves the response for a {@code __COMMIT__} request.
     *
     * <p>Rules are evaluated in declaration order; the first match wins. If no rule matches, the
     * default response is {@code PENDING}. The collector must use strictly monotonic {@code
     * batchId}s across reconnects and restores, and after a restart should resume from a {@code
     * batchId} above the last acknowledged watermark.
     *
     * <ol>
     *   <li>{@link #matchCommitted} — {@code batchId <= lastCommittedBatchId} (includes probe with
     *       batchId <= 0); returns {@code ACK:<watermark>}.
     *   <li>{@link #matchInFlight} — batch appears in {@link #pendingBatchRecordCounts} or {@link
     *       #drainedBatchIds}; returns {@code PENDING}.
     *   <li>{@link #matchNeverReceived} — {@code batchId > lastReceivedBatchId}; returns {@code
     *       RETRY}.
     *   <li>{@link #matchLostAfterRestore} — {@code batchId <= sessionFloorWatermark} after
     *       restore; returns {@code RESEND}.
     *   <li>Default — conservative {@code PENDING}.
     * </ol>
     *
     * @return {@code "ACK:<watermark>"}, {@code "PENDING"}, {@code "RETRY"}, or {@code "RESEND"}
     */
    public String resolveCommitResponse(long batchId) {
        return firstMatch(
                        batchId,
                        this::matchCommitted,
                        this::matchInFlight,
                        this::matchNeverReceived,
                        this::matchLostAfterRestore)
                .orElse(EdgeSocketResponseCode.PENDING.getCode());
    }

    /** {@code batchId <= lastCommittedBatchId} (includes probe with batchId <= 0). */
    private Optional<String> matchCommitted(long batchId) {
        return batchId <= lastCommittedBatchId
                ? Optional.of(EdgeSocketResponseCode.ACK.withPayload(lastCommittedBatchId))
                : Optional.empty();
    }

    /** Records exist in {@link #pendingBatchRecordCounts} or {@link #drainedBatchIds}. */
    private Optional<String> matchInFlight(long batchId) {
        return pendingBatchRecordCounts.containsKey(batchId) || drainedBatchIds.contains(batchId)
                ? Optional.of(EdgeSocketResponseCode.PENDING.getCode())
                : Optional.empty();
    }

    /** No {@code __BATCH__} has been accepted for this batchId yet. */
    private Optional<String> matchNeverReceived(long batchId) {
        return batchId > lastReceivedBatchId
                ? Optional.of(EdgeSocketResponseCode.RETRY.getCode())
                : Optional.empty();
    }

    /** Within the previous session's received batch-id horizon but absent from current state. */
    private Optional<String> matchLostAfterRestore(long batchId) {
        return batchId <= sessionFloorWatermark
                ? Optional.of(EdgeSocketResponseCode.RESEND.getCode())
                : Optional.empty();
    }

    @FunctionalInterface
    interface CommitRule {
        Optional<String> apply(long batchId);
    }

    private static Optional<String> firstMatch(long batchId, CommitRule... rules) {
        for (CommitRule rule : rules) {
            Optional<String> result = rule.apply(batchId);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    /**
     * Serializes this state for a SeaTunnel checkpoint. {@code queueSnapshot} is supplied by the
     * caller (typically the ingress queue's point-in-time {@code snapshot()}) so this class does
     * not retain a reference to the live queue. The array is non-null and may be empty.
     *
     * <p>Records the contiguous drained watermark under {@code checkpointId} in {@link
     * #checkpointBatchWatermarks}; it is applied to {@link #lastCommittedBatchId} when {@link
     * #notifyCheckpointComplete(long)} runs for that id.
     *
     * <p>Binary layout matches {@link #restoreState(byte[])} (high-water marks, pending counts,
     * drained ids, then each {@link EdgeSocketQueuedRecord} in {@code queueSnapshot}). A {@link
     * #STATE_VERSION} byte is written as the first byte.
     */
    public byte[] snapshotState(long checkpointId, EdgeSocketQueuedRecord[] queueSnapshot)
            throws IOException {
        long snapshotWatermark = computeContiguousDrainedWatermark(lastCommittedBatchId);
        checkpointBatchWatermarks.put(checkpointId, snapshotWatermark);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            out.writeByte(STATE_VERSION);
            out.writeLong(lastCommittedBatchId);
            out.writeLong(lastReceivedBatchId);
            out.writeLong(snapshotWatermark);
            out.writeInt(pendingBatchRecordCounts.size());
            for (Map.Entry<Long, Integer> entry : pendingBatchRecordCounts.entrySet()) {
                out.writeLong(entry.getKey());
                out.writeInt(entry.getValue());
            }
            out.writeInt(drainedBatchIds.size());
            for (Long batchId : drainedBatchIds) {
                out.writeLong(batchId);
            }
            out.writeInt(queueSnapshot.length);
            for (EdgeSocketQueuedRecord record : queueSnapshot) {
                out.writeLong(record.getBatchId());
                out.writeInt(record.getCompressionType().ordinal());
                byte[] payload = record.getPayloadBytes();
                out.writeInt(payload.length);
                out.write(payload);
            }
            out.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    /**
     * Restores this state object from bytes produced by {@link #snapshotState(long,
     * EdgeSocketQueuedRecord[])} on a previous run (job failover / reader restart).
     *
     * <p>Deserializes {@link #lastCommittedBatchId}, {@link #lastReceivedBatchId}, the
     * snapshot-line watermark at save time, {@link #pendingBatchRecordCounts}, and {@link
     * #drainedBatchIds}. Then advances {@link #lastCommittedBatchId} to {@code max(committed,
     * snapshotWatermark)} and runs {@link #clearCommittedBatchState(long)} so bookkeeping matches
     * the resumed checkpoint. In-flight checkpoint bookkeeping is cleared ({@link
     * #checkpointBatchWatermarks}); {@link #sessionFloorWatermark} is set to the highest batchId
     * received in the previous session for {@link #resolveCommitResponse(long)} (e.g. {@code
     * RESEND} for batches within the previous session's received range that were not re-received
     * yet).
     *
     * <p>Returns queued records that were persisted inside the snapshot but not yet emitted; the
     * caller must {@code offer} them back into the reader's queue in order (see reader restore
     * path). If the byte array ends after the bookkeeping section (older format or empty tail),
     * returns an empty list.
     */
    public List<EdgeSocketQueuedRecord> restoreState(byte[] restoredState) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(restoredState))) {
            in.readByte();
            lastCommittedBatchId = in.readLong();
            lastReceivedBatchId = in.readLong();
            long restoredSnapshotWatermark = in.readLong();

            pendingBatchRecordCounts.clear();
            int pendingSize = in.readInt();
            for (int i = 0; i < pendingSize; i++) {
                pendingBatchRecordCounts.put(in.readLong(), in.readInt());
            }

            drainedBatchIds.clear();
            int drainedSize = in.readInt();
            for (int i = 0; i < drainedSize; i++) {
                drainedBatchIds.add(in.readLong());
            }

            lastCommittedBatchId = Math.max(lastCommittedBatchId, restoredSnapshotWatermark);
            clearCommittedBatchState(lastCommittedBatchId);
            checkpointBatchWatermarks.clear();
            sessionFloorWatermark = lastReceivedBatchId;

            if (in.available() == 0) {
                return new ArrayList<>();
            }

            int queuedSize = in.readInt();
            EdgeSocketCompressionType[] compressionValues = EdgeSocketCompressionType.values();
            List<EdgeSocketQueuedRecord> records = new ArrayList<>(queuedSize);
            for (int i = 0; i < queuedSize; i++) {
                long batchId = in.readLong();
                EdgeSocketCompressionType compression = compressionValues[in.readInt()];
                int payloadLen = in.readInt();
                byte[] payload = new byte[payloadLen];
                in.readFully(payload);
                records.add(new EdgeSocketQueuedRecord(batchId, payload, compression));
            }
            return records;
        } catch (IOException deserializeException) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "Restore edge socket batch checkpoint state failed",
                    deserializeException);
        }
    }

    public void notifyCheckpointComplete(long checkpointId) {
        Long completedWatermark = checkpointBatchWatermarks.remove(checkpointId);
        if (completedWatermark == null) {
            return;
        }
        if (completedWatermark > lastCommittedBatchId) {
            lastCommittedBatchId = completedWatermark;
        }
        clearCommittedBatchState(lastCommittedBatchId);
    }

    public void notifyCheckpointAborted(long checkpointId) {
        checkpointBatchWatermarks.remove(checkpointId);
    }

    private long computeContiguousDrainedWatermark(long startWatermark) {
        long watermark = startWatermark;
        while (drainedBatchIds.contains(watermark + 1)) {
            watermark++;
        }
        return watermark;
    }

    private void clearCommittedBatchState(long committedWatermark) {
        pendingBatchRecordCounts.keySet().removeIf(batchId -> batchId <= committedWatermark);
        drainedBatchIds.removeIf(batchId -> batchId <= committedWatermark);
    }
}
