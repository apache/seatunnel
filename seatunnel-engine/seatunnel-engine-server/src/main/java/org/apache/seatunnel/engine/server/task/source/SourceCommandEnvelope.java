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

package org.apache.seatunnel.engine.server.task.source;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32;

/** Immutable, versioned command envelope crossing Source runtime thread and node boundaries. */
public final class SourceCommandEnvelope implements Serializable {
    public static final int CURRENT_PROTOCOL_VERSION = 1;
    public static final int MAX_WIRE_PAYLOAD_BYTES = 16 * 1024 * 1024;
    public static final int MAX_CHUNK_COUNT = 100_000;
    public static final int MAX_IDENTIFIER_LENGTH = 256;

    private final int protocolVersion;
    private final long jobId;
    private final long sourceRuntimeId;
    private final String coordinatorEpoch;
    private final String senderAttemptId;
    private final String targetAttemptId;
    private final long senderSequence;
    private final String commandId;
    private final SourceCommandKind kind;
    private final SourceCommandDurability durability;
    private final int payloadVersion;
    private final int codecId;
    private final String assignmentGroupId;
    private final int chunkIndex;
    private final int chunkCount;
    private final long checksum;
    private final byte[] payload;
    private final long admittedNanos;

    public SourceCommandEnvelope(
            int protocolVersion,
            long jobId,
            long sourceRuntimeId,
            String coordinatorEpoch,
            String senderAttemptId,
            String targetAttemptId,
            long senderSequence,
            String commandId,
            SourceCommandKind kind,
            SourceCommandDurability durability,
            int payloadVersion,
            int codecId,
            String assignmentGroupId,
            int chunkIndex,
            int chunkCount,
            long checksum,
            byte[] payload,
            long admittedNanos) {
        this(
                protocolVersion,
                jobId,
                sourceRuntimeId,
                coordinatorEpoch,
                senderAttemptId,
                targetAttemptId,
                senderSequence,
                commandId,
                kind,
                durability,
                payloadVersion,
                codecId,
                assignmentGroupId,
                chunkIndex,
                chunkCount,
                checksum,
                payload,
                admittedNanos,
                true);
    }

    private SourceCommandEnvelope(
            int protocolVersion,
            long jobId,
            long sourceRuntimeId,
            String coordinatorEpoch,
            String senderAttemptId,
            String targetAttemptId,
            long senderSequence,
            String commandId,
            SourceCommandKind kind,
            SourceCommandDurability durability,
            int payloadVersion,
            int codecId,
            String assignmentGroupId,
            int chunkIndex,
            int chunkCount,
            long checksum,
            byte[] payload,
            long admittedNanos,
            boolean copyPayload) {
        this.protocolVersion = protocolVersion;
        this.jobId = jobId;
        this.sourceRuntimeId = sourceRuntimeId;
        this.coordinatorEpoch = requireText(coordinatorEpoch, "coordinatorEpoch");
        this.senderAttemptId = requireText(senderAttemptId, "senderAttemptId");
        this.targetAttemptId = requireText(targetAttemptId, "targetAttemptId");
        this.senderSequence = senderSequence;
        this.commandId = requireText(commandId, "commandId");
        this.kind = requireNonNull(kind, "kind");
        this.durability = requireNonNull(durability, "durability");
        this.payloadVersion = payloadVersion;
        this.codecId = codecId;
        this.assignmentGroupId = assignmentGroupId == null ? "" : assignmentGroupId;
        if (this.assignmentGroupId.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException(
                    "Source command assignmentGroupId exceeds wire limit");
        }
        this.chunkIndex = chunkIndex;
        this.chunkCount = chunkCount;
        this.checksum = checksum;
        this.payload =
                payload == null
                        ? new byte[0]
                        : copyPayload ? Arrays.copyOf(payload, payload.length) : payload;
        if (this.payload.length > MAX_WIRE_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Source command payload exceeds wire hard limit");
        }
        this.admittedNanos = admittedNanos;
        validateHeader();
    }

    public static SourceCommandEnvelope create(
            long jobId,
            long sourceRuntimeId,
            String coordinatorEpoch,
            String senderAttemptId,
            String targetAttemptId,
            long senderSequence,
            SourceCommandKind kind,
            SourceCommandDurability durability,
            int payloadVersion,
            int codecId,
            String assignmentGroupId,
            int chunkIndex,
            int chunkCount,
            byte[] payload) {
        byte[] actualPayload = payload == null ? new byte[0] : payload;
        return new SourceCommandEnvelope(
                CURRENT_PROTOCOL_VERSION,
                jobId,
                sourceRuntimeId,
                coordinatorEpoch,
                senderAttemptId,
                targetAttemptId,
                senderSequence,
                UUID.randomUUID().toString(),
                kind,
                durability,
                payloadVersion,
                codecId,
                assignmentGroupId,
                chunkIndex,
                chunkCount,
                checksum(actualPayload),
                actualPayload,
                0L);
    }

    public SourceCommandEnvelope markAdmitted(long nowNanos) {
        return new SourceCommandEnvelope(
                protocolVersion,
                jobId,
                sourceRuntimeId,
                coordinatorEpoch,
                senderAttemptId,
                targetAttemptId,
                senderSequence,
                commandId,
                kind,
                durability,
                payloadVersion,
                codecId,
                assignmentGroupId,
                chunkIndex,
                chunkCount,
                checksum,
                payload,
                nowNanos,
                false);
    }

    public boolean hasValidChecksum() {
        return checksum == checksum(payload);
    }

    public int estimatedSizeBytes() {
        return 192
                + payload.length
                + utf8Length(coordinatorEpoch)
                + utf8Length(senderAttemptId)
                + utf8Length(targetAttemptId)
                + utf8Length(commandId)
                + utf8Length(assignmentGroupId);
    }

    public boolean usesReservedCapacity() {
        return kind.isReservedControl() || durability == SourceCommandDurability.TERMINAL;
    }

    private void validateHeader() {
        if (protocolVersion <= 0 || payloadVersion <= 0 || codecId < 0) {
            throw new IllegalArgumentException("Source command versions and codec must be valid");
        }
        if (senderSequence <= 0) {
            throw new IllegalArgumentException("Source command senderSequence must be positive");
        }
        if (chunkCount <= 0
                || chunkCount > MAX_CHUNK_COUNT
                || chunkIndex < 0
                || chunkIndex >= chunkCount) {
            throw new IllegalArgumentException("Invalid Source command chunk metadata");
        }
    }

    private static long checksum(byte[] payload) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        return crc32.getValue();
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (value.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException(field + " exceeds wire limit");
        }
        return value;
    }

    private static <T> T requireNonNull(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " must not be null");
        }
        return value;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public long getJobId() {
        return jobId;
    }

    public long getSourceRuntimeId() {
        return sourceRuntimeId;
    }

    public String getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public String getSenderAttemptId() {
        return senderAttemptId;
    }

    public String getTargetAttemptId() {
        return targetAttemptId;
    }

    public long getSenderSequence() {
        return senderSequence;
    }

    public String getCommandId() {
        return commandId;
    }

    public SourceCommandKind getKind() {
        return kind;
    }

    public SourceCommandDurability getDurability() {
        return durability;
    }

    public int getPayloadVersion() {
        return payloadVersion;
    }

    public int getCodecId() {
        return codecId;
    }

    public String getAssignmentGroupId() {
        return assignmentGroupId;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public long getChecksum() {
        return checksum;
    }

    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public int getPayloadSize() {
        return payload.length;
    }

    public long getAdmittedNanos() {
        return admittedNanos;
    }
}
