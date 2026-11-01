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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.CRC32;

/** Versioned serializer for the coordinator assignment ownership ledger. */
public final class SourceAssignmentTrackerSerializer {
    private static final int MAGIC = 0x53544154;
    private static final int VERSION = 1;
    private static final int MAX_ENTRIES = 1_000_000;

    private SourceAssignmentTrackerSerializer() {}

    public static byte[] serialize(Collection<SourceAssignmentTracker.Entry> entries)
            throws IOException {
        if (entries == null || entries.size() > MAX_ENTRIES) {
            throw new IllegalArgumentException("Invalid assignment tracker entry count");
        }
        byte[] payload;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(entries.size());
            for (SourceAssignmentTracker.Entry entry : entries) {
                writeString(out, entry.getCommandId());
                writeString(out, entry.getAssignmentGroupId());
                out.writeLong(entry.getSenderSequence());
                out.writeInt(entry.getTargetSubtask());
                writeString(out, entry.getTargetAttemptId());
                out.writeInt(entry.getChunkIndex());
                out.writeInt(entry.getChunkCount());
                out.writeInt(entry.getSplitIds().size());
                List<byte[]> splitPayloads = entry.getSplitPayloads();
                for (int i = 0; i < entry.getSplitIds().size(); i++) {
                    writeString(out, entry.getSplitIds().get(i));
                    writeBytes(out, splitPayloads.get(i));
                }
                out.writeInt(entry.getState().getCode());
                out.writeLong(entry.getIncludedCheckpointId());
                out.writeLong(entry.getCreatedEpochMillis());
            }
            out.flush();
            payload = bytes.toByteArray();
        }
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 20);
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(payload.length);
            out.write(payload);
            out.writeLong(checksum(payload));
            out.flush();
            return bytes.toByteArray();
        }
    }

    public static List<SourceAssignmentTracker.Entry> deserialize(byte[] serialized)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            require(in.readInt() == MAGIC, "Invalid assignment tracker magic");
            int version = in.readInt();
            require(version == VERSION, "Unsupported assignment tracker version " + version);
            int payloadLength = in.readInt();
            require(
                    payloadLength >= 0 && payloadLength <= in.available() - Long.BYTES,
                    "Invalid assignment tracker payload length");
            byte[] payload = new byte[payloadLength];
            in.readFully(payload);
            require(in.readLong() == checksum(payload), "Assignment tracker checksum mismatch");
            require(in.available() == 0, "Trailing assignment tracker bytes");
            return deserializePayload(payload);
        }
    }

    private static List<SourceAssignmentTracker.Entry> deserializePayload(byte[] payload)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int count = in.readInt();
            require(count >= 0 && count <= MAX_ENTRIES, "Invalid assignment tracker entry count");
            List<SourceAssignmentTracker.Entry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String commandId = readString(in);
                String groupId = readString(in);
                long sequence = in.readLong();
                int subtask = in.readInt();
                String attempt = readString(in);
                int chunkIndex = in.readInt();
                int chunkCount = in.readInt();
                int splitCount = in.readInt();
                require(
                        splitCount >= 0 && splitCount <= MAX_ENTRIES,
                        "Invalid tracked split count");
                List<String> splitIds = new ArrayList<>(splitCount);
                List<byte[]> splitPayloads = new ArrayList<>(splitCount);
                for (int split = 0; split < splitCount; split++) {
                    splitIds.add(readString(in));
                    splitPayloads.add(readBytes(in));
                }
                SourceAssignmentState state = SourceAssignmentState.fromCode(in.readInt());
                long includedCheckpointId = in.readLong();
                long createdEpochMillis = in.readLong();
                require(createdEpochMillis >= 0, "Invalid assignment creation timestamp");
                entries.add(
                        new SourceAssignmentTracker.Entry(
                                commandId,
                                groupId,
                                sequence,
                                subtask,
                                attempt,
                                chunkIndex,
                                chunkCount,
                                splitIds,
                                splitPayloads,
                                state,
                                includedCheckpointId,
                                createdEpochMillis));
            }
            require(in.available() == 0, "Trailing assignment tracker payload");
            return entries;
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid assignment tracker state value", e);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        require(length >= 0 && length <= in.available(), "Invalid assignment field length");
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static long checksum(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload);
        return crc.getValue();
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }
}
