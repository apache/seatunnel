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

import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.CRC32;

/** Explicit, checksummed serializer for managed Reader checkpoint metadata version 1. */
public final class ManagedReaderCheckpointStateSerializer {
    static final int MAGIC = 0x53544d52;
    static final int VERSION = 1;
    private static final int MAX_COLLECTION_SIZE = 1_000_000;

    private ManagedReaderCheckpointStateSerializer() {}

    public static byte[] serialize(ManagedReaderCheckpointState state) throws IOException {
        byte[] payload;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(state.getRuntimeMode().getCode());
            out.writeInt(state.getRuntimeProtocolVersion());
            out.writeInt(state.getConnectorStateVersion());
            writeString(out, state.getCapabilityDigest());
            writeString(out, state.getReaderAttemptId());
            writeString(out, state.getCoordinatorEpoch());
            out.writeLong(state.getAppliedCommandWatermark());
            out.writeInt(state.getAppliedCommandGaps().size());
            for (Long gap : state.getAppliedCommandGaps()) {
                out.writeLong(gap);
            }
            out.writeLong(state.getNoMoreSplitsGeneration());
            ManagedSourceLifecycle.Snapshot lifecycle = state.getLifecycleSnapshot();
            out.writeInt(lifecycle.getMainState().getCode());
            out.writeInt(lifecycle.getSchemaState().getCode());
            writeString(out, lifecycle.getSchemaPhase());
            out.writeLong(lifecycle.getSchemaCheckpointId());
            out.writeLong(lifecycle.getSchemaRequestEpoch());
            out.writeBoolean(lifecycle.isCloseLatched());
            out.writeInt(state.getCheckpointOwnedSplitIds().size());
            for (String splitId : state.getCheckpointOwnedSplitIds()) {
                writeString(out, splitId);
            }
            List<byte[]> splitStates = state.getConnectorSplitStates();
            out.writeInt(splitStates.size());
            for (byte[] splitState : splitStates) {
                writeBytes(out, splitState);
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

    public static ManagedReaderCheckpointState deserialize(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            require(in.readInt() == MAGIC, "Not a managed Reader checkpoint state");
            int version = in.readInt();
            require(version == VERSION, "Unsupported managed Reader checkpoint version " + version);
            int payloadLength = in.readInt();
            require(
                    payloadLength >= 0 && payloadLength <= in.available() - Long.BYTES,
                    "Invalid managed Reader checkpoint payload length");
            byte[] payload = new byte[payloadLength];
            in.readFully(payload);
            long expectedChecksum = in.readLong();
            require(in.available() == 0, "Trailing managed Reader checkpoint bytes");
            require(
                    expectedChecksum == checksum(payload),
                    "Managed Reader checkpoint checksum mismatch");
            return deserializePayload(payload);
        }
    }

    public static boolean isManagedState(byte[] serialized) {
        if (serialized == null || serialized.length < Integer.BYTES) {
            return false;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            return in.readInt() == MAGIC;
        } catch (IOException e) {
            return false;
        }
    }

    private static ManagedReaderCheckpointState deserializePayload(byte[] payload)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            ManagedSourceRuntimeMode runtimeMode = ManagedSourceRuntimeMode.fromCode(in.readInt());
            int runtimeProtocolVersion = in.readInt();
            int connectorStateVersion = in.readInt();
            String capabilityDigest = readString(in);
            String readerAttemptId = readString(in);
            String coordinatorEpoch = readString(in);
            long appliedWatermark = in.readLong();
            int gapCount = readCount(in, "applied command gap");
            SortedSet<Long> appliedGaps = new TreeSet<>();
            for (int i = 0; i < gapCount; i++) {
                appliedGaps.add(in.readLong());
            }
            long noMoreSplitsGeneration = in.readLong();
            ManagedSourceLifecycleState mainState =
                    ManagedSourceLifecycleState.fromCode(in.readInt());
            SchemaChangeSubState schemaState = SchemaChangeSubState.fromCode(in.readInt());
            String schemaPhase = readString(in);
            long schemaCheckpointId = in.readLong();
            long schemaRequestEpoch = in.readLong();
            boolean closeLatched = in.readBoolean();
            ManagedSourceLifecycle.Snapshot lifecycle =
                    new ManagedSourceLifecycle.Snapshot(
                            mainState,
                            schemaState,
                            schemaPhase,
                            schemaCheckpointId,
                            schemaRequestEpoch,
                            closeLatched);
            int ownedSplitCount = readCount(in, "checkpoint-owned split");
            List<String> checkpointOwnedSplitIds = new ArrayList<>(ownedSplitCount);
            for (int i = 0; i < ownedSplitCount; i++) {
                checkpointOwnedSplitIds.add(readString(in));
            }
            int splitCount = readCount(in, "connector split state");
            List<byte[]> splitStates = new ArrayList<>(splitCount);
            for (int i = 0; i < splitCount; i++) {
                splitStates.add(readBytes(in));
            }
            require(in.available() == 0, "Trailing managed Reader checkpoint payload");
            return new ManagedReaderCheckpointState(
                    runtimeMode,
                    runtimeProtocolVersion,
                    connectorStateVersion,
                    capabilityDigest,
                    readerAttemptId,
                    coordinatorEpoch,
                    appliedWatermark,
                    appliedGaps,
                    noMoreSplitsGeneration,
                    lifecycle,
                    checkpointOwnedSplitIds,
                    splitStates);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid managed Reader checkpoint enum value", e);
        }
    }

    private static int readCount(DataInputStream in, String field) throws IOException {
        int count = in.readInt();
        require(count >= 0 && count <= MAX_COLLECTION_SIZE, "Invalid " + field + " count");
        return count;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        require(length >= 0 && length <= in.available(), "Invalid length-prefixed field");
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
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
