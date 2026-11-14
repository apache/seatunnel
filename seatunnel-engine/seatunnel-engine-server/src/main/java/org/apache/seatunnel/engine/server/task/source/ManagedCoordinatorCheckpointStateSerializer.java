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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.CRC32;

/** Version 1 serializer for managed Source coordinator checkpoint state. */
public final class ManagedCoordinatorCheckpointStateSerializer {
    private static final int MAGIC = 0x53544d43;
    private static final int VERSION = 1;
    private static final int MAX_READERS = 1_000_000;

    private ManagedCoordinatorCheckpointStateSerializer() {}

    public static byte[] serialize(ManagedCoordinatorCheckpointState state) throws IOException {
        byte[] payload;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(state.getRuntimeMode().getCode());
            out.writeInt(state.getRuntimeProtocolVersion());
            out.writeInt(state.getConnectorStateVersion());
            writeString(out, state.getCapabilityDigest());
            out.writeInt(state.getSourceParallelism());
            writeBytes(out, state.getConnectorEnumeratorState());
            writeBytes(out, state.getAssignmentTrackerState());
            out.writeInt(state.getNextReaderCommandSequences().size());
            for (Map.Entry<Integer, Long> sequence :
                    new TreeMap<>(state.getNextReaderCommandSequences()).entrySet()) {
                out.writeInt(sequence.getKey());
                out.writeLong(sequence.getValue());
            }
            out.writeInt(state.getNoMoreSplitsSubtasks().size());
            for (Integer subtask : new TreeSet<>(state.getNoMoreSplitsSubtasks())) {
                out.writeInt(subtask);
            }
            out.writeBoolean(state.isAllReadersNoMoreSplits());
            out.writeLong(state.getNextNoMoreSplitsGeneration());
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

    public static ManagedCoordinatorCheckpointState deserialize(byte[] serialized)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            require(in.readInt() == MAGIC, "Not a managed coordinator checkpoint");
            int version = in.readInt();
            require(version == VERSION, "Unsupported managed coordinator version " + version);
            int payloadLength = in.readInt();
            require(
                    payloadLength >= 0 && payloadLength <= in.available() - Long.BYTES,
                    "Invalid managed coordinator payload length");
            byte[] payload = new byte[payloadLength];
            in.readFully(payload);
            require(in.readLong() == checksum(payload), "Managed coordinator checksum mismatch");
            require(in.available() == 0, "Trailing managed coordinator bytes");
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

    private static ManagedCoordinatorCheckpointState deserializePayload(byte[] payload)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            ManagedSourceRuntimeMode mode = ManagedSourceRuntimeMode.fromCode(in.readInt());
            int protocolVersion = in.readInt();
            int connectorStateVersion = in.readInt();
            String digest = readString(in);
            int sourceParallelism = in.readInt();
            require(sourceParallelism > 0, "Invalid managed Source parallelism");
            byte[] connectorState = readBytes(in);
            byte[] trackerState = readBytes(in);
            int readerCount = in.readInt();
            require(
                    readerCount >= 0 && readerCount <= MAX_READERS,
                    "Invalid reader sequence count");
            Map<Integer, Long> sequences = new HashMap<>(readerCount);
            for (int i = 0; i < readerCount; i++) {
                int subtask = in.readInt();
                long sequence = in.readLong();
                require(subtask >= 0, "Invalid Reader command subtask");
                require(sequence > 0, "Invalid Reader command sequence");
                require(sequences.put(subtask, sequence) == null, "Duplicate Reader sequence");
            }
            int noMoreCount = in.readInt();
            require(
                    noMoreCount >= 0 && noMoreCount <= MAX_READERS,
                    "Invalid no-more-splits subtask count");
            Set<Integer> noMoreSubtasks = new HashSet<>(noMoreCount);
            for (int i = 0; i < noMoreCount; i++) {
                int subtask = in.readInt();
                require(subtask >= 0, "Invalid no-more-splits subtask");
                require(noMoreSubtasks.add(subtask), "Duplicate no-more-splits subtask");
            }
            boolean allReadersNoMoreSplits = in.readBoolean();
            long nextNoMoreSplitsGeneration = in.readLong();
            require(nextNoMoreSplitsGeneration >= 0, "Invalid no-more-splits generation");
            require(in.available() == 0, "Trailing managed coordinator payload");
            return new ManagedCoordinatorCheckpointState(
                    mode,
                    protocolVersion,
                    connectorStateVersion,
                    digest,
                    sourceParallelism,
                    connectorState,
                    trackerState,
                    sequences,
                    noMoreSubtasks,
                    allReadersNoMoreSplits,
                    nextNoMoreSplitsGeneration);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid managed coordinator runtime mode", e);
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
        require(length >= 0 && length <= in.available(), "Invalid managed coordinator field");
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
