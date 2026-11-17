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

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Explicit payload codecs kept separate from Hazelcast operation serialization. */
public final class SourceCommandCodec {
    public static final int EMPTY_CODEC = 0;
    public static final int SPLIT_ASSIGNMENT_CODEC = 1;
    public static final int BARRIER_CODEC = 3;
    public static final int CHECKPOINT_ID_CODEC = 4;
    public static final int COMMAND_APPLIED_CODEC = 5;
    public static final int READER_CHECKPOINT_REPORT_CODEC = 6;
    public static final int RESTORED_SPLITS_CODEC = 7;
    public static final int NO_MORE_SPLITS_CODEC = 8;
    public static final int PAYLOAD_VERSION = 1;

    private static final int SPLIT_MAGIC = 0x53544153;
    private static final int BARRIER_MAGIC = 0x53544252;
    private static final int MAX_COLLECTION_SIZE = 100_000;

    private SourceCommandCodec() {}

    public static byte[] encodeSplitAssignment(List<String> splitIds, List<byte[]> splitBytes) {
        if (splitIds.size() != splitBytes.size()) {
            throw new IllegalArgumentException("Split ids and payloads must have the same size");
        }
        requireEncodableCount(splitIds.size(), "split assignment");
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(SPLIT_MAGIC);
            out.writeInt(PAYLOAD_VERSION);
            out.writeInt(splitIds.size());
            for (int i = 0; i < splitIds.size(); i++) {
                writeString(out, splitIds.get(i));
                writeBytes(out, splitBytes.get(i));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode Source split assignment", e);
        }
    }

    public static SplitAssignment decodeSplitAssignment(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            require(in.readInt() == SPLIT_MAGIC, "Invalid split assignment magic");
            require(in.readInt() == PAYLOAD_VERSION, "Unsupported split assignment version");
            int count = in.readInt();
            require(count >= 0 && count <= MAX_COLLECTION_SIZE, "Invalid split assignment count");
            List<String> splitIds = new ArrayList<>(count);
            List<byte[]> splitBytes = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                splitIds.add(readString(in));
                splitBytes.add(readBytes(in));
            }
            require(in.available() == 0, "Trailing split assignment payload");
            return new SplitAssignment(splitIds, splitBytes);
        }
    }

    /** Encodes the current engine checkpoint barrier without Java native serialization. */
    public static byte[] encodeBarrier(Barrier barrier) {
        if (!(barrier instanceof CheckpointBarrier)) {
            throw new IllegalArgumentException(
                    "Managed Source protocol version 1 only supports CheckpointBarrier");
        }
        CheckpointBarrier checkpointBarrier = (CheckpointBarrier) barrier;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(BARRIER_MAGIC);
            out.writeInt(PAYLOAD_VERSION);
            out.writeLong(checkpointBarrier.getId());
            out.writeLong(checkpointBarrier.getTimestamp());
            writeString(out, checkpointBarrier.getCheckpointType().getName());
            writeTaskLocations(out, checkpointBarrier.getPrepareCloseTasks());
            writeTaskLocations(out, checkpointBarrier.getClosedTasks());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode managed Source barrier", e);
        }
    }

    /** Decodes and validates a versioned checkpoint barrier. */
    public static Barrier decodeBarrier(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            require(in.readInt() == BARRIER_MAGIC, "Invalid managed Source barrier magic");
            require(in.readInt() == PAYLOAD_VERSION, "Unsupported managed Source barrier version");
            long checkpointId = in.readLong();
            long timestamp = in.readLong();
            CheckpointType checkpointType;
            try {
                checkpointType = CheckpointType.fromName(readString(in));
            } catch (IllegalArgumentException e) {
                throw new IOException("Unknown managed Source checkpoint type", e);
            }
            Set<TaskLocation> prepareCloseTasks = readTaskLocations(in);
            Set<TaskLocation> closedTasks = readTaskLocations(in);
            require(in.available() == 0, "Trailing managed Source barrier payload");
            try {
                return new CheckpointBarrier(
                        checkpointId, timestamp, checkpointType, prepareCloseTasks, closedTasks);
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid managed Source checkpoint barrier", e);
            }
        }
    }

    public static byte[] encodeCheckpointId(long checkpointId) {
        if (checkpointId < 0) {
            throw new IllegalArgumentException("Managed Source checkpoint id must be non-negative");
        }
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(Long.BYTES);
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(checkpointId);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode checkpoint id", e);
        }
    }

    public static long decodeCheckpointId(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            long checkpointId = in.readLong();
            require(checkpointId >= 0, "Managed Source checkpoint id must be non-negative");
            require(in.available() == 0, "Trailing checkpoint id payload");
            return checkpointId;
        }
    }

    /** Encodes the monotonic Reader generation for a reconstructable no-more-splits command. */
    public static byte[] encodeNoMoreSplits(long generation) {
        if (generation <= 0) {
            throw new IllegalArgumentException(
                    "Managed Source no-more-splits generation must be positive");
        }
        return encodeCheckpointId(generation);
    }

    /** Decodes and validates a no-more-splits Reader generation. */
    public static long decodeNoMoreSplits(byte[] payload) throws IOException {
        long generation = decodeCheckpointId(payload);
        require(generation > 0, "Managed Source no-more-splits generation must be positive");
        return generation;
    }

    public static byte[] encodeCommandApplied(String commandId, List<String> splitIds) {
        requireEncodableCount(splitIds.size(), "applied split");
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            writeString(out, commandId);
            out.writeInt(splitIds.size());
            for (String splitId : splitIds) {
                writeString(out, splitId);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode Source command application", e);
        }
    }

    public static CommandApplied decodeCommandApplied(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            String commandId = readString(in);
            int count = in.readInt();
            require(count >= 0 && count <= MAX_COLLECTION_SIZE, "Invalid applied split count");
            List<String> splitIds = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                splitIds.add(readString(in));
            }
            require(in.available() == 0, "Trailing command application payload");
            return new CommandApplied(commandId, splitIds);
        }
    }

    public static byte[] encodeReaderCheckpointReport(
            long checkpointId, long appliedWatermark, List<String> splitIds) {
        if (checkpointId < 0 || appliedWatermark < 0) {
            throw new IllegalArgumentException(
                    "Reader checkpoint id and applied watermark must be non-negative");
        }
        requireEncodableCount(splitIds.size(), "checkpoint split");
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(checkpointId);
            out.writeLong(appliedWatermark);
            out.writeInt(splitIds.size());
            for (String splitId : splitIds) {
                writeString(out, splitId);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode Reader checkpoint report", e);
        }
    }

    public static ReaderCheckpointReport decodeReaderCheckpointReport(byte[] payload)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            long checkpointId = in.readLong();
            long appliedWatermark = in.readLong();
            require(
                    checkpointId >= 0 && appliedWatermark >= 0,
                    "Reader checkpoint id and applied watermark must be non-negative");
            int count = in.readInt();
            require(count >= 0 && count <= MAX_COLLECTION_SIZE, "Invalid checkpoint split count");
            List<String> splitIds = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                splitIds.add(readString(in));
            }
            require(in.available() == 0, "Trailing Reader checkpoint report payload");
            return new ReaderCheckpointReport(checkpointId, appliedWatermark, splitIds);
        }
    }

    /**
     * Encodes connector split states together with assignment ownership proofs restored from the
     * same completed checkpoint.
     */
    public static byte[] encodeRestoredSplits(
            List<byte[]> connectorSplitStates, List<String> checkpointOwnedSplitIds) {
        requireEncodableCount(connectorSplitStates.size(), "restored connector split");
        requireEncodableCount(checkpointOwnedSplitIds.size(), "restored assignment proof");
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(connectorSplitStates.size());
            for (byte[] connectorSplitState : connectorSplitStates) {
                writeBytes(out, connectorSplitState);
            }
            out.writeInt(checkpointOwnedSplitIds.size());
            for (String splitId : checkpointOwnedSplitIds) {
                writeString(out, splitId);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode restored Source splits", e);
        }
    }

    /** Decodes a bounded restored-split transfer on the coordinator event-loop owner. */
    public static RestoredSplits decodeRestoredSplits(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int stateCount = readCount(in, "restored connector split");
            List<byte[]> connectorSplitStates = new ArrayList<>(stateCount);
            for (int i = 0; i < stateCount; i++) {
                connectorSplitStates.add(readBytes(in));
            }
            int proofCount = readCount(in, "restored assignment proof");
            List<String> checkpointOwnedSplitIds = new ArrayList<>(proofCount);
            for (int i = 0; i < proofCount; i++) {
                checkpointOwnedSplitIds.add(readString(in));
            }
            require(in.available() == 0, "Trailing restored Source split payload");
            return new RestoredSplits(connectorSplitStates, checkpointOwnedSplitIds);
        }
    }

    static int splitAssignmentBaseSize() {
        return Integer.BYTES * 3;
    }

    static int splitAssignmentEntrySize(String splitId, byte[] splitPayload) {
        return Math.addExact(encodedStringSize(splitId), encodedBytesSize(splitPayload));
    }

    static int readerCheckpointReportBaseSize() {
        return Long.BYTES * 2 + Integer.BYTES;
    }

    static int readerCheckpointReportEntrySize(String splitId) {
        return encodedStringSize(splitId);
    }

    static int restoredSplitsBaseSize() {
        return Integer.BYTES * 2;
    }

    static int restoredSplitStateEntrySize(byte[] state) {
        return encodedBytesSize(state);
    }

    static int restoredSplitProofEntrySize(String splitId) {
        return encodedStringSize(splitId);
    }

    static int maxCollectionSize() {
        return MAX_COLLECTION_SIZE;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeBytes(out, bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static int encodedStringSize(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Managed Source string payload must not be null");
        }
        return encodedBytesSize(value.getBytes(StandardCharsets.UTF_8));
    }

    private static int encodedBytesSize(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Managed Source byte payload must not be null");
        }
        return Math.addExact(Integer.BYTES, value.length);
    }

    private static void requireEncodableCount(int count, String field) {
        if (count < 0 || count > MAX_COLLECTION_SIZE) {
            throw new IllegalArgumentException("Invalid " + field + " count");
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        require(length >= 0 && length <= in.available(), "Invalid length-prefixed payload");
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static int readCount(DataInputStream in, String field) throws IOException {
        int count = in.readInt();
        require(count >= 0 && count <= MAX_COLLECTION_SIZE, "Invalid " + field + " count");
        return count;
    }

    private static void writeTaskLocations(DataOutputStream out, Set<TaskLocation> taskLocations)
            throws IOException {
        List<TaskLocation> ordered = new ArrayList<>(taskLocations);
        ordered.sort(Comparator.comparingLong(TaskLocation::getTaskID));
        out.writeInt(ordered.size());
        for (TaskLocation location : ordered) {
            TaskGroupLocation group = location.getTaskGroupLocation();
            out.writeLong(group.getJobId());
            out.writeInt(group.getPipelineId());
            out.writeLong(group.getTaskGroupId());
            out.writeLong(location.getTaskID());
        }
    }

    private static Set<TaskLocation> readTaskLocations(DataInputStream in) throws IOException {
        int count = readCount(in, "barrier task location");
        Set<TaskLocation> locations = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            TaskGroupLocation group =
                    new TaskGroupLocation(in.readLong(), in.readInt(), in.readLong());
            TaskLocation location = new TaskLocation();
            location.setTaskGroupLocation(group);
            location.setTaskID(in.readLong());
            require(locations.add(location), "Duplicate managed Source barrier task");
        }
        return locations;
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }

    /** Decoded connector split identifiers and serializer payloads. */
    public static final class SplitAssignment {
        private final List<String> splitIds;
        private final List<byte[]> splitBytes;

        private SplitAssignment(List<String> splitIds, List<byte[]> splitBytes) {
            this.splitIds = Collections.unmodifiableList(splitIds);
            this.splitBytes = Collections.unmodifiableList(splitBytes);
        }

        public List<String> getSplitIds() {
            return splitIds;
        }

        public List<byte[]> getSplitBytes() {
            return splitBytes;
        }
    }

    /** Applied assignment command identifier and stable split identifiers. */
    public static final class CommandApplied {
        private final String commandId;
        private final List<String> splitIds;

        private CommandApplied(String commandId, List<String> splitIds) {
            this.commandId = commandId;
            this.splitIds = Collections.unmodifiableList(splitIds);
        }

        public String getCommandId() {
            return commandId;
        }

        public List<String> getSplitIds() {
            return splitIds;
        }
    }

    /** Reader checkpoint inclusion proof delivered to the assignment tracker. */
    public static final class ReaderCheckpointReport {
        private final long checkpointId;
        private final long appliedWatermark;
        private final List<String> splitIds;

        private ReaderCheckpointReport(
                long checkpointId, long appliedWatermark, List<String> splitIds) {
            this.checkpointId = checkpointId;
            this.appliedWatermark = appliedWatermark;
            this.splitIds = Collections.unmodifiableList(splitIds);
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        public long getAppliedWatermark() {
            return appliedWatermark;
        }

        public List<String> getSplitIds() {
            return splitIds;
        }
    }

    /** Connector split states and engine-owned assignment proofs from one restore chunk. */
    public static final class RestoredSplits {
        private final List<byte[]> connectorSplitStates;
        private final List<String> checkpointOwnedSplitIds;

        private RestoredSplits(
                List<byte[]> connectorSplitStates, List<String> checkpointOwnedSplitIds) {
            this.connectorSplitStates = Collections.unmodifiableList(connectorSplitStates);
            this.checkpointOwnedSplitIds = Collections.unmodifiableList(checkpointOwnedSplitIds);
        }

        public List<byte[]> getConnectorSplitStates() {
            return connectorSplitStates;
        }

        public List<String> getCheckpointOwnedSplitIds() {
            return checkpointOwnedSplitIds;
        }
    }
}
