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

package org.apache.seatunnel.engine.server.trace;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Encodes and decodes the compact binary payload stored on sampled rows for stain tracing. */
public final class StainTracePayload {
    private StainTracePayload() {}

    public static final int MAGIC = 0x53545452; // 'STTR'
    public static final short VERSION = 1;
    public static final int HEADER_LENGTH = 4 + 2 + 8 + 8 + 2;
    public static final int ENTRY_LENGTH = 1 + 8 + 8;

    private static final int TRACE_ID_OFFSET = 4 + 2;
    private static final int START_TS_OFFSET = TRACE_ID_OFFSET + 8;
    private static final int COUNT_OFFSET = START_TS_OFFSET + 8;

    /**
     * Creates a new payload header for a freshly sampled row before any stage entries are appended.
     */
    public static byte[] init(long traceId, long startTsMs) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(MAGIC);
        buffer.putShort(VERSION);
        buffer.putLong(traceId);
        buffer.putLong(startTsMs);
        buffer.putShort((short) 0);
        return buffer.array();
    }

    public static long readTraceId(byte[] payload) {
        if (!isValid(payload)) {
            throw new IllegalArgumentException("Invalid stain trace payload");
        }
        return ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong(TRACE_ID_OFFSET);
    }

    public static boolean isValid(byte[] payload) {
        if (payload == null || payload.length < HEADER_LENGTH) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int magic = buffer.getInt(0);
        if (magic != MAGIC) {
            return false;
        }
        short ver = buffer.getShort(4);
        if (ver != VERSION) {
            return false;
        }
        int count = readUnsignedShort(payload, COUNT_OFFSET);
        long expected = (long) HEADER_LENGTH + (long) count * ENTRY_LENGTH;
        return expected == payload.length;
    }

    /**
     * Appends one stage entry unless the payload is invalid or already reached its entry budget.
     */
    public static AppendResult append(
            byte[] payload, StainTraceStage stage, long taskId, long tsMs, int maxEntries) {
        if (!isValid(payload)) {
            return AppendResult.invalid(payload);
        }
        int count = readUnsignedShort(payload, COUNT_OFFSET);
        if (count >= maxEntries) {
            return AppendResult.truncated(payload);
        }
        byte[] newPayload = Arrays.copyOf(payload, payload.length + ENTRY_LENGTH);
        ByteBuffer buffer = ByteBuffer.wrap(newPayload).order(ByteOrder.BIG_ENDIAN);
        buffer.position(payload.length);
        buffer.put(stage.getCode());
        buffer.putLong(taskId);
        buffer.putLong(tsMs);
        writeUnsignedShort(newPayload, COUNT_OFFSET, count + 1);
        return AppendResult.appended(newPayload);
    }

    /** A single decoded payload entry: stage code, task ID, and timestamp in millis. */
    public static final class Entry {
        public final int stageCode;
        public final long taskId;
        public final long tsMs;

        Entry(int stageCode, long taskId, long tsMs) {
            this.stageCode = stageCode;
            this.taskId = taskId;
            this.tsMs = tsMs;
        }
    }

    /** Decode the start timestamp (ms) from the payload header. */
    public static long readStartTsMs(byte[] payload) {
        return ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong(START_TS_OFFSET);
    }

    /** Decodes all stage entries from the payload and returns an empty list for invalid bytes. */
    public static List<Entry> readEntries(byte[] payload) {
        if (!isValid(payload)) {
            return new ArrayList<>();
        }
        int count = readUnsignedShort(payload, COUNT_OFFSET);
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        List<Entry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int offset = HEADER_LENGTH + i * ENTRY_LENGTH;
            int stageCode = payload[offset] & 0xFF;
            long taskId = buf.getLong(offset + 1);
            long tsMs = buf.getLong(offset + 9);
            entries.add(new Entry(stageCode, taskId, tsMs));
        }
        return entries;
    }

    private static int readUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }

    private static void writeUnsignedShort(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) ((value >>> 8) & 0xFF);
        bytes[offset + 1] = (byte) (value & 0xFF);
    }

    public enum AppendStatus {
        APPENDED,
        TRUNCATED,
        INVALID
    }

    public static final class AppendResult {
        private final byte[] payload;
        private final AppendStatus status;

        private AppendResult(byte[] payload, AppendStatus status) {
            this.payload = payload;
            this.status = status;
        }

        public static AppendResult appended(byte[] payload) {
            return new AppendResult(payload, AppendStatus.APPENDED);
        }

        public static AppendResult truncated(byte[] payload) {
            return new AppendResult(payload, AppendStatus.TRUNCATED);
        }

        public static AppendResult invalid(byte[] payload) {
            return new AppendResult(payload, AppendStatus.INVALID);
        }

        public byte[] getPayload() {
            return payload;
        }

        public AppendStatus getStatus() {
            return status;
        }
    }
}
