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

package org.apache.seatunnel.trace.collector.payload;

import org.apache.seatunnel.trace.collector.model.TraceEntry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public final class SttrPayloadDecoder {
    private SttrPayloadDecoder() {}

    private static final int MAGIC = 0x53545452; // 'STTR'
    private static final short VERSION = 1;
    private static final int HEADER_LENGTH = 4 + 2 + 8 + 8 + 2;
    private static final int ENTRY_LENGTH = 1 + 8 + 8;
    private static final int MAX_ENTRY_COUNT = 2048;

    private static final int TRACE_ID_OFFSET = 4 + 2;
    private static final int START_TS_OFFSET = TRACE_ID_OFFSET + 8;
    private static final int COUNT_OFFSET = START_TS_OFFSET + 8;

    public static DecodedPayload decode(byte[] payload) {
        if (!isValid(payload)) {
            throw new IllegalArgumentException("Invalid STTR payload");
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        long traceId = buffer.getLong(TRACE_ID_OFFSET);
        long startTsMs = buffer.getLong(START_TS_OFFSET);
        int count = readUnsignedShort(payload, COUNT_OFFSET);
        List<TraceEntry> entries = new ArrayList<>(count);

        int pos = HEADER_LENGTH;
        for (int i = 0; i < count; i++) {
            int stage = payload[pos] & 0xFF;
            long taskId = buffer.getLong(pos + 1);
            long tsMs = buffer.getLong(pos + 1 + 8);
            entries.add(new TraceEntry(i, stage, taskId, tsMs, null, null, null));
            pos += ENTRY_LENGTH;
        }
        return new DecodedPayload(traceId, startTsMs, count, entries);
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
        if (count > MAX_ENTRY_COUNT) {
            return false;
        }
        long expected = (long) HEADER_LENGTH + (long) count * ENTRY_LENGTH;
        return expected == payload.length;
    }

    private static int readUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }

    public static final class DecodedPayload {
        private final long traceId;
        private final long startTsMs;
        private final int entryCount;
        private final List<TraceEntry> entries;

        DecodedPayload(long traceId, long startTsMs, int entryCount, List<TraceEntry> entries) {
            this.traceId = traceId;
            this.startTsMs = startTsMs;
            this.entryCount = entryCount;
            this.entries = entries;
        }

        public long getTraceId() {
            return traceId;
        }

        public long getStartTsMs() {
            return startTsMs;
        }

        public int getEntryCount() {
            return entryCount;
        }

        public List<TraceEntry> getEntries() {
            return entries;
        }
    }
}
