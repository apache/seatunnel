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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class SttrPayloadDecoderTest {

    @Test
    void testDecode() {
        long traceId = 123L;
        long startTs = 1700000000000L;
        byte[] payload = buildPayload(traceId, startTs);

        Assertions.assertTrue(SttrPayloadDecoder.isValid(payload));
        SttrPayloadDecoder.DecodedPayload decoded = SttrPayloadDecoder.decode(payload);
        Assertions.assertEquals(traceId, decoded.getTraceId());
        Assertions.assertEquals(startTs, decoded.getStartTsMs());
        Assertions.assertEquals(2, decoded.getEntryCount());

        List<TraceEntry> entries = decoded.getEntries();
        Assertions.assertEquals(2, entries.size());
        Assertions.assertEquals(0, entries.get(0).getIndex());
        Assertions.assertEquals(1, entries.get(0).getStage());
        Assertions.assertEquals(10L, entries.get(0).getTaskId());

        Assertions.assertEquals(1, entries.get(1).getIndex());
        Assertions.assertEquals(6, entries.get(1).getStage());
        Assertions.assertEquals(99L, entries.get(1).getTaskId());
    }

    @Test
    void testInvalidPayload() {
        byte[] bad = new byte[] {1, 2, 3};
        Assertions.assertFalse(SttrPayloadDecoder.isValid(bad));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> SttrPayloadDecoder.decode(bad));
    }

    @Test
    void testCountOverMaxIsInvalid() {
        byte[] payload = buildPayloadWithCount(2049);
        Assertions.assertFalse(SttrPayloadDecoder.isValid(payload));
    }

    private static byte[] buildPayload(long traceId, long startTsMs) {
        int headerLen = 4 + 2 + 8 + 8 + 2;
        int entryLen = 1 + 8 + 8;
        int count = 2;
        ByteBuffer buffer =
                ByteBuffer.allocate(headerLen + count * entryLen).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(0x53545452);
        buffer.putShort((short) 1);
        buffer.putLong(traceId);
        buffer.putLong(startTsMs);
        buffer.putShort((short) count);

        // entry 0: stage=1, taskId=10, ts=startTs+1
        buffer.put((byte) 1);
        buffer.putLong(10L);
        buffer.putLong(startTsMs + 1);
        // entry 1: stage=6, taskId=99, ts=startTs+2
        buffer.put((byte) 6);
        buffer.putLong(99L);
        buffer.putLong(startTsMs + 2);

        return buffer.array();
    }

    private static byte[] buildPayloadWithCount(int count) {
        int headerLen = 4 + 2 + 8 + 8 + 2;
        int entryLen = 1 + 8 + 8;
        ByteBuffer buffer =
                ByteBuffer.allocate(headerLen + count * entryLen).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(0x53545452);
        buffer.putShort((short) 1);
        buffer.putLong(1L);
        buffer.putLong(2L);
        buffer.putShort((short) count);
        return buffer.array();
    }
}
