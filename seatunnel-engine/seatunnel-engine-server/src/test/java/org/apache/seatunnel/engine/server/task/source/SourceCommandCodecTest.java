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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

class SourceCommandCodecTest {

    @Test
    void shouldRoundTripVersionedCommandPayloads() throws Exception {
        SourceCommandCodec.SplitAssignment assignment =
                SourceCommandCodec.decodeSplitAssignment(
                        SourceCommandCodec.encodeSplitAssignment(
                                Arrays.asList("split-1", "split-2"),
                                Arrays.asList(new byte[] {1}, new byte[] {2})));
        Assertions.assertEquals(Arrays.asList("split-1", "split-2"), assignment.getSplitIds());
        Assertions.assertArrayEquals(new byte[] {2}, assignment.getSplitBytes().get(1));

        SourceCommandCodec.CommandApplied applied =
                SourceCommandCodec.decodeCommandApplied(
                        SourceCommandCodec.encodeCommandApplied(
                                "command-1", Collections.singletonList("split-1")));
        Assertions.assertEquals("command-1", applied.getCommandId());

        SourceCommandCodec.ReaderCheckpointReport report =
                SourceCommandCodec.decodeReaderCheckpointReport(
                        SourceCommandCodec.encodeReaderCheckpointReport(
                                9L, 3L, Collections.singletonList("split-1")));
        Assertions.assertEquals(9L, report.getCheckpointId());
        Assertions.assertEquals(3L, report.getAppliedWatermark());

        SourceCommandCodec.RestoredSplits restored =
                SourceCommandCodec.decodeRestoredSplits(
                        SourceCommandCodec.encodeRestoredSplits(
                                Collections.singletonList(new byte[] {3}),
                                Collections.singletonList("split-1")));
        Assertions.assertArrayEquals(new byte[] {3}, restored.getConnectorSplitStates().get(0));
        Assertions.assertEquals(
                Collections.singletonList("split-1"), restored.getCheckpointOwnedSplitIds());
        Assertions.assertEquals(
                4L,
                SourceCommandCodec.decodeNoMoreSplits(SourceCommandCodec.encodeNoMoreSplits(4L)));
    }

    @Test
    void shouldRejectTrailingOrInvalidPayloadData() {
        byte[] valid =
                SourceCommandCodec.encodeCommandApplied(
                        "command-1", Collections.singletonList("split-1"));
        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);

        Assertions.assertThrows(
                IOException.class, () -> SourceCommandCodec.decodeCommandApplied(trailing));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> SourceCommandCodec.encodeNoMoreSplits(0L));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> SourceCommandCodec.encodeCheckpointId(-1L));
        Assertions.assertThrows(
                IOException.class,
                () ->
                        SourceCommandCodec.decodeCheckpointId(
                                ByteBuffer.allocate(Long.BYTES).putLong(-1L).array()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SourceCommandCodec.encodeReaderCheckpointReport(
                                1L, -1L, Collections.emptyList()));
        Assertions.assertThrows(
                IOException.class,
                () ->
                        SourceCommandCodec.decodeReaderCheckpointReport(
                                ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES)
                                        .putLong(1L)
                                        .putLong(-1L)
                                        .putInt(0)
                                        .array()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SourceCommandCodec.encodeSplitAssignment(
                                Collections.singletonList("split-1"), Collections.emptyList()));
    }
}
