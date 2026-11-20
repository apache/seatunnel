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

package org.apache.seatunnel.engine.checkpoint.storage.state;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests the v2 (STIP-45) chunk and metadata formats and the directory layout. */
public class CheckpointStateFormatTest {

    @Test
    public void testChunkRoundTripKeepsOrderAndNullSegments() throws Exception {
        List<byte[]> states = Arrays.asList(bytes("a"), null, new byte[0], bytes("split-42"));

        List<byte[]> decoded = ChunkCodec.decode(ChunkCodec.encode(states), "test");

        Assertions.assertEquals(4, decoded.size());
        Assertions.assertArrayEquals(bytes("a"), decoded.get(0));
        Assertions.assertNull(decoded.get(1));
        Assertions.assertArrayEquals(new byte[0], decoded.get(2));
        Assertions.assertArrayEquals(bytes("split-42"), decoded.get(3));
    }

    @Test
    public void testEmptyChunkRoundTrip() throws Exception {
        Assertions.assertTrue(
                ChunkCodec.decode(ChunkCodec.encode(Collections.emptyList()), "test").isEmpty());
    }

    @Test
    public void testTruncatedOrPaddedChunkIsRejected() {
        byte[] encoded = ChunkCodec.encode(Arrays.asList(bytes("abc"), bytes("def")));

        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        Assertions.assertThrows(
                CheckpointStorageException.class, () -> ChunkCodec.decode(truncated, "test"));

        byte[] padded = Arrays.copyOf(encoded, encoded.length + 1);
        Assertions.assertThrows(
                CheckpointStorageException.class, () -> ChunkCodec.decode(padded, "test"));

        byte[] hugeCount = encoded.clone();
        hugeCount[0] = 0x7f;
        Assertions.assertThrows(
                CheckpointStorageException.class, () -> ChunkCodec.decode(hugeCount, "test"));
    }

    @Test
    public void testMetadataRoundTrip() throws Exception {
        byte[] payload = bytes("serialized-completed-checkpoint");

        MetadataEnvelope decoded =
                MetadataEnvelope.decode(MetadataEnvelope.of(payload).encode(), "test");

        Assertions.assertEquals(MetadataEnvelope.CURRENT_VERSION, decoded.getVersion());
        Assertions.assertEquals(0, decoded.getFlags());
        Assertions.assertArrayEquals(payload, decoded.getPayload());
    }

    @Test
    public void testCorruptedMetadataIsRejected() {
        byte[] encoded = MetadataEnvelope.of(bytes("payload")).encode();

        byte[] flippedPayload = encoded.clone();
        flippedPayload[flippedPayload.length - 1] ^= 0x01;
        CheckpointStorageException crcFailure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> MetadataEnvelope.decode(flippedPayload, "chk-1/_metadata"));
        Assertions.assertTrue(crcFailure.getMessage().contains("CRC"));
        Assertions.assertTrue(crcFailure.getMessage().contains("chk-1/_metadata"));

        byte[] badMagic = encoded.clone();
        badMagic[0] = 'X';
        Assertions.assertThrows(
                CheckpointStorageException.class, () -> MetadataEnvelope.decode(badMagic, "t"));

        byte[] partial = Arrays.copyOf(encoded, encoded.length - 3);
        Assertions.assertThrows(
                CheckpointStorageException.class, () -> MetadataEnvelope.decode(partial, "t"));

        Assertions.assertThrows(
                CheckpointStorageException.class, () -> MetadataEnvelope.decode(new byte[3], "t"));

        byte[] futureVersion = new MetadataEnvelope(3, 0, bytes("p")).encode();
        Assertions.assertThrows(
                CheckpointStorageException.class,
                () -> MetadataEnvelope.decode(futureVersion, "t"));
    }

    @Test
    public void testLayoutPaths() {
        Assertions.assertEquals("100/1/chk-7", CheckpointFileLayout.checkpointDir("100", "1", 7));
        Assertions.assertEquals(
                "100/1/chk-7/_metadata", CheckpointFileLayout.metadataPath("100", "1", 7));

        String subtask =
                CheckpointFileLayout.chunkPath(
                        new ChunkDescriptor("100", "1", 7, "ActionStateKey - Source[0]", 3));
        Assertions.assertTrue(
                subtask.matches("100/1/chk-7/subtask-[0-9a-f]{16}-3\\.chunk"), subtask);

        String coordinator =
                CheckpointFileLayout.chunkPath(
                        new ChunkDescriptor("100", "1", 7, "ActionStateKey - Source[0]", -1));
        Assertions.assertTrue(coordinator.endsWith("-coordinator.chunk"), coordinator);

        // deterministic: the same descriptor always maps to the same location
        Assertions.assertEquals(
                subtask,
                CheckpointFileLayout.chunkPath(
                        new ChunkDescriptor("100", "1", 7, "ActionStateKey - Source[0]", 3)));
        Assertions.assertNotEquals(
                subtask,
                CheckpointFileLayout.chunkPath(
                        new ChunkDescriptor("100", "1", 7, "ActionStateKey - Sink[0]", 3)));
    }

    @Test
    public void testLayoutRejectsUnsafeSegments() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointFileLayout.checkpointDir("../etc", "1", 1));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointFileLayout.checkpointDir("100", "..", 1));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointFileLayout.checkpointDir("100", "1", -1));
    }

    @Test
    public void testParseCheckpointIdOnlyAcceptsEngineDirectories() {
        Assertions.assertEquals(12, CheckpointFileLayout.parseCheckpointId("chk-12"));
        Assertions.assertEquals(-1, CheckpointFileLayout.parseCheckpointId("chk-"));
        Assertions.assertEquals(-1, CheckpointFileLayout.parseCheckpointId("chk-12.tmp"));
        Assertions.assertEquals(-1, CheckpointFileLayout.parseCheckpointId("chk--1"));
        Assertions.assertEquals(-1, CheckpointFileLayout.parseCheckpointId("savepoint-1"));
        Assertions.assertEquals(
                -1, CheckpointFileLayout.parseCheckpointId("chk-99999999999999999999"));
        Assertions.assertEquals(-1, CheckpointFileLayout.parseCheckpointId(null));
    }

    @Test
    public void testInlineStateSizeCountsBytes() {
        Assertions.assertEquals(
                11,
                new InlineStateHandle(Arrays.asList(bytes("abc"), null, bytes("12345678")))
                        .getStateSize());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
