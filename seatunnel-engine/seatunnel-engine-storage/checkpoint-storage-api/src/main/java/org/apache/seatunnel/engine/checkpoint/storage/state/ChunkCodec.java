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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CorruptCheckpointException;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Encodes the state segments of one subtask action into the chunk file format:
 *
 * <pre>
 * [entryCount: int] { [length: int][bytes] }*
 * </pre>
 *
 * <p>A {@code null} segment is written with length {@code -1} so that restored state lists keep
 * their positions exactly as they were snapshotted.
 */
public final class ChunkCodec {

    private static final int NULL_LENGTH = -1;

    private ChunkCodec() {}

    /** Returns the CRC32 of an encoded chunk, as stored in {@link FileChunkStateHandle}. */
    public static long checksum(byte[] encoded) {
        CRC32 crc = new CRC32();
        crc.update(encoded, 0, encoded.length);
        return crc.getValue();
    }

    /** Encodes the segments into a chunk file body. */
    public static byte[] encode(List<byte[]> states) {
        long total = Integer.BYTES;
        for (byte[] segment : states) {
            total += Integer.BYTES + (segment == null ? 0 : segment.length);
        }
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Chunk exceeds the maximum supported size of 2GB: " + total + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) total);
        buffer.putInt(states.size());
        for (byte[] segment : states) {
            if (segment == null) {
                buffer.putInt(NULL_LENGTH);
            } else {
                buffer.putInt(segment.length);
                buffer.put(segment);
            }
        }
        return buffer.array();
    }

    /**
     * Decodes a chunk file body.
     *
     * @param data chunk file content
     * @param location chunk location, used in error messages only
     * @return the state segments, in snapshot order
     * @throws CorruptCheckpointException if the content is truncated or malformed
     */
    public static List<byte[]> decode(byte[] data, String location)
            throws CheckpointStorageException {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int count = buffer.getInt();
            // every entry needs at least its 4-byte length, which bounds a corrupt count
            if (count < 0 || (long) count * Integer.BYTES > buffer.remaining()) {
                throw new CorruptCheckpointException(
                        String.format(
                                "Checkpoint chunk %s declares an invalid entry count %d",
                                location, count));
            }
            if (count == 0) {
                ensureFullyConsumed(buffer, location);
                return Collections.emptyList();
            }
            List<byte[]> states = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int length = buffer.getInt();
                if (length == NULL_LENGTH) {
                    states.add(null);
                    continue;
                }
                if (length < 0 || length > buffer.remaining()) {
                    throw new CorruptCheckpointException(
                            String.format(
                                    "Checkpoint chunk %s entry %d has invalid length %d",
                                    location, i, length));
                }
                byte[] segment = new byte[length];
                buffer.get(segment);
                states.add(segment);
            }
            ensureFullyConsumed(buffer, location);
            return states;
        } catch (BufferUnderflowException e) {
            throw new CorruptCheckpointException(
                    String.format("Checkpoint chunk %s is truncated", location), e);
        }
    }

    private static void ensureFullyConsumed(ByteBuffer buffer, String location)
            throws CheckpointStorageException {
        if (buffer.hasRemaining()) {
            throw new CorruptCheckpointException(
                    String.format(
                            "Checkpoint chunk %s has %d unexpected trailing bytes",
                            location, buffer.remaining()));
        }
    }
}
