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

import lombok.ToString;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * The content of a v2 {@code _metadata} file, the commit point of a checkpoint.
 *
 * <p>Binary layout (big endian):
 *
 * <pre>
 * [MAGIC "STCKMD": 6 bytes][version: int][flags: int][payloadLength: int][CRC32(payload): int]
 * [payload: payloadLength bytes]
 * </pre>
 *
 * <p>The payload is opaque to the storage layer; the engine stores the serialized checkpoint (whose
 * subtask states are {@link StateHandle}s) in it. The CRC lets readers detect partially written
 * files on object stores that have no atomic rename.
 */
@ToString(exclude = "payload")
public final class MetadataEnvelope {

    public static final int CURRENT_VERSION = 2;

    static final byte[] MAGIC = "STCKMD".getBytes(StandardCharsets.US_ASCII);

    static final int HEADER_LENGTH = MAGIC.length + 4 * Integer.BYTES;

    private final int version;

    private final int flags;

    private final byte[] payload;

    public MetadataEnvelope(int version, int flags, byte[] payload) {
        this.version = version;
        this.flags = flags;
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    /** Creates an envelope of the {@link #CURRENT_VERSION} without flags. */
    public static MetadataEnvelope of(byte[] payload) {
        return new MetadataEnvelope(CURRENT_VERSION, 0, payload);
    }

    public int getVersion() {
        return version;
    }

    public int getFlags() {
        return flags;
    }

    public byte[] getPayload() {
        return payload;
    }

    /** Encodes the envelope into the {@code _metadata} file format. */
    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH + payload.length);
        buffer.put(MAGIC);
        buffer.putInt(version);
        buffer.putInt(flags);
        buffer.putInt(payload.length);
        buffer.putInt(crc32(payload));
        buffer.put(payload);
        return buffer.array();
    }

    /**
     * Decodes and validates a {@code _metadata} file.
     *
     * @param data the file content
     * @param location the file location, used in error messages only
     * @return the decoded envelope
     * @throws CheckpointStorageException if the magic, version, length or CRC does not match
     */
    public static MetadataEnvelope decode(byte[] data, String location)
            throws CheckpointStorageException {
        if (data == null || data.length < HEADER_LENGTH) {
            throw new CheckpointStorageException(
                    String.format(
                            "Checkpoint metadata %s is truncated: %d bytes, header needs %d",
                            location, data == null ? 0 : data.length, HEADER_LENGTH));
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] magic = new byte[MAGIC.length];
        buffer.get(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new CheckpointStorageException(
                    String.format("Checkpoint metadata %s has an invalid magic header", location));
        }
        int version = buffer.getInt();
        if (version != CURRENT_VERSION) {
            throw new CheckpointStorageException(
                    String.format(
                            "Checkpoint metadata %s has unsupported version %d, expected %d",
                            location, version, CURRENT_VERSION));
        }
        int flags = buffer.getInt();
        int payloadLength = buffer.getInt();
        int expectedCrc = buffer.getInt();
        if (payloadLength < 0 || payloadLength != buffer.remaining()) {
            throw new CheckpointStorageException(
                    String.format(
                            "Checkpoint metadata %s declares %d payload bytes but has %d",
                            location, payloadLength, buffer.remaining()));
        }
        byte[] payload = new byte[payloadLength];
        buffer.get(payload);
        int actualCrc = crc32(payload);
        if (actualCrc != expectedCrc) {
            throw new CheckpointStorageException(
                    String.format(
                            "Checkpoint metadata %s failed the CRC check (expected %08x, actual %08x)",
                            location, expectedCrc, actualCrc));
        }
        return new MetadataEnvelope(version, flags, payload);
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
