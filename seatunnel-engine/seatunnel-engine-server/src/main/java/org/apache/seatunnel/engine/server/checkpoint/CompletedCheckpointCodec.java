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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.serializer.api.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Stable envelope for persisted {@link CompletedCheckpoint} payloads.
 *
 * <p>The storage API still persists {@code PipelineState.states}; this codec only changes the bytes
 * inside that field. If the magic is absent, restore intentionally falls back to the legacy raw
 * serializer payload so already-written checkpoints remain readable.
 */
public final class CompletedCheckpointCodec {

    private static final int MAGIC = 0x53544350;
    private static final int FORMAT_VERSION = 1;
    private static final int DIGEST_LENGTH = 32;
    private static final String DIGEST_ALGORITHM = "SHA-256";

    private CompletedCheckpointCodec() {}

    /**
     * Serializes a checkpoint.
     *
     * <p>Normal checkpoints keep the legacy raw payload so ordinary jobs remain downgrade-readable.
     * Dynamic lookup anchor checkpoints use the versioned envelope because their completed metadata
     * is part of the new fact-position durability protocol.
     */
    public static byte[] encode(CompletedCheckpoint checkpoint, Serializer serializer)
            throws IOException {
        byte[] payload = serializer.serialize(checkpoint);
        if (checkpoint.getCheckpointIntent().isNormalCheckpoint()) {
            return payload;
        }
        byte[] digest = sha256(payload);
        return ByteBuffer.allocate(Integer.BYTES * 3 + payload.length + digest.length)
                .putInt(MAGIC)
                .putInt(FORMAT_VERSION)
                .putInt(payload.length)
                .put(payload)
                .put(digest)
                .array();
    }

    /** Decodes either the new envelope format or a legacy raw checkpoint payload. */
    public static CompletedCheckpoint decode(byte[] states, Serializer serializer)
            throws IOException {
        if (states == null || states.length < Integer.BYTES) {
            throw new IOException("Checkpoint payload is empty or too small");
        }
        ByteBuffer buffer = ByteBuffer.wrap(states);
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            return serializer.deserialize(states, CompletedCheckpoint.class);
        }
        if (buffer.remaining() < Integer.BYTES * 2 + DIGEST_LENGTH) {
            throw new IOException("Versioned checkpoint envelope is truncated");
        }
        int formatVersion = buffer.getInt();
        if (formatVersion != FORMAT_VERSION) {
            throw new IOException(
                    "Unsupported checkpoint envelope format version: " + formatVersion);
        }
        int payloadLength = buffer.getInt();
        if (payloadLength < 0 || buffer.remaining() != payloadLength + DIGEST_LENGTH) {
            throw new IOException("Invalid checkpoint envelope payload length: " + payloadLength);
        }
        byte[] payload = new byte[payloadLength];
        buffer.get(payload);
        byte[] expectedDigest = new byte[DIGEST_LENGTH];
        buffer.get(expectedDigest);
        byte[] actualDigest = sha256(payload);
        if (!Arrays.equals(expectedDigest, actualDigest)) {
            throw new IOException("Checkpoint envelope SHA-256 digest mismatch");
        }
        return serializer.deserialize(payload, CompletedCheckpoint.class);
    }

    private static byte[] sha256(byte[] payload) throws IOException {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM).digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is required by the Java runtime", e);
        }
    }
}
