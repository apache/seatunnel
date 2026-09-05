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

package org.apache.seatunnel.engine.server.dag.physical;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Canonical data/control envelope for dynamic lookup channel messages.
 *
 * <p>The payload digest is part of duplicate detection. Reusing the same attempt and sequence with
 * different payload bytes is a protocol violation and must fail fast.
 */
public final class ChannelEnvelope implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ChannelAttemptId attemptId;
    private final long sequence;
    private final EnvelopeCategory category;
    private final byte[] payload;
    private final byte[] canonicalDigest;

    public ChannelEnvelope(
            ChannelAttemptId attemptId, long sequence, EnvelopeCategory category, byte[] payload) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be non-negative: " + sequence);
        }
        this.attemptId = Objects.requireNonNull(attemptId, "attemptId");
        this.sequence = sequence;
        this.category = Objects.requireNonNull(category, "category");
        this.payload = copy(Objects.requireNonNull(payload, "payload"));
        this.canonicalDigest = digest(attemptId, sequence, category, this.payload);
    }

    public ChannelAttemptId getAttemptId() {
        return attemptId;
    }

    public long getSequence() {
        return sequence;
    }

    public EnvelopeCategory getCategory() {
        return category;
    }

    public byte[] getPayload() {
        return copy(payload);
    }

    public byte[] getCanonicalDigest() {
        return copy(canonicalDigest);
    }

    private static byte[] digest(
            ChannelAttemptId attemptId, long sequence, EnvelopeCategory category, byte[] payload) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(attemptId.getChannelKey().toCanonicalBytes());
            messageDigest.update(longBytes(attemptId.getJobExecutionEpoch()));
            messageDigest.update(longBytes(attemptId.getSourceDeploymentAttempt()));
            messageDigest.update(longBytes(attemptId.getTargetDeploymentAttempt()));
            messageDigest.update(longBytes(attemptId.getConnectionEpoch()));
            messageDigest.update(longBytes(sequence));
            messageDigest.update(intBytes(category.getWireCode()));
            messageDigest.update(intBytes(payload.length));
            messageDigest.update(payload);
            return messageDigest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static byte[] copy(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /** Envelope payload category. */
    public enum EnvelopeCategory {
        DATA(0),
        CONTROL(1),
        BARRIER(2);

        private final int wireCode;

        EnvelopeCategory(int wireCode) {
            this.wireCode = wireCode;
        }

        public int getWireCode() {
            return wireCode;
        }
    }
}
