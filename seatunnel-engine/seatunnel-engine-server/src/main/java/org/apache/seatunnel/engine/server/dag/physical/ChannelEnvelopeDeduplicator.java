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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Duplicate detector for channel envelopes.
 *
 * <p>The same attempt and sequence may be observed more than once after retries. It is accepted
 * only when the canonical digest is identical; a different digest means the transport reused an
 * identity for conflicting content.
 */
public final class ChannelEnvelopeDeduplicator implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Upper bound for remembered envelope identities within one channel attempt. */
    private static final int DEFAULT_MAX_APPLIED_DIGESTS = 65536;

    /** Max remembered identities before a new envelope must fail fast. */
    private final int maxAppliedDigests;

    private final Map<EnvelopeIdentity, byte[]> appliedDigests = new ConcurrentHashMap<>();

    public ChannelEnvelopeDeduplicator() {
        this(DEFAULT_MAX_APPLIED_DIGESTS);
    }

    ChannelEnvelopeDeduplicator(int maxAppliedDigests) {
        if (maxAppliedDigests <= 0) {
            throw new IllegalArgumentException("maxAppliedDigests must be positive");
        }
        this.maxAppliedDigests = maxAppliedDigests;
    }

    /** Records an envelope and returns {@code true} when this is the first observation. */
    public boolean accept(ChannelEnvelope envelope) {
        EnvelopeIdentity identity =
                new EnvelopeIdentity(envelope.getAttemptId(), envelope.getSequence());
        byte[] digest = envelope.getCanonicalDigest();
        byte[] existing = appliedDigests.get(identity);
        if (existing != null) {
            return validateDuplicate(envelope, existing, digest);
        }
        if (appliedDigests.size() >= maxAppliedDigests) {
            throw new IllegalStateException(
                    "CHANNEL_ENVELOPE_DEDUP_LIMIT_EXCEEDED: max=" + maxAppliedDigests);
        }
        byte[] previous = appliedDigests.putIfAbsent(identity, digest);
        if (previous == null) {
            return true;
        }
        return validateDuplicate(envelope, previous, digest);
    }

    private static boolean validateDuplicate(
            ChannelEnvelope envelope, byte[] previous, byte[] digest) {
        if (!Arrays.equals(previous, digest)) {
            throw new IllegalStateException(
                    "CHANNEL_ENVELOPE_IDENTITY_DIGEST_CONFLICT: attempt="
                            + envelope.getAttemptId()
                            + ", sequence="
                            + envelope.getSequence());
        }
        return false;
    }

    private static final class EnvelopeIdentity implements Serializable {
        private static final long serialVersionUID = 1L;

        private final ChannelAttemptId attemptId;
        private final long sequence;

        private EnvelopeIdentity(ChannelAttemptId attemptId, long sequence) {
            this.attemptId = attemptId;
            this.sequence = sequence;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof EnvelopeIdentity)) {
                return false;
            }
            EnvelopeIdentity that = (EnvelopeIdentity) other;
            return sequence == that.sequence && attemptId.equals(that.attemptId);
        }

        @Override
        public int hashCode() {
            int result = attemptId.hashCode();
            result = 31 * result + Long.hashCode(sequence);
            return result;
        }
    }
}
