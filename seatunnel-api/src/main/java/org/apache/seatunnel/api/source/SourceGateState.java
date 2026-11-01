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

package org.apache.seatunnel.api.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Versioned fact source gate state owned by a gate-capable reader.
 *
 * <p>The state records whether no more splits has been observed and stores prepared split payloads
 * with their digests. It does not transfer ownership to the dynamic lookup operator.
 */
public final class SourceGateState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Current state format version. */
    public static final int CURRENT_VERSION = 1;

    private final int version;
    private final boolean gateOpen;
    private final boolean noMoreSplits;
    private final List<PreparedSplit> preparedSplits;

    public SourceGateState(
            boolean gateOpen, boolean noMoreSplits, List<PreparedSplit> preparedSplits) {
        this(CURRENT_VERSION, gateOpen, noMoreSplits, preparedSplits);
    }

    public SourceGateState(
            int version,
            boolean gateOpen,
            boolean noMoreSplits,
            List<PreparedSplit> preparedSplits) {
        if (version <= 0 || version > CURRENT_VERSION) {
            throw new IllegalArgumentException("Unsupported source gate state version: " + version);
        }
        this.version = version;
        this.gateOpen = gateOpen;
        this.noMoreSplits = noMoreSplits;
        this.preparedSplits =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(preparedSplits, "preparedSplits")));
    }

    public int getVersion() {
        return version;
    }

    public boolean isGateOpen() {
        return gateOpen;
    }

    public boolean isNoMoreSplits() {
        return noMoreSplits;
    }

    public List<PreparedSplit> getPreparedSplits() {
        return preparedSplits;
    }

    /** Serialized split payload retained by the source reader. */
    public static final class PreparedSplit implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String splitId;
        private final byte[] serializedSplit;
        private final byte[] serializedSplitDigest;

        public PreparedSplit(String splitId, byte[] serializedSplit, byte[] serializedSplitDigest) {
            this.splitId = Objects.requireNonNull(splitId, "splitId");
            this.serializedSplit = copy(serializedSplit, "serializedSplit");
            this.serializedSplitDigest = copy(serializedSplitDigest, "serializedSplitDigest");
        }

        public String getSplitId() {
            return splitId;
        }

        public byte[] getSerializedSplit() {
            return copy(serializedSplit, "serializedSplit");
        }

        public byte[] getSerializedSplitDigest() {
            return copy(serializedSplitDigest, "serializedSplitDigest");
        }

        private static byte[] copy(byte[] bytes, String fieldName) {
            Objects.requireNonNull(bytes, fieldName);
            byte[] copied = new byte[bytes.length];
            System.arraycopy(bytes, 0, copied, 0, bytes.length);
            return copied;
        }
    }
}
