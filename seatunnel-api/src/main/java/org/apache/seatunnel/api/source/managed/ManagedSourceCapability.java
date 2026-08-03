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

package org.apache.seatunnel.api.source.managed;

import org.apache.seatunnel.api.annotation.Experimental;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Versioned behavioral descriptor used to qualify a connector for the managed Source runtime.
 *
 * <p>This descriptor is deliberately not a single opt-in boolean. The engine persists its digest in
 * checkpoint metadata and rejects restore when the runtime contract changes without an explicit
 * migration.
 */
@Experimental
public final class ManagedSourceCapability implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int CURRENT_RUNTIME_PROTOCOL_VERSION = 1;

    private static final ManagedSourceCapability LEGACY =
            new Builder().runtimeProtocolVersion(CURRENT_RUNTIME_PROTOCOL_VERSION).build();

    private final int runtimeProtocolVersion;
    private final int connectorStateVersion;
    private final boolean supportsManagedReader;
    private final boolean supportsManagedCoordinator;
    private final boolean supportsBoundedPoll;
    private final boolean supportsWakeup;
    private final boolean supportsAttemptFencing;

    /**
     * Whether the connector answered the {@code usesSourceEvents} question at all.
     *
     * <p>A managed capability must answer it explicitly, so that a connector relying on {@code
     * SourceEvent} delivery is rejected during lane selection instead of failing mid-job.
     */
    private final boolean sourceEventsDeclared;

    private final boolean usesSourceEvents;
    private final boolean supportsAsyncEnumerator;
    private final boolean stableSplitIdentifiers;
    private final String capabilityDigest;

    private ManagedSourceCapability(Builder builder) {
        this.runtimeProtocolVersion = builder.runtimeProtocolVersion;
        this.connectorStateVersion = builder.connectorStateVersion;
        this.supportsManagedReader = builder.supportsManagedReader;
        this.supportsManagedCoordinator = builder.supportsManagedCoordinator;
        this.supportsBoundedPoll = builder.supportsBoundedPoll;
        this.supportsWakeup = builder.supportsWakeup;
        this.supportsAttemptFencing = builder.supportsAttemptFencing;
        this.sourceEventsDeclared = builder.usesSourceEvents != null;
        this.usesSourceEvents = Boolean.TRUE.equals(builder.usesSourceEvents);
        this.supportsAsyncEnumerator = builder.supportsAsyncEnumerator;
        this.stableSplitIdentifiers = builder.stableSplitIdentifiers;
        validate();
        this.capabilityDigest = sha256(canonicalForm());
    }

    public static ManagedSourceCapability legacy() {
        return LEGACY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getRuntimeProtocolVersion() {
        return runtimeProtocolVersion;
    }

    public int getConnectorStateVersion() {
        return connectorStateVersion;
    }

    public boolean supportsManagedReader() {
        return supportsManagedReader;
    }

    public boolean supportsManagedCoordinator() {
        return supportsManagedCoordinator;
    }

    public boolean supportsBoundedPoll() {
        return supportsBoundedPoll;
    }

    public boolean supportsWakeup() {
        return supportsWakeup;
    }

    public boolean supportsAttemptFencing() {
        return supportsAttemptFencing;
    }

    public boolean usesSourceEvents() {
        return usesSourceEvents;
    }

    public boolean supportsAsyncEnumerator() {
        return supportsAsyncEnumerator;
    }

    public boolean hasStableSplitIdentifiers() {
        return stableSplitIdentifiers;
    }

    public String getCapabilityDigest() {
        return capabilityDigest;
    }

    public boolean isLegacy() {
        return !supportsManagedReader && !supportsManagedCoordinator;
    }

    private void validate() {
        if (runtimeProtocolVersion <= 0) {
            throw new IllegalArgumentException("runtimeProtocolVersion must be positive");
        }
        if (connectorStateVersion <= 0) {
            throw new IllegalArgumentException("connectorStateVersion must be positive");
        }
        if ((supportsManagedReader || supportsManagedCoordinator) && !sourceEventsDeclared) {
            throw new IllegalArgumentException(
                    "Managed Source capability must declare whether the connector uses SourceEvents");
        }
        if (supportsManagedReader
                && (!supportsBoundedPoll
                        || !supportsWakeup
                        || !supportsAttemptFencing
                        || !stableSplitIdentifiers)) {
            throw new IllegalArgumentException(
                    "A managed reader requires bounded poll, wakeup, attempt fencing, and stable split identifiers");
        }
        if (supportsManagedReader && !supportsManagedCoordinator) {
            throw new IllegalArgumentException(
                    "A managed reader requires a managed coordinator for ordered barriers and checkpoint callbacks");
        }
        if (supportsAsyncEnumerator && !supportsManagedCoordinator) {
            throw new IllegalArgumentException(
                    "An async enumerator requires managed coordinator support");
        }
    }

    private String canonicalForm() {
        return runtimeProtocolVersion
                + "|"
                + connectorStateVersion
                + "|"
                + supportsManagedReader
                + "|"
                + supportsManagedCoordinator
                + "|"
                + supportsBoundedPoll
                + "|"
                + supportsWakeup
                + "|"
                + supportsAttemptFencing
                + "|"
                + usesSourceEvents
                + "|"
                + supportsAsyncEnumerator
                + "|"
                + stableSplitIdentifiers;
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }

    public static final class Builder {
        private int runtimeProtocolVersion = CURRENT_RUNTIME_PROTOCOL_VERSION;
        private int connectorStateVersion = 1;
        private boolean supportsManagedReader;
        private boolean supportsManagedCoordinator;
        private boolean supportsBoundedPoll;
        private boolean supportsWakeup;
        private boolean supportsAttemptFencing;
        private Boolean usesSourceEvents;
        private boolean supportsAsyncEnumerator;
        private boolean stableSplitIdentifiers;

        public Builder runtimeProtocolVersion(int runtimeProtocolVersion) {
            this.runtimeProtocolVersion = runtimeProtocolVersion;
            return this;
        }

        public Builder connectorStateVersion(int connectorStateVersion) {
            this.connectorStateVersion = connectorStateVersion;
            return this;
        }

        public Builder supportsManagedReader(boolean supportsManagedReader) {
            this.supportsManagedReader = supportsManagedReader;
            return this;
        }

        public Builder supportsManagedCoordinator(boolean supportsManagedCoordinator) {
            this.supportsManagedCoordinator = supportsManagedCoordinator;
            return this;
        }

        public Builder supportsBoundedPoll(boolean supportsBoundedPoll) {
            this.supportsBoundedPoll = supportsBoundedPoll;
            return this;
        }

        public Builder supportsWakeup(boolean supportsWakeup) {
            this.supportsWakeup = supportsWakeup;
            return this;
        }

        public Builder supportsAttemptFencing(boolean supportsAttemptFencing) {
            this.supportsAttemptFencing = supportsAttemptFencing;
            return this;
        }

        /**
         * Declares whether the connector exchanges {@code SourceEvent}s with its enumerator.
         *
         * <p>Managed capabilities must call this method. Protocol version 1 has no versioned {@code
         * SourceEvent} codec, so declaring {@code true} keeps the connector on the legacy lane by
         * failing selection instead of throwing from the first event at runtime.
         */
        public Builder usesSourceEvents(boolean usesSourceEvents) {
            this.usesSourceEvents = usesSourceEvents;
            return this;
        }

        public Builder supportsAsyncEnumerator(boolean supportsAsyncEnumerator) {
            this.supportsAsyncEnumerator = supportsAsyncEnumerator;
            return this;
        }

        public Builder stableSplitIdentifiers(boolean stableSplitIdentifiers) {
            this.stableSplitIdentifiers = stableSplitIdentifiers;
            return this;
        }

        public ManagedSourceCapability build() {
            return new ManagedSourceCapability(this);
        }
    }
}
