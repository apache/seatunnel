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

package org.apache.seatunnel.engine.common.runtime.source;

import java.io.Serializable;

/** Immutable lane and capability identity written into the physical deployment descriptor. */
public final class ManagedSourceRuntimeSelection implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ManagedSourceRuntimeMode mode;
    private final int runtimeProtocolVersion;
    private final int connectorStateVersion;
    private final String capabilityDigest;
    private final boolean checkpointEnabled;

    public ManagedSourceRuntimeSelection(
            ManagedSourceRuntimeMode mode,
            int runtimeProtocolVersion,
            int connectorStateVersion,
            String capabilityDigest) {
        this(mode, runtimeProtocolVersion, connectorStateVersion, capabilityDigest, true);
    }

    public ManagedSourceRuntimeSelection(
            ManagedSourceRuntimeMode mode,
            int runtimeProtocolVersion,
            int connectorStateVersion,
            String capabilityDigest,
            boolean checkpointEnabled) {
        if (mode == null || capabilityDigest == null) {
            throw new IllegalArgumentException("Managed Source runtime selection must not be null");
        }
        if (mode == ManagedSourceRuntimeMode.LEGACY) {
            if (runtimeProtocolVersion != 0
                    || connectorStateVersion != 0
                    || !capabilityDigest.isEmpty()) {
                throw new IllegalArgumentException(
                        "Legacy Source runtime selection must not carry managed metadata");
            }
        } else if (runtimeProtocolVersion <= 0
                || connectorStateVersion <= 0
                || capabilityDigest.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Managed Source runtime selection metadata is invalid");
        }
        this.mode = mode;
        this.runtimeProtocolVersion = runtimeProtocolVersion;
        this.connectorStateVersion = connectorStateVersion;
        this.capabilityDigest = capabilityDigest;
        this.checkpointEnabled = checkpointEnabled;
    }

    public ManagedSourceRuntimeMode getMode() {
        return mode;
    }

    public int getRuntimeProtocolVersion() {
        return runtimeProtocolVersion;
    }

    public int getConnectorStateVersion() {
        return connectorStateVersion;
    }

    public String getCapabilityDigest() {
        return capabilityDigest;
    }

    public boolean isCheckpointEnabled() {
        return checkpointEnabled;
    }

    /** Returns the same immutable lane selection with the job checkpoint mode attached. */
    public ManagedSourceRuntimeSelection withCheckpointEnabled(boolean checkpointEnabled) {
        return new ManagedSourceRuntimeSelection(
                mode,
                runtimeProtocolVersion,
                connectorStateVersion,
                capabilityDigest,
                checkpointEnabled);
    }

    public static ManagedSourceRuntimeSelection legacy() {
        return new ManagedSourceRuntimeSelection(ManagedSourceRuntimeMode.LEGACY, 0, 0, "");
    }
}
