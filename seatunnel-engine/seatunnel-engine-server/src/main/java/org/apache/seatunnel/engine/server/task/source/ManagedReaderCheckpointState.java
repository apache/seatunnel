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

import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/** Connector split state plus engine-owned ordering metadata for one managed Reader checkpoint. */
public final class ManagedReaderCheckpointState {
    private final ManagedSourceRuntimeMode runtimeMode;
    private final int runtimeProtocolVersion;
    private final int connectorStateVersion;
    private final String capabilityDigest;
    private final String readerAttemptId;
    private final String coordinatorEpoch;
    private final long appliedCommandWatermark;
    private final SortedSet<Long> appliedCommandGaps;
    private final long noMoreSplitsGeneration;
    private final ManagedSourceLifecycle.Snapshot lifecycleSnapshot;
    private final List<String> checkpointOwnedSplitIds;
    private final List<byte[]> connectorSplitStates;

    public ManagedReaderCheckpointState(
            ManagedSourceRuntimeMode runtimeMode,
            int runtimeProtocolVersion,
            int connectorStateVersion,
            String capabilityDigest,
            String readerAttemptId,
            String coordinatorEpoch,
            long appliedCommandWatermark,
            SortedSet<Long> appliedCommandGaps,
            long noMoreSplitsGeneration,
            ManagedSourceLifecycle.Snapshot lifecycleSnapshot,
            List<String> checkpointOwnedSplitIds,
            List<byte[]> connectorSplitStates) {
        if (runtimeMode == null
                || runtimeMode == ManagedSourceRuntimeMode.LEGACY
                || runtimeProtocolVersion <= 0
                || connectorStateVersion <= 0
                || isBlank(capabilityDigest)
                || isBlank(readerAttemptId)
                || isBlank(coordinatorEpoch)
                || appliedCommandWatermark < 0
                || appliedCommandGaps == null
                || noMoreSplitsGeneration < 0
                || lifecycleSnapshot == null
                || checkpointOwnedSplitIds == null
                || connectorSplitStates == null) {
            throw new IllegalArgumentException("Invalid managed Reader checkpoint metadata");
        }
        if (appliedCommandGaps.stream()
                .anyMatch(gap -> gap == null || gap <= appliedCommandWatermark)) {
            throw new IllegalArgumentException(
                    "Applied command gaps must be greater than the contiguous watermark");
        }
        if (checkpointOwnedSplitIds.stream().anyMatch(ManagedReaderCheckpointState::isBlank)
                || new HashSet<>(checkpointOwnedSplitIds).size()
                        != checkpointOwnedSplitIds.size()) {
            throw new IllegalArgumentException(
                    "Checkpoint-owned split identifiers must be non-blank and unique");
        }
        if (connectorSplitStates.stream().anyMatch(state -> state == null)) {
            throw new IllegalArgumentException("Connector split checkpoint state must not be null");
        }
        this.runtimeMode = runtimeMode;
        this.runtimeProtocolVersion = runtimeProtocolVersion;
        this.connectorStateVersion = connectorStateVersion;
        this.capabilityDigest = capabilityDigest;
        this.readerAttemptId = readerAttemptId;
        this.coordinatorEpoch = coordinatorEpoch;
        this.appliedCommandWatermark = appliedCommandWatermark;
        this.appliedCommandGaps =
                Collections.unmodifiableSortedSet(new TreeSet<>(appliedCommandGaps));
        this.noMoreSplitsGeneration = noMoreSplitsGeneration;
        this.lifecycleSnapshot = lifecycleSnapshot;
        this.checkpointOwnedSplitIds =
                Collections.unmodifiableList(new ArrayList<>(checkpointOwnedSplitIds));
        List<byte[]> copiedStates = new ArrayList<>(connectorSplitStates.size());
        for (byte[] state : connectorSplitStates) {
            copiedStates.add(state.clone());
        }
        this.connectorSplitStates = Collections.unmodifiableList(copiedStates);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public ManagedSourceRuntimeMode getRuntimeMode() {
        return runtimeMode;
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

    public String getReaderAttemptId() {
        return readerAttemptId;
    }

    public String getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long getAppliedCommandWatermark() {
        return appliedCommandWatermark;
    }

    public SortedSet<Long> getAppliedCommandGaps() {
        return appliedCommandGaps;
    }

    public long getNoMoreSplitsGeneration() {
        return noMoreSplitsGeneration;
    }

    public ManagedSourceLifecycle.Snapshot getLifecycleSnapshot() {
        return lifecycleSnapshot;
    }

    public List<String> getCheckpointOwnedSplitIds() {
        return checkpointOwnedSplitIds;
    }

    public List<byte[]> getConnectorSplitStates() {
        List<byte[]> copiedStates = new ArrayList<>(connectorSplitStates.size());
        for (byte[] state : connectorSplitStates) {
            copiedStates.add(state.clone());
        }
        return copiedStates;
    }
}
