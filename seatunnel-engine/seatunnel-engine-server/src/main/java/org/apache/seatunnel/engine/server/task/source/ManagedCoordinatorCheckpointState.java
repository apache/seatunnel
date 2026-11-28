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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Connector enumerator state plus engine-owned coordinator runtime state. */
public final class ManagedCoordinatorCheckpointState {
    private final ManagedSourceRuntimeMode runtimeMode;
    private final int runtimeProtocolVersion;
    private final int connectorStateVersion;
    private final String capabilityDigest;
    private final int sourceParallelism;
    private final byte[] connectorEnumeratorState;
    private final byte[] assignmentTrackerState;
    private final Map<Integer, Long> nextReaderCommandSequences;
    private final Set<Integer> noMoreSplitsSubtasks;
    private final boolean allReadersNoMoreSplits;
    private final long nextNoMoreSplitsGeneration;

    public ManagedCoordinatorCheckpointState(
            ManagedSourceRuntimeMode runtimeMode,
            int runtimeProtocolVersion,
            int connectorStateVersion,
            String capabilityDigest,
            int sourceParallelism,
            byte[] connectorEnumeratorState,
            byte[] assignmentTrackerState,
            Map<Integer, Long> nextReaderCommandSequences,
            Set<Integer> noMoreSplitsSubtasks,
            boolean allReadersNoMoreSplits,
            long nextNoMoreSplitsGeneration) {
        if (runtimeMode == null
                || runtimeMode == ManagedSourceRuntimeMode.LEGACY
                || runtimeProtocolVersion <= 0
                || connectorStateVersion <= 0
                || capabilityDigest == null
                || capabilityDigest.trim().isEmpty()
                || sourceParallelism <= 0
                || connectorEnumeratorState == null
                || assignmentTrackerState == null
                || nextReaderCommandSequences == null
                || noMoreSplitsSubtasks == null
                || nextNoMoreSplitsGeneration < 0) {
            throw new IllegalArgumentException("Invalid managed coordinator checkpoint metadata");
        }
        if (nextReaderCommandSequences.entrySet().stream()
                .anyMatch(
                        entry ->
                                entry.getKey() == null
                                        || entry.getKey() < 0
                                        || entry.getValue() == null
                                        || entry.getValue() <= 0)) {
            throw new IllegalArgumentException("Invalid managed Reader command sequence");
        }
        if (noMoreSplitsSubtasks.stream().anyMatch(subtask -> subtask == null || subtask < 0)) {
            throw new IllegalArgumentException("Invalid no-more-splits subtask");
        }
        this.runtimeMode = runtimeMode;
        this.runtimeProtocolVersion = runtimeProtocolVersion;
        this.connectorStateVersion = connectorStateVersion;
        this.capabilityDigest = capabilityDigest;
        this.sourceParallelism = sourceParallelism;
        this.connectorEnumeratorState = connectorEnumeratorState.clone();
        this.assignmentTrackerState = assignmentTrackerState.clone();
        this.nextReaderCommandSequences =
                Collections.unmodifiableMap(new HashMap<>(nextReaderCommandSequences));
        this.noMoreSplitsSubtasks =
                Collections.unmodifiableSet(new HashSet<>(noMoreSplitsSubtasks));
        this.allReadersNoMoreSplits = allReadersNoMoreSplits;
        this.nextNoMoreSplitsGeneration = nextNoMoreSplitsGeneration;
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

    public int getSourceParallelism() {
        return sourceParallelism;
    }

    public byte[] getConnectorEnumeratorState() {
        return connectorEnumeratorState.clone();
    }

    public byte[] getAssignmentTrackerState() {
        return assignmentTrackerState.clone();
    }

    public Map<Integer, Long> getNextReaderCommandSequences() {
        return nextReaderCommandSequences;
    }

    public Set<Integer> getNoMoreSplitsSubtasks() {
        return noMoreSplitsSubtasks;
    }

    public boolean isAllReadersNoMoreSplits() {
        return allReadersNoMoreSplits;
    }

    public long getNextNoMoreSplitsGeneration() {
        return nextNoMoreSplitsGeneration;
    }
}
