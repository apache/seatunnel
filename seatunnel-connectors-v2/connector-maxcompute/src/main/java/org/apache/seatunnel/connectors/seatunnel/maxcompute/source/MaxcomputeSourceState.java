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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Persisted MaxCompute split assignment state.
 *
 * <p>Older checkpoints do not contain the lazy-assignment marker and therefore deserialize as
 * legacy eager state. They are restored only from reader-returned unfinished splits.
 */
public class MaxcomputeSourceState implements Serializable {
    private static final long serialVersionUID = 3097170139569235106L;

    /** Splits assigned when the state was snapshotted. */
    private final Set<MaxcomputeSourceSplit> assignedSplit;

    /** Index of the next source table to split lazily. */
    private final int nextTableIndex;

    /** Inclusive row offset for the next lazily generated split. */
    private final long nextRowStart;

    /** Whether this state was produced by the lazy assignment implementation. */
    private final boolean lazySplitAssignment;

    /**
     * Constructs a legacy eager-assignment state for deserialization compatibility.
     *
     * @param assignedSplit assigned splits recorded by an older implementation
     */
    public MaxcomputeSourceState(Set<MaxcomputeSourceSplit> assignedSplit) {
        this(assignedSplit, 0, 0L, false);
    }

    /**
     * Constructs a lazy-assignment state.
     *
     * @param assignedSplit currently assigned split metadata
     * @param nextTableIndex next source table index
     * @param nextRowStart next row offset in the source table
     */
    public MaxcomputeSourceState(
            Set<MaxcomputeSourceSplit> assignedSplit, int nextTableIndex, long nextRowStart) {
        this(assignedSplit, nextTableIndex, nextRowStart, true);
    }

    private MaxcomputeSourceState(
            Set<MaxcomputeSourceSplit> assignedSplit,
            int nextTableIndex,
            long nextRowStart,
            boolean lazySplitAssignment) {
        this.assignedSplit = assignedSplit == null ? new HashSet<>() : new HashSet<>(assignedSplit);
        this.nextTableIndex = nextTableIndex;
        this.nextRowStart = nextRowStart;
        this.lazySplitAssignment = lazySplitAssignment;
    }

    public Set<MaxcomputeSourceSplit> getAssignedSplit() {
        return assignedSplit;
    }

    /**
     * Returns the next source table cursor.
     *
     * @return zero-based source table index
     */
    public int getNextTableIndex() {
        return nextTableIndex;
    }

    /**
     * Returns the row offset for the next lazy split.
     *
     * @return next row offset in the current table
     */
    public long getNextRowStart() {
        return nextRowStart;
    }

    /**
     * Returns whether this state uses the lazy split cursor introduced after eager checkpoints.
     *
     * @return true for lazy state; false for legacy eager state
     */
    public boolean isLazySplitAssignment() {
        return lazySplitAssignment;
    }
}
