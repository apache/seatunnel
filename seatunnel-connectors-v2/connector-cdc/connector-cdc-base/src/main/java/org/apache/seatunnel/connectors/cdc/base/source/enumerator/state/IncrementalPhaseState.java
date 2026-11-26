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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator.state;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import io.debezium.relational.TableId;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** A {@link PendingSplitsState} for pending incremental splits. */
@Data
public class IncrementalPhaseState implements PendingSplitsState {

    // Preserve compatibility with checkpoints written when this state had no fields.
    private static final long serialVersionUID = -6809026812298443356L;

    private final Offset startupOffset;

    /**
     * The stop offset resolved once at the enumerator when the snapshot phase completes ({@code
     * stop.mode = latest}). Stored in the checkpoint so that a restart reuses the same value
     * instead of re-resolving (and drifting) it. {@code null} for other stop modes and for
     * checkpoints written before this field existed.
     */
    private final Offset stopOffset;

    private final boolean splitAssigned;

    /** Incremental splits generated but not yet assigned to a reader. */
    private final List<IncrementalSplit> remainingSplits;

    /**
     * Tables already covered by an incremental split (either assigned to a reader or queued in
     * {@link #remainingSplits}). Always empty when {@link #assignedTablesCheckpointed} is {@code
     * false}.
     */
    private final Set<TableId> assignedTables;

    /**
     * Whether {@link #assignedTables} was explicitly recorded in this checkpoint. {@code false} for
     * checkpoints written before {@link #assignedTables} existed.
     */
    private final boolean assignedTablesCheckpointed;

    public IncrementalPhaseState() {
        this(null, null, false, Collections.emptyList(), Collections.emptySet(), false);
    }

    public IncrementalPhaseState(Offset startupOffset) {
        this(startupOffset, null, false, Collections.emptyList(), Collections.emptySet(), false);
    }

    public IncrementalPhaseState(Offset startupOffset, Offset stopOffset) {
        this(
                startupOffset,
                stopOffset,
                false,
                Collections.emptyList(),
                Collections.emptySet(),
                false);
    }

    public IncrementalPhaseState(Offset startupOffset, Offset stopOffset, boolean splitAssigned) {
        this(
                startupOffset,
                stopOffset,
                splitAssigned,
                Collections.emptyList(),
                Collections.emptySet(),
                false);
    }

    public IncrementalPhaseState(
            Offset startupOffset,
            Offset stopOffset,
            boolean splitAssigned,
            List<IncrementalSplit> remainingSplits) {
        this(
                startupOffset,
                stopOffset,
                splitAssigned,
                remainingSplits,
                Collections.emptySet(),
                false);
    }

    public IncrementalPhaseState(
            Offset startupOffset,
            Offset stopOffset,
            boolean splitAssigned,
            List<IncrementalSplit> remainingSplits,
            Set<TableId> assignedTables) {
        this(startupOffset, stopOffset, splitAssigned, remainingSplits, assignedTables, true);
    }

    private IncrementalPhaseState(
            Offset startupOffset,
            Offset stopOffset,
            boolean splitAssigned,
            List<IncrementalSplit> remainingSplits,
            Set<TableId> assignedTables,
            boolean assignedTablesCheckpointed) {
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
        this.splitAssigned = splitAssigned;
        this.remainingSplits =
                remainingSplits == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(new ArrayList<>(remainingSplits));
        this.assignedTables =
                assignedTables == null
                        ? Collections.emptySet()
                        : Collections.unmodifiableSet(new LinkedHashSet<>(assignedTables));
        this.assignedTablesCheckpointed = assignedTablesCheckpointed;
    }

    /**
     * Re-normalizes fields added after earlier releases (such as {@link #remainingSplits} and
     * {@link #assignedTables}) when deserializing legacy checkpoints whose streams omit them.
     */
    private Object readResolve() {
        return new IncrementalPhaseState(
                startupOffset,
                stopOffset,
                splitAssigned,
                remainingSplits,
                assignedTables,
                assignedTablesCheckpointed);
    }
}
