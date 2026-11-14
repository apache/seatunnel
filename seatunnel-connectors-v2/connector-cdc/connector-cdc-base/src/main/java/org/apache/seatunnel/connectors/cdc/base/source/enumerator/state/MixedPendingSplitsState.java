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

import io.debezium.relational.TableId;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Checkpoint state for a hybrid source with table-specific incremental lower bounds. */
@Getter
public class MixedPendingSplitsState implements PendingSplitsState {

    /** The state of tables that are synchronized with snapshots. */
    private final SnapshotPhaseState snapshotPhaseState;

    /** The state of the final incremental split assignment. */
    private final IncrementalPhaseState incrementalPhaseState;

    /** The fixed set of tables that must be initialized by a snapshot. */
    private final Set<TableId> snapshotTables;

    /** The fixed lower bounds for tables without a snapshot. */
    private final Map<TableId, Offset> tableStartOffsets;

    public MixedPendingSplitsState(
            SnapshotPhaseState snapshotPhaseState,
            IncrementalPhaseState incrementalPhaseState,
            Set<TableId> snapshotTables,
            Map<TableId, Offset> tableStartOffsets) {
        this.snapshotPhaseState = snapshotPhaseState;
        this.incrementalPhaseState = incrementalPhaseState;
        this.snapshotTables = new HashSet<>(snapshotTables);
        this.tableStartOffsets = new HashMap<>(tableStartOffsets);
    }
}
