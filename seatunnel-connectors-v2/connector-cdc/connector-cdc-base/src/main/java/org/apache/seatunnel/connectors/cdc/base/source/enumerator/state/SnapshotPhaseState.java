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

import org.apache.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceSplitReader;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import io.debezium.relational.TableId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/** A {@link PendingSplitsState} for pending snapshot splits. */
@Getter
@ToString
@EqualsAndHashCode
public class SnapshotPhaseState implements PendingSplitsState {

    /** The tables in the checkpoint. */
    private final List<TableId> remainingTables;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final List<TableId> alreadyProcessedTables;

    /** The splits in the checkpoint. */
    private final List<SnapshotSplit> remainingSplits;

    /**
     * The snapshot splits that the {@link IncrementalSourceEnumerator} has assigned to {@link
     * IncrementalSourceSplitReader}s.
     */
    private final Map<String, SnapshotSplit> assignedSplits;

    /**
     * The offsets of completed (snapshot) splits that the {@link IncrementalSourceEnumerator} has
     * received from {@link IncrementalSourceSplitReader}s.
     */
    private final Map<String, Offset> splitCompletedOffsets;

    /**
     * Whether the snapshot split assigner is completed, which indicates there is no more splits and
     * all records of splits have been completely processed in the pipeline.
     */
    private final boolean isAssignerCompleted;

    /** Whether the table identifier is case sensitive. */
    private final boolean isTableIdCaseSensitive;

    /** Whether the remaining tables are keep when snapshot state. */
    private final boolean isRemainingTablesCheckpointed;

    public SnapshotPhaseState(
            List<TableId> alreadyProcessedTables,
            List<SnapshotSplit> remainingSplits,
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, Offset> splitCompletedOffsets,
            boolean isAssignerCompleted,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed) {
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitCompletedOffsets = splitCompletedOffsets;
        this.isAssignerCompleted = isAssignerCompleted;
        this.remainingTables = remainingTables;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
    }
}
