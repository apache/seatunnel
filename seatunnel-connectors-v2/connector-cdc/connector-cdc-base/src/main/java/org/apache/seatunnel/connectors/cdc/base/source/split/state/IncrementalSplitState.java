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

package org.apache.seatunnel.connectors.cdc.base.source.split.state;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import io.debezium.relational.TableId;
import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The state of split to describe the change log of table(s). */
@Getter
@Setter
public class IncrementalSplitState extends SourceSplitStateBase {

    private List<TableId> tableIds;

    private final Set<TableId> capturedTableIds;

    /** Minimum watermark for SnapshotSplits for all tables in this IncrementalSplit */
    private Offset startupOffset;

    /** Last checkpoint position observed for each captured table in this split. */
    private Map<TableId, Offset> tableStartupOffsets;

    /** Obtained by configuration, may not end */
    private Offset stopOffset;

    private Offset maxSnapshotSplitsHighWatermark;
    private volatile boolean enterPureIncrementPhase;

    public IncrementalSplitState(IncrementalSplit split) {
        super(split);
        this.tableIds = split.getTableIds();
        this.capturedTableIds = new HashSet<>(tableIds);
        this.startupOffset = split.getStartupOffset();
        this.stopOffset = split.getStopOffset();
        this.tableStartupOffsets =
                split.getTableStartupOffsets() == null
                        ? new HashMap<>()
                        : new HashMap<>(split.getTableStartupOffsets());

        if (split.getCompletedSnapshotSplitInfos().isEmpty()) {
            this.maxSnapshotSplitsHighWatermark = null;
            this.enterPureIncrementPhase = true;
        } else {
            this.maxSnapshotSplitsHighWatermark =
                    split.getCompletedSnapshotSplitInfos().stream()
                            .filter(e -> e.getWatermark() != null)
                            .max(Comparator.comparing(o -> o.getWatermark().getHighWatermark()))
                            .map(e -> e.getWatermark().getHighWatermark())
                            .get();
            this.enterPureIncrementPhase = false;
        }
    }

    @Override
    public IncrementalSplit toSourceSplit() {
        final IncrementalSplit incrementalSplit = split.asIncrementalSplit();
        return new IncrementalSplit(
                incrementalSplit.splitId(),
                getTableIds(),
                getStartupOffset(),
                getStopOffset(),
                incrementalSplit.getCompletedSnapshotSplitInfos(),
                getTableStartupOffsets());
    }

    /**
     * Advances the split and per-table checkpoint positions. Heartbeats advance every captured
     * table, while records and schema changes advance only their own table; no watermark moves
     * backwards when replayed records arrive out of order.
     */
    public void setStartupOffset(Offset startupOffset, TableId tableId) {
        if (startupOffset == null) {
            return;
        }
        if (this.startupOffset == null || startupOffset.isAfter(this.startupOffset)) {
            this.startupOffset = startupOffset;
        }
        if (tableId == null) {
            // Heartbeats have a source-wide offset that safely advances every captured table.
            for (TableId capturedTableId : capturedTableIds) {
                advanceTableStartupOffset(capturedTableId, startupOffset);
            }
        } else if (capturedTableIds.contains(tableId)) {
            advanceTableStartupOffset(tableId, startupOffset);
        }
    }

    private void advanceTableStartupOffset(TableId tableId, Offset startupOffset) {
        Offset currentStartupOffset = tableStartupOffsets.get(tableId);
        if (currentStartupOffset == null || startupOffset.isAfter(currentStartupOffset)) {
            tableStartupOffsets.put(tableId, startupOffset);
        }
    }

    public synchronized boolean markEnterPureIncrementPhaseIfNeed(Offset currentRecordPosition) {
        if (enterPureIncrementPhase) {
            return false;
        }

        if (currentRecordPosition.isAtOrAfter(maxSnapshotSplitsHighWatermark)) {
            split.asIncrementalSplit().getCompletedSnapshotSplitInfos().clear();
            this.enterPureIncrementPhase = true;
            return true;
        }

        return false;
    }

    public synchronized boolean autoEnterPureIncrementPhaseIfAllowed() {
        if (!enterPureIncrementPhase
                && maxSnapshotSplitsHighWatermark.compareTo(startupOffset) == 0) {
            split.asIncrementalSplit().getCompletedSnapshotSplitInfos().clear();
            enterPureIncrementPhase = true;
            return true;
        }
        return false;
    }
}
