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

package org.apache.seatunnel.connectors.cdc.base.source.split;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.relational.TableId;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ToString
@Getter
public class IncrementalSplit extends SourceSplitBase {
    private static final long serialVersionUID = 1L;

    /** All the tables that this incremental split needs to capture. */
    private final List<TableId> tableIds;

    /** Minimum watermark for SnapshotSplits for all tables in this IncrementalSplit */
    private final Offset startupOffset;

    /** Per-table lower bounds used when tables have different initial synchronization policies. */
    private Map<TableId, Offset> tableStartOffsets;

    /** Obtained by configuration, may not end */
    private final Offset stopOffset;

    /**
     * SnapshotSplit information for all tables in this IncrementalSplit. <br>
     * Used to support Exactly-Once.
     */
    private final List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos;

    // Remove in the next version
    @Deprecated private SeaTunnelDataType checkpointDataType;
    private List<CatalogTable> checkpointTables;

    // debezium history table changes
    private final Map<TableId, byte[]> historyTableChanges;

    public IncrementalSplit(
            String splitId,
            List<TableId> capturedTables,
            Offset startupOffset,
            Offset stopOffset,
            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos) {
        this(
                splitId,
                capturedTables,
                startupOffset,
                stopOffset,
                completedSnapshotSplitInfos,
                Collections.emptyMap(),
                new ArrayList<>(),
                new HashMap<>());
    }

    @Deprecated
    public IncrementalSplit(IncrementalSplit split, SeaTunnelDataType checkpointDataType) {
        this(
                split.splitId(),
                split.getTableIds(),
                split.getStartupOffset(),
                split.getStopOffset(),
                split.getCompletedSnapshotSplitInfos(),
                split.getTableStartOffsets(),
                checkpointDataType);
    }

    public IncrementalSplit(
            IncrementalSplit split,
            List<CatalogTable> tables,
            Map<TableId, byte[]> historyTableChanges) {
        this(
                split.splitId(),
                split.getTableIds(),
                split.getStartupOffset(),
                split.getStopOffset(),
                split.getCompletedSnapshotSplitInfos(),
                split.getTableStartOffsets(),
                tables,
                historyTableChanges);
    }

    @Deprecated
    public IncrementalSplit(
            String splitId,
            List<TableId> capturedTables,
            Offset startupOffset,
            Offset stopOffset,
            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos,
            SeaTunnelDataType checkpointDataType) {
        this(
                splitId,
                capturedTables,
                startupOffset,
                stopOffset,
                completedSnapshotSplitInfos,
                Collections.emptyMap(),
                checkpointDataType);
    }

    @Deprecated
    public IncrementalSplit(
            String splitId,
            List<TableId> capturedTables,
            Offset startupOffset,
            Offset stopOffset,
            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos,
            Map<TableId, Offset> tableStartOffsets,
            SeaTunnelDataType checkpointDataType) {
        super(splitId);
        this.tableIds = capturedTables;
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
        this.completedSnapshotSplitInfos = completedSnapshotSplitInfos;
        this.tableStartOffsets =
                tableStartOffsets == null
                        ? Collections.emptyMap()
                        : new HashMap<>(tableStartOffsets);
        this.checkpointDataType = checkpointDataType;
        this.historyTableChanges = new HashMap<>();
    }

    public IncrementalSplit(
            String splitId,
            List<TableId> capturedTables,
            Offset startupOffset,
            Offset stopOffset,
            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos,
            List<CatalogTable> checkpointTables,
            Map<TableId, byte[]> historyTableChanges) {
        this(
                splitId,
                capturedTables,
                startupOffset,
                stopOffset,
                completedSnapshotSplitInfos,
                Collections.emptyMap(),
                checkpointTables,
                historyTableChanges);
    }

    public IncrementalSplit(
            String splitId,
            List<TableId> capturedTables,
            Offset startupOffset,
            Offset stopOffset,
            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos,
            Map<TableId, Offset> tableStartOffsets,
            List<CatalogTable> checkpointTables,
            Map<TableId, byte[]> historyTableChanges) {
        super(splitId);
        this.tableIds = capturedTables;
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
        this.completedSnapshotSplitInfos = completedSnapshotSplitInfos;
        this.tableStartOffsets =
                tableStartOffsets == null
                        ? Collections.emptyMap()
                        : new HashMap<>(tableStartOffsets);
        this.checkpointTables = checkpointTables;
        this.historyTableChanges = historyTableChanges;
    }

    /**
     * Returns the table-specific lower bounds for this split.
     *
     * <p>Checkpoints written before this field was introduced deserialize it as {@code null}; treat
     * them as having no table-specific lower bounds to preserve restore compatibility.
     */
    public Map<TableId, Offset> getTableStartOffsets() {
        return tableStartOffsets == null ? Collections.emptyMap() : tableStartOffsets;
    }
}
