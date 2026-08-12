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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ToString
@Getter
public class IncrementalSplit extends SourceSplitBase {
    private static final long serialVersionUID = 1L;

    /** All the tables that this incremental split needs to capture. */
    private final List<TableId> tableIds;

    /** Minimum watermark for SnapshotSplits for all tables in this IncrementalSplit */
    private final Offset startupOffset;

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
        super(splitId);
        this.tableIds = capturedTables;
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
        this.completedSnapshotSplitInfos = completedSnapshotSplitInfos;
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
        super(splitId);
        this.tableIds = capturedTables;
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
        this.completedSnapshotSplitInfos = completedSnapshotSplitInfos;
        this.checkpointTables = checkpointTables;
        this.historyTableChanges = historyTableChanges;
    }

    /** Returns restored checkpoint state limited to the tables captured by the current job. */
    public IncrementalSplit pruneTables(Collection<TableId> capturedTables) {
        Set<TableId> capturedTableSet = new HashSet<>(capturedTables);
        List<TableId> filteredTableIds =
                tableIds.stream().filter(capturedTableSet::contains).collect(Collectors.toList());
        List<CompletedSnapshotSplitInfo> filteredCompletedSnapshotSplitInfos =
                completedSnapshotSplitInfos.stream()
                        .filter(info -> capturedTableSet.contains(info.getTableId()))
                        .collect(Collectors.toList());
        List<CatalogTable> filteredCheckpointTables =
                checkpointTables == null
                        ? null
                        : checkpointTables.stream()
                                .filter(
                                        table ->
                                                capturedTableSet.contains(
                                                        new TableId(
                                                                table.getTablePath()
                                                                        .getDatabaseName(),
                                                                table.getTablePath()
                                                                        .getSchemaName(),
                                                                table.getTablePath()
                                                                        .getTableName())))
                                .collect(Collectors.toList());
        Map<TableId, byte[]> filteredHistoryTableChanges =
                historyTableChanges == null
                        ? null
                        : historyTableChanges.entrySet().stream()
                                .filter(entry -> capturedTableSet.contains(entry.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new IncrementalSplit(
                splitId(),
                filteredTableIds,
                startupOffset,
                stopOffset,
                filteredCompletedSnapshotSplitInfos,
                filteredCheckpointTables,
                filteredHistoryTableChanges);
    }
}
