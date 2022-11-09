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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitBase;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Assigner for snapshot split.
 */
@Slf4j
public class SnapshotSplitAssigner<C extends SourceConfig> implements SplitAssigner {
    private final List<TableId> alreadyProcessedTables;
    private final List<SchemalessSnapshotSplit> remainingSplits;
    private final Map<String, SchemalessSnapshotSplit> assignedSplits;
    private final Map<TableId, TableChanges.TableChange> tableSchemas;
    private final Map<String, Offset> splitFinishedOffsets;
    private boolean assignerFinished;

    private final C sourceConfig;
    private final int currentParallelism;
    private final LinkedList<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private ChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;

    @Nullable
    private Long checkpointIdToFinish;
    private final DataSourceDialect<C> dialect;
    private final OffsetFactory offsetFactory;

    public SnapshotSplitAssigner(
        C sourceConfig,
        int currentParallelism,
        List<TableId> remainingTables,
        boolean isTableIdCaseSensitive,
        DataSourceDialect<C> dialect,
        OffsetFactory offsetFactory) {
        this(
            sourceConfig,
            currentParallelism,
            new ArrayList<>(),
            new ArrayList<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            false,
            remainingTables,
            isTableIdCaseSensitive,
            true,
            dialect,
            offsetFactory);
    }

    public SnapshotSplitAssigner(
        C sourceConfig,
        int currentParallelism,
        SnapshotPendingSplitsState checkpoint,
        DataSourceDialect<C> dialect,
        OffsetFactory offsetFactory) {
        this(
            sourceConfig,
            currentParallelism,
            checkpoint.getAlreadyProcessedTables(),
            checkpoint.getRemainingSplits(),
            checkpoint.getAssignedSplits(),
            checkpoint.getTableSchemas(),
            checkpoint.getSplitFinishedOffsets(),
            checkpoint.isAssignerFinished(),
            checkpoint.getRemainingTables(),
            checkpoint.isTableIdCaseSensitive(),
            checkpoint.isRemainingTablesCheckpointed(),
            dialect,
            offsetFactory);
    }

    private SnapshotSplitAssigner(
        C sourceConfig,
        int currentParallelism,
        List<TableId> alreadyProcessedTables,
        List<SchemalessSnapshotSplit> remainingSplits,
        Map<String, SchemalessSnapshotSplit> assignedSplits,
        Map<TableId, TableChanges.TableChange> tableSchemas,
        Map<String, Offset> splitFinishedOffsets,
        boolean assignerFinished,
        List<TableId> remainingTables,
        boolean isTableIdCaseSensitive,
        boolean isRemainingTablesCheckpointed,
        DataSourceDialect<C> dialect,
        OffsetFactory offsetFactory) {
        this.sourceConfig = sourceConfig;
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.tableSchemas = tableSchemas;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerFinished = assignerFinished;
        this.remainingTables = new LinkedList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {
        chunkSplitter = dialect.createChunkSplitter(sourceConfig);

        // the legacy state didn't snapshot remaining tables, discovery remaining table here
        if (!isRemainingTablesCheckpointed && !assignerFinished) {
            try {
                final List<TableId> discoverTables = dialect.discoverDataCollections(sourceConfig);
                discoverTables.removeAll(alreadyProcessedTables);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = dialect.isDataCollectionIdCaseSensitive(sourceConfig);
            } catch (Exception e) {
                throw new SeaTunnelException(
                    "Failed to discover remaining tables to capture", e);
            }
        }
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            Iterator<SchemalessSnapshotSplit> iterator = remainingSplits.iterator();
            SchemalessSnapshotSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split.toSnapshotSplit(tableSchemas.get(split.getTableId())));
        } else {
            // it's turn for new table
            TableId nextTable = remainingTables.pollFirst();
            if (nextTable != null) {
                // split the given table into chunks (snapshot splits)
                Collection<SnapshotSplit> splits = chunkSplitter.generateSplits(nextTable);
                final Map<TableId, TableChanges.TableChange> tableSchema = new HashMap<>();
                if (!splits.isEmpty()) {
                    tableSchema.putAll(splits.iterator().next().getTableSchemas());
                }
                final List<SchemalessSnapshotSplit> schemalessSnapshotSplits =
                    splits.stream()
                        .map(SnapshotSplit::toSchemalessSnapshotSplit)
                        .collect(Collectors.toList());
                remainingSplits.addAll(schemalessSnapshotSplits);
                tableSchemas.putAll(tableSchema);
                alreadyProcessedTables.add(nextTable);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return !allSplitsFinished();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        if (waitingForFinishedSplits()) {
            log.error(
                "The assigner is not ready to offer finished split information, this should not be called");
            throw new SeaTunnelException(
                "The assigner is not ready to offer finished split information, this should not be called");
        }
        final List<SchemalessSnapshotSplit> assignedSnapshotSplit =
            assignedSplits.values().stream()
                .sorted(Comparator.comparing(SourceSplitBase::splitId))
                .collect(Collectors.toList());
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        for (SchemalessSnapshotSplit split : assignedSnapshotSplit) {
            Offset finishedOffset = splitFinishedOffsets.get(split.splitId());
            finishedSnapshotSplitInfos.add(
                new FinishedSnapshotSplitInfo(
                    split.getTableId(),
                    split.splitId(),
                    split.getSplitStart(),
                    split.getSplitEnd(),
                    finishedOffset,
                    offsetFactory));
        }
        return finishedSnapshotSplitInfos;
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        this.splitFinishedOffsets.putAll(splitFinishedOffsets);
        if (allSplitsFinished()) {
            // Skip the waiting checkpoint when current parallelism is 1 which means we do not need
            // to care about the global output data order of snapshot splits and stream split.
            if (currentParallelism == 1) {
                assignerFinished = true;
                log.info(
                    "Snapshot split assigner received all splits finished and the job parallelism is 1, snapshot split assigner is turn into finished status.");

            } else {
                log.info(
                    "Snapshot split assigner received all splits finished, waiting for a complete checkpoint to mark the assigner finished.");
            }
        }
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        for (SourceSplitBase split : splits) {
            tableSchemas.putAll(split.asSnapshotSplit().getTableSchemas());
            remainingSplits.add(split.asSnapshotSplit().toSchemalessSnapshotSplit());
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSplits.remove(split.splitId());
            splitFinishedOffsets.remove(split.splitId());
        }
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        SnapshotPendingSplitsState state =
            new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                tableSchemas,
                splitFinishedOffsets,
                assignerFinished,
                remainingTables,
                isTableIdCaseSensitive,
                true);
        // we need a complete checkpoint before mark this assigner to be finished, to wait for all
        // records of snapshot splits are completely processed
        if (checkpointIdToFinish == null && !assignerFinished && allSplitsFinished()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // finished, then we can mark snapshot assigner as finished.
        if (checkpointIdToFinish != null && !assignerFinished && allSplitsFinished()) {
            assignerFinished = checkpointId >= checkpointIdToFinish;
            log.info("Snapshot split assigner is turn into finished status.");
        }
    }

    @Override
    public void close() {
    }

    /**
     * Indicates there is no more splits available in this assigner.
     */
    public boolean noMoreSplits() {
        return remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /**
     * Returns whether the snapshot split assigner is finished, which indicates there is no more
     * splits and all records of splits have been completely processed in the pipeline.
     */
    public boolean isFinished() {
        return assignerFinished;
    }

    public Map<String, SchemalessSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<TableId, TableChanges.TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public Map<String, Offset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are finished which means no more splits and all assigned splits
     * are finished.
     */
    private boolean allSplitsFinished() {
        return noMoreSplits() && assignedSplits.size() == splitFinishedOffsets.size();
    }
}
