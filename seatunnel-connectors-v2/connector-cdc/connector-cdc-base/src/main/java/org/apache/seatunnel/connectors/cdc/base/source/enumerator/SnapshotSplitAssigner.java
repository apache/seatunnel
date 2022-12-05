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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Assigner for snapshot split.
 */
public class SnapshotSplitAssigner<C extends SourceConfig> implements SplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitAssigner.class);

    private final SplitAssigner.Context<C> context;

    private final C sourceConfig;
    private final List<TableId> alreadyProcessedTables;
    private final List<SnapshotSplit> remainingSplits;
    private final Map<String, SnapshotSplit> assignedSplits;
    private final Map<String, Offset> splitCompletedOffsets;
    private boolean assignerCompleted;
    private final int currentParallelism;
    private final LinkedList<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private ChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;

    private Long checkpointIdToFinish;
    private final DataSourceDialect<C> dialect;

    SnapshotSplitAssigner(
        SplitAssigner.Context<C> context,
        int currentParallelism,
        List<TableId> remainingTables,
        boolean isTableIdCaseSensitive,
        DataSourceDialect<C> dialect) {
        this(
            context,
            currentParallelism,
            new ArrayList<>(),
            new ArrayList<>(),
            new HashMap<>(),
            new HashMap<>(),
            false,
            remainingTables,
            isTableIdCaseSensitive,
            true,
            dialect);
    }

    SnapshotSplitAssigner(
        SplitAssigner.Context<C> context,
        int currentParallelism,
        SnapshotPhaseState checkpoint,
        DataSourceDialect<C> dialect) {
        this(
            context,
            currentParallelism,
            checkpoint.getAlreadyProcessedTables(),
            checkpoint.getRemainingSplits(),
            checkpoint.getAssignedSplits(),
            checkpoint.getSplitCompletedOffsets(),
            checkpoint.isAssignerCompleted(),
            checkpoint.getRemainingTables(),
            checkpoint.isTableIdCaseSensitive(),
            checkpoint.isRemainingTablesCheckpointed(),
            dialect);
    }

    private SnapshotSplitAssigner(
        SplitAssigner.Context<C> context,
        int currentParallelism,
        List<TableId> alreadyProcessedTables,
        List<SnapshotSplit> remainingSplits,
        Map<String, SnapshotSplit> assignedSplits,
        Map<String, Offset> splitCompletedOffsets,
        boolean assignerCompleted,
        List<TableId> remainingTables,
        boolean isTableIdCaseSensitive,
        boolean isRemainingTablesCheckpointed,
        DataSourceDialect<C> dialect) {
        this.context = context;
        this.sourceConfig = context.getSourceConfig();
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitCompletedOffsets = splitCompletedOffsets;
        this.assignerCompleted = assignerCompleted;
        this.remainingTables = new LinkedList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.dialect = dialect;
    }

    @Override
    public void open() {
        chunkSplitter = dialect.createChunkSplitter(sourceConfig);

        // the legacy state didn't snapshot remaining tables, discovery remaining table here
        if (!isRemainingTablesCheckpointed && !assignerCompleted) {
            try {
                final List<TableId> discoverTables = dialect.discoverDataCollections(sourceConfig);
                context.getCapturedTables().addAll(discoverTables);
                discoverTables.removeAll(alreadyProcessedTables);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = dialect.isDataCollectionIdCaseSensitive(sourceConfig);
            } catch (Exception e) {
                throw new RuntimeException("Failed to discover remaining tables to capture", e);
            }
        }
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            Iterator<SnapshotSplit> iterator = remainingSplits.iterator();
            SnapshotSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for new table
            TableId nextTable = remainingTables.pollFirst();
            if (nextTable != null) {
                // split the given table into chunks (snapshot splits)
                Collection<SnapshotSplit> splits = chunkSplitter.generateSplits(nextTable);
                remainingSplits.addAll(splits);
                alreadyProcessedTables.add(nextTable);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean waitingForCompletedSplits() {
        return !allSplitsCompleted();
    }

    @Override
    public void onCompletedSplits(List<SnapshotSplitWatermark> completedSplitWatermarks) {
        completedSplitWatermarks.forEach(m -> this.splitCompletedOffsets.put(m.getSplitId(), m.getHighWatermark()));
        if (allSplitsCompleted()) {
            // Skip the waiting checkpoint when current parallelism is 1 which means we do not need
            // to care about the global output data order of snapshot splits and incremental split.
            if (currentParallelism == 1) {
                assignerCompleted = true;
                LOG.info("Snapshot split assigner received all splits completed and the job parallelism is 1, snapshot split assigner is turn into completed status.");
            } else {
                LOG.info("Snapshot split assigner received all splits completed, waiting for a complete checkpoint to mark the assigner completed.");
            }
        }
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        for (SourceSplitBase split : splits) {
            remainingSplits.add(split.asSnapshotSplit());
            // we should remove the add-backed splits from the assigned list, because they are failed
            assignedSplits.remove(split.splitId());
            splitCompletedOffsets.remove(split.splitId());
        }
    }

    @Override
    public SnapshotPhaseState snapshotState(long checkpointId) {
        SnapshotPhaseState state =
            new SnapshotPhaseState(
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                splitCompletedOffsets,
                assignerCompleted,
                remainingTables,
                isTableIdCaseSensitive,
                true);
        // we need a complete checkpoint before mark this assigner to be completed, to wait for all
        // records of snapshot splits are completely processed
        if (checkpointIdToFinish == null && !assignerCompleted && allSplitsCompleted()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // completed, then we can mark snapshot assigner as completed.
        if (checkpointIdToFinish != null && !assignerCompleted && allSplitsCompleted()) {
            assignerCompleted = checkpointId >= checkpointIdToFinish;
            LOG.info("Snapshot split assigner is turn into completed status.");
        }
    }

    /**
     * Indicates there is no more splits available in this assigner.
     */
    public boolean noMoreSplits() {
        return remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /**
     * Returns whether the snapshot split assigner is completed, which indicates there is no more
     * splits and all records of splits have been completely processed in the pipeline.
     */
    public boolean isCompleted() {
        return assignerCompleted;
    }

    public Map<String, SnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<String, Offset> getSplitCompletedOffsets() {
        return splitCompletedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are completed which means no more splits and all assigned splits
     * are completed.
     */
    private boolean allSplitsCompleted() {
        return noMoreSplits() && assignedSplits.size() == splitCompletedOffsets.size();
    }
}
