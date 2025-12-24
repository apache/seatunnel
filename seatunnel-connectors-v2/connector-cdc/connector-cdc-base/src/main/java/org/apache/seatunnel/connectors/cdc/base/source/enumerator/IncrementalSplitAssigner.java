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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/** Assigner for incremental split. */
public class IncrementalSplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSplitAssigner.class);
    protected static final String INCREMENTAL_SPLIT_ID = "incremental-split-%d";

    private final SplitAssigner.Context<C> context;

    private final int incrementalParallelism;

    private final OffsetFactory offsetFactory;

    /**
     * Maximum watermark in SnapshotSplits per table. <br>
     * Used to delete information in completedSnapshotSplitInfos, reducing state size. <br>
     * Used to support Exactly-Once.
     */
    private final Map<TableId, Offset> tableWatermarks = new HashMap<>();

    private boolean splitAssigned = false;

    private final List<IncrementalSplit> remainingSplits = new ArrayList<>();

    private final Map<String, IncrementalSplit> assignedSplits = new HashMap<>();

    private boolean startWithSnapshotMinimumOffset = true;
    private List<CatalogTable> checkpointTables;
    private Map<TableId, byte[]> historyTableChanges;

    public IncrementalSplitAssigner(
            SplitAssigner.Context<C> context,
            int incrementalParallelism,
            OffsetFactory offsetFactory) {
        this.context = context;
        this.incrementalParallelism = incrementalParallelism;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {}

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            Iterator<IncrementalSplit> iterator = remainingSplits.iterator();
            IncrementalSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split);
        }
        if (splitAssigned) {
            return Optional.empty();
        }
        List<IncrementalSplit> incrementalSplits =
                createIncrementalSplits(startWithSnapshotMinimumOffset);
        remainingSplits.addAll(incrementalSplits);
        splitAssigned = true;
        return getNext();
    }

    /** Indicates there is no more splits available in this assigner. */
    public boolean noMoreSplits() {
        return getRemainingTables().isEmpty() && remainingSplits.isEmpty();
    }

    private Set<TableId> getRemainingTables() {
        Set<TableId> allTables = new HashSet<>(context.getCapturedTables());
        assignedSplits.values().forEach(split -> split.getTableIds().forEach(allTables::remove));
        return allTables;
    }

    @Override
    public boolean waitingForCompletedSplits() {
        return false;
    }

    @Override
    public void onCompletedSplits(List<SnapshotSplitWatermark> completedSplitWatermarks) {
        // do nothing
        completedSplitWatermarks.forEach(
                watermark ->
                        context.getSplitCompletedOffsets().put(watermark.getSplitId(), watermark));
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        // we don't store the split, but will re-create incremental split later
        splits.stream()
                .map(SourceSplitBase::asIncrementalSplit)
                .forEach(
                        incrementalSplit -> {
                            Offset startupOffset = incrementalSplit.getStartupOffset();
                            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos =
                                    incrementalSplit.getCompletedSnapshotSplitInfos();
                            for (CompletedSnapshotSplitInfo info : completedSnapshotSplitInfos) {
                                if (!context.getCapturedTables().contains(info.getTableId())) {
                                    continue;
                                }
                                context.getSplitCompletedOffsets()
                                        .put(info.getSplitId(), info.getWatermark());
                                context.getAssignedSnapshotSplit()
                                        .put(info.getSplitId(), info.asSnapshotSplit());
                            }
                            for (TableId tableId : incrementalSplit.getTableIds()) {
                                if (!context.getCapturedTables().contains(tableId)) {
                                    continue;
                                }
                                tableWatermarks.put(tableId, startupOffset);
                            }
                            checkpointTables = incrementalSplit.getCheckpointTables();
                            historyTableChanges = incrementalSplit.getHistoryTableChanges();
                        });
        if (!tableWatermarks.isEmpty()) {
            this.startWithSnapshotMinimumOffset = false;
        }
    }

    @Override
    public IncrementalPhaseState snapshotState(long checkpointId) {
        return new IncrementalPhaseState();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    // ------------------------------------------------------------------------------------------

    public List<IncrementalSplit> createIncrementalSplits(boolean startWithSnapshotMinimumOffset) {
        Set<TableId> allTables = new HashSet<>(context.getCapturedTables());
        assignedSplits.values().forEach(split -> split.getTableIds().forEach(allTables::remove));
        List<TableId>[] capturedTables = new List[incrementalParallelism];
        int i = 0;
        for (TableId tableId : allTables) {
            int index = i % incrementalParallelism;
            if (capturedTables[index] == null) {
                capturedTables[index] = new ArrayList<>();
            }
            capturedTables[index].add(tableId);
            i++;
        }
        i = 0;
        List<IncrementalSplit> incrementalSplits = new ArrayList<>();
        for (List<TableId> capturedTable : capturedTables) {
            incrementalSplits.add(
                    createIncrementalSplit(capturedTable, i++, startWithSnapshotMinimumOffset));
        }
        return incrementalSplits;
    }

    private IncrementalSplit createIncrementalSplit(
            List<TableId> capturedTables, int index, boolean startWithSnapshotMinimumOffset) {
        C sourceConfig = context.getSourceConfig();
        final List<SnapshotSplit> assignedSnapshotSplit =
                context.getAssignedSnapshotSplit().values().stream()
                        .filter(split -> capturedTables.contains(split.getTableId()))
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, SnapshotSplitWatermark> splitCompletedOffsets =
                context.getSplitCompletedOffsets();
        final List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos = new ArrayList<>();
        Offset minOffset = null;
        for (SnapshotSplit split : assignedSnapshotSplit) {
            SnapshotSplitWatermark splitWatermark = splitCompletedOffsets.get(split.splitId());
            if (startWithSnapshotMinimumOffset) {
                // find the min offset of change log
                Offset splitOffset =
                        sourceConfig.isExactlyOnce()
                                ? splitWatermark.getHighWatermark()
                                : splitWatermark.getLowWatermark();
                if (minOffset == null || splitOffset.isBefore(minOffset)) {
                    minOffset = splitOffset;
                    LOG.debug(
                            "Find the min offset {} of change log in split {}",
                            splitOffset,
                            splitWatermark);
                }
            }
            completedSnapshotSplitInfos.add(
                    new CompletedSnapshotSplitInfo(
                            split.splitId(),
                            split.getTableId(),
                            split.getSplitKeyType(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            splitWatermark));
        }
        for (TableId tableId : capturedTables) {
            Offset watermark = tableWatermarks.get(tableId);
            if (minOffset == null || (watermark != null && watermark.isBefore(minOffset))) {
                minOffset = watermark;
                LOG.debug(
                        "Find the min offset {} of change log in table-watermarks {}",
                        watermark,
                        tableId);
            }
        }
        Offset incrementalSplitStartOffset =
                minOffset != null
                        ? minOffset
                        : sourceConfig.getStartupConfig().getStartupOffset(offsetFactory);
        return new IncrementalSplit(
                String.format(INCREMENTAL_SPLIT_ID, index),
                capturedTables,
                incrementalSplitStartOffset,
                sourceConfig.getStopConfig().getStopOffset(offsetFactory),
                completedSnapshotSplitInfos,
                checkpointTables,
                historyTableChanges);
    }

    @VisibleForTesting
    void setSplitAssigned(boolean assigned) {
        this.splitAssigned = assigned;
    }

    public boolean completedSnapshotPhase(List<TableId> tableIds) {
        checkArgument(splitAssigned && noMoreSplits());

        for (String splitKey : new ArrayList<>(context.getAssignedSnapshotSplit().keySet())) {
            SnapshotSplit assignedSplit = context.getAssignedSnapshotSplit().get(splitKey);
            if (tableIds.contains(assignedSplit.getTableId())) {
                context.getAssignedSnapshotSplit().remove(splitKey);
                context.getSplitCompletedOffsets().remove(assignedSplit.splitId());
            }
        }
        return context.getAssignedSnapshotSplit().isEmpty()
                && context.getSplitCompletedOffsets().isEmpty();
    }

    public boolean waitingForAssignedSplits() {
        return !(splitAssigned && noMoreSplits());
    }
}
