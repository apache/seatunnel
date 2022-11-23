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
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Assigner for incremental split.
 */
public class IncrementalSplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSplitAssigner.class);
    protected static final String INCREMENTAL_SPLIT_ID = "incremental-split-%d";

    private final SplitAssigner.Context<C> context;

    private final int incrementalParallelism;

    private final OffsetFactory offsetFactory;

    /**
     * Maximum watermark in SnapshotSplits per table.
     * <br> Used to delete information in completedSnapshotSplitInfos, reducing state size.
     * <br> Used to support Exactly-Once.
     */
    private final Map<TableId, Offset> tableWatermarks = new HashMap<>();

    private boolean splitAssigned = false;

    private final List<IncrementalSplit> remainingSplits = new ArrayList<>();

    private final Map<String, IncrementalSplit> assignedSplits = new HashMap<>();

    public IncrementalSplitAssigner(
        SplitAssigner.Context<C> context,
        int incrementalParallelism,
        OffsetFactory offsetFactory) {
        this.context = context;
        this.incrementalParallelism = incrementalParallelism;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {
    }

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
        List<IncrementalSplit> incrementalSplits = createIncrementalSplits();
        remainingSplits.addAll(incrementalSplits);
        splitAssigned = true;
        return getNext();
    }

    /**
     * Indicates there is no more splits available in this assigner.
     */
    public boolean noMoreSplits() {
        return getRemainingTables().isEmpty() && remainingSplits.isEmpty();
    }

    private Set<TableId> getRemainingTables() {
        Set<TableId> allTables = new HashSet<>(context.getCapturedTables());
        assignedSplits.values()
            .forEach(split -> split.getTableIds().forEach(allTables::remove));
        return allTables;
    }

    @Override
    public boolean waitingForCompletedSplits() {
        return false;
    }

    @Override
    public void onCompletedSplits(List<SnapshotSplitWatermark> completedSplitWatermarks) {
        // do nothing
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        // we don't store the split, but will re-create incremental split later
        splits.stream()
            .map(SourceSplitBase::asIncrementalSplit)
            .forEach(incrementalSplit -> {
                Offset startupOffset = incrementalSplit.getStartupOffset();
                List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos = incrementalSplit.getCompletedSnapshotSplitInfos();
                for (CompletedSnapshotSplitInfo info : completedSnapshotSplitInfos) {
                    context.getSplitCompletedOffsets().put(info.getSplitId(), info.getWatermark());
                    context.getAssignedSnapshotSplit().put(info.getSplitId(), info.asSnapshotSplit());
                }
                for (TableId tableId : incrementalSplit.getTableIds()) {
                    tableWatermarks.put(tableId, startupOffset);
                }
            });
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

    public List<IncrementalSplit> createIncrementalSplits() {
        Set<TableId> allTables = new HashSet<>(context.getCapturedTables());
        assignedSplits.values()
            .forEach(split -> split.getTableIds().forEach(allTables::remove));
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
            incrementalSplits.add(createIncrementalSplit(capturedTable, i++));
        }
        return incrementalSplits;
    }

    private IncrementalSplit createIncrementalSplit(List<TableId> capturedTables, int index) {
        final List<SnapshotSplit> assignedSnapshotSplit =
            context.getAssignedSnapshotSplit().values().stream()
                .filter(split -> capturedTables.contains(split.getTableId()))
                .sorted(Comparator.comparing(SourceSplitBase::splitId))
                .collect(Collectors.toList());

        Map<String, Offset> splitCompletedOffsets = context.getSplitCompletedOffsets();
        final List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos = new ArrayList<>();
        Offset minOffset = null;
        for (SnapshotSplit split : assignedSnapshotSplit) {
            // find the min offset of change log
            Offset changeLogOffset = splitCompletedOffsets.get(split.splitId());
            if (minOffset == null || changeLogOffset.isBefore(minOffset)) {
                minOffset = changeLogOffset;
            }
            completedSnapshotSplitInfos.add(
                new CompletedSnapshotSplitInfo(
                    split.splitId(),
                    split.getTableId(),
                    split.getSplitKeyType(),
                    split.getSplitStart(),
                    split.getSplitEnd(),
                    changeLogOffset));
        }
        for (TableId tableId : capturedTables) {
            Offset watermark = tableWatermarks.get(tableId);
            if (minOffset == null || watermark.isBefore(minOffset)) {
                minOffset = watermark;
            }
        }
        C sourceConfig = context.getSourceConfig();
        return new IncrementalSplit(
            String.format(INCREMENTAL_SPLIT_ID, index),
            capturedTables,
            minOffset != null ? minOffset : sourceConfig.getStartupConfig().getStartupOffset(offsetFactory),
            sourceConfig.getStopConfig().getStopOffset(offsetFactory),
            completedSnapshotSplitInfos);
    }
}
