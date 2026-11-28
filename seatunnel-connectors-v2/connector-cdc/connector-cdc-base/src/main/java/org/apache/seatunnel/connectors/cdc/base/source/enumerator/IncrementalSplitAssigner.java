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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Assigner for incremental split.
 *
 * <p><b>Thread safety:</b> Mutable state in this assigner (such as {@code remainingSplits}, {@code
 * inFlightSplits}, {@code assignedSplits}, {@code assignedTables}, and {@code
 * pendingReturnedTables}) is not internally synchronized and must be guarded by the enumerator
 * {@code context} monitor.
 */
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

    /**
     * Offset each restored table had already been read up to. It is the startup offset of the split
     * the table was restored from, or a later offset that split still carried for the table. Unlike
     * {@link #tableWatermarks} it does not decide where an incremental split starts; it only tells
     * the reader which records of the table were already emitted. Used to support Exactly-Once.
     */
    private final Map<TableId, Offset> tableStartupOffsets = new HashMap<>();

    private boolean splitAssigned = false;

    private final List<IncrementalSplit> remainingSplits = new ArrayList<>();

    /**
     * Incremental splits returned by {@link #getNext()} whose {@code context.assignSplit(...)}
     * handoff has not yet completed. Included in {@link #snapshotState(long)} so a checkpoint taken
     * while delivery is in transit cannot drop the split.
     */
    private final Map<String, IncrementalSplit> inFlightSplits = new LinkedHashMap<>();

    private final Map<String, IncrementalSplit> assignedSplits = new HashMap<>();

    /**
     * Captured tables already covered by an incremental split (either assigned to a reader or
     * queued in {@link #remainingSplits}). Persisted in {@link IncrementalPhaseState} so tables
     * added across a restore are discovered without duplicating splits still held by readers.
     */
    private final Set<TableId> assignedTables = new LinkedHashSet<>();

    /**
     * Tables returned via {@link #addSplits(Collection)} after initial split generation that still
     * need incremental splits created for them.
     */
    private final Set<TableId> pendingReturnedTables = new LinkedHashSet<>();

    private boolean startWithSnapshotMinimumOffset = true;
    private List<CatalogTable> checkpointTables;
    private final Set<TablePath> ownedCheckpointTablePaths = new HashSet<>();
    private Map<TableId, byte[]> historyTableChanges;
    private final Set<TableId> ownedHistoryTableIds = new HashSet<>();

    /**
     * The startup offset resolved on fresh enumerator creation and reused after checkpoint restore.
     */
    private Offset startupOffset;

    /**
     * The stop offset resolved once when the snapshot phase completes ({@code stop.mode = latest}),
     * reused after checkpoint restore so that a restart does not re-resolve (and drift) it.
     */
    private Offset resolvedStopOffset;

    private final boolean restoredFromCheckpoint;

    public IncrementalSplitAssigner(
            SplitAssigner.Context<C> context,
            int incrementalParallelism,
            OffsetFactory offsetFactory) {
        this.context = context;
        this.incrementalParallelism = incrementalParallelism;
        this.offsetFactory = offsetFactory;
        this.restoredFromCheckpoint = false;
        StartupConfig startupConfig =
                context.getSourceConfig() == null
                        ? null
                        : context.getSourceConfig().getStartupConfig();
        this.startupOffset =
                startupConfig == null ? null : startupConfig.getStartupOffset(offsetFactory);
    }

    public IncrementalSplitAssigner(
            SplitAssigner.Context<C> context,
            int incrementalParallelism,
            OffsetFactory offsetFactory,
            IncrementalPhaseState checkpointState) {
        this.context = context;
        this.incrementalParallelism = incrementalParallelism;
        this.offsetFactory = offsetFactory;
        this.restoredFromCheckpoint = true;
        this.startupOffset = checkpointState == null ? null : checkpointState.getStartupOffset();
        this.resolvedStopOffset = checkpointState == null ? null : checkpointState.getStopOffset();
        this.splitAssigned = checkpointState != null && checkpointState.isSplitAssigned();
        if (checkpointState != null) {
            Set<TableId> capturedTables = context.getCapturedTables();
            for (IncrementalSplit split : checkpointState.getRemainingSplits()) {
                IncrementalSplit pruned = filterSplitTables(split, capturedTables::contains);
                if (pruned != null) {
                    this.remainingSplits.add(pruned);
                }
            }
            if (checkpointState.isAssignedTablesCheckpointed()) {
                checkpointState.getAssignedTables().stream()
                        .filter(capturedTables::contains)
                        .forEach(this.assignedTables::add);
                this.remainingSplits.forEach(
                        split -> this.assignedTables.addAll(split.getTableIds()));
            } else if (this.splitAssigned) {
                // Legacy checkpoint written before assignedTables existed: assume currently
                // captured tables are already covered so readers keeping splits locally (e.g.
                // Flink) do not receive duplicate splits. Hybrid restores derive legacy
                // assignedTables from SnapshotPhaseState in IncrementalSource.restore().
                this.assignedTables.addAll(capturedTables);
            }
        }
    }

    @Override
    public void open() {}

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (splitAssigned) {
            Set<TableId> tablesToCreate = getPendingTablesToAssign();
            if (!tablesToCreate.isEmpty()) {
                List<IncrementalSplit> createdSplits =
                        createIncrementalSplitsForTables(
                                tablesToCreate,
                                startWithSnapshotMinimumOffset,
                                collectUsedSplitIds(remainingSplits),
                                computeTargetSplitParallelism(remainingSplits));
                pendingReturnedTables.clear();
                assignedTables.addAll(tablesToCreate);
                remainingSplits.addAll(createdSplits);
            }
        }
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            Iterator<IncrementalSplit> iterator = remainingSplits.iterator();
            IncrementalSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            assignedTables.addAll(split.getTableIds());
            return Optional.of(split);
        }
        if (splitAssigned) {
            return Optional.empty();
        }
        pendingReturnedTables.clear();
        List<IncrementalSplit> incrementalSplits =
                createIncrementalSplits(startWithSnapshotMinimumOffset);
        incrementalSplits.forEach(split -> assignedTables.addAll(split.getTableIds()));
        remainingSplits.addAll(incrementalSplits);
        splitAssigned = true;
        return getNext();
    }

    @Override
    public void onSplitAssigning(SourceSplitBase split) {
        if (split.isIncrementalSplit()) {
            inFlightSplits.put(split.splitId(), split.asIncrementalSplit());
        }
    }

    @Override
    public void onSplitAssigned(SourceSplitBase split) {
        if (split.isIncrementalSplit()) {
            inFlightSplits.remove(split.splitId());
        }
    }

    @Override
    public void onSplitAssignFailed(SourceSplitBase split) {
        if (split.isIncrementalSplit()) {
            IncrementalSplit incrementalSplit = inFlightSplits.remove(split.splitId());
            if (incrementalSplit != null) {
                assignedSplits.remove(incrementalSplit.splitId());
                remainingSplits.add(0, incrementalSplit);
            }
        }
    }

    /** Indicates there is no more splits available in this assigner. */
    public boolean noMoreSplits() {
        if (!remainingSplits.isEmpty()) {
            return false;
        }
        if (splitAssigned) {
            return getPendingTablesToAssign().isEmpty();
        }
        return getRemainingTables().isEmpty();
    }

    private Set<TableId> getPendingTablesToAssign() {
        Set<TableId> tables = new LinkedHashSet<>(pendingReturnedTables);
        tables.addAll(getUncoveredTables());
        return tables;
    }

    private Set<TableId> getUncoveredTables() {
        Set<TableId> uncoveredTables = new LinkedHashSet<>(context.getCapturedTables());
        uncoveredTables.removeAll(assignedTables);
        assignedSplits
                .values()
                .forEach(split -> split.getTableIds().forEach(uncoveredTables::remove));
        inFlightSplits
                .values()
                .forEach(split -> split.getTableIds().forEach(uncoveredTables::remove));
        remainingSplits.forEach(split -> split.getTableIds().forEach(uncoveredTables::remove));
        return uncoveredTables;
    }

    private Set<TableId> getRemainingTables() {
        Set<TableId> allTables = new LinkedHashSet<>(context.getCapturedTables());
        assignedSplits.values().forEach(split -> split.getTableIds().forEach(allTables::remove));
        inFlightSplits.values().forEach(split -> split.getTableIds().forEach(allTables::remove));
        remainingSplits.forEach(split -> split.getTableIds().forEach(allTables::remove));
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
        if (splits.isEmpty()) {
            return;
        }
        Set<TableId> returnedTables = new LinkedHashSet<>();
        // we don't store the split, but will re-create incremental split later
        splits.stream()
                .map(SourceSplitBase::asIncrementalSplit)
                .forEach(
                        incrementalSplit -> {
                            assignedSplits.remove(incrementalSplit.splitId());
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
                                returnedTables.add(tableId);
                                pendingReturnedTables.add(tableId);
                                tableWatermarks.put(tableId, startupOffset);
                                tableStartupOffsets.put(
                                        tableId,
                                        laterOf(
                                                startupOffset,
                                                incrementalSplit
                                                        .getTableStartupOffsets()
                                                        .get(tableId)));
                            }
                            if (this.startupOffset == null) {
                                this.startupOffset = startupOffset;
                            }
                            // Mirror the startupOffset restoration for the resolved latest
                            // stop offset: a checkpoint written before the stopOffset field
                            // existed has none, so on restore the re-created incremental
                            // split carries the previously resolved stop offset. Reuse it
                            // here instead of re-resolving (and drifting) at split creation.
                            // If several splits with already-diverged stop offsets are handed
                            // back in the same call, the first one processed wins (they can
                            // only diverge in the same narrow legacy-checkpoint upgrade case).
                            if (resolvedStopOffset == null
                                    && context.getSourceConfig() != null
                                    && context.getSourceConfig().getStopConfig().getStopMode()
                                            == StopMode.LATEST
                                    && incrementalSplit.getStopOffset() != null) {
                                resolvedStopOffset = incrementalSplit.getStopOffset();
                            }
                            Set<TableId> splitOwnedTables =
                                    incrementalSplit.getTableIds() == null
                                            ? Collections.emptySet()
                                            : new HashSet<>(incrementalSplit.getTableIds());
                            if (incrementalSplit.getCheckpointTables() != null) {
                                mergeCheckpointTables(
                                        incrementalSplit.getCheckpointTables(), splitOwnedTables);
                            }
                            if (incrementalSplit.getHistoryTableChanges() != null
                                    && !incrementalSplit.getHistoryTableChanges().isEmpty()) {
                                if (historyTableChanges == null) {
                                    historyTableChanges = new HashMap<>();
                                }
                                incrementalSplit
                                        .getHistoryTableChanges()
                                        .forEach(
                                                (tableId, change) -> {
                                                    if (!context.getCapturedTables()
                                                            .contains(tableId)) {
                                                        return;
                                                    }
                                                    if (splitOwnedTables.contains(tableId)) {
                                                        historyTableChanges.put(tableId, change);
                                                        ownedHistoryTableIds.add(tableId);
                                                    } else if (!ownedHistoryTableIds.contains(
                                                            tableId)) {
                                                        historyTableChanges.putIfAbsent(
                                                                tableId, change);
                                                    }
                                                });
                            }
                        });
        if (!returnedTables.isEmpty()) {
            pruneReturnedTablesFromRemainingSplits(returnedTables);
        }
        if (!tableWatermarks.isEmpty()) {
            this.startWithSnapshotMinimumOffset = false;
        }
    }

    @Override
    public IncrementalPhaseState snapshotState(long checkpointId) {
        List<IncrementalSplit> checkpointRemainingSplits =
                new ArrayList<>(inFlightSplits.size() + remainingSplits.size());
        checkpointRemainingSplits.addAll(inFlightSplits.values());
        checkpointRemainingSplits.addAll(remainingSplits);
        if (splitAssigned && !pendingReturnedTables.isEmpty()) {
            checkpointRemainingSplits.addAll(
                    createIncrementalSplitsForTables(
                            new LinkedHashSet<>(pendingReturnedTables),
                            startWithSnapshotMinimumOffset,
                            collectUsedSplitIds(checkpointRemainingSplits),
                            computeTargetSplitParallelism(checkpointRemainingSplits)));
        }
        Set<TableId> checkpointAssignedTables = new LinkedHashSet<>(assignedTables);
        checkpointRemainingSplits.forEach(
                split -> checkpointAssignedTables.addAll(split.getTableIds()));
        return new IncrementalPhaseState(
                startupOffset,
                resolvedStopOffset,
                splitAssigned,
                checkpointRemainingSplits,
                checkpointAssignedTables);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    // ------------------------------------------------------------------------------------------

    public List<IncrementalSplit> createIncrementalSplits(boolean startWithSnapshotMinimumOffset) {
        return createIncrementalSplitsForTables(
                getRemainingTables(),
                startWithSnapshotMinimumOffset,
                collectUsedSplitIds(remainingSplits),
                incrementalParallelism);
    }

    private int computeTargetSplitParallelism(Collection<IncrementalSplit> queuedSplits) {
        int occupiedSlots = queuedSplits.size();
        for (String assignedSplitId : assignedSplits.keySet()) {
            if (!inFlightSplits.containsKey(assignedSplitId)) {
                occupiedSlots++;
            }
        }
        Set<TableId> stillHeldTables = new HashSet<>(assignedTables);
        queuedSplits.forEach(split -> split.getTableIds().forEach(stillHeldTables::remove));
        assignedSplits
                .values()
                .forEach(split -> split.getTableIds().forEach(stillHeldTables::remove));
        inFlightSplits
                .values()
                .forEach(split -> split.getTableIds().forEach(stillHeldTables::remove));
        stillHeldTables.removeAll(pendingReturnedTables);
        int estimatedRestoredReaderSplits = 0;
        if (!stillHeldTables.isEmpty() && !assignedTables.isEmpty()) {
            int proportional =
                    (int)
                            Math.ceil(
                                    (double) stillHeldTables.size()
                                            * incrementalParallelism
                                            / assignedTables.size());
            estimatedRestoredReaderSplits =
                    Math.min(stillHeldTables.size(), Math.max(1, proportional));
        }
        return Math.max(1, incrementalParallelism - occupiedSlots - estimatedRestoredReaderSplits);
    }

    private List<IncrementalSplit> createIncrementalSplitsForTables(
            Set<TableId> tables,
            boolean startWithSnapshotMinimumOffset,
            Set<String> usedSplitIds,
            int targetParallelism) {
        int bucketCount = Math.max(1, targetParallelism);
        List<TableId>[] capturedTables = new List[bucketCount];
        int i = 0;
        for (TableId tableId : tables) {
            int index = i % bucketCount;
            if (capturedTables[index] == null) {
                capturedTables[index] = new ArrayList<>();
            }
            capturedTables[index].add(tableId);
            i++;
        }
        i = 0;
        List<IncrementalSplit> incrementalSplits = new ArrayList<>();
        for (List<TableId> capturedTable : capturedTables) {
            // Restored split pruning can shrink the captured-table set below the configured
            // parallelism, so some round-robin buckets may legitimately stay empty.
            if (capturedTable == null || capturedTable.isEmpty()) {
                i++;
                continue;
            }
            int splitIndex = allocateSplitIndex(i++, usedSplitIds);
            incrementalSplits.add(
                    createIncrementalSplit(
                            capturedTable, splitIndex, startWithSnapshotMinimumOffset));
        }
        return incrementalSplits;
    }

    private Set<String> collectUsedSplitIds(Collection<IncrementalSplit> queuedSplits) {
        Set<String> usedSplitIds = new HashSet<>(assignedSplits.keySet());
        usedSplitIds.addAll(inFlightSplits.keySet());
        queuedSplits.forEach(split -> usedSplitIds.add(split.splitId()));
        return usedSplitIds;
    }

    private int allocateSplitIndex(int preferredIndex, Set<String> usedSplitIds) {
        int candidate = preferredIndex;
        while (!usedSplitIds.add(String.format(INCREMENTAL_SPLIT_ID, candidate))) {
            candidate++;
        }
        return candidate;
    }

    private void mergeCheckpointTables(
            List<CatalogTable> splitCheckpointTables, Set<TableId> splitOwnedTables) {
        if (checkpointTables == null) {
            checkpointTables = new ArrayList<>();
        }
        if (splitCheckpointTables.isEmpty()) {
            return;
        }
        Map<TablePath, CatalogTable> merged = new LinkedHashMap<>();
        for (CatalogTable existing : checkpointTables) {
            merged.put(existing.getTablePath(), existing);
        }
        for (CatalogTable table : splitCheckpointTables) {
            TablePath tablePath = table.getTablePath();
            TableId tableId =
                    new TableId(
                            table.getTableId().getDatabaseName(),
                            table.getTableId().getSchemaName(),
                            table.getTableId().getTableName());
            if (splitOwnedTables.contains(tableId)) {
                merged.put(tablePath, table);
                ownedCheckpointTablePaths.add(tablePath);
            } else if (!ownedCheckpointTablePaths.contains(tablePath)) {
                merged.putIfAbsent(tablePath, table);
            }
        }
        checkpointTables = new ArrayList<>(merged.values());
    }

    private void pruneReturnedTablesFromRemainingSplits(Set<TableId> returnedTables) {
        List<IncrementalSplit> updatedRemaining = new ArrayList<>(remainingSplits.size());
        for (IncrementalSplit split : remainingSplits) {
            IncrementalSplit pruned =
                    filterSplitTables(split, tableId -> !returnedTables.contains(tableId));
            if (pruned != null) {
                updatedRemaining.add(pruned);
            }
        }
        remainingSplits.clear();
        remainingSplits.addAll(updatedRemaining);
    }

    private static IncrementalSplit filterSplitTables(
            IncrementalSplit split, Predicate<TableId> keepTable) {
        List<TableId> originalTables = split.getTableIds();
        if (originalTables == null || originalTables.isEmpty()) {
            return null;
        }
        List<TableId> keptTables =
                originalTables.stream().filter(keepTable).collect(Collectors.toList());
        if (keptTables.size() == originalTables.size()) {
            return split;
        }
        if (keptTables.isEmpty()) {
            return null;
        }
        Set<TableId> keptTableSet = new HashSet<>(keptTables);
        List<CompletedSnapshotSplitInfo> keptCompletedSnapshotSplitInfos =
                split.getCompletedSnapshotSplitInfos() == null
                        ? new ArrayList<>()
                        : split.getCompletedSnapshotSplitInfos().stream()
                                .filter(info -> keptTableSet.contains(info.getTableId()))
                                .collect(Collectors.toList());
        Map<TableId, byte[]> keptHistoryTableChanges =
                split.getHistoryTableChanges() == null
                        ? null
                        : split.getHistoryTableChanges().entrySet().stream()
                                .filter(entry -> keptTableSet.contains(entry.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<TableId, Offset> keptTableStartupOffsets =
                split.getTableStartupOffsets().entrySet().stream()
                        .filter(entry -> keptTableSet.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new IncrementalSplit(
                split.splitId(),
                keptTables,
                split.getStartupOffset(),
                split.getStopOffset(),
                keptCompletedSnapshotSplitInfos,
                split.getCheckpointTables(),
                keptHistoryTableChanges,
                keptTableStartupOffsets);
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
            if (startWithSnapshotMinimumOffset
                    || !tableWatermarks.containsKey(split.getTableId())) {
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
        if (minOffset == null && startupOffset == null) {
            if (restoredFromCheckpoint
                    && sourceConfig.getStartupConfig().getStartupMode()
                            == StartupMode.COMMITTED_OFFSET) {
                throw new IllegalStateException(
                        "The restored committed-offset checkpoint does not contain its startup offset");
            }
            startupOffset = sourceConfig.getStartupConfig().getStartupOffset(offsetFactory);
        }
        Offset incrementalSplitStartOffset = minOffset != null ? minOffset : startupOffset;
        // Restored tables from different splits can be regrouped into this split, which starts at
        // the earliest of their offsets. Keep the offset of every table that was already read
        // further, so the reader does not emit those records a second time.
        Map<TableId, Offset> splitTableStartupOffsets = new HashMap<>();
        for (TableId tableId : capturedTables) {
            Offset tableStartupOffset = tableStartupOffsets.get(tableId);
            if (tableStartupOffset != null
                    && incrementalSplitStartOffset != null
                    && tableStartupOffset.isAfter(incrementalSplitStartOffset)) {
                splitTableStartupOffsets.put(tableId, tableStartupOffset);
            }
        }
        // stop.mode=latest: resolve the stop offset exactly once here, at the first
        // incremental split creation, and reuse the same value for every subsequent split
        // and for the checkpoint. This is the single authoritative resolution point (the
        // split handed to the reader and the checkpointed value can never diverge, so a
        // restore cannot silently move the stop boundary). Other stop modes keep the
        // split's configured offset from StopConfig.
        if (resolvedStopOffset == null
                && sourceConfig.getStopConfig().getStopMode() == StopMode.LATEST) {
            // The latest() resolution opens a fresh JDBC connection to query the current
            // binlog position; a transient failure at this exact snapshot-to-incremental
            // transition point must not fail the whole job, so retry with a short backoff
            // (mirrors the connection-factory retry pattern).
            resolvedStopOffset = resolveLatestStopOffsetWithRetry(sourceConfig);
            LOG.info(
                    "stop.mode=latest: resolved stop offset {} at incremental split creation",
                    resolvedStopOffset);
        }
        Offset incrementalSplitStopOffset =
                resolvedStopOffset != null
                        ? resolvedStopOffset
                        : sourceConfig.getStopConfig().getStopOffset(offsetFactory);
        Map<TableId, byte[]> splitHistoryTableChanges =
                historyTableChanges == null
                        ? null
                        : historyTableChanges.entrySet().stream()
                                .filter(entry -> capturedTables.contains(entry.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new IncrementalSplit(
                String.format(INCREMENTAL_SPLIT_ID, index),
                capturedTables,
                incrementalSplitStartOffset,
                incrementalSplitStopOffset,
                completedSnapshotSplitInfos,
                checkpointTables,
                splitHistoryTableChanges,
                splitTableStartupOffsets);
    }

    private static Offset laterOf(Offset offset, Offset other) {
        if (offset == null) {
            return other;
        }
        return other != null && other.isAfter(offset) ? other : offset;
    }

    @VisibleForTesting
    void setSplitAssigned(boolean assigned) {
        this.splitAssigned = assigned;
    }

    public boolean completedSnapshotPhase(List<TableId> tableIds) {
        checkArgument(splitAssigned);

        for (String splitKey : new ArrayList<>(context.getAssignedSnapshotSplit().keySet())) {
            SnapshotSplit assignedSplit = context.getAssignedSnapshotSplit().get(splitKey);
            if (tableIds.contains(assignedSplit.getTableId())) {
                context.getAssignedSnapshotSplit().remove(splitKey);
                context.getSplitCompletedOffsets().remove(assignedSplit.splitId());
            }
        }
        boolean completed =
                context.getAssignedSnapshotSplit().isEmpty()
                        && context.getSplitCompletedOffsets().isEmpty();
        return completed;
    }

    public boolean waitingForAssignedSplits() {
        return !(splitAssigned && noMoreSplits());
    }

    /**
     * Resolves the latest stop offset with retry, so a transient failure of the underlying JDBC
     * query (e.g. a momentarily unreachable database at the snapshot-to-incremental transition)
     * does not fail the whole job. Mirrors the retry pattern used by the CDC connection factory.
     *
     * <p>Threading note: this runs on the job's own split-enumerator coordinator thread (invoked
     * via {@code IncrementalSourceEnumerator.assignSplits()} → {@code getNext()}), which is per-job
     * and not shared across concurrently running sources; the bounded backoff (at most 300ms +
     * 600ms) blocks only this job's enumerator, matching the connection-factory retry.
     */
    private Offset resolveLatestStopOffsetWithRetry(C sourceConfig) {
        final int maxRetries = 3;
        for (int attempt = 1; ; attempt++) {
            try {
                return sourceConfig.getStopConfig().getStopOffset(offsetFactory);
            } catch (RuntimeException e) {
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(300L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SeaTunnelException(
                                "Interrupted while retrying latest stop offset resolution", ie);
                    }
                    LOG.warn("Resolving latest stop offset failed, retry times {}", attempt, e);
                } else {
                    LOG.error("Resolving latest stop offset failed after {} attempts", attempt, e);
                    throw new SeaTunnelException(
                            "Failed to resolve the latest stop offset after "
                                    + attempt
                                    + " attempts",
                            e);
                }
            }
        }
    }
}
