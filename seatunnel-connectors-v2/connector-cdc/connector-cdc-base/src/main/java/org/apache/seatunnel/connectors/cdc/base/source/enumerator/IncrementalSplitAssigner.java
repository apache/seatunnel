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
                            checkpointTables = incrementalSplit.getCheckpointTables();
                            historyTableChanges = incrementalSplit.getHistoryTableChanges();
                        });
        if (!tableWatermarks.isEmpty()) {
            this.startWithSnapshotMinimumOffset = false;
        }
    }

    @Override
    public IncrementalPhaseState snapshotState(long checkpointId) {
        return new IncrementalPhaseState(startupOffset, resolvedStopOffset);
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
        return new IncrementalSplit(
                String.format(INCREMENTAL_SPLIT_ID, index),
                capturedTables,
                incrementalSplitStartOffset,
                incrementalSplitStopOffset,
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
