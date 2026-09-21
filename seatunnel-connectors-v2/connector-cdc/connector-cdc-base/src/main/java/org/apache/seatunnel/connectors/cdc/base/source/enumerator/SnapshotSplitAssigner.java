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

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.api.cdc.CdcSnapshotSplitProgress;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.progress.CdcEnumeratorProgressSource;
import org.apache.seatunnel.connectors.cdc.base.source.progress.CdcProgressPositions;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/** Assigner for snapshot split. */
public class SnapshotSplitAssigner<C extends SourceConfig>
        implements SplitAssigner, CdcEnumeratorProgressSource {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitAssigner.class);

    private final SplitAssigner.Context<C> context;

    private final C sourceConfig;
    private final List<TableId> alreadyProcessedTables;
    private final Queue<SnapshotSplit> remainingSplits;
    private final Map<String, SnapshotSplit> assignedSplits;
    private final Map<String, SnapshotSplitWatermark> splitCompletedOffsets;
    private final Map<String, SnapshotSplit> activeSplits;
    private boolean assignerCompleted;
    private final int currentParallelism;
    private final Deque<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private ChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;

    private Long checkpointIdToFinish;
    private final DataSourceDialect<C> dialect;

    // Mutations use this assigner's monitor, but discovery/chunking and progress reads never do.
    private boolean discoveringTables;
    private boolean chunkingTable;
    private volatile CdcEnumeratorProgressReport latestProgress;

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
            Map<String, SnapshotSplitWatermark> splitCompletedOffsets,
            boolean assignerCompleted,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            DataSourceDialect<C> dialect) {
        this.context = context;
        this.sourceConfig = context.getSourceConfig();
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = Collections.synchronizedList(alreadyProcessedTables);
        this.remainingSplits = new ArrayDeque<>(remainingSplits);
        this.assignedSplits = new ConcurrentHashMap<>(assignedSplits);
        this.splitCompletedOffsets = new ConcurrentHashMap<>(splitCompletedOffsets);
        this.activeSplits = new TreeMap<>(assignedSplits);
        this.splitCompletedOffsets.keySet().forEach(this.activeSplits::remove);
        this.assignerCompleted = assignerCompleted;
        this.remainingTables = new ArrayDeque<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.dialect = dialect;
        this.discoveringTables = !isRemainingTablesCheckpointed && !assignerCompleted;
        publishProgress();

        LOG.info("SnapshotSplitAssigner created with remaining tables: {}", this.remainingTables);
        LOG.info(
                "SnapshotSplitAssigner created with remaining splits: [{}]",
                this.remainingSplits.stream()
                        .map(SnapshotSplit::splitId)
                        .collect(Collectors.joining(",")));
        LOG.info(
                "SnapshotSplitAssigner created with assigned splits: {}",
                this.assignedSplits.keySet());
    }

    @Override
    public void open() {
        ChunkSplitter openedSplitter = dialect.createChunkSplitter(sourceConfig);

        // the legacy state didn't snapshot remaining tables, discovery remaining table here
        if (!isRemainingTablesCheckpointed && !assignerCompleted) {
            try {
                final List<TableId> discoverTables = dialect.discoverDataCollections(sourceConfig);
                context.getCapturedTables().addAll(discoverTables);
                discoverTables.removeAll(alreadyProcessedTables);
                boolean caseSensitive = dialect.isDataCollectionIdCaseSensitive(sourceConfig);
                synchronized (this) {
                    this.remainingTables.addAll(discoverTables);
                    this.isTableIdCaseSensitive = caseSensitive;
                    this.discoveringTables = false;
                    publishProgress();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to discover remaining tables to capture", e);
            }
        }
        synchronized (this) {
            chunkSplitter = openedSplitter;
        }
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        while (true) {
            final TableId nextTable;
            final ChunkSplitter splitter;
            synchronized (this) {
                if (chunkSplitter == null) {
                    return Optional.empty();
                }
                SnapshotSplit split = remainingSplits.poll();
                if (split != null) {
                    assignedSplits.put(split.splitId(), split);
                    activeSplits.put(split.splitId(), split);
                    context.getAssignedSnapshotSplit().put(split.splitId(), split);
                    publishProgress();
                    return Optional.of(split);
                }
                if (chunkingTable || remainingTables.isEmpty()) {
                    return Optional.empty();
                }
                // Keep this table unchunked until generation succeeds, including in checkpoints.
                nextTable = remainingTables.peekFirst();
                splitter = chunkSplitter;
                chunkingTable = true;
                publishProgress();
            }
            final Collection<SnapshotSplit> splits;
            try {
                splits = splitter.generateSplits(nextTable);
            } catch (RuntimeException | Error failure) {
                synchronized (this) {
                    chunkingTable = false;
                }
                throw failure;
            }
            synchronized (this) {
                remainingSplits.addAll(splits);
                alreadyProcessedTables.add(nextTable);
                remainingTables.removeFirst();
                chunkingTable = false;
                publishProgress();
            }
        }
    }

    @Override
    public synchronized boolean waitingForCompletedSplits() {
        return !allSplitsCompleted();
    }

    @Override
    public synchronized void onCompletedSplits(
            List<SnapshotSplitWatermark> completedSplitWatermarks) {
        completedSplitWatermarks.forEach(
                watermark -> {
                    String splitId = watermark.getSplitId();
                    if (!assignedSplits.containsKey(splitId)) {
                        // A returned or already retired split cannot complete this assignment.
                        return;
                    }
                    this.splitCompletedOffsets.put(splitId, watermark);
                    this.activeSplits.remove(watermark.getSplitId());
                });
        if (allSplitsCompleted()) {
            if (currentParallelism == 1) {
                // A single-reader job completes immediately. Zeta disables checkpointing
                // entirely for batch jobs without 'checkpoint.interval', so waiting for
                // notifyCheckpointComplete would hang such a job forever. The failover risk
                // of skipping the checkpoint wait is covered by the durable finished-unacked
                // splits in the reader's own checkpoint, whose re-report on restore and the
                // back-fill in restoreCompletedSnapshotSplit reconstruct the completion state
                // without replaying the splits.
                assignerCompleted = true;
                LOG.info(
                        "Snapshot split assigner received all splits completed at parallelism 1, snapshot split assigner is turn into completed status.");
            } else {
                // Multi-reader jobs must wait for a complete checkpoint before switching to
                // the incremental phase, so that all records of snapshot splits are completely
                // processed in the pipeline and no incremental record can overtake a snapshot
                // record of the same key.
                LOG.info(
                        "Snapshot split assigner received all splits completed at parallelism {}, waiting for a complete checkpoint to mark the assigner completed.",
                        currentParallelism);
            }
        }
        publishProgress();
    }

    @Override
    public synchronized void addSplits(Collection<SourceSplitBase> splits) {
        for (SourceSplitBase split : splits) {
            SnapshotSplit snapshotSplit = split.asSnapshotSplit();
            if (restoreCompletedSnapshotSplit(snapshotSplit)) {
                LOG.info(
                        "Restore completed snapshot split {} from checkpoint without replaying it",
                        snapshotSplit.splitId());
                continue;
            }
            remainingSplits.add(snapshotSplit);
            // we should remove the add-backed splits from the assigned list, because they are
            // failed
            assignedSplits.remove(snapshotSplit.splitId());
            splitCompletedOffsets.remove(snapshotSplit.splitId());
            activeSplits.remove(snapshotSplit.splitId());
            assignerCompleted = false;
            checkpointIdToFinish = null;
        }
        publishProgress();
    }

    @Override
    public synchronized SnapshotPhaseState snapshotState(long checkpointId) {
        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        new ArrayList<>(alreadyProcessedTables),
                        remainingSplits.isEmpty()
                                ? new ArrayList<>()
                                : new ArrayList<>(remainingSplits),
                        new HashMap<>(assignedSplits),
                        new HashMap<>(splitCompletedOffsets),
                        assignerCompleted,
                        remainingTables.isEmpty()
                                ? new ArrayList<>()
                                : new ArrayList<>(remainingTables),
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
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // completed, then we can mark snapshot assigner as completed.
        if (checkpointIdToFinish != null && !assignerCompleted && allSplitsCompleted()) {
            assignerCompleted = checkpointId >= checkpointIdToFinish;
            LOG.info("Snapshot split assigner is turn into completed status.");
        }
    }

    /** Indicates there is no more splits available in this assigner. */
    public synchronized boolean noMoreSplits() {
        return !discoveringTables && remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /**
     * Returns whether the snapshot split assigner is completed, which indicates there is no more
     * splits and all records of splits have been completely processed in the pipeline.
     */
    public synchronized boolean isCompleted() {
        return assignerCompleted;
    }

    @Override
    public CdcEnumeratorProgressReport getCdcEnumeratorProgress(
            String connectorType, String positionType) {
        CdcEnumeratorProgressReport snapshot = latestProgress;
        List<CdcSnapshotSplitProgress> details =
                snapshot.getActiveSplits().stream()
                        .map(
                                split ->
                                        new CdcSnapshotSplitProgress(
                                                split.getSplitId(),
                                                split.getTablePath(),
                                                positionType(split.getLowWatermark(), positionType),
                                                positionType(
                                                        split.getHighWatermark(), positionType)))
                        .collect(Collectors.toList());
        return new CdcEnumeratorProgressReport(
                connectorType,
                snapshot.getSnapshotAssignmentStatus(),
                snapshot.getAssignedSplitCount(),
                snapshot.getCompletedSplitCount(),
                snapshot.getRunningSplitCount(),
                snapshot.getPreparedRemainingSplitCount(),
                snapshot.getRemainingUnchunkedTableCount(),
                details,
                snapshot.isActiveSplitsTruncated());
    }

    private CdcProgressValue<CdcProgressPosition> positionType(
            CdcProgressValue<CdcProgressPosition> position, String positionType) {
        return position.getValue() == null
                ? position
                : CdcProgressValue.exact(
                        new CdcProgressPosition(
                                positionType,
                                position.getValue().getSchemaVersion(),
                                position.getValue().getValues()));
    }

    /** Publishes one complete transition; readers never traverse mutable assigner state. */
    private void publishProgress() {
        int activeSplitCount = activeSplits.size();
        List<CdcSnapshotSplitProgress> activeSplitProgress =
                activeSplits.entrySet().stream()
                        .limit(CdcEnumeratorProgressReport.MAX_ACTIVE_SPLITS)
                        .map(entry -> activeSplitProgress(entry.getValue(), "UNKNOWN"))
                        .collect(Collectors.toList());
        latestProgress =
                new CdcEnumeratorProgressReport(
                        "UNKNOWN",
                        snapshotAssignmentStatus(),
                        CdcProgressValue.exact(assignedSplits.size()),
                        CdcProgressValue.exact(splitCompletedOffsets.size()),
                        CdcProgressValue.exact(activeSplitCount),
                        CdcProgressValue.exact(remainingSplits.size()),
                        CdcProgressValue.exact(remainingTables.size()),
                        activeSplitProgress,
                        activeSplitCount > activeSplitProgress.size());
    }

    private CdcSnapshotAssignmentStatus snapshotAssignmentStatus() {
        if (discoveringTables || !remainingTables.isEmpty()) {
            return CdcSnapshotAssignmentStatus.DISCOVERING;
        }
        if (!remainingSplits.isEmpty()) {
            return CdcSnapshotAssignmentStatus.ASSIGNING;
        }
        return CdcSnapshotAssignmentStatus.COMPLETED;
    }

    private CdcSnapshotSplitProgress activeSplitProgress(SnapshotSplit split, String positionType) {
        CdcProgressPosition lowWatermark =
                CdcProgressPositions.fromOffset(positionType, split.getLowWatermark());
        CdcProgressPosition highWatermark =
                CdcProgressPositions.fromOffset(positionType, split.getHighWatermark());
        return new CdcSnapshotSplitProgress(
                split.splitId(),
                split.getTableId().toString(),
                lowWatermark == null
                        ? CdcProgressValue.unavailable()
                        : CdcProgressValue.exact(lowWatermark),
                highWatermark == null
                        ? CdcProgressValue.unavailable()
                        : CdcProgressValue.exact(highWatermark));
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are completed which means no more splits and all assigned splits
     * are completed.
     */
    private boolean allSplitsCompleted() {
        return noMoreSplits() && assignedSplits.size() == splitCompletedOffsets.size();
    }

    /**
     * Returns whether the restored split already has durable completion state in the checkpoint,
     * and back-fills any missing completion watermark from the split itself.
     *
     * <p>A finished reader that never reported its watermark before failover (for example because
     * the reader crashed immediately after marking the split as snapshot-read-finished but before
     * its next CompletedSnapshotSplitsReportEvent went out) can show up after restore as a finished
     * split whose watermark has not yet been checkpointed. Without back-fill, the enumerator would
     * re-enqueue the split and the snapshot phase would never finish.
     *
     * <p>The split is skipped on add-back in that case, and the missing watermark is reconstructed
     * from the split's own low/high watermark. If the split was not finished before the failover we
     * still re-enqueue it so the reader can replay it from its persisted state.
     */
    private boolean restoreCompletedSnapshotSplit(SnapshotSplit snapshotSplit) {
        if (!snapshotSplit.isSnapshotReadFinished()
                || !assignedSplits.containsKey(snapshotSplit.splitId())) {
            return false;
        }
        splitCompletedOffsets.putIfAbsent(
                snapshotSplit.splitId(),
                new SnapshotSplitWatermark(
                        snapshotSplit.splitId(),
                        snapshotSplit.getLowWatermark(),
                        snapshotSplit.getHighWatermark()));
        activeSplits.remove(snapshotSplit.splitId());
        return true;
    }

    @VisibleForTesting
    Map<String, SnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    @VisibleForTesting
    Map<String, SnapshotSplitWatermark> getSplitCompletedOffsets() {
        return splitCompletedOffsets;
    }

    public synchronized boolean completedSnapshotPhase(List<TableId> tableIds) {
        checkArgument(isCompleted() && allSplitsCompleted());

        for (String splitKey : new ArrayList<>(assignedSplits.keySet())) {
            SnapshotSplit assignedSplit = assignedSplits.get(splitKey);
            if (tableIds.contains(assignedSplit.getTableId())) {
                assignedSplits.remove(splitKey);
                splitCompletedOffsets.remove(assignedSplit.splitId());
                activeSplits.remove(assignedSplit.splitId());
            }
        }

        publishProgress();
        return assignedSplits.isEmpty() && splitCompletedOffsets.isEmpty();
    }
}
