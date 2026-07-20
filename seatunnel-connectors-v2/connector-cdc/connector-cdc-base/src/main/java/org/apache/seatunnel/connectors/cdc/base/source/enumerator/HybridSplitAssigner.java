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

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/** Assigner for Hybrid split which contains snapshot splits and incremental splits. */
public class HybridSplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSplitAssigner.class);

    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;

    private final IncrementalSplitAssigner<C> incrementalSplitAssigner;

    /**
     * When {@code true}, the assigner stops after the snapshot phase completes and never assigns
     * incremental (binlog) splits, so the job becomes bounded and finishes cleanly. Derived from
     * the source startup mode (see {@code StartupMode#SNAPSHOT}); it is not persisted in checkpoint
     * state because the startup mode is immutable for the lifetime of the job.
     */
    private final boolean snapshotOnly;

    public HybridSplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            int incrementalParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this(
                context,
                currentParallelism,
                incrementalParallelism,
                remainingTables,
                isTableIdCaseSensitive,
                dialect,
                offsetFactory,
                false);
    }

    public HybridSplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            int incrementalParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory,
            boolean snapshotOnly) {
        this(
                new SnapshotSplitAssigner<>(
                        context,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        dialect),
                new IncrementalSplitAssigner<>(context, incrementalParallelism, offsetFactory),
                snapshotOnly);
    }

    public HybridSplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            int incrementalParallelism,
            HybridPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this(
                context,
                currentParallelism,
                incrementalParallelism,
                checkpoint,
                dialect,
                offsetFactory,
                false);
    }

    public HybridSplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            int incrementalParallelism,
            HybridPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory,
            boolean snapshotOnly) {
        this(
                new SnapshotSplitAssigner<>(
                        context,
                        currentParallelism,
                        snapshotPhaseStateForRestore(checkpoint, snapshotOnly),
                        dialect),
                new IncrementalSplitAssigner<>(context, incrementalParallelism, offsetFactory),
                snapshotOnly);
    }

    private static SnapshotPhaseState snapshotPhaseStateForRestore(
            HybridPendingSplitsState checkpoint, boolean snapshotOnly) {
        SnapshotPhaseState snapshotState = checkpoint.getSnapshotPhaseState();
        if (!snapshotOnly || snapshotState.isRemainingTablesCheckpointed()) {
            return snapshotState;
        }
        return new SnapshotPhaseState(
                snapshotState.getAlreadyProcessedTables(),
                snapshotState.getRemainingSplits(),
                snapshotState.getAssignedSplits(),
                snapshotState.getSplitCompletedOffsets(),
                snapshotState.isAssignerCompleted(),
                snapshotState.getRemainingTables(),
                snapshotState.isTableIdCaseSensitive(),
                true);
    }

    private HybridSplitAssigner(
            SnapshotSplitAssigner<C> snapshotSplitAssigner,
            IncrementalSplitAssigner<C> incrementalSplitAssigner,
            boolean snapshotOnly) {
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.incrementalSplitAssigner = incrementalSplitAssigner;
        this.snapshotOnly = snapshotOnly;
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (!snapshotSplitAssigner.noMoreSplits()) {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
        if (!snapshotSplitAssigner.isCompleted()) {
            // incremental split is not ready by now
            return Optional.empty();
        }
        if (snapshotOnly) {
            // snapshot-only mode: the snapshot phase is complete, so do not transition into
            // incremental (binlog) reading. Returning empty here, together with
            // waitingForCompletedSplits() returning false, lets the enumerator signal
            // no-more-splits
            // so the bounded job can finish cleanly.
            return Optional.empty();
        }
        // incremental split assigning
        if (!incrementalSplitAssigner.noMoreSplits()) {
            // we need to wait snapshot-assigner to be completed before
            // assigning the incremental split. Otherwise, records emitted from incremental split
            // might be out-of-order in terms of same primary key with snapshot splits.
            return incrementalSplitAssigner.getNext();
        }
        // no more splits for the assigner
        return Optional.empty();
    }

    @Override
    public boolean waitingForCompletedSplits() {
        if (snapshotOnly) {
            // A checkpoint is only required before transitioning to incremental reading. Snapshot-
            // only jobs can finish as soon as every snapshot split reports completion.
            return snapshotSplitAssigner.waitingForCompletedSplits();
        }
        return snapshotSplitAssigner.waitingForCompletedSplits()
                || incrementalSplitAssigner.waitingForAssignedSplits();
    }

    @Override
    public void onCompletedSplits(List<SnapshotSplitWatermark> completedSplitWatermarks) {
        snapshotSplitAssigner.onCompletedSplits(completedSplitWatermarks);
        incrementalSplitAssigner.onCompletedSplits(completedSplitWatermarks);
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        List<SourceSplitBase> snapshotSplits = new ArrayList<>();
        List<SourceSplitBase> incrementalSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                incrementalSplits.add(split);
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
        incrementalSplitAssigner.addSplits(incrementalSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId),
                incrementalSplitAssigner.snapshotState(checkpointId));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
        incrementalSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @VisibleForTesting
    IncrementalSplitAssigner<C> getIncrementalSplitAssigner() {
        return incrementalSplitAssigner;
    }

    @VisibleForTesting
    SnapshotSplitAssigner<C> getSnapshotSplitAssigner() {
        return snapshotSplitAssigner;
    }

    public boolean completedSnapshotPhase(List<TableId> tableIds) {
        return Arrays.asList(
                        snapshotSplitAssigner.completedSnapshotPhase(tableIds),
                        incrementalSplitAssigner.completedSnapshotPhase(tableIds))
                .stream()
                .allMatch(Predicate.isEqual(true));
    }
}
