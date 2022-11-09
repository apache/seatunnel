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

import org.apache.seatunnel.connectors.seatunnel.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state.PendingSplitsState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.StreamSplit;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Assigner for Hybrid split which contains snapshot splits and stream splits. */
public class HybridSplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private static final String STREAM_SPLIT_ID = "stream-split";

    private final int splitMetaGroupSize;

    private boolean isStreamSplitAssigned;

    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;

    private final OffsetFactory offsetFactory;

    public HybridSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this(
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        dialect,
                        offsetFactory),
                false,
                sourceConfig.getSplitMetaGroupSize(),
                offsetFactory);
    }

    public HybridSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this(
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        dialect,
                        offsetFactory),
                checkpoint.isStreamSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize(),
                offsetFactory);
    }

    private HybridSplitAssigner(
            SnapshotSplitAssigner<C> snapshotSplitAssigner,
            boolean isStreamSplitAssigned,
            int splitMetaGroupSize,
            OffsetFactory offsetFactory) {
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (snapshotSplitAssigner.noMoreSplits()) {
            // stream split assigning
            if (isStreamSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (snapshotSplitAssigner.isFinished()) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the stream split. Otherwise, records emitted from stream split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isStreamSplitAssigned = true;
                return Optional.of(createStreamSplit());
            } else {
                // stream split is not ready by now
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        List<SourceSplitBase> snapshotSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create stream split later
                isStreamSplitAssigned = false;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isStreamSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    public StreamSplit createStreamSplit() {
        final List<SchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, Offset> splitFinishedOffsets = snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        Offset minOffset = null;
        for (SchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min offset of change log
            Offset changeLogOffset = splitFinishedOffsets.get(split.splitId());
            if (minOffset == null || changeLogOffset.isBefore(minOffset)) {
                minOffset = changeLogOffset;
            }
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            changeLogOffset,
                            offsetFactory));
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them

        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new StreamSplit(
                STREAM_SPLIT_ID,
                minOffset == null ? offsetFactory.createInitialOffset() : minOffset,
                offsetFactory.createNoStoppingOffset(),
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }
}
