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

package org.apache.seatunnel.connectors.cdc.base.source.progress;

import org.apache.seatunnel.api.cdc.CdcCheckpointProgress;
import org.apache.seatunnel.api.cdc.CdcIncrementalProgress;
import org.apache.seatunnel.api.cdc.CdcProgressPhase;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressSnapshot;
import org.apache.seatunnel.api.cdc.CdcProgressSupportGroup;
import org.apache.seatunnel.api.cdc.CdcProgressSupportLevel;
import org.apache.seatunnel.api.cdc.CdcSnapshotProgress;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Helpers for building normalized CDC progress snapshots from connector CDC state. */
public final class CdcProgressSnapshots {

    private CdcProgressSnapshots() {}

    public static CdcProgressSnapshot forSnapshotPhase(
            String connectorType, SnapshotPhaseState snapshotState, long lastProgressTime) {
        SnapshotSplit currentSplit = currentSnapshotSplit(snapshotState);
        SnapshotSplitWatermark currentWatermark = currentCompletedWatermark(snapshotState);
        CdcSnapshotProgress snapshotProgress =
                CdcSnapshotProgress.builder()
                        .assignedSplitCount(size(snapshotState.getAssignedSplits()))
                        .completedSplitCount(size(snapshotState.getSplitCompletedOffsets()))
                        .runningSplitCount(runningSplitCount(snapshotState))
                        .remainingSplitCount(size(snapshotState.getRemainingSplits()))
                        .currentTable(currentTable(currentSplit))
                        .currentSplitId(currentSplit == null ? null : currentSplit.splitId())
                        .lowWatermark(lowWatermark(currentSplit, currentWatermark))
                        .highWatermark(highWatermark(currentSplit, currentWatermark))
                        .supportLevel(CdcProgressSupportLevel.EXACT)
                        .build();

        return CdcProgressSnapshot.builder()
                .connectorType(connectorType)
                .splitId(currentSplit == null ? null : currentSplit.splitId())
                .phase(
                        snapshotState.isAssignerCompleted()
                                ? CdcProgressPhase.CATCH_UP
                                : CdcProgressPhase.SNAPSHOT)
                .snapshotProgress(snapshotProgress)
                .supportLevels(supportLevels(CdcProgressSupportGroup.SNAPSHOT_PROGRESS))
                .lastProgressTime(lastProgressTime)
                .build();
    }

    public static CdcProgressSnapshot forIncrementalSplit(
            String connectorType, IncrementalSplit split, long lastProgressTime) {
        CdcProgressPosition currentPosition = toPosition(split.getStartupOffset());
        CdcIncrementalProgress incrementalProgress =
                CdcIncrementalProgress.builder()
                        .currentConsumedPosition(currentPosition)
                        .lastProgressTime(lastProgressTime)
                        .supportLevel(CdcProgressSupportLevel.EXACT)
                        .build();
        CdcCheckpointProgress checkpointProgress =
                CdcCheckpointProgress.builder()
                        .lastCheckpointedPosition(currentPosition)
                        .supportLevel(CdcProgressSupportLevel.EXACT)
                        .build();

        return CdcProgressSnapshot.builder()
                .connectorType(connectorType)
                .splitId(split.splitId())
                .phase(
                        split.getCompletedSnapshotSplitInfos().isEmpty()
                                ? CdcProgressPhase.INCREMENTAL
                                : CdcProgressPhase.CATCH_UP)
                .incrementalProgress(incrementalProgress)
                .checkpointProgress(checkpointProgress)
                .rawPosition(currentPosition)
                .supportLevels(
                        supportLevels(
                                CdcProgressSupportGroup.INCREMENTAL_PROGRESS,
                                CdcProgressSupportGroup.CHECKPOINT_PROGRESS,
                                CdcProgressSupportGroup.RAW_POSITION))
                .lastProgressTime(lastProgressTime)
                .build();
    }

    public static CdcProgressPosition toPosition(Offset offset) {
        if (offset == null || offset.getOffset() == null || offset.getOffset().isEmpty()) {
            return CdcProgressPosition.empty();
        }
        return CdcProgressPosition.of(offset.getOffset());
    }

    private static Map<CdcProgressSupportGroup, CdcProgressSupportLevel> supportLevels(
            CdcProgressSupportGroup... groups) {
        if (groups.length == 0) {
            return Collections.emptyMap();
        }
        Map<CdcProgressSupportGroup, CdcProgressSupportLevel> supportLevels =
                new EnumMap<>(CdcProgressSupportGroup.class);
        for (CdcProgressSupportGroup group : groups) {
            supportLevels.put(group, CdcProgressSupportLevel.EXACT);
        }
        return supportLevels;
    }

    private static int runningSplitCount(SnapshotPhaseState snapshotState) {
        return Math.max(
                size(snapshotState.getAssignedSplits())
                        - size(snapshotState.getSplitCompletedOffsets()),
                0);
    }

    private static SnapshotSplit currentSnapshotSplit(SnapshotPhaseState snapshotState) {
        if (snapshotState.getAssignedSplits() != null
                && !snapshotState.getAssignedSplits().isEmpty()) {
            return snapshotState.getAssignedSplits().values().iterator().next();
        }
        List<SnapshotSplit> remainingSplits = snapshotState.getRemainingSplits();
        if (remainingSplits != null && !remainingSplits.isEmpty()) {
            return remainingSplits.get(0);
        }
        return null;
    }

    private static SnapshotSplitWatermark currentCompletedWatermark(
            SnapshotPhaseState snapshotState) {
        if (snapshotState.getSplitCompletedOffsets() == null
                || snapshotState.getSplitCompletedOffsets().isEmpty()) {
            return null;
        }
        return snapshotState.getSplitCompletedOffsets().values().iterator().next();
    }

    private static String currentTable(SnapshotSplit split) {
        return split == null || split.getTableId() == null ? null : split.getTableId().toString();
    }

    private static CdcProgressPosition lowWatermark(
            SnapshotSplit split, SnapshotSplitWatermark watermark) {
        Offset offset =
                split != null && split.getLowWatermark() != null
                        ? split.getLowWatermark()
                        : watermark == null ? null : watermark.getLowWatermark();
        return toPosition(offset);
    }

    private static CdcProgressPosition highWatermark(
            SnapshotSplit split, SnapshotSplitWatermark watermark) {
        Offset offset =
                split != null && split.getHighWatermark() != null
                        ? split.getHighWatermark()
                        : watermark == null ? null : watermark.getHighWatermark();
        return toPosition(offset);
    }

    private static int size(Map<?, ?> map) {
        return map == null ? 0 : map.size();
    }

    private static int size(List<?> list) {
        return list == null ? 0 : list.size();
    }
}
