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

package org.apache.seatunnel.engine.server.checkpoint.monitor;

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.InProgressCheckpoint;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.SubtaskStatistics;
import org.apache.seatunnel.engine.server.checkpoint.TaskStatistics;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class CheckpointMonitorService {

    private final SeaTunnelEngineContext engineContext;
    private final int maxHistorySize;

    private volatile CheckpointOverviewStateStore overviewMap;

    public CheckpointMonitorService(SeaTunnelEngineContext engineContext, int maxHistorySize) {
        this.engineContext = engineContext;
        this.maxHistorySize = maxHistorySize;
    }

    private CheckpointOverviewStateStore getOverviewMap() {
        if (overviewMap == null) {
            synchronized (this) {
                if (overviewMap == null) {
                    overviewMap =
                            engineContext
                                    .getStateStores()
                                    .auxiliary()
                                    .checkpointOverviewStateStore();
                }
            }
        }
        return overviewMap;
    }

    public void onCheckpointTriggered(
            long jobId,
            int pipelineId,
            long checkpointId,
            CheckpointType checkpointType,
            long triggerTimestamp,
            int totalSubtasks) {
        updateOverview(
                jobId,
                pipelineId,
                pipeline -> {
                    pipeline.getCounts().incrementTriggered();
                    pipeline.getCounts().incrementInProgress();
                    removeInProgressIfExists(pipeline, checkpointId);
                    pipeline.getInProgress()
                            .add(
                                    new InProgressCheckpoint(
                                            checkpointId,
                                            checkpointType,
                                            triggerTimestamp,
                                            0,
                                            totalSubtasks));
                });
    }

    public void onCheckpointAcknowledge(
            long jobId, int pipelineId, long checkpointId, int acknowledged, int total) {
        updateOverview(
                jobId,
                pipelineId,
                pipeline ->
                        pipeline.getInProgress().stream()
                                .filter(cp -> cp.getCheckpointId() == checkpointId)
                                .findFirst()
                                .ifPresent(
                                        cp -> {
                                            cp.setAcknowledgedSubtasks(acknowledged);
                                            cp.setTotalSubtasks(total);
                                        }));
    }

    public void onCheckpointCompleted(CompletedCheckpoint checkpoint, long stateSizeBytes) {
        updateOverview(
                checkpoint.getJobId(),
                checkpoint.getPipelineId(),
                pipeline -> {
                    pipeline.getCounts().incrementCompleted();
                    removeInProgressIfExists(pipeline, checkpoint.getCheckpointId());
                    CheckpointInfo info =
                            CheckpointInfo.builder()
                                    .checkpointId(checkpoint.getCheckpointId())
                                    .checkpointType(checkpoint.getCheckpointType())
                                    .status(CheckpointStatus.COMPLETED)
                                    .triggerTimestamp(checkpoint.getCheckpointTimestamp())
                                    .completedTimestamp(checkpoint.getCompletedTimestamp())
                                    .durationMillis(
                                            checkpoint.getCompletedTimestamp()
                                                    - checkpoint.getCheckpointTimestamp())
                                    .stateSize(stateSizeBytes)
                                    .build();
                    pipeline.setLatestCompleted(info);
                    if (checkpoint.getCheckpointType().isSavepoint()) {
                        pipeline.setLatestSavepoint(info);
                    }
                    pipeline.addHistory(
                            CheckpointHistoryEntry.builder()
                                    .jobId(checkpoint.getJobId())
                                    .pipelineId(checkpoint.getPipelineId())
                                    .checkpointInfo(info)
                                    .build(),
                            maxHistorySize);
                });
    }

    public void onCheckpointFailed(
            long jobId,
            int pipelineId,
            long checkpointId,
            CheckpointType type,
            CheckpointCloseReason reason,
            Throwable cause,
            long triggerTimestamp) {
        updateOverview(
                jobId,
                pipelineId,
                pipeline -> {
                    pipeline.getCounts().incrementFailed();
                    removeInProgressIfExists(pipeline, checkpointId);
                    CheckpointInfo info =
                            CheckpointInfo.builder()
                                    .checkpointId(checkpointId)
                                    .checkpointType(type)
                                    .status(
                                            CheckpointCloseReason.CHECKPOINT_COORDINATOR_COMPLETED
                                                            == reason
                                                    ? CheckpointStatus.CANCELED
                                                    : CheckpointStatus.FAILED)
                                    .triggerTimestamp(triggerTimestamp)
                                    .failureReason(
                                            cause == null
                                                    ? reason.message()
                                                    : Strings.nullToEmpty(reason.message())
                                                            + " - "
                                                            + cause.getMessage())
                                    .build();
                    pipeline.setLatestFailed(info);
                    pipeline.addHistory(
                            CheckpointHistoryEntry.builder()
                                    .jobId(jobId)
                                    .pipelineId(pipelineId)
                                    .checkpointInfo(info)
                                    .build(),
                            maxHistorySize);
                });
    }

    public void onPipelineRestored(long jobId, int pipelineId) {
        updateOverview(jobId, pipelineId, pipeline -> pipeline.getCounts().incrementRestored());
    }

    public void cleanupJob(long jobId) {
        getOverviewMap().remove(jobId);
    }

    public Optional<CheckpointOverview> getOverview(long jobId) {
        CheckpointOverview overview = getOverviewMap().get(jobId);
        return Optional.ofNullable(overview);
    }

    public List<CheckpointHistoryEntry> getHistory(
            long jobId, Integer pipelineId, int limit, CheckpointStatus status) {
        CheckpointOverview overview = getOverviewMap().get(jobId);
        if (overview == null) {
            return Collections.emptyList();
        }
        List<CheckpointHistoryEntry> entries = new ArrayList<>();
        if (pipelineId == null) {
            overview.getPipelines().values().forEach(p -> entries.addAll(p.getHistory()));
        } else {
            PipelineCheckpointOverview pipelineOverview = overview.getPipelines().get(pipelineId);
            if (pipelineOverview != null) {
                entries.addAll(pipelineOverview.getHistory());
            }
        }

        return entries.stream()
                .filter(entry -> status == null || entry.getCheckpointInfo().getStatus() == status)
                .sorted(
                        (left, right) ->
                                Long.compare(
                                        right.getCheckpointInfo().getTriggerTimestamp(),
                                        left.getCheckpointInfo().getTriggerTimestamp()))
                .limit(limit)
                .collect(Collectors.toList());
    }

    public void clearInProgress(long jobId, int pipelineId) {
        updateOverview(
                jobId,
                pipelineId,
                pipeline -> {
                    pipeline.getCounts().setInProgress(0);
                    pipeline.getInProgress().clear();
                });
    }

    public long getOverviewJobCount() {
        return getOverviewMap().getOverviewJobCount();
    }

    public long getInProgressCheckpointCount() {
        return getOverviewMap().getInProgressCheckpointCount();
    }

    public long getRetainedHistoryCount() {
        return getOverviewMap().getRetainedHistoryCount();
    }

    private void updateOverview(
            long jobId, int pipelineId, Consumer<PipelineCheckpointOverview> consumer) {
        getOverviewMap().updateOverview(jobId, pipelineId, consumer);
    }

    private void removeInProgressIfExists(PipelineCheckpointOverview pipeline, long checkpointId) {
        pipeline.getInProgress().removeIf(cp -> cp.getCheckpointId() == checkpointId);
    }

    public static long calculateStateSize(CompletedCheckpoint checkpoint) {
        return checkpoint.getTaskStatistics().values().stream()
                .map(TaskStatistics::getSubtaskStats)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .filter(Objects::nonNull)
                .mapToLong(SubtaskStatistics::getStateSize)
                .sum();
    }
}
