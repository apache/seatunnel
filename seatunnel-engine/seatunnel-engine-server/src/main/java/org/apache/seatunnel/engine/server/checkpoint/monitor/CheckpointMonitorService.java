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

import org.apache.seatunnel.engine.common.Constant;
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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class CheckpointMonitorService {

    private final NodeEngine nodeEngine;
    private volatile IMap<Long, CheckpointOverview> overviewMap;
    private final int maxHistorySize;
    private final AtomicLong overviewJobCount = new AtomicLong();
    private final AtomicLong inProgressCheckpointCount = new AtomicLong();
    private final AtomicLong retainedHistoryCount = new AtomicLong();
    private final Object overviewStatsLock = new Object();
    private final Set<Long> overviewDirtyJobIds = ConcurrentHashMap.newKeySet();
    private final Map<Long, CheckpointOverviewStats> overviewStatsByJobId = new HashMap<>();
    private final AtomicBoolean overviewStatsInitializing = new AtomicBoolean(false);
    private volatile UUID overviewListenerId;

    public CheckpointMonitorService(NodeEngine nodeEngine, int maxHistorySize) {
        this.nodeEngine = nodeEngine;
        this.maxHistorySize = maxHistorySize;
    }

    private IMap<Long, CheckpointOverview> getOverviewMap() {
        if (overviewMap == null) {
            synchronized (this) {
                if (overviewMap == null) {
                    overviewMap =
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_CHECKPOINT_MONITOR);
                    initOverviewStats();
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
        return overviewJobCount.get();
    }

    public long getInProgressCheckpointCount() {
        return inProgressCheckpointCount.get();
    }

    public long getRetainedHistoryCount() {
        return retainedHistoryCount.get();
    }

    private void updateOverview(
            long jobId, int pipelineId, Consumer<PipelineCheckpointOverview> consumer) {
        getOverviewMap()
                .compute(
                        jobId,
                        (id, overview) -> {
                            CheckpointOverview snapshot =
                                    overview == null ? new CheckpointOverview(jobId) : overview;
                            PipelineCheckpointOverview pipeline =
                                    snapshot.getOrCreatePipeline(pipelineId);
                            consumer.accept(pipeline);
                            snapshot.setUpdatedAt(System.currentTimeMillis());
                            return snapshot;
                        });
    }

    private void removeInProgressIfExists(PipelineCheckpointOverview pipeline, long checkpointId) {
        pipeline.getInProgress().removeIf(cp -> cp.getCheckpointId() == checkpointId);
    }

    private void initOverviewStats() {
        removeOverviewListener();
        overviewListenerId =
                overviewMap.addEntryListener(new CheckpointOverviewEntryListener(), true);
        overviewStatsInitializing.set(true);
        overviewDirtyJobIds.clear();

        Map<Long, CheckpointOverviewStats> snapshotStats = new HashMap<>();
        overviewMap.forEach(
                (jobId, overview) -> snapshotStats.put(jobId, toOverviewStats(overview)));

        Set<Long> dirtyJobIds = new HashSet<>(overviewDirtyJobIds);
        for (Long jobId : dirtyJobIds) {
            snapshotStats.put(jobId, toOverviewStats(overviewMap.get(jobId)));
        }

        synchronized (overviewStatsLock) {
            overviewStatsByJobId.clear();
            overviewJobCount.set(0L);
            inProgressCheckpointCount.set(0L);
            retainedHistoryCount.set(0L);
            snapshotStats.forEach(this::replaceOverviewStatsLocked);
            overviewStatsInitializing.set(false);

            Set<Long> postSnapshotDirtyJobIds = new HashSet<>(overviewDirtyJobIds);
            overviewDirtyJobIds.clear();
            for (Long jobId : postSnapshotDirtyJobIds) {
                replaceOverviewStatsLocked(jobId, toOverviewStats(overviewMap.get(jobId)));
            }
        }
    }

    private void removeOverviewListener() {
        if (overviewMap != null && overviewListenerId != null) {
            overviewMap.removeEntryListener(overviewListenerId);
            overviewListenerId = null;
        }
        overviewStatsInitializing.set(false);
        overviewDirtyJobIds.clear();
        synchronized (overviewStatsLock) {
            overviewStatsByJobId.clear();
        }
        overviewJobCount.set(0L);
        inProgressCheckpointCount.set(0L);
        retainedHistoryCount.set(0L);
    }

    private CheckpointOverviewStats toOverviewStats(CheckpointOverview overview) {
        if (overview == null) {
            return CheckpointOverviewStats.EMPTY;
        }
        return new CheckpointOverviewStats(
                1L, getInProgressCount(overview), getHistoryCount(overview));
    }

    private void replaceOverviewStatsLocked(Long jobId, CheckpointOverviewStats stats) {
        CheckpointOverviewStats currentStats = overviewStatsByJobId.get(jobId);
        if (currentStats != null) {
            overviewJobCount.addAndGet(-currentStats.jobCount);
            inProgressCheckpointCount.addAndGet(-currentStats.inProgressCheckpointCount);
            retainedHistoryCount.addAndGet(-currentStats.retainedHistoryCount);
        }

        if (stats.isEmpty()) {
            overviewStatsByJobId.remove(jobId);
            return;
        }

        overviewStatsByJobId.put(jobId, stats);
        overviewJobCount.addAndGet(stats.jobCount);
        inProgressCheckpointCount.addAndGet(stats.inProgressCheckpointCount);
        retainedHistoryCount.addAndGet(stats.retainedHistoryCount);
    }

    private long getInProgressCount(CheckpointOverview overview) {
        return overview.getPipelines().values().stream()
                .filter(Objects::nonNull)
                .mapToLong(pipelineOverview -> pipelineOverview.getInProgress().size())
                .sum();
    }

    private long getHistoryCount(CheckpointOverview overview) {
        return overview.getPipelines().values().stream()
                .filter(Objects::nonNull)
                .mapToLong(pipelineOverview -> pipelineOverview.getHistory().size())
                .sum();
    }

    private final class CheckpointOverviewEntryListener
            implements EntryAddedListener<Long, CheckpointOverview>,
                    EntryUpdatedListener<Long, CheckpointOverview>,
                    EntryRemovedListener<Long, CheckpointOverview>,
                    EntryExpiredListener<Long, CheckpointOverview> {

        @Override
        public void entryAdded(EntryEvent<Long, CheckpointOverview> event) {
            replaceOverviewStats(event.getKey(), event.getValue());
        }

        @Override
        public void entryUpdated(EntryEvent<Long, CheckpointOverview> event) {
            replaceOverviewStats(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(EntryEvent<Long, CheckpointOverview> event) {
            replaceOverviewStats(event.getKey(), null);
        }

        @Override
        public void entryExpired(EntryEvent<Long, CheckpointOverview> event) {
            replaceOverviewStats(event.getKey(), null);
        }
    }

    private void replaceOverviewStats(Long jobId, CheckpointOverview overview) {
        if (overviewStatsInitializing.get()) {
            overviewDirtyJobIds.add(jobId);
            return;
        }
        synchronized (overviewStatsLock) {
            if (overviewStatsInitializing.get()) {
                overviewDirtyJobIds.add(jobId);
                return;
            }
            replaceOverviewStatsLocked(jobId, toOverviewStats(overview));
        }
    }

    private static final class CheckpointOverviewStats {
        private static final CheckpointOverviewStats EMPTY =
                new CheckpointOverviewStats(0L, 0L, 0L);

        private final long jobCount;
        private final long inProgressCheckpointCount;
        private final long retainedHistoryCount;

        private CheckpointOverviewStats(
                long jobCount, long inProgressCheckpointCount, long retainedHistoryCount) {
            this.jobCount = jobCount;
            this.inProgressCheckpointCount = inProgressCheckpointCount;
            this.retainedHistoryCount = retainedHistoryCount;
        }

        private boolean isEmpty() {
            return jobCount == 0L && inProgressCheckpointCount == 0L && retainedHistoryCount == 0L;
        }
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
