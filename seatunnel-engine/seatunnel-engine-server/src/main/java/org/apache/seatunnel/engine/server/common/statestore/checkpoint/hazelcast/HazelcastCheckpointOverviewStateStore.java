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

package org.apache.seatunnel.engine.server.common.statestore.checkpoint.hazelcast;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Hazelcast-backed implementation of {@link CheckpointOverviewStateStore}. */
public class HazelcastCheckpointOverviewStateStore
        implements CheckpointOverviewStateStore, AutoCloseable {

    private final IMap<Long, CheckpointOverview> overviewMap;
    private final AtomicLong overviewJobCount = new AtomicLong();
    private final AtomicLong inProgressCheckpointCount = new AtomicLong();
    private final AtomicLong retainedHistoryCount = new AtomicLong();
    private final Object overviewStatsLock = new Object();
    private final Set<Long> dirtyJobIds = ConcurrentHashMap.newKeySet();
    private final Map<Long, CheckpointOverviewStats> statsByJobId = new HashMap<>();
    private final AtomicBoolean statsInitializing = new AtomicBoolean(false);
    private volatile UUID overviewListenerId;

    public HazelcastCheckpointOverviewStateStore(IMap<Long, CheckpointOverview> overviewMap) {
        this.overviewMap = Objects.requireNonNull(overviewMap, "overviewMap");
        initStats();
    }

    @Override
    public CheckpointOverview get(Long jobId) {
        return overviewMap.get(jobId);
    }

    @Override
    public void put(Long jobId, CheckpointOverview overview) {
        overviewMap.put(jobId, overview);
    }

    @Override
    public CheckpointOverview putIfAbsent(Long jobId, CheckpointOverview overview) {
        return overviewMap.putIfAbsent(jobId, overview);
    }

    @Override
    public void remove(Long jobId) {
        overviewMap.remove(jobId);
    }

    @Override
    public boolean containsKey(Long jobId) {
        return overviewMap.containsKey(jobId);
    }

    @Override
    public boolean isEmpty() {
        return overviewJobCount.get() == 0L;
    }

    @Override
    public int size() {
        return Math.toIntExact(overviewJobCount.get());
    }

    @Override
    public void updateOverview(
            long jobId, int pipelineId, Consumer<PipelineCheckpointOverview> updater) {
        overviewMap.compute(
                jobId,
                (id, overview) -> {
                    CheckpointOverview snapshot =
                            overview == null ? new CheckpointOverview(jobId) : overview;
                    PipelineCheckpointOverview pipeline = snapshot.getOrCreatePipeline(pipelineId);
                    updater.accept(pipeline);
                    snapshot.setUpdatedAt(System.currentTimeMillis());
                    return snapshot;
                });
    }

    @Override
    public long getOverviewJobCount() {
        return overviewJobCount.get();
    }

    @Override
    public long getInProgressCheckpointCount() {
        return inProgressCheckpointCount.get();
    }

    @Override
    public long getRetainedHistoryCount() {
        return retainedHistoryCount.get();
    }

    @Override
    public void close() {
        removeOverviewListener();
    }

    private void initStats() {
        removeOverviewListener();
        overviewListenerId =
                overviewMap.addEntryListener(new CheckpointOverviewEntryListener(), true);
        statsInitializing.set(true);
        dirtyJobIds.clear();

        Map<Long, CheckpointOverviewStats> snapshotStats = new HashMap<>();
        overviewMap.forEach(
                (jobId, overview) -> snapshotStats.put(jobId, toOverviewStats(overview)));

        Set<Long> dirtyJobIdsDuringSnapshot = new HashSet<>(dirtyJobIds);
        for (Long jobId : dirtyJobIdsDuringSnapshot) {
            snapshotStats.put(jobId, toOverviewStats(overviewMap.get(jobId)));
        }

        synchronized (overviewStatsLock) {
            statsByJobId.clear();
            overviewJobCount.set(0L);
            inProgressCheckpointCount.set(0L);
            retainedHistoryCount.set(0L);
            snapshotStats.forEach(this::replaceOverviewStatsLocked);
            statsInitializing.set(false);

            Set<Long> postSnapshotDirtyJobIds = new HashSet<>(dirtyJobIds);
            dirtyJobIds.clear();
            for (Long jobId : postSnapshotDirtyJobIds) {
                replaceOverviewStatsLocked(jobId, toOverviewStats(overviewMap.get(jobId)));
            }
        }
    }

    private void removeOverviewListener() {
        if (overviewListenerId != null) {
            overviewMap.removeEntryListener(overviewListenerId);
            overviewListenerId = null;
        }
        statsInitializing.set(false);
        dirtyJobIds.clear();
        synchronized (overviewStatsLock) {
            statsByJobId.clear();
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
        CheckpointOverviewStats currentStats = statsByJobId.get(jobId);
        if (currentStats != null) {
            overviewJobCount.addAndGet(-currentStats.jobCount);
            inProgressCheckpointCount.addAndGet(-currentStats.inProgressCheckpointCount);
            retainedHistoryCount.addAndGet(-currentStats.retainedHistoryCount);
        }

        if (stats.isEmpty()) {
            statsByJobId.remove(jobId);
            return;
        }

        statsByJobId.put(jobId, stats);
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
                .map(PipelineCheckpointOverview::getHistory)
                .filter(Objects::nonNull)
                .mapToLong(Collection::size)
                .sum();
    }

    private void replaceOverviewStats(Long jobId, CheckpointOverview overview) {
        if (statsInitializing.get()) {
            dirtyJobIds.add(jobId);
            return;
        }
        synchronized (overviewStatsLock) {
            if (statsInitializing.get()) {
                dirtyJobIds.add(jobId);
                return;
            }
            replaceOverviewStatsLocked(jobId, toOverviewStats(overview));
        }
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
}
