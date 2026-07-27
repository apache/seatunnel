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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Checkpoint-aware state-store-backed counter.
 *
 * <p>Row processing updates local atomics only. Local deltas are captured on checkpoint barriers
 * and published to the shared state store only after the checkpoint is reported complete, keeping
 * Hazelcast operations out of the per-row hot path and avoiding committed counters for aborted
 * checkpoints.
 */
public class StateStoreErrorHandlerCounter implements ErrorHandlerCounter {

    private static final String KEY_VERSION = "v1";
    private static final String TOTAL_COUNTER = "total";
    private static final String ERROR_COUNTER = "error";

    private final CounterStateStore<String> counterStore;
    private final String totalRecordsKey;
    private final String errorRecordsKey;
    private final AtomicLong localTotalRecords = new AtomicLong();
    private final AtomicLong localErrorRecords = new AtomicLong();
    private final AtomicLong committedLocalTotalRecords = new AtomicLong();
    private final AtomicLong committedLocalErrorRecords = new AtomicLong();
    private final AtomicLong visibleCommittedTotalRecords = new AtomicLong();
    private final AtomicLong visibleCommittedErrorRecords = new AtomicLong();
    private final NavigableMap<Long, CounterSnapshot> pendingSnapshots = new TreeMap<>();

    public StateStoreErrorHandlerCounter(
            CounterStateStore<String> counterStore,
            long jobId,
            int pipelineId,
            long actionId,
            String stageName) {
        this.counterStore = Objects.requireNonNull(counterStore, "counterStore");
        String scopeKey = buildScopeKey(jobId, pipelineId, actionId, stageName);
        this.totalRecordsKey = scopeKey + ":" + TOTAL_COUNTER;
        this.errorRecordsKey = scopeKey + ":" + ERROR_COUNTER;
        initializeIfAbsent(totalRecordsKey);
        initializeIfAbsent(errorRecordsKey);
        this.visibleCommittedTotalRecords.set(getOrZero(totalRecordsKey));
        this.visibleCommittedErrorRecords.set(getOrZero(errorRecordsKey));
    }

    @Override
    public long incrementTotalRecords() {
        return visibleCommittedTotalRecords.get()
                + localTotalRecords.incrementAndGet()
                - committedLocalTotalRecords.get();
    }

    @Override
    public long incrementErrorRecords() {
        return visibleCommittedErrorRecords.get()
                + localErrorRecords.incrementAndGet()
                - committedLocalErrorRecords.get();
    }

    @Override
    public long getTotalRecords() {
        return visibleCommittedTotalRecords.get()
                + localTotalRecords.get()
                - committedLocalTotalRecords.get();
    }

    @Override
    public long getErrorRecords() {
        return visibleCommittedErrorRecords.get()
                + localErrorRecords.get()
                - committedLocalErrorRecords.get();
    }

    @Override
    public synchronized void snapshotState(long checkpointId) {
        pendingSnapshots.put(
                checkpointId,
                new CounterSnapshot(localTotalRecords.get(), localErrorRecords.get()));
    }

    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        CounterSnapshot snapshot =
                pendingSnapshots.floorEntry(checkpointId) == null
                        ? null
                        : pendingSnapshots.floorEntry(checkpointId).getValue();
        if (snapshot == null) {
            return;
        }

        long totalDelta = snapshot.totalRecords - committedLocalTotalRecords.get();
        long errorDelta = snapshot.errorRecords - committedLocalErrorRecords.get();
        if (totalDelta > 0) {
            visibleCommittedTotalRecords.set(addAndGet(totalRecordsKey, totalDelta));
            committedLocalTotalRecords.addAndGet(totalDelta);
        }
        if (errorDelta > 0) {
            visibleCommittedErrorRecords.set(addAndGet(errorRecordsKey, errorDelta));
            committedLocalErrorRecords.addAndGet(errorDelta);
        }
        pendingSnapshots.headMap(checkpointId, true).clear();
    }

    @Override
    public synchronized void notifyCheckpointAborted(long checkpointId) {
        pendingSnapshots.remove(checkpointId);
    }

    static String buildScopeKey(long jobId, int pipelineId, long actionId, String stageName) {
        return String.join(
                ":",
                KEY_VERSION,
                String.valueOf(jobId),
                String.valueOf(pipelineId),
                String.valueOf(actionId),
                Objects.requireNonNull(stageName, "stageName"));
    }

    private void initializeIfAbsent(String key) {
        counterStore.initializeIfAbsent(key, 0L);
    }

    private long addAndGet(String key, long delta) {
        Long current = counterStore.addAndGet(key, delta);
        if (current != null) {
            return current;
        }
        initializeIfAbsent(key);
        current = counterStore.addAndGet(key, delta);
        if (current == null) {
            throw new IllegalStateException("Error handler counter is absent after initialize");
        }
        return current;
    }

    private long getOrZero(String key) {
        Long current = counterStore.get(key);
        return current == null ? 0L : current;
    }

    private static final class CounterSnapshot implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long totalRecords;
        private final long errorRecords;

        private CounterSnapshot(long totalRecords, long errorRecords) {
            this.totalRecords = totalRecords;
            this.errorRecords = errorRecords;
        }
    }
}
