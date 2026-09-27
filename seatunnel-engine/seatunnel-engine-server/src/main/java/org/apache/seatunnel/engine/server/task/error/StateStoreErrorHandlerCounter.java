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

import java.util.Objects;

/**
 * State-store-backed counter shared by all parallel subtasks of one stage.
 *
 * <p>Each row updates the shared state store immediately so stage-wide hard limits such as
 * max_error_records are enforced against the current parallel total instead of checkpoint-delayed
 * local snapshots.
 */
public class StateStoreErrorHandlerCounter implements ErrorHandlerCounter {

    private static final String KEY_VERSION = "v1";
    private static final String TOTAL_COUNTER = "total";
    private static final String ERROR_COUNTER = "error";

    private final CounterStateStore<String> counterStore;
    private final String totalRecordsKey;
    private final String errorRecordsKey;

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
    }

    @Override
    public long incrementTotalRecords() {
        return addAndGet(totalRecordsKey, 1L);
    }

    @Override
    public long incrementErrorRecords() {
        return addAndGet(errorRecordsKey, 1L);
    }

    @Override
    public long getTotalRecords() {
        return getOrZero(totalRecordsKey);
    }

    @Override
    public long getErrorRecords() {
        return getOrZero(errorRecordsKey);
    }

    @Override
    public void snapshotState(long checkpointId) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

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
}
