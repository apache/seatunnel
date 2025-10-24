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

package org.apache.seatunnel.api.table.coordinator;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.exception.SchemaCoordinationException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/** global unified coordinator for handling schema changes */
@Slf4j
public class SchemaCoordinator implements Serializable {

    private static final Map<String, SchemaCoordinator> JOB_COORDINATORS =
            new ConcurrentHashMap<>();
    private final Map<TableIdentifier, SchemaChangeState> schemaChangeStates;
    private final Map<TableIdentifier, Long> schemaVersions;
    private final Map<TableIdentifier, ReentrantLock> schemaLocks;
    private final Map<TableIdentifier, Map<String, CompletableFuture<SchemaResponse>>>
            pendingRequests;
    private final Map<TableIdentifier, Long> latestSchemaChangeTime = new ConcurrentHashMap<>();

    public SchemaCoordinator() {
        this.schemaChangeStates = new ConcurrentHashMap<>();
        this.schemaVersions = new ConcurrentHashMap<>();
        this.schemaLocks = new ConcurrentHashMap<>();
        this.pendingRequests = new ConcurrentHashMap<>();
    }

    public static SchemaCoordinator getOrCreateInstance(String jobId) {
        return JOB_COORDINATORS.computeIfAbsent(
                jobId,
                k -> {
                    log.info("Creating new SchemaCoordinator instance for job: {}", jobId);
                    return new SchemaCoordinator();
                });
    }

    public static void removeInstance(String jobId) {
        SchemaCoordinator removed = JOB_COORDINATORS.remove(jobId);
        if (removed != null) {
            log.info("Removed SchemaCoordinator instance for job: {}", jobId);
        }
    }

    public void notifyFlushSuccessful(String jobId, TableIdentifier tableId) {
        log.info("Received flush notification: jobId={}, tableId={}", jobId, tableId);

        ReentrantLock lock = schemaLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        lock.lock();
        try {
            SchemaChangeState state = schemaChangeStates.get(tableId);
            if (state == null) {
                log.warn("No schema change state found for table: {}", tableId);
                log.info("Available schema change states: {}", schemaChangeStates.keySet());
                return;
            }

            if (!state.getJobId().equals(jobId)) {
                log.warn(
                        "Job ID mismatch: received jobId={}, expected jobId={}, tableId={}",
                        jobId,
                        state.getJobId(),
                        tableId);
                return;
            }

            // update the schema change status
            state.incrementFlushCount();
            log.info(
                    "Received flush successful notification for table {} ({}), count: {}/{}",
                    tableId,
                    jobId,
                    state.getFlushCount(),
                    state.getTotalOperators());

            if (state.isAllFlushed()) {
                // all operators have completed flushing. The schema change can now be finalized
                log.info("All operators flushed for table {}, completing schema change", tableId);
                completeSchemaChange(tableId, jobId);
            }
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<SchemaResponse> requestSchemaChange(
            TableIdentifier tableId, String jobId, CatalogTable newSchema, int totalOperators) {

        ReentrantLock lock = schemaLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        lock.lock();
        try {
            SchemaChangeState existingState = schemaChangeStates.get(tableId);
            if (existingState != null && existingState.getJobId() != null) {
                log.warn(
                        "Schema change already in progress for table: {}, existing jobId: {}, new jobId: {}",
                        tableId,
                        existingState.getJobId(),
                        jobId);
                return CompletableFuture.completedFuture(
                        SchemaResponse.failure(
                                new SchemaCoordinationException(
                                                SchemaEvolutionErrorCode
                                                        .SCHEMA_CHANGE_ALREADY_IN_PROGRESS,
                                                "Schema change already in progress",
                                                tableId,
                                                jobId)
                                        .getMessage()));
            }

            Long lastProcessedTime = latestSchemaChangeTime.get(tableId);
            if (lastProcessedTime != null && newSchema != null) {
                long currentTime = System.currentTimeMillis();
                if (currentTime <= lastProcessedTime) {
                    log.warn(
                            "Received outdated schema change event for table: {}, current time: {}, last processed: {}",
                            tableId,
                            currentTime,
                            lastProcessedTime);
                    return CompletableFuture.completedFuture(
                            SchemaResponse.failure(
                                    new SchemaValidationException(
                                                    SchemaEvolutionErrorCode.OUTDATED_SCHEMA_EVENT,
                                                    "Schema change event is outdated",
                                                    tableId,
                                                    jobId)
                                            .getMessage()));
                }
            }

            if (newSchema != null && !isValidSchema(newSchema)) {
                log.error("Invalid schema structure for table: {}", tableId);
                return CompletableFuture.completedFuture(
                        SchemaResponse.failure(
                                new SchemaValidationException(
                                                SchemaEvolutionErrorCode.INVALID_SCHEMA_STRUCTURE,
                                                "Invalid schema structure",
                                                tableId,
                                                jobId)
                                        .getMessage()));
            }
            log.info("Processing schema change for table: {}, job: {}", tableId, jobId);

            CatalogTable currentSchema = null;
            if (existingState != null) {
                currentSchema = existingState.getCurrentSchema();
            }

            SchemaChangeState state =
                    new SchemaChangeState(currentSchema, newSchema, jobId, totalOperators);

            schemaChangeStates.put(tableId, state);
            log.info(
                    "SchemaChangeStates tableId: {}, state: {}",
                    tableId,
                    schemaChangeStates.get(tableId));

            // update the latest schema change time
            if (newSchema != null) {
                latestSchemaChangeTime.put(tableId, System.currentTimeMillis());
            }

            return waitForSchemaChangeConfirmation(tableId, jobId);
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<SchemaResponse> waitForSchemaChangeConfirmation(
            TableIdentifier tableId, String jobId) {
        CompletableFuture<SchemaResponse> future = new CompletableFuture<>();

        // add the request to the waiting queue
        pendingRequests.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>()).put(jobId, future);
        return future;
    }

    private void completeSchemaChange(TableIdentifier tableId, String jobId) {
        ReentrantLock lock = schemaLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        lock.lock();
        try {
            SchemaChangeState state = schemaChangeStates.get(tableId);
            if (state == null) {
                log.warn("No schema change state found for table: {} during completion", tableId);
                return;
            }

            if (!jobId.equals(state.getJobId())) {
                log.warn(
                        "Job ID mismatch during completion: expected={}, actual={}, table={}",
                        jobId,
                        state.getJobId(),
                        tableId);
                return;
            }

            if (!state.isAllFlushed()) {
                log.warn(
                        "Attempting to complete schema change but not all operators have flushed: {}/{} for table {}",
                        state.getFlushCount(),
                        state.getTotalOperators(),
                        tableId);
                return;
            }

            long newVersion = schemaVersions.getOrDefault(tableId, 0L) + 1;
            schemaVersions.put(tableId, newVersion);

            SchemaResponse response = SchemaResponse.success(state.getNewSchema(), newVersion);

            Map<String, CompletableFuture<SchemaResponse>> tableRequests =
                    pendingRequests.get(tableId);
            if (tableRequests != null) {
                int completedCount = 0;
                for (CompletableFuture<SchemaResponse> future : tableRequests.values()) {
                    if (!future.isDone()) {
                        future.complete(response);
                        completedCount++;
                    }
                }
                log.info(
                        "Completed {} pending schema change requests for table {}",
                        completedCount,
                        tableId);
                tableRequests.clear();
            }

            state.setCurrentSchema(state.getNewSchema());
            state.setNewSchema(null);
            state.setJobId(null);
            state.resetFlushCount();

            latestSchemaChangeTime.remove(tableId);
            log.info("Schema change completed for table {}, version: {}", tableId, newVersion);
        } finally {
            lock.unlock();
        }
    }

    private boolean isValidSchema(CatalogTable schema) {
        if (schema == null) {
            return false;
        }

        if (schema.getTableId() == null) {
            log.error("Schema has null table identifier");
            return false;
        }

        if (schema.getTableSchema() == null) {
            log.error("Schema has null table schema");
            return false;
        }

        if (schema.getTableSchema().getColumns().isEmpty()) {
            log.error("Schema has no columns");
            return false;
        }

        return true;
    }

    @Getter
    @Setter
    @ToString
    private static class SchemaChangeState {
        private CatalogTable currentSchema;
        private CatalogTable newSchema;
        private String jobId;
        private final AtomicInteger flushCount;
        private final int totalOperators;

        public SchemaChangeState(
                CatalogTable currentSchema,
                CatalogTable newSchema,
                String jobId,
                int totalOperators) {
            this.currentSchema = currentSchema;
            this.newSchema = newSchema;
            this.jobId = jobId;
            this.flushCount = new AtomicInteger(0);
            this.totalOperators = totalOperators;
        }

        public int getFlushCount() {
            return flushCount.get();
        }

        public void incrementFlushCount() {
            flushCount.incrementAndGet();
        }

        public void resetFlushCount() {
            flushCount.set(0);
        }

        public boolean isAllFlushed() {
            return flushCount.get() >= totalOperators;
        }
    }
}
