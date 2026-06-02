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

package org.apache.seatunnel.translation.flink.schema.coordinator;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.exception.SchemaCoordinationException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;

import lombok.extern.slf4j.Slf4j;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Local coordinator for schema change synchronization. This coordinator only manages temporary
 * communication between SchemaOperator and sink subtasks. All persistent state is managed by
 * BroadcastSchemaSinkOperator in Flink State.
 *
 * <p>Schema changes (DDL like ALTER TABLE) are database-level operations that only need to be
 * executed once. In Flink's parallel execution model, SchemaOperator sends schema change events via
 * output.collect() which routes to only ONE downstream subtask based on partitioning. Therefore,
 * this coordinator completes schema change requests when ANY single subtask successfully applies
 * the change, rather than waiting for all subtasks.
 */
@Slf4j
public class LocalSchemaCoordinator {

    private static final Map<String, WeakReference<LocalSchemaCoordinator>> instances =
            new ConcurrentHashMap<>();
    private static final ScheduledExecutorService cleanupExecutor =
            new ScheduledThreadPoolExecutor(
                    1,
                    r -> {
                        Thread t = new Thread(r, "LocalSchemaCoordinator-Cleanup");
                        t.setDaemon(true);
                        return t;
                    });
    private static final long DEFAULT_REQUEST_TTL_MS = 300_000L;
    private static final long CLEANUP_INTERVAL_MS = 60_000L;
    private final String jobId;
    private final long requestTtlMs;
    private final Set<Integer> activeSinkSubtasks = ConcurrentHashMap.newKeySet();
    private final Map<String, TimestampedPendingRequest> pendingRequests =
            new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> receivedAcks = new ConcurrentHashMap<>();
    private final Map<Integer, SinkStateProvider> sinkStateProviders = new ConcurrentHashMap<>();

    private LocalSchemaCoordinator(String jobId, long requestTtlMs) {
        this.jobId = jobId;
        this.requestTtlMs = requestTtlMs;

        cleanupExecutor.scheduleWithFixedDelay(
                this::performPeriodicCleanup,
                CLEANUP_INTERVAL_MS,
                CLEANUP_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        log.info(
                "Created LocalSchemaCoordinator for jobId: {} with TTL: {}ms", jobId, requestTtlMs);
    }

    public static LocalSchemaCoordinator getInstance(String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("JobId cannot be null or empty");
        }

        return instances
                .compute(
                        jobId,
                        (key, weakRef) -> {
                            LocalSchemaCoordinator coordinator = null;
                            if (weakRef != null) {
                                coordinator = weakRef.get();
                            }

                            if (coordinator == null) {
                                coordinator =
                                        new LocalSchemaCoordinator(jobId, DEFAULT_REQUEST_TTL_MS);
                                log.info(
                                        "Created new LocalSchemaCoordinator instance for jobId: {}",
                                        jobId);
                            }

                            return new WeakReference<>(coordinator);
                        })
                .get();
    }

    /**
     * @deprecated Sink parallelism is now tracked dynamically via {@link
     *     #registerSinkStateProvider} and {@link #unregisterSinkSubtask}. This method is kept for
     *     backward compatibility but is effectively a no-op.
     */
    @Deprecated
    public void registerSinkParallelism(int parallelism) {
        log.info(
                "Registered sink parallelism hint: {} for jobId: {} (active tracking used instead)",
                parallelism,
                jobId);
    }

    public void registerSinkStateProvider(int subtaskId, SinkStateProvider provider) {
        sinkStateProviders.put(subtaskId, provider);
        activeSinkSubtasks.add(subtaskId);
        log.info("Registered sink state provider for subtask {} in jobId: {}", subtaskId, jobId);
    }

    public void unregisterSinkSubtask(int subtaskId) {
        boolean removed = activeSinkSubtasks.remove(subtaskId);
        sinkStateProviders.remove(subtaskId);
        if (!removed) {
            return;
        }
        int remaining = activeSinkSubtasks.size();
        log.info(
                "Sink subtask {} unregistered (closed). Active sink subtasks remaining: {} in jobId: {}",
                subtaskId,
                remaining,
                jobId);

        // Check if any pending requests can now be completed
        // (Since we only need 1 ACK for DDL, this typically won't change anything,
        // but we keep it for edge cases where all subtasks close before any ACK)
        for (Map.Entry<String, TimestampedPendingRequest> entry : pendingRequests.entrySet()) {
            String key = entry.getKey();
            TimestampedPendingRequest request = entry.getValue();
            Set<Integer> applied = receivedAcks.get(key);

            // If we already have at least 1 ACK, complete the request
            if (applied != null && !applied.isEmpty()) {
                if (request.appliedPhaseCompleteAtomic.compareAndSet(false, true)) {
                    boolean success = request.allSuccess.get();
                    request.future.complete(success);
                    log.info(
                            "After subtask {} unregistered, completing schema change request for "
                                    + "table {} (epoch {}) with {} ACK(s). Result: {}",
                            subtaskId,
                            request.tableId,
                            request.epoch,
                            applied.size(),
                            success);
                }
            }
        }
    }

    public SchemaProcessingStatus querySchemaProcessingStatus(TableIdentifier tableId, long epoch) {
        if (sinkStateProviders.isEmpty()) {
            log.warn(
                    "No sink state providers registered, assuming schema change not processed for table {} epoch {}",
                    tableId,
                    epoch);
            return SchemaProcessingStatus.NOT_PROCESSED;
        }

        int processedCount = 0;
        int totalProviders = sinkStateProviders.size();

        for (Map.Entry<Integer, SinkStateProvider> entry : sinkStateProviders.entrySet()) {
            int subtaskId = entry.getKey();
            SinkStateProvider provider = entry.getValue();

            try {
                Long lastProcessedEpoch = provider.getLastProcessedEpoch(tableId);
                if (lastProcessedEpoch != null && lastProcessedEpoch >= epoch) {
                    processedCount++;
                    log.debug(
                            "Subtask {} has processed epoch {} for table {}, last processed: {}",
                            subtaskId,
                            epoch,
                            tableId,
                            lastProcessedEpoch);
                } else {
                    log.debug(
                            "Subtask {} has NOT processed epoch {} for table {}, last processed: {}",
                            subtaskId,
                            epoch,
                            tableId,
                            lastProcessedEpoch);
                }
            } catch (Exception e) {
                log.error("Error querying state from sink subtask {}", subtaskId, e);
            }
        }

        if (processedCount == 0) {
            return SchemaProcessingStatus.NOT_PROCESSED;
        } else if (processedCount == totalProviders) {
            return SchemaProcessingStatus.FULLY_PROCESSED;
        } else {
            return SchemaProcessingStatus.PARTIALLY_PROCESSED;
        }
    }

    public enum SchemaProcessingStatus {
        NOT_PROCESSED,
        PARTIALLY_PROCESSED,
        FULLY_PROCESSED
    }

    public boolean requestSchemaChange(TableIdentifier tableId, long epoch, long timeoutMs)
            throws InterruptedException, SchemaCoordinationException {
        String key = tableId.toString() + "#" + epoch;
        int totalSubtasks = activeSinkSubtasks.size();
        if (totalSubtasks == 0) {
            log.warn(
                    "No active sink subtasks. Cannot coordinate schema change for table {} (epoch {}). "
                            + "Assuming success to avoid deadlock.",
                    tableId,
                    epoch);
            return true;
        }
        // Schema changes (DDL) are database-level operations that only need to execute once.
        // Due to Flink's partitioning, only one subtask receives the schema change event,
        // so we only need 1 ACK to confirm the DDL was applied successfully.
        //
        // Precondition: sink subtasks that do NOT receive the schema-change event directly
        // (because Flink's partitioning routed it elsewhere) must have their local schema
        // view refreshed through BroadcastSchemaSinkOperator's broadcast/state path.
        // If that broadcast path is incomplete, those subtasks will silently apply the old
        // schema to new-format rows — a data-corruption risk. Any change to the broadcast
        // path must preserve this invariant, and a multi-table (≥2 tables, parallelism ≥2)
        // E2E test should guard it so regressions are caught immediately.
        int expectedAcks = 1;
        log.info(
                "Requesting schema change for table {} (epoch {}). Waiting for at least {} of {} "
                        + "sink subtasks to apply the DDL (database-level operation).",
                tableId,
                epoch,
                expectedAcks,
                totalSubtasks);

        long now = System.currentTimeMillis();
        TimestampedPendingRequest request =
                new TimestampedPendingRequest(
                        tableId, epoch, expectedAcks, now, Math.min(timeoutMs, requestTtlMs));

        pendingRequests.put(key, request);
        receivedAcks.put(key, ConcurrentHashMap.newKeySet());

        try {
            Boolean result = request.future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (result == null) {
                throw SchemaCoordinationException.conflict(tableId, jobId, jobId);
            }
            if (!result) {
                throw SchemaCoordinationException.conflict(tableId, jobId, jobId);
            }
            return result;
        } catch (TimeoutException e) {
            log.error(
                    "Schema change request for table {} (epoch {}) timed out after {}ms. "
                            + "Checkpoint may not have completed in time.",
                    tableId,
                    epoch,
                    timeoutMs);
            request.future.cancel(true);
            throw SchemaCoordinationException.timeout(tableId, jobId, timeoutMs / 1000, e);
        } catch (ExecutionException e) {
            log.error(
                    "Schema change request for table {} (epoch {}) failed with execution exception.",
                    tableId,
                    epoch,
                    e);
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    e.getMessage(),
                    tableId,
                    jobId,
                    e);
        } finally {
            pendingRequests.remove(key);
            receivedAcks.remove(key);
        }
    }

    public void notifySchemaChangeApplied(
            TableIdentifier tableId, long epoch, int subtaskId, boolean success) {
        String key = tableId.toString() + "#" + epoch;
        TimestampedPendingRequest request = pendingRequests.get(key);

        if (request == null) {
            log.warn(
                    "Received application notification for unknown schema change request: table {} (epoch {}), subtask {}",
                    tableId,
                    epoch,
                    subtaskId);
            return;
        }

        // check if this subtask already applied
        Set<Integer> appliedSubtasks = receivedAcks.get(key);
        if (appliedSubtasks == null) {
            log.warn(
                    "Received application notification but no ack set found for table {} (epoch {}), subtask {}",
                    tableId,
                    epoch,
                    subtaskId);
            return;
        }

        if (appliedSubtasks.contains(subtaskId)) {
            log.warn(
                    "Subtask {} already applied schema change for table {} (epoch {}). Ignoring duplicate notification.",
                    subtaskId,
                    tableId,
                    epoch);
            return;
        }

        appliedSubtasks.add(subtaskId);
        // Schema changes only need 1 successful application since they're database-level operations
        int requiredAcks = request.expectedAcks; // This is now 1
        log.info(
                "Subtask {} applied schema change for table {} (epoch {}), success: {}. "
                        + "{} subtask(s) applied (need {} for completion).",
                subtaskId,
                tableId,
                epoch,
                success,
                appliedSubtasks.size(),
                requiredAcks);

        if (!success) {
            request.allSuccess.set(false);
        }

        // Complete when we have at least 1 successful ACK (DDL only needs to run once)
        if (appliedSubtasks.size() >= requiredAcks && success) {
            if (request.appliedPhaseCompleteAtomic.compareAndSet(false, true)) {
                request.future.complete(true);
                log.info(
                        "Schema change for table {} (epoch {}) successfully applied by subtask {}. "
                                + "DDL execution complete (database-level operation).",
                        tableId,
                        epoch,
                        subtaskId);
            }
        } else if (appliedSubtasks.size() >= requiredAcks && !success) {
            // If the only ACK we got was a failure, complete with failure
            if (request.appliedPhaseCompleteAtomic.compareAndSet(false, true)) {
                request.future.complete(false);
                log.error(
                        "Schema change for table {} (epoch {}) failed on subtask {}.",
                        tableId,
                        epoch,
                        subtaskId);
            }
        }
    }

    private void performPeriodicCleanup() {
        try {
            int cleanedRequests = 0;
            int cleanedAcks = 0;

            // clean expired pending requests
            for (Iterator<Map.Entry<String, TimestampedPendingRequest>> iterator =
                            pendingRequests.entrySet().iterator();
                    iterator.hasNext(); ) {
                Map.Entry<String, TimestampedPendingRequest> entry = iterator.next();
                if (entry.getValue().isExpired() && entry.getValue().future.isDone()) {
                    iterator.remove();
                    cleanedRequests++;
                }
            }

            // clean orphaned ack sets
            for (Iterator<Map.Entry<String, Set<Integer>>> iterator =
                            receivedAcks.entrySet().iterator();
                    iterator.hasNext(); ) {
                Map.Entry<String, Set<Integer>> entry = iterator.next();
                if (!pendingRequests.containsKey(entry.getKey())) {
                    iterator.remove();
                    cleanedAcks++;
                }
            }

            if (cleanedRequests > 0 || cleanedAcks > 0) {
                log.info(
                        "Periodic cleanup for jobId: {} completed. Cleaned {} expired requests, {} orphaned acks. "
                                + "Active requests: {}",
                        jobId,
                        cleanedRequests,
                        cleanedAcks,
                        pendingRequests.size());
            }
        } catch (Exception e) {
            log.error("Error during periodic cleanup for jobId: {}", jobId, e);
        }
    }

    private static class TimestampedPendingRequest {
        final TableIdentifier tableId;
        final long epoch;
        final int expectedAcks;
        final long createdTime;
        final long ttlMs;
        CompletableFuture<Boolean> future;
        final AtomicBoolean allSuccess;
        final AtomicBoolean appliedPhaseCompleteAtomic = new AtomicBoolean(false);

        TimestampedPendingRequest(
                TableIdentifier tableId,
                long epoch,
                int expectedAcks,
                long createdTime,
                long ttlMs) {
            this.tableId = tableId;
            this.epoch = epoch;
            this.expectedAcks = expectedAcks;
            this.createdTime = createdTime;
            this.ttlMs = ttlMs;
            this.future = new CompletableFuture<>();
            this.allSuccess = new AtomicBoolean(true);
        }

        boolean isExpired() {
            return System.currentTimeMillis() - createdTime > ttlMs;
        }
    }
}
