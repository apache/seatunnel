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
    private volatile int sinkParallelism = 0;
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

    public void registerSinkParallelism(int parallelism) {
        this.sinkParallelism = parallelism;
        log.info(
                "Registered sink parallelism: {} for schema change coordination in jobId: {}",
                parallelism,
                jobId);
    }

    public void registerSinkStateProvider(int subtaskId, SinkStateProvider provider) {
        sinkStateProviders.put(subtaskId, provider);
        log.info("Registered sink state provider for subtask {} in jobId: {}", subtaskId, jobId);
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
        int expectedAcks = sinkParallelism;
        if (expectedAcks == 0) {
            log.warn(
                    "Sink parallelism not registered yet. Cannot coordinate schema change for table {} (epoch {}). "
                            + "Assuming success to avoid deadlock.",
                    tableId,
                    epoch);
            return true;
        }
        log.info(
                "Requesting schema change for table {} (epoch {}). Waiting for all {} sink subtasks to apply after checkpoint completion.",
                tableId,
                epoch,
                expectedAcks);

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
        log.info(
                "Subtask {} applied schema change for table {} (epoch {}), success: {}. {}/{} subtasks applied.",
                subtaskId,
                tableId,
                epoch,
                success,
                appliedSubtasks.size(),
                request.expectedAcks);

        if (!success) {
            request.allSuccess.set(false);
        }

        // if all subtasks have applied, complete the future
        if (appliedSubtasks.size() >= request.expectedAcks) {
            if (request.appliedPhaseCompleteAtomic.compareAndSet(false, true)) {
                boolean allSuccess = request.allSuccess.get();
                request.future.complete(allSuccess);
                log.info(
                        "All {} subtasks have applied schema change for table {} (epoch {}). Completing request with result: {}",
                        request.expectedAcks,
                        tableId,
                        epoch,
                        allSuccess);
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
