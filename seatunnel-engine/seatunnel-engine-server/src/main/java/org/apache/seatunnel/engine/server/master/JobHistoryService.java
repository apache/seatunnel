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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStatusData;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.telemetry.log.operation.CleanLogOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobHistoryService {

    private final NodeEngine nodeEngine;

    /**
     * IMap key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link JobStatus} {@link PipelineStatus} {@link
     * org.apache.seatunnel.engine.server.execution.ExecutionState}
     *
     * <p>This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node
     * active
     */
    private final IMap<Object, Object> runningJobStateIMap;

    private final ILogger logger;

    /**
     * key: job id; <br>
     * value: job master;
     */
    private final Map<Long, JobMaster> runningJobMasterMap;

    /**
     * key: job id; <br>
     * value: PendingJobInfo;
     */
    private final Map<Long, PendingJobInfo> pendingJobInfoMap;

    /** finishedJobVertexInfoImap key is jobId and value is JobDAGInfo */
    private final IMap<Long, JobDAGInfo> finishedJobDAGInfoImap;

    /**
     * finishedJobStateImap key is jobId and value is jobState(json) JobStateData Indicates the
     * status of the job, pipeline, and task
     */
    @Getter private final IMap<Long, JobState> finishedJobStateImap;

    private static final int MAX_MONITORING_JOB_NAME_LENGTH = 256;

    private static final int MAX_MONITORING_ERROR_SUMMARY_LENGTH = 1024;

    private static final int MAX_MONITORING_DRAIN_BATCH_SIZE = 1000;

    private static final long MONITORING_RETRY_DELAY_SECONDS = 5L;

    private static final long MONITORING_CAPACITY_OR_LOCK_WAIT_MILLIS = 100L;

    private static final int MAX_LOCAL_MONITORING_RETRY_RECORDS = 1000;

    // TTL-bound sequence ledger queried by the monitoring REST endpoint.
    private final IMap<Long, JobMonitoringRecord> finishedJobMonitoringImap;

    // Durable sequence, retention-head, and job-to-sequence watermarks.
    private final IMap<String, Long> finishedJobMonitoringMetadataImap;

    // Durable sidecar outbox; entries are removed only after ledger commit.
    private final IQueue<JobMonitoringRecord> pendingJobMonitoringQueue;

    // Bounded durable overflow used when the primary queue cannot accept a record.
    private final IMap<Long, JobMonitoringRecord> overflowJobMonitoringImap;

    // Last-resort retry buffer used only when the distributed outbox is temporarily unavailable.
    private final Map<Long, JobMonitoringRecord> localPendingJobMonitoringRecords =
            new ConcurrentHashMap<>();

    // Prevents concurrent drain workers in one coordinator.
    private final AtomicBoolean monitoringDrainScheduled = new AtomicBoolean(false);

    // Node-local delta retained until it can be merged into the distributed dropped counter.
    private final AtomicLong pendingDroppedJobMonitoringRecords = new AtomicLong();

    private final IMap<Long, JobMetrics> finishedJobMetricsImap;

    private final ObjectMapper objectMapper;

    private final int finishedJobExpireTime;

    private final Map<String, AtomicLong> finishedJobCleanupTotals = new ConcurrentHashMap<>();

    public JobHistoryService(
            NodeEngine nodeEngine,
            IMap<Object, Object> runningJobStateIMap,
            ILogger logger,
            Map<Long, PendingJobInfo> pendingJobMasterMap,
            Map<Long, JobMaster> runningJobMasterMap,
            IMap<Long, JobState> finishedJobStateImap,
            IMap<Long, JobMonitoringRecord> finishedJobMonitoringImap,
            IMap<String, Long> finishedJobMonitoringMetadataImap,
            IQueue<JobMonitoringRecord> pendingJobMonitoringQueue,
            IMap<Long, JobMonitoringRecord> overflowJobMonitoringImap,
            IMap<Long, JobMetrics> finishedJobMetricsImap,
            IMap<Long, JobDAGInfo> finishedJobVertexInfoImap,
            int finishedJobExpireTime) {
        this.nodeEngine = nodeEngine;
        this.runningJobStateIMap = runningJobStateIMap;
        this.logger = logger;
        this.pendingJobInfoMap = pendingJobMasterMap;
        this.runningJobMasterMap = runningJobMasterMap;
        this.finishedJobStateImap = finishedJobStateImap;
        this.finishedJobMonitoringImap = finishedJobMonitoringImap;
        this.finishedJobMonitoringMetadataImap = finishedJobMonitoringMetadataImap;
        this.pendingJobMonitoringQueue = pendingJobMonitoringQueue;
        this.overflowJobMonitoringImap = overflowJobMonitoringImap;
        this.finishedJobMetricsImap = finishedJobMetricsImap;
        this.finishedJobDAGInfoImap = finishedJobVertexInfoImap;
        this.finishedJobStateImap.addEntryListener(
                new FinishedJobExpiredListener<>(Constant.IMAP_FINISHED_JOB_STATE), true);
        this.finishedJobMonitoringImap.addEntryListener(
                new FinishedJobExpiredListener<>(Constant.IMAP_FINISHED_JOB_MONITORING), true);
        this.finishedJobMetricsImap.addEntryListener(
                new FinishedJobExpiredListener<>(Constant.IMAP_FINISHED_JOB_METRICS), true);
        this.finishedJobDAGInfoImap.addEntryListener(
                new JobInfoExpiredListener(Constant.IMAP_FINISHED_JOB_VERTEX_INFO), true);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.finishedJobExpireTime = finishedJobExpireTime;
        safelyScheduleMonitoringDrain(0L);
    }

    // Gets the status of a running and completed job.
    public String listAllJob() {
        List<JobStatusData> status = getJobStatusData();
        try {
            return objectMapper.writeValueAsString(status);
        } catch (JsonProcessingException e) {
            logger.severe("Failed to list all job", e);
            throw new SeaTunnelEngineException(e);
        }
    }

    public List<JobStatusData> getJobStatusData() {
        List<JobStatusData> status = new ArrayList<>();
        final List<JobState> runningJobStateList =
                runningJobMasterMap.values().stream()
                        .map(master -> toJobStateMapper(master, true))
                        .collect(Collectors.toList());
        Set<Long> runningJonIds =
                runningJobStateList.stream().map(JobState::getJobId).collect(Collectors.toSet());

        List<JobState> pendingJobStateList =
                pendingJobInfoMap.entrySet().stream()
                        .map(
                                entry -> {
                                    Long jobId = entry.getKey();
                                    JobImmutableInformation jobImmutableInformation =
                                            entry.getValue()
                                                    .getJobMaster()
                                                    .getJobImmutableInformation();
                                    return new JobState(
                                            jobId,
                                            jobImmutableInformation.getJobName(),
                                            JobStatus.PENDING,
                                            jobImmutableInformation.getCreateTime(),
                                            null,
                                            null,
                                            null,
                                            null);
                                })
                        .collect(Collectors.toList());
        Set<Long> pendingJobIds =
                pendingJobStateList.stream().map(JobState::getJobId).collect(Collectors.toSet());

        Stream.concat(
                        Stream.concat(runningJobStateList.stream(), pendingJobStateList.stream()),
                        finishedJobStateImap.values().stream()
                                .filter(
                                        jobState ->
                                                !runningJonIds.contains(jobState.getJobId())
                                                        && !pendingJobIds.contains(
                                                                jobState.getJobId())))
                .forEach(
                        jobState -> {
                            JobStatusData jobStatusData =
                                    new JobStatusData(
                                            jobState.getJobId(),
                                            jobState.getJobName(),
                                            jobState.getJobStatus(),
                                            jobState.getSubmitTime(),
                                            jobState.getStartTime(),
                                            jobState.getFinishTime());
                            status.add(jobStatusData);
                        });
        return status;
    }

    // Get detailed status of a single job
    public JobState getJobDetailState(Long jobId) {
        if (pendingJobInfoMap.containsKey(jobId)) {
            // return pending job state
            JobImmutableInformation jobImmutableInformation =
                    pendingJobInfoMap.get(jobId).getJobMaster().getJobImmutableInformation();
            return new JobState(
                    jobId,
                    jobImmutableInformation.getJobName(),
                    JobStatus.PENDING,
                    jobImmutableInformation.getCreateTime(),
                    null,
                    null,
                    null,
                    null);
        }
        return runningJobMasterMap.containsKey(jobId)
                ? toJobStateMapper(runningJobMasterMap.get(jobId), false)
                : finishedJobStateImap.getOrDefault(jobId, null);
    }

    public JobMetrics getJobMetrics(Long jobId) {
        return finishedJobMetricsImap.getOrDefault(jobId, JobMetrics.empty());
    }

    public JobDAGInfo getJobDAGInfo(Long jobId) {
        return finishedJobDAGInfoImap.getOrDefault(jobId, null);
    }

    // Get detailed status of a single job as json
    public String getJobDetailStateAsString(Long jobId) {
        JobState jobStatus = getJobDetailState(jobId);
        if (null != jobStatus) {
            try {
                return objectMapper.writeValueAsString(jobStatus);
            } catch (JsonProcessingException e) {
                logger.severe("serialize jobStateMapper err", e);
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put("err", "serialize jobStateMapper err");
                return objectNode.toString();
            }
        }
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("err", String.format("jobId : %s not found", jobId));
        return objectNode.toString();
    }

    public void storeFinishedJobState(JobMaster jobMaster) {
        JobState jobState = toJobStateMapper(jobMaster, false);
        jobState.setErrorMessage(jobMaster.getErrorMessage());
        storeFinishedJobState(jobState);
    }

    public void storeFinishedJobState(JobState jobState) {
        finishedJobStateImap.put(jobState.jobId, jobState, finishedJobExpireTime, TimeUnit.MINUTES);
        tryEnqueueFinishedJobMonitoringRecord(jobState);
    }

    /**
     * Appends one compact monitoring record under a cluster-wide sequence lock.
     *
     * <p>The record is stored before the committed watermark advances. Readers therefore never
     * advance past a sequence whose value is not yet visible. A record left immediately after the
     * watermark by a member failure is reconciled by the next drain writer.
     *
     * @param record pending terminal state to expose to monitoring clients
     */
    private void storeFinishedJobMonitoringRecord(JobMonitoringRecord record) {
        finishedJobMonitoringMetadataImap.lock(
                Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY);
        try {
            long committedSequence = reconcileMonitoringSequence();
            String jobSequenceKey = monitoringJobSequenceKey(record.getJobId());
            Long existingSequence = finishedJobMonitoringMetadataImap.get(jobSequenceKey);
            if (existingSequence != null
                    && finishedJobMonitoringImap.containsKey(existingSequence)) {
                return;
            }
            if (committedSequence == Long.MAX_VALUE) {
                throw new SeaTunnelEngineException(
                        "The finished-job monitoring sequence is exhausted.");
            }
            long nextSequence = committedSequence + 1;
            putMonitoringRecord(nextSequence, record);
            finishedJobMonitoringMetadataImap.put(
                    jobSequenceKey, nextSequence, finishedJobExpireTime, TimeUnit.MINUTES);
            finishedJobMonitoringMetadataImap.putIfAbsent(
                    Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, nextSequence);
            finishedJobMonitoringMetadataImap.put(
                    Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, nextSequence);
        } finally {
            finishedJobMonitoringMetadataImap.unlock(
                    Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY);
        }
    }

    private long reconcileMonitoringSequence() {
        long committedSequence =
                finishedJobMonitoringMetadataImap.getOrDefault(
                        Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, 0L);
        long headSequence =
                finishedJobMonitoringMetadataImap.getOrDefault(
                        Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, 1L);
        if (committedSequence == Long.MAX_VALUE) {
            return committedSequence;
        }
        if (headSequence > committedSequence + 1) {
            committedSequence = headSequence - 1;
            finishedJobMonitoringMetadataImap.put(
                    Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, committedSequence);
        }
        JobMonitoringRecord uncommittedRecord =
                finishedJobMonitoringImap.get(committedSequence + 1);
        if (uncommittedRecord == null) {
            return committedSequence;
        }
        long reconciledSequence = committedSequence + 1;
        finishedJobMonitoringMetadataImap.put(
                monitoringJobSequenceKey(uncommittedRecord.getJobId()),
                reconciledSequence,
                finishedJobExpireTime,
                TimeUnit.MINUTES);
        finishedJobMonitoringMetadataImap.putIfAbsent(
                Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, reconciledSequence);
        finishedJobMonitoringMetadataImap.put(
                Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, reconciledSequence);
        return reconciledSequence;
    }

    private void putMonitoringRecord(long sequence, JobMonitoringRecord pendingRecord) {
        JobMonitoringRecord storedRecord =
                new JobMonitoringRecord(
                        sequence,
                        pendingRecord.getJobId(),
                        pendingRecord.getJobName(),
                        pendingRecord.getJobStatus(),
                        pendingRecord.getSubmitTime(),
                        pendingRecord.getStartTime(),
                        pendingRecord.getFinishTime(),
                        pendingRecord.getObservedTime(),
                        pendingRecord.getErrorSummary());
        finishedJobMonitoringImap.put(
                sequence, storedRecord, finishedJobExpireTime, TimeUnit.MINUTES);
    }

    private JobMonitoringRecord toMonitoringRecord(JobState jobState) {
        return new JobMonitoringRecord(
                0L,
                jobState.getJobId(),
                truncate(jobState.getJobName(), MAX_MONITORING_JOB_NAME_LENGTH),
                jobState.getJobStatus(),
                jobState.getSubmitTime(),
                jobState.getStartTime(),
                jobState.getFinishTime(),
                System.currentTimeMillis(),
                truncate(jobState.getErrorMessage(), MAX_MONITORING_ERROR_SUMMARY_LENGTH));
    }

    private String monitoringJobSequenceKey(Long jobId) {
        return Constant.FINISHED_JOB_MONITORING_JOB_SEQUENCE_KEY_PREFIX + jobId;
    }

    /**
     * Persists the sidecar after authoritative history is already visible.
     *
     * <p>The 100 ms argument bounds queue-capacity and lock waiting only. Hazelcast RPC failures
     * can still wait for the configured operation timeout. Such failures are isolated and cannot
     * undo the authoritative finished-state write.
     */
    private void enqueueFinishedJobMonitoringRecord(JobMonitoringRecord record) {
        long jobId = record.getJobId();
        try {
            boolean persisted =
                    pendingJobMonitoringQueue.offer(
                            record, MONITORING_CAPACITY_OR_LOCK_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            if (!persisted) {
                retainMonitoringRetry(jobId, record);
                logger.warning(
                        String.format(
                                "Timed out persisting the monitoring outbox record for job %s",
                                jobId));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retainMonitoringRetry(jobId, record);
            logger.warning(
                    String.format(
                            "Interrupted while persisting the monitoring outbox record for job %s",
                            jobId),
                    e);
        } catch (RuntimeException e) {
            retainMonitoringRetry(jobId, record);
            logger.warning(
                    String.format(
                            "Failed to persist the monitoring outbox record for job %s", jobId),
                    e);
        }
        safelyScheduleMonitoringDrain(0L);
    }

    private void retainMonitoringRetry(long jobId, JobMonitoringRecord record) {
        boolean overflowLockAcquired = false;
        try {
            overflowLockAcquired =
                    finishedJobMonitoringMetadataImap.tryLock(
                            Constant.FINISHED_JOB_MONITORING_OVERFLOW_LOCK_KEY,
                            MONITORING_CAPACITY_OR_LOCK_WAIT_MILLIS,
                            TimeUnit.MILLISECONDS);
            if (!overflowLockAcquired) {
                retainLocalMonitoringRetry(jobId, record);
                return;
            }
            if (overflowJobMonitoringImap.containsKey(jobId)
                    || overflowJobMonitoringImap.size() < MAX_LOCAL_MONITORING_RETRY_RECORDS) {
                overflowJobMonitoringImap.put(jobId, record);
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warning(
                    String.format(
                            "Interrupted while persisting the monitoring overflow record for job %s",
                            jobId),
                    e);
        } catch (RuntimeException e) {
            logger.warning(
                    String.format(
                            "Failed to persist the monitoring overflow record for job %s", jobId),
                    e);
        } finally {
            if (overflowLockAcquired) {
                try {
                    finishedJobMonitoringMetadataImap.unlock(
                            Constant.FINISHED_JOB_MONITORING_OVERFLOW_LOCK_KEY);
                } catch (RuntimeException e) {
                    logger.warning("Failed to release the monitoring overflow lock", e);
                }
            }
        }
        retainLocalMonitoringRetry(jobId, record);
    }

    private void retainLocalMonitoringRetry(long jobId, JobMonitoringRecord record) {
        synchronized (localPendingJobMonitoringRecords) {
            if (localPendingJobMonitoringRecords.size() < MAX_LOCAL_MONITORING_RETRY_RECORDS
                    || localPendingJobMonitoringRecords.containsKey(jobId)) {
                localPendingJobMonitoringRecords.put(jobId, record);
                return;
            }
        }
        long droppedRecords = incrementDroppedJobMonitoringRecords();
        logger.severe(
                String.format(
                        "Dropped the monitoring sidecar for job %s because the durable outbox and "
                                + "the bounded local retry buffer are unavailable; dropped total: %s",
                        jobId, droppedRecords));
    }

    /** Returns monitoring records dropped after both bounded outbox tiers were unavailable. */
    public long getDroppedJobMonitoringRecords() {
        synchronized (pendingDroppedJobMonitoringRecords) {
            long pendingDelta = pendingDroppedJobMonitoringRecords.get();
            try {
                return finishedJobMonitoringMetadataImap.getOrDefault(
                                Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY, 0L)
                        + pendingDelta;
            } catch (RuntimeException e) {
                return pendingDelta;
            }
        }
    }

    long incrementDroppedJobMonitoringRecords() {
        synchronized (pendingDroppedJobMonitoringRecords) {
            pendingDroppedJobMonitoringRecords.incrementAndGet();
            return flushPendingDroppedJobMonitoringRecords();
        }
    }

    /**
     * Atomically merges this coordinator's pending delta into the distributed dropped counter.
     *
     * <p>The caller must hold the {@link #pendingDroppedJobMonitoringRecords} monitor. The delta is
     * cleared only after the distributed put succeeds, so a temporary metadata failure can be
     * retried without losing or double-counting records.
     */
    private long flushPendingDroppedJobMonitoringRecords() {
        long pendingDelta = pendingDroppedJobMonitoringRecords.get();
        boolean lockAcquired = false;
        try {
            lockAcquired =
                    finishedJobMonitoringMetadataImap.tryLock(
                            Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY,
                            MONITORING_CAPACITY_OR_LOCK_WAIT_MILLIS,
                            TimeUnit.MILLISECONDS);
            if (!lockAcquired) {
                return pendingDelta;
            }
            long persistentTotal =
                    finishedJobMonitoringMetadataImap.getOrDefault(
                            Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY, 0L);
            if (pendingDelta > 0L) {
                persistentTotal += pendingDelta;
                finishedJobMonitoringMetadataImap.put(
                        Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY, persistentTotal);
                pendingDroppedJobMonitoringRecords.addAndGet(-pendingDelta);
            }
            return persistentTotal;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return pendingDelta;
        } catch (RuntimeException e) {
            return pendingDelta;
        } finally {
            if (lockAcquired) {
                try {
                    finishedJobMonitoringMetadataImap.unlock(
                            Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY);
                } catch (RuntimeException e) {
                    logger.warning("Failed to release the monitoring dropped-records lock", e);
                }
            }
        }
    }

    private void tryEnqueueFinishedJobMonitoringRecord(JobState jobState) {
        try {
            enqueueFinishedJobMonitoringRecord(toMonitoringRecord(jobState));
        } catch (RuntimeException e) {
            logger.warning(
                    String.format(
                            "Failed to enqueue the monitoring sidecar for finished job %s",
                            jobState.getJobId()),
                    e);
        }
    }

    /** Schedules an outbox drain while isolating executor failures from job completion. */
    private void safelyScheduleMonitoringDrain(long delaySeconds) {
        try {
            scheduleMonitoringDrain(delaySeconds);
        } catch (RuntimeException e) {
            monitoringDrainScheduled.set(false);
            logger.warning("Failed to schedule the job monitoring outbox drain", e);
        }
    }

    /** Periodic coordinator hook that wakes a durable outbox left by executor rejection. */
    public void retryPendingJobMonitoringRecords() {
        synchronized (pendingDroppedJobMonitoringRecords) {
            flushPendingDroppedJobMonitoringRecords();
        }
        safelyScheduleMonitoringDrain(0L);
    }

    /** Ensures at most one drain worker is submitted by this service instance. */
    private void scheduleMonitoringDrain(long delaySeconds) {
        if (!monitoringDrainScheduled.compareAndSet(false, true)) {
            return;
        }
        Runnable drainTask = this::drainPendingJobMonitoringRecords;
        if (delaySeconds == 0L) {
            nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, drainTask);
        } else {
            nodeEngine.getExecutionService().schedule(drainTask, delaySeconds, TimeUnit.SECONDS);
        }
    }

    /**
     * Moves at most one bounded batch from the durable outbox into the sequence ledger.
     *
     * <p>The ledger append is idempotent by job id. The outbox entry is removed only after the
     * committed sequence is visible, so a coordinator failure can cause a retry but not a lost
     * terminal job.
     */
    private void drainPendingJobMonitoringRecords() {
        boolean retryRequired = false;
        boolean drainLockAcquired = false;
        try {
            int processedRecords = 0;
            for (Map.Entry<Long, JobMonitoringRecord> entry :
                    overflowJobMonitoringImap.entrySet()) {
                if (processedRecords >= MAX_MONITORING_DRAIN_BATCH_SIZE) {
                    break;
                }
                if (!pendingJobMonitoringQueue.offer(entry.getValue())) {
                    retryRequired = true;
                    break;
                }
                overflowJobMonitoringImap.remove(entry.getKey(), entry.getValue());
                processedRecords++;
            }
            for (Map.Entry<Long, JobMonitoringRecord> entry :
                    localPendingJobMonitoringRecords.entrySet()) {
                if (processedRecords >= MAX_MONITORING_DRAIN_BATCH_SIZE) {
                    break;
                }
                if (!pendingJobMonitoringQueue.offer(entry.getValue())) {
                    retryRequired = true;
                    break;
                }
                localPendingJobMonitoringRecords.remove(entry.getKey(), entry.getValue());
                processedRecords++;
            }

            drainLockAcquired =
                    finishedJobMonitoringMetadataImap.tryLock(
                            Constant.FINISHED_JOB_MONITORING_DRAIN_LOCK_KEY,
                            MONITORING_CAPACITY_OR_LOCK_WAIT_MILLIS,
                            TimeUnit.MILLISECONDS);
            if (!drainLockAcquired) {
                retryRequired = true;
            } else {
                while (processedRecords < MAX_MONITORING_DRAIN_BATCH_SIZE) {
                    JobMonitoringRecord record = pendingJobMonitoringQueue.peek();
                    if (record == null) {
                        break;
                    }
                    storeFinishedJobMonitoringRecord(record);
                    pendingJobMonitoringQueue.poll();
                    processedRecords++;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retryRequired = true;
            logger.warning("Interrupted while draining pending job monitoring records", e);
        } catch (RuntimeException e) {
            retryRequired = true;
            logger.warning("Failed to drain pending job monitoring records", e);
        } finally {
            if (drainLockAcquired) {
                try {
                    finishedJobMonitoringMetadataImap.unlock(
                            Constant.FINISHED_JOB_MONITORING_DRAIN_LOCK_KEY);
                } catch (RuntimeException e) {
                    logger.warning("Failed to release the job monitoring drain lock", e);
                }
            }
            monitoringDrainScheduled.set(false);
        }
        boolean pendingRecordsRemain = retryRequired;
        if (!pendingRecordsRemain) {
            try {
                pendingRecordsRemain =
                        !pendingJobMonitoringQueue.isEmpty()
                                || !overflowJobMonitoringImap.isEmpty()
                                || !localPendingJobMonitoringRecords.isEmpty();
            } catch (RuntimeException e) {
                retryRequired = true;
                pendingRecordsRemain = true;
                logger.warning("Failed to inspect the job monitoring outbox", e);
            }
        }
        if (pendingRecordsRemain) {
            safelyScheduleMonitoringDrain(retryRequired ? MONITORING_RETRY_DELAY_SECONDS : 0L);
        }
    }

    private String truncate(String value, int maximumLength) {
        if (value == null || value.length() <= maximumLength) {
            return value;
        }
        return value.substring(0, maximumLength);
    }

    public void storeFinishedPipelineMetrics(long jobId, JobMetrics metrics) {
        finishedJobMetricsImap.computeIfAbsent(jobId, key -> JobMetrics.of(new HashMap<>()));
        JobMetrics newMetrics = finishedJobMetricsImap.get(jobId).merge(metrics);
        finishedJobMetricsImap.put(jobId, newMetrics, finishedJobExpireTime, TimeUnit.MINUTES);
    }

    private JobState toJobStateMapper(JobMaster jobMaster, boolean simple) {
        Long jobId = jobMaster.getJobImmutableInformation().getJobId();
        Map<PipelineLocation, PipelineStateData> pipelineStateMapperMap = new HashMap<>();
        if (!simple) {
            try {
                jobMaster
                        .getPhysicalPlan()
                        .getPipelineList()
                        .forEach(
                                pipeline -> {
                                    PipelineLocation pipelineLocation =
                                            pipeline.getPipelineLocation();
                                    PipelineStatus pipelineState =
                                            (PipelineStatus)
                                                    runningJobStateIMap.get(pipelineLocation);
                                    Map<TaskGroupLocation, ExecutionState> taskStateMap =
                                            new HashMap<>();
                                    pipeline.getCoordinatorVertexList()
                                            .forEach(
                                                    coordinator -> {
                                                        TaskGroupLocation taskGroupLocation =
                                                                coordinator.getTaskGroupLocation();
                                                        taskStateMap.put(
                                                                taskGroupLocation,
                                                                (ExecutionState)
                                                                        runningJobStateIMap.get(
                                                                                taskGroupLocation));
                                                    });
                                    pipeline.getPhysicalVertexList()
                                            .forEach(
                                                    task -> {
                                                        TaskGroupLocation taskGroupLocation =
                                                                task.getTaskGroupLocation();
                                                        taskStateMap.put(
                                                                taskGroupLocation,
                                                                (ExecutionState)
                                                                        runningJobStateIMap.get(
                                                                                taskGroupLocation));
                                                    });

                                    PipelineStateData pipelineStateData =
                                            new PipelineStateData(pipelineState, taskStateMap);
                                    pipelineStateMapperMap.put(pipelineLocation, pipelineStateData);
                                });
            } catch (Exception e) {
                logger.warning("get job pipeline state err", e);
            }
        }
        JobStatus jobStatus =
                Optional.ofNullable(runningJobStateIMap.get(jobId))
                        .map(status -> ((JobStatus) status))
                        .orElse(jobMaster.getJobStatus());
        String jobName = jobMaster.getJobImmutableInformation().getJobName();
        long submitTime = jobMaster.getJobImmutableInformation().getCreateTime();
        Long startTime = jobMaster.getStateTimestamp(JobStatus.SCHEDULED);
        Long finishTime = null;
        if (jobStatus != null && jobStatus.isEndState()) {
            finishTime = jobMaster.getStateTimestamp(jobStatus);
        }
        return new JobState(
                jobId,
                jobName,
                jobStatus,
                submitTime,
                startTime,
                finishTime,
                pipelineStateMapperMap,
                null);
    }

    public void storeJobInfo(long jobId, JobDAGInfo jobInfo) {
        finishedJobDAGInfoImap.put(jobId, jobInfo, finishedJobExpireTime, TimeUnit.MINUTES);
    }

    public Map<String, Long> getFinishedJobRecordCounts() {
        Map<String, Long> counts = new HashMap<>();
        counts.put(Constant.IMAP_FINISHED_JOB_STATE, (long) finishedJobStateImap.size());
        counts.put(Constant.IMAP_FINISHED_JOB_MONITORING, (long) finishedJobMonitoringImap.size());
        counts.put(
                Constant.IMAP_FINISHED_JOB_MONITORING_PENDING,
                (long) pendingJobMonitoringQueue.size());
        counts.put(
                Constant.IMAP_FINISHED_JOB_MONITORING_OVERFLOW,
                (long) overflowJobMonitoringImap.size());
        counts.put(Constant.IMAP_FINISHED_JOB_METRICS, (long) finishedJobMetricsImap.size());
        counts.put(Constant.IMAP_FINISHED_JOB_VERTEX_INFO, (long) finishedJobDAGInfoImap.size());
        return counts;
    }

    public Map<String, Long> getFinishedJobCleanupTotals() {
        Map<String, Long> counts = new HashMap<>();
        counts.put(
                Constant.IMAP_FINISHED_JOB_STATE,
                getFinishedJobCleanupTotal(Constant.IMAP_FINISHED_JOB_STATE));
        counts.put(
                Constant.IMAP_FINISHED_JOB_MONITORING,
                getFinishedJobCleanupTotal(Constant.IMAP_FINISHED_JOB_MONITORING));
        counts.put(
                Constant.IMAP_FINISHED_JOB_METRICS,
                getFinishedJobCleanupTotal(Constant.IMAP_FINISHED_JOB_METRICS));
        counts.put(
                Constant.IMAP_FINISHED_JOB_VERTEX_INFO,
                getFinishedJobCleanupTotal(Constant.IMAP_FINISHED_JOB_VERTEX_INFO));
        return counts;
    }

    private long getFinishedJobCleanupTotal(String storeName) {
        AtomicLong total = finishedJobCleanupTotals.get(storeName);
        return total == null ? 0L : total.get();
    }

    private void incrementFinishedJobCleanupTotal(String storeName) {
        finishedJobCleanupTotals
                .computeIfAbsent(storeName, key -> new AtomicLong())
                .incrementAndGet();
    }

    @AllArgsConstructor
    @Data
    public static final class JobState implements Serializable {
        private static final long serialVersionUID = -1176348098833918960L;
        private Long jobId;
        private String jobName;
        private JobStatus jobStatus;
        private long submitTime;
        private Long startTime;
        private Long finishTime;
        private Map<PipelineLocation, PipelineStateData> pipelineStateMapperMap;
        private String errorMessage;
    }

    @AllArgsConstructor
    @Data
    public static final class PipelineStateData implements Serializable {
        private static final long serialVersionUID = -7875004875757861958L;
        private PipelineStatus pipelineStatus;
        private Map<TaskGroupLocation, ExecutionState> executionStateMap;
    }

    private class FinishedJobExpiredListener<T> implements EntryExpiredListener<Long, T> {
        private final String storeName;

        private FinishedJobExpiredListener(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void entryExpired(EntryEvent<Long, T> event) {
            incrementFinishedJobCleanupTotal(storeName);
            if (Constant.IMAP_FINISHED_JOB_MONITORING.equals(storeName)) {
                try {
                    advanceMonitoringHead(event.getKey() + 1);
                } catch (RuntimeException e) {
                    logger.warning("Failed to advance the job monitoring retention head", e);
                }
            }
        }
    }

    /**
     * Advances the retained-sequence head monotonically when ledger entries expire.
     *
     * <p>Expiration callbacks can arrive out of order. Taking the maximum is safe because a later
     * sequence can expire only after every earlier record's TTL has elapsed.
     */
    private void advanceMonitoringHead(long candidateHeadSequence) {
        finishedJobMonitoringMetadataImap.lock(Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY);
        try {
            long currentHeadSequence =
                    finishedJobMonitoringMetadataImap.getOrDefault(
                            Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, 1L);
            if (candidateHeadSequence > currentHeadSequence) {
                finishedJobMonitoringMetadataImap.put(
                        Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, candidateHeadSequence);
            }
        } finally {
            finishedJobMonitoringMetadataImap.unlock(
                    Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY);
        }
    }

    private class JobInfoExpiredListener implements EntryExpiredListener<Long, JobDAGInfo> {
        private final String storeName;

        private JobInfoExpiredListener(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void entryExpired(EntryEvent<Long, JobDAGInfo> event) {
            incrementFinishedJobCleanupTotal(storeName);
            Long jobId = event.getKey();
            JobDAGInfo jobDagInfo = event.getOldValue();
            if (jobDagInfo == null) {
                return;
            }
            try {
                Set<ExecutionAddress> historyExecutionPlan =
                        Optional.ofNullable(jobDagInfo.getHistoryExecutionPlan())
                                .orElseGet(Collections::emptySet);
                Stream.concat(
                                historyExecutionPlan.stream(),
                                Stream.of(jobDagInfo.getMaster()).filter(Objects::nonNull))
                        .forEach(
                                address -> {
                                    logger.info(
                                            "clean job log, jobId: "
                                                    + jobId
                                                    + ", address: "
                                                    + address);
                                    try {
                                        NodeEngineUtil.sendOperationToMemberNode(
                                                        nodeEngine,
                                                        new CleanLogOperation(jobId),
                                                        new Address(
                                                                address.getHostname(),
                                                                address.getPort()))
                                                .join();
                                    } catch (UnknownHostException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
            } catch (Exception e) {
                logger.warning("clean job log err", e);
            }
        }
    }
}
