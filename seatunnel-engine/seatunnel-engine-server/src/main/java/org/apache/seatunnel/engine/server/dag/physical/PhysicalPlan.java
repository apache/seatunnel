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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStateEvent;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineExecutionState;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.map.IMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class PhysicalPlan {

    private final List<SubPlan> pipelineList;

    private final AtomicInteger finishedPipelineNum = new AtomicInteger(0);

    private final AtomicInteger canceledPipelineNum = new AtomicInteger(0);

    private final AtomicInteger failedPipelineNum = new AtomicInteger(0);

    private final JobImmutableInformation jobImmutableInformation;

    private final IMap<Object, Object> runningJobStateIMap;

    /**
     * Timestamps (in milliseconds) as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /** when job status turn to end, complete this future. */
    private CompletableFuture<JobResult> jobEndFuture;

    /** The error throw by subPlan, should be set when subPlan throw error. */
    private final AtomicReference<String> errorBySubPlan = new AtomicReference<>();

    private final String jobFullName;

    private final long jobId;

    private JobMaster jobMaster;

    private EngineConfig engineConfig;

    private Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures =
            new HashMap<>();

    /** Whether we make the job end when pipeline turn to end state. */
    private boolean makeJobEndWhenPipelineEnded = true;

    private volatile boolean isRunning = false;

    private volatile JobStatus currJobStatus;

    public PhysicalPlan(
            @NonNull List<SubPlan> pipelineList,
            @NonNull ExecutorService executorService,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull IMap<Object, Object> runningJobStateIMap,
            @NonNull IMap<Object, Long[]> runningJobStateTimestampsIMap) {
        this.jobImmutableInformation = jobImmutableInformation;
        this.jobId = jobImmutableInformation.getJobId();
        JobStatus initializedJobStatus =
                DistributedStateTransition.initialize(
                        runningJobStateIMap,
                        jobId,
                        JobStatus.CREATED,
                        JobStatus.class,
                        runningJobStateTimestampsIMap,
                        JobStatus.values().length,
                        JobStatus.INITIALIZING.ordinal(),
                        initializationTimestamp,
                        JobStatus.CREATED.ordinal());

        this.pipelineList = pipelineList;
        if (pipelineList.isEmpty()) {
            throw new UnknownPhysicalPlanException(
                    "The physical plan didn't have any can execute pipeline");
        }
        this.jobFullName =
                String.format(
                        "Job %s (%s)",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId());

        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.currJobStatus = initializedJobStatus;
    }

    public void setJobMaster(JobMaster jobMaster) {
        this.jobMaster = jobMaster;
        pipelineList.forEach(pipeline -> pipeline.setJobMaster(jobMaster));
        this.engineConfig = jobMaster.getEngineConfig();
    }

    public PassiveCompletableFuture<JobResult> initStateFuture() {
        jobEndFuture = new CompletableFuture<>();
        pipelineList.forEach(this::addPipelineEndCallback);
        return new PassiveCompletableFuture<>(jobEndFuture);
    }

    public void addPipelineEndCallback(SubPlan subPlan) {
        PassiveCompletableFuture<PipelineExecutionState> future = subPlan.initStateFuture();
        future.thenAcceptAsync(
                pipelineState -> {
                    try {
                        log.info(
                                "{} future complete with state {}",
                                subPlan.getPipelineFullName(),
                                pipelineState.getPipelineStatus());
                        if (PipelineStatus.CANCELED.equals(pipelineState.getPipelineStatus())) {
                            canceledPipelineNum.incrementAndGet();
                        } else if (PipelineStatus.FAILED.equals(
                                pipelineState.getPipelineStatus())) {
                            failedPipelineNum.incrementAndGet();
                            errorBySubPlan.compareAndSet(null, pipelineState.getThrowableMsg());
                            if (makeJobEndWhenPipelineEnded) {
                                log.info(
                                        String.format(
                                                "cancel job %s because makeJobEndWhenPipelineEnded is true",
                                                jobFullName));
                                updateJobState(JobStatus.FAILING);
                            }
                        }

                        if (finishedPipelineNum.incrementAndGet() == this.pipelineList.size()) {
                            JobStatus jobStatus;
                            if (failedPipelineNum.get() > 0) {
                                jobStatus = JobStatus.FAILED;
                                updateJobState(jobStatus);
                            } else if (canceledPipelineNum.get() > 0) {
                                jobStatus = JobStatus.CANCELED;
                                updateJobState(jobStatus);
                            } else {
                                if (this.getJobStatus() == JobStatus.DOING_SAVEPOINT) {
                                    jobStatus = JobStatus.SAVEPOINT_DONE;
                                } else {
                                    jobStatus = JobStatus.FINISHED;
                                }
                                updateJobState(jobStatus);
                            }
                        }
                    } catch (Throwable e) {
                        // Because only cancelJob or releasePipelineResource can throw exception, so
                        // we only output log here
                        log.error(ExceptionUtils.getMessage(e));
                    }
                },
                jobMaster.getExecutorService());
    }

    public void cancelJob() {
        JobStatus jobStatus = getJobStatus();
        if (jobStatus == null) {
            log.error("{} job state is null, cannot cancel", jobFullName);
            return;
        }
        if (jobStatus.isEndState()) {
            log.warn(
                    String.format(
                            "%s is in end state %s, can not be cancel", jobFullName, jobStatus));
            return;
        }

        if (jobStatus.ordinal() <= JobStatus.PENDING.ordinal()) {
            // Tasks with the status 'INITIALIZING', 'CREATED', 'PENDING' need to be set directly to
            // the 'CANCELLED' state because it has not yet started running
            updateJobState(JobStatus.CANCELED);
            jobEndFuture.complete(new JobResult(JobStatus.CANCELED));
        } else {
            updateJobState(JobStatus.CANCELING);
        }
    }

    public void savepointJob() {
        JobStatus jobStatus = getJobStatus();
        if (jobStatus.isEndState()) {
            log.warn(
                    String.format(
                            "%s is in end state %s, can not do savepoint", jobFullName, jobStatus));
            return;
        }
        updateJobState(JobStatus.DOING_SAVEPOINT);
    }

    /**
     * Stop every pipeline after a savepoint failure while the job is in {@link
     * JobStatus#DOING_SAVEPOINT}.
     *
     * <p>Some pipelines may already have completed the savepoint and stopped when another pipeline
     * fails. Reverting only the global job state to {@link JobStatus#RUNNING} would leave those
     * completed pipelines stopped while the job appears to be running. The same fallback used by a
     * stop request during {@code DOING_SAVEPOINT} finishes completed pipelines and force-stops the
     * rest so the job reaches one terminal state.
     */
    public synchronized void savepointFailed() {
        if (getJobStatus() == JobStatus.DOING_SAVEPOINT) {
            log.warn(
                    "{} savepoint failed, stop all pipelines with checkpoint fallback",
                    jobFullName);
            stopJob();
        }
    }

    public void stopJob() {
        JobStatus jobStatus = getJobStatus();
        if (jobStatus.isEndState()) {
            log.warn("{} is in end state {}, can not be stop", jobFullName, jobStatus);
            return;
        }

        if (jobStatus.ordinal() <= JobStatus.PENDING.ordinal()) {
            // Tasks with the status 'INITIALIZING', 'CREATED', 'PENDING' need to be set directly to
            // the 'CANCELLED' state because it has not yet started running
            updateJobState(JobStatus.CANCELED);
            completeJobEndFuture(new JobResult(JobStatus.CANCELED, null));
        } else if (jobStatus == JobStatus.DOING_SAVEPOINT) {
            this.pipelineList.forEach(SubPlan::stopPipelineWithCheckpointFallback);
        } else {
            updateJobState(JobStatus.CANCELING);
            this.pipelineList.forEach(SubPlan::forceStopPipeline);
        }
    }

    public List<SubPlan> getPipelineList() {
        return pipelineList;
    }

    public synchronized Long getStateTimestamp(@NonNull JobStatus jobStatus) {
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(jobId);
        if (stateTimestamps == null) {
            return null;
        }
        return stateTimestamps[jobStatus.ordinal()];
    }

    public synchronized void updateJobState(@NonNull JobStatus targetState) {
        try {
            DistributedStateTransition.Result<JobStatus> transitionResult =
                    RetryUtils.retryWithException(
                            () ->
                                    DistributedStateTransition.transition(
                                            runningJobStateIMap,
                                            jobId,
                                            currJobStatus,
                                            JobStatus.CREATED,
                                            targetState,
                                            JobStatus.class,
                                            JobStatus::isEndState,
                                            this::canPersistState,
                                            this::lockMissingStatePersistence,
                                            this::unlockMissingStatePersistence,
                                            runningJobStateTimestampsIMap,
                                            JobStatus.values().length,
                                            targetState.ordinal()),
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    ExceptionUtil::isOperationNeedRetryException,
                                    Constant.OPERATION_RETRY_SLEEP));
            JobStatus current = transitionResult.getPreviousState();
            if (transitionResult.isStateEntryMissing()) {
                log.warn(
                        "{} job state entry missing from distributed map (possibly due to node "
                                + "removal during scaling down), using local state {} as fallback, "
                                + "target state: {}",
                        jobFullName,
                        current,
                        targetState);
            }
            if (transitionResult.isPersistenceBlocked()) {
                log.info(
                        "{} job state persistence is blocked by its generation fence", jobFullName);
                return;
            }
            if (!transitionResult.isTransitioned()) {
                JobStatus localState = this.currJobStatus;
                this.currJobStatus = transitionResult.getCurrentState();
                log.info(
                        "{} did not transition to {} because distributed state {} won the race",
                        jobFullName,
                        targetState,
                        transitionResult.getCurrentState());
                if (localState != transitionResult.getCurrentState()) {
                    reportJobStateEvent(transitionResult.getCurrentState());
                    stateProcess();
                }
                return;
            }
            this.currJobStatus = targetState;
            log.info(
                    String.format(
                            "%s turned from state %s to %s.", jobFullName, current, targetState));
            reportJobStateEvent(targetState);
            stateProcess();
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
            if (!targetState.equals(JobStatus.FAILING)) {
                makeJobFailing(e);
            }
        }
    }

    /**
     * Checks whether this physical plan's exact generation still owns distributed persistence.
     *
     * <p>Construction runs before the JobMaster is attached and is protected by the coordinator's
     * job cleanup lock; later transitions require the durable owner token to match.
     */
    private boolean canPersistState() {
        return jobMaster == null || jobMaster.isStatePersistenceAllowed();
    }

    /**
     * Locks the cleanup fence only when a transition is about to recreate a missing state key.
     *
     * <p>Existing job-state updates remain synchronized only by their own state-key lock.
     */
    private void lockMissingStatePersistence() {
        if (jobMaster != null) {
            jobMaster.lockStatePersistenceFence();
        }
    }

    /**
     * Releases the missing-key cleanup fence after the state-key lock has been released.
     *
     * <p>This release order preserves the cleanup-to-state lock hierarchy.
     */
    private void unlockMissingStatePersistence() {
        if (jobMaster != null) {
            jobMaster.unlockStatePersistenceFence();
        }
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }

    public JobStatus getJobStatus() {
        JobStatus status = (JobStatus) runningJobStateIMap.get(jobId);
        if (status == null) {
            log.warn(
                    "{} job state entry missing from distributed map, "
                            + "using local cached state {} as fallback",
                    jobFullName,
                    currJobStatus);
            return currJobStatus;
        }
        return status;
    }

    public String getJobFullName() {
        return jobFullName;
    }

    public void makeJobFailing(Throwable e) {
        errorBySubPlan.compareAndSet(null, ExceptionUtils.getMessage(e));
        updateJobState(JobStatus.FAILING);
    }

    public synchronized void startJob() {
        isRunning = true;
        log.info("{} state process is start", getJobFullName());
        updateJobState(JobStatus.SCHEDULED);
        stateProcess();
    }

    public void stopJobStateProcess() {
        isRunning = false;
        log.info("{} state process is stop", getJobFullName());
    }

    private synchronized void stateProcess() {
        if (!isRunning) {
            log.warn(String.format("%s state process is stopped", jobFullName));
            return;
        }
        JobStatus jobStatus = getJobStatus();
        switch (jobStatus) {
            case CREATED:
                updateJobState(JobStatus.SCHEDULED);
                break;
            case PENDING:
            case SCHEDULED:
                getPipelineList()
                        .forEach(
                                subPlan -> {
                                    if (PipelineStatus.CREATED.equals(
                                            subPlan.getCurrPipelineStatus())) {
                                        subPlan.startSubPlanStateProcess();
                                    }
                                });
                updateJobState(JobStatus.RUNNING);
                break;
            case RUNNING:
            case DOING_SAVEPOINT:
                break;
            case FAILING:
            case CANCELING:
                jobMaster.neverNeedRestore();
                getPipelineList().forEach(SubPlan::cancelPipeline);
                break;
            case FAILED:
            case CANCELED:
            case SAVEPOINT_DONE:
            case FINISHED:
                stopJobStateProcess();
                jobEndFuture.complete(new JobResult(jobStatus, errorBySubPlan.get()));
                break;
            default:
                throw new IllegalArgumentException("Unknown Job State: " + jobStatus);
        }
    }

    private void reportJobStateEvent(JobStatus jobStatus) {
        try {
            if (jobStatus.isEndState()
                    || (this.engineConfig != null
                            && this.engineConfig.isReportNonTerminalJobState())) {
                jobMaster
                        .getCoordinatorService()
                        .getEventProcessor()
                        .process(
                                new JobStateEvent(
                                        jobId,
                                        jobImmutableInformation.getJobConfig().getName(),
                                        jobStatus));
            }
        } catch (Exception e) {
            log.warn("Failed to report job {} state event", jobId, e);
        }
    }

    public void completeJobEndFuture(JobResult jobResult) {
        jobEndFuture.complete(jobResult);
    }

    public Map<TaskGroupLocation, CompletableFuture<SlotProfile>> getPreApplyResourceFutures() {
        return preApplyResourceFutures;
    }

    public void setPreApplyResourceFutures(
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures) {
        this.preApplyResourceFutures = preApplyResourceFutures;
    }
}
