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
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
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

    private Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures =
            new HashMap<>();

    /** Whether we make the job end when pipeline turn to end state. */
    private boolean makeJobEndWhenPipelineEnded = true;

    private volatile boolean isRunning = false;

    public PhysicalPlan(
            @NonNull List<SubPlan> pipelineList,
            @NonNull ExecutorService executorService,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull IMap<Object, Object> runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap) {
        this.jobImmutableInformation = jobImmutableInformation;
        this.jobId = jobImmutableInformation.getJobId();
        Long[] stateTimestamps = new Long[JobStatus.values().length];
        if (runningJobStateTimestampsIMap.get(jobId) == null) {
            stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
            runningJobStateTimestampsIMap.put(jobId, stateTimestamps);
        }

        if (runningJobStateIMap.get(jobId) == null) {
            // We must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap.
            // Because if a new Master Node become active, we can recover ExecutionState and
            // PipelineState and JobStatus
            // from TaskExecutionService. But we can not recover stateTimestamps.
            stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsIMap.put(jobId, stateTimestamps);

            runningJobStateIMap.put(jobId, JobStatus.CREATED);
        }

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
    }

    public void setJobMaster(JobMaster jobMaster) {
        this.jobMaster = jobMaster;
        pipelineList.forEach(pipeline -> pipeline.setJobMaster(jobMaster));
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
        if (getJobStatus().isEndState()) {
            log.warn(
                    String.format(
                            "%s is in end state %s, can not be cancel",
                            jobFullName, getJobStatus()));
            return;
        }

        updateJobState(JobStatus.CANCELING);
    }

    public void savepointJob() {
        if (getJobStatus().isEndState()) {
            log.warn(
                    String.format(
                            "%s is in end state %s, can not do savepoint",
                            jobFullName, getJobStatus()));
            return;
        }
        updateJobState(JobStatus.DOING_SAVEPOINT);
    }

    public List<SubPlan> getPipelineList() {
        return pipelineList;
    }

    private void updateStateTimestamps(@NonNull JobStatus targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update
        // runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(jobId);
        stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsIMap.set(jobId, stateTimestamps);
    }

    public synchronized void updateJobState(@NonNull JobStatus targetState) {
        try {
            JobStatus current = (JobStatus) runningJobStateIMap.get(jobId);
            log.debug(
                    String.format(
                            "Try to update the %s state from %s to %s",
                            jobFullName, current, targetState));

            if (current.equals(targetState)) {
                log.info(
                        "{} current state equals target state: {}, skip", jobFullName, targetState);
                return;
            }

            // consistency check
            if (current.isEndState()) {
                String message = "Job is trying to leave terminal state " + current;
                throw new SeaTunnelEngineException(message);
            }

            // now do the actual state transition
            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            RetryUtils.retryWithException(
                    () -> {
                        updateStateTimestamps(targetState);
                        runningJobStateIMap.set(jobId, targetState);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            ExceptionUtil::isOperationNeedRetryException,
                            Constant.OPERATION_RETRY_SLEEP));
            log.info(
                    String.format(
                            "%s turned from state %s to %s.", jobFullName, current, targetState));
            stateProcess();
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
            if (!targetState.equals(JobStatus.FAILING)) {
                makeJobFailing(e);
            }
        }
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }

    public JobStatus getJobStatus() {
        return (JobStatus) runningJobStateIMap.get(jobId);
    }

    public String getJobFullName() {
        return jobFullName;
    }

    public void makeJobFailing(Throwable e) {
        errorBySubPlan.compareAndSet(null, ExceptionUtils.getMessage(e));
        updateJobState(JobStatus.FAILING);
    }

    public void startJob() {
        isRunning = true;
        log.info("{} state process is start", getJobFullName());
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
        switch (getJobStatus()) {
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
                jobEndFuture.complete(new JobResult(getJobStatus(), errorBySubPlan.get()));
                return;
            default:
                throw new IllegalArgumentException("Unknown Job State: " + getJobStatus());
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
