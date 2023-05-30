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
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineExecutionState;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import lombok.NonNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PhysicalPlan {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalPlan.class);

    private final List<SubPlan> pipelineList;

    private AtomicInteger finishedPipelineNum = new AtomicInteger(0);

    private AtomicInteger canceledPipelineNum = new AtomicInteger(0);

    private AtomicInteger failedPipelineNum = new AtomicInteger(0);

    private final JobImmutableInformation jobImmutableInformation;

    private final IMap<Object, Object> runningJobStateIMap;

    /**
     * Timestamps (in milliseconds) as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * when job status turn to end, complete this future. And then the waitForCompleteByPhysicalPlan
     * in {@link org.apache.seatunnel.engine.server.scheduler.JobScheduler} whenComplete method will
     * be called.
     */
    private CompletableFuture<JobResult> jobEndFuture;

    /** The error throw by subPlan, should be set when subPlan throw error. */
    private final AtomicReference<String> errorBySubPlan = new AtomicReference<>();

    private final String jobFullName;

    private final long jobId;

    private JobMaster jobMaster;

    /** Whether we make the job end when pipeline turn to end state. */
    private boolean makeJobEndWhenPipelineEnded = true;

    public PhysicalPlan(
            @NonNull List<SubPlan> pipelineList,
            @NonNull ExecutorService executorService,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull IMap runningJobStateIMap,
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
                        if (PipelineStatus.CANCELED.equals(pipelineState.getPipelineStatus())) {
                            canceledPipelineNum.incrementAndGet();
                            if (makeJobEndWhenPipelineEnded) {
                                LOGGER.info(
                                        String.format(
                                                "cancel job %s because makeJobEndWhenPipelineEnded is true",
                                                jobFullName));
                                cancelJob();
                            }
                        } else if (PipelineStatus.FAILED.equals(
                                pipelineState.getPipelineStatus())) {
                            failedPipelineNum.incrementAndGet();
                            errorBySubPlan.compareAndSet(null, pipelineState.getThrowableMsg());
                            if (makeJobEndWhenPipelineEnded) {
                                LOGGER.info(
                                        String.format(
                                                "cancel job %s because makeJobEndWhenPipelineEnded is true",
                                                jobFullName));
                                cancelJob();
                            }
                        }

                        if (finishedPipelineNum.incrementAndGet() == this.pipelineList.size()) {
                            JobStatus jobStatus;
                            if (failedPipelineNum.get() > 0) {
                                jobStatus = JobStatus.FAILING;
                                updateJobState(jobStatus);
                            } else if (canceledPipelineNum.get() > 0) {
                                jobStatus = JobStatus.CANCELED;
                                turnToEndState(jobStatus);
                            } else {
                                jobStatus = JobStatus.FINISHED;
                                turnToEndState(jobStatus);
                            }
                            jobEndFuture.complete(new JobResult(jobStatus, errorBySubPlan.get()));
                        }
                    } catch (Throwable e) {
                        // Because only cancelJob or releasePipelineResource can throw exception, so
                        // we only output log here
                        LOGGER.severe(ExceptionUtils.getMessage(e));
                    }
                },
                jobMaster.getExecutorService());
    }

    public void cancelJob() {
        jobMaster.neverNeedRestore();
        if (getJobStatus().isEndState()) {
            LOGGER.warning(
                    String.format(
                            "%s is in end state %s, can not be cancel",
                            jobFullName, getJobStatus()));
            return;
        }

        // If an active Master Node done and another Master Node active, we can not know whether
        // cancelRunningJob
        // complete. So we need cancelRunningJob again.
        if (JobStatus.CANCELLING.equals(getJobStatus())) {
            cancelJobPipelines();
            return;
        }
        updateJobState((JobStatus) runningJobStateIMap.get(jobId), JobStatus.CANCELLING);
        cancelJobPipelines();
    }

    private void cancelJobPipelines() {
        List<CompletableFuture<Void>> collect =
                pipelineList.stream()
                        .map(
                                pipeline ->
                                        CompletableFuture.runAsync(
                                                pipeline::cancelPipeline,
                                                jobMaster.getExecutorService()))
                        .collect(Collectors.toList());

        try {
            CompletableFuture<Void> voidCompletableFuture =
                    CompletableFuture.allOf(collect.toArray(new CompletableFuture[0]));
            voidCompletableFuture.join();
        } catch (Exception e) {
            LOGGER.severe(
                    String.format(
                            "%s cancel error with exception: %s",
                            jobFullName, ExceptionUtils.getMessage(e)));
        }
    }

    public List<SubPlan> getPipelineList() {
        return pipelineList;
    }

    private void turnToEndState(@NonNull JobStatus endState) {
        synchronized (this) {
            // consistency check
            JobStatus current = (JobStatus) runningJobStateIMap.get(jobId);
            if (current.isEndState()) {
                String message = "Job is trying to leave terminal state " + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (!endState.isEndState()) {
                String message = "Need a end state, not " + endState;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // notify checkpoint manager
            jobMaster.getCheckpointManager().shutdown(endState);

            LOGGER.info(String.format("%s end with state %s", getJobFullName(), endState));
            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            updateStateTimestamps(endState);

            runningJobStateIMap.put(jobId, endState);
        }
    }

    private void updateStateTimestamps(@NonNull JobStatus targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update
        // runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(jobId);
        stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsIMap.set(jobId, stateTimestamps);
    }

    public boolean updateJobState(@NonNull JobStatus targetState) {
        synchronized (this) {
            return updateJobState((JobStatus) runningJobStateIMap.get(jobId), targetState);
        }
    }

    public boolean updateJobState(@NonNull JobStatus current, @NonNull JobStatus targetState) {
        synchronized (this) {
            // consistency check
            if (current.isEndState()) {
                String message = "Job is trying to leave terminal state " + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // now do the actual state transition
            if (current.equals(runningJobStateIMap.get(jobId))) {
                LOGGER.info(
                        String.format(
                                "Job %s (%s) turn from state %s to %s.",
                                jobImmutableInformation.getJobConfig().getName(),
                                jobId,
                                current,
                                targetState));

                // we must update runningJobStateTimestampsIMap first and then can update
                // runningJobStateIMap
                updateStateTimestamps(targetState);

                runningJobStateIMap.set(jobId, targetState);
                return true;
            } else {
                return false;
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
}
