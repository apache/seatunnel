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
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PhysicalPlan {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalPlan.class);
    /**
     * The max num pipeline can restore.
     */
    public static final int PIPELINE_MAX_RESTORE_NUM = 3; // TODO should set by config

    private final List<SubPlan> pipelineList;

    private AtomicInteger finishedPipelineNum = new AtomicInteger(0);

    private AtomicInteger canceledPipelineNum = new AtomicInteger(0);

    private AtomicInteger failedPipelineNum = new AtomicInteger(0);

    private final JobImmutableInformation jobImmutableInformation;

    private final IMap<Object, Object> runningJobStateIMap;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * when job status turn to end, complete this future. And then the waitForCompleteByPhysicalPlan
     * in {@link org.apache.seatunnel.engine.server.scheduler.JobScheduler} whenComplete method will be called.
     */
    private CompletableFuture<JobStatus> jobEndFuture;

    private final ExecutorService executorService;

    private final String jobFullName;

    private final long jobId;

    private final Map<Integer, CompletableFuture> pipelineSchedulerFutureMap;

    private JobMaster jobMaster;

    /**
     * If the job or pipeline cancel by user, needRestore will be false
     **/
    private volatile boolean needRestore = true;

    /**
     * Whether we make the job end when pipeline turn to end state.
     */
    private boolean makeJobEndWhenPipelineEnded = true;

    public PhysicalPlan(@NonNull List<SubPlan> pipelineList,
                        @NonNull ExecutorService executorService,
                        @NonNull JobImmutableInformation jobImmutableInformation,
                        long initializationTimestamp,
                        @NonNull IMap runningJobStateIMap,
                        @NonNull IMap runningJobStateTimestampsIMap) {
        this.executorService = executorService;
        this.jobImmutableInformation = jobImmutableInformation;
        this.jobId = jobImmutableInformation.getJobId();
        Long[] stateTimestamps = new Long[JobStatus.values().length];
        if (runningJobStateTimestampsIMap.get(jobId) == null) {
            stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
            runningJobStateTimestampsIMap.put(jobId, stateTimestamps);
        }

        if (runningJobStateIMap.get(jobId) == null) {
            // We must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap.
            // Because if a new Master Node become active, we can recover ExecutionState and PipelineState and JobStatus
            // from TaskExecutionService. But we can not recover stateTimestamps.
            stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsIMap.put(jobId, stateTimestamps);

            runningJobStateIMap.put(jobId, JobStatus.CREATED);
        }

        this.pipelineList = pipelineList;
        if (pipelineList.isEmpty()) {
            throw new UnknownPhysicalPlanException("The physical plan didn't have any can execute pipeline");
        }
        this.jobFullName = String.format("Job %s (%s)", jobImmutableInformation.getJobConfig().getName(),
            jobImmutableInformation.getJobId());

        pipelineSchedulerFutureMap = new ConcurrentHashMap<>(pipelineList.size());
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
    }

    public void setJobMaster(JobMaster jobMaster) {
        this.jobMaster = jobMaster;
        pipelineList.forEach(pipeline -> pipeline.setJobMaster(jobMaster));
    }

    public PassiveCompletableFuture<JobStatus> initStateFuture() {
        jobEndFuture = new CompletableFuture<>();
        pipelineList.forEach(this::addPipelineEndCallback);
        return new PassiveCompletableFuture<>(jobEndFuture);
    }

    public void addPipelineEndCallback(SubPlan subPlan) {
        PassiveCompletableFuture<PipelineStatus> future = subPlan.initStateFuture();
        future.thenAcceptAsync(pipelineState -> {
            try {
                // Notify checkpoint manager when the pipeline end, Whether the pipeline will be restarted or not
                jobMaster.getCheckpointManager()
                    .listenPipelineRetry(subPlan.getPipelineLocation().getPipelineId(), subPlan.getPipelineState()).join();
                if (PipelineStatus.CANCELED.equals(pipelineState)) {
                    if (canRestorePipeline(subPlan)) {
                        subPlan.restorePipeline();
                        return;
                    }
                    canceledPipelineNum.incrementAndGet();
                    if (makeJobEndWhenPipelineEnded) {
                        LOGGER.info(String.format("cancel job %s because makeJobEndWhenPipelineEnded is true", jobFullName));
                        cancelJob();
                    }
                    LOGGER.info(String.format("release the pipeline %s resource", subPlan.getPipelineFullName()));
                    jobMaster.releasePipelineResource(subPlan);
                } else if (PipelineStatus.FAILED.equals(pipelineState)) {
                    if (canRestorePipeline(subPlan)) {
                        subPlan.restorePipeline();
                        return;
                    }
                    failedPipelineNum.incrementAndGet();
                    if (makeJobEndWhenPipelineEnded) {
                        cancelJob();
                    }
                    jobMaster.releasePipelineResource(subPlan);
                    LOGGER.severe("Pipeline Failed, Begin to cancel other pipelines in this job.");
                }

                notifyCheckpointManagerPipelineEnd(subPlan);

                if (finishedPipelineNum.incrementAndGet() == this.pipelineList.size()) {
                    if (failedPipelineNum.get() > 0) {
                        updateJobState(JobStatus.FAILING);
                    } else if (canceledPipelineNum.get() > 0) {
                        turnToEndState(JobStatus.CANCELED);
                    } else {
                        turnToEndState(JobStatus.FINISHED);
                    }
                    jobEndFuture.complete((JobStatus) runningJobStateIMap.get(jobId));
                }
            } catch (Throwable e) {
                // Because only cancelJob or releasePipelineResource can throw exception, so we only output log here
                LOGGER.severe("Never come here ", e);
            }
        });
    }

    /**
     * only call when the pipeline will never restart
     * @param subPlan subPlan
     */
    private void notifyCheckpointManagerPipelineEnd(@NonNull SubPlan subPlan) {
        jobMaster.getCheckpointManager()
            .listenPipeline(subPlan.getPipelineLocation().getPipelineId(), subPlan.getPipelineState()).join();
    }

    private boolean canRestorePipeline(SubPlan subPlan) {
        return needRestore && subPlan.getPipelineRestoreNum() < PIPELINE_MAX_RESTORE_NUM;
    }

    public void cancelJob() {
        if (getJobStatus().isEndState()) {
            LOGGER.warning(String.format("%s is in end state %s, can not be cancel", jobFullName, getJobStatus()));
            return;
        }

        // If an active Master Node done and another Master Node active, we can not know whether cancelRunningJob
        // complete. So we need cancelRunningJob again.
        if (JobStatus.CANCELLING.equals(getJobStatus())) {
            cancelJobPipelines();
            return;
        }
        updateJobState((JobStatus) runningJobStateIMap.get(jobId), JobStatus.CANCELLING);
        cancelJobPipelines();
    }

    private void cancelJobPipelines() {
        List<CompletableFuture<Void>> collect = pipelineList.stream().map(pipeline -> {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                pipeline.cancelPipeline();
            });
            return future;
        }).filter(x -> x != null).collect(Collectors.toList());

        try {
            CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                collect.toArray(new CompletableFuture[0]));
            voidCompletableFuture.join();
        } catch (Exception e) {
            LOGGER.severe(
                String.format("%s cancel error with exception: %s", jobFullName, ExceptionUtils.getMessage(e)));
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
            // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
            updateStateTimestamps(endState);

            runningJobStateIMap.put(jobId, endState);
        }
    }

    private void updateStateTimestamps(@NonNull JobStatus targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
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
                LOGGER.info(String.format("Job %s (%s) turn from state %s to %s.",
                    jobImmutableInformation.getJobConfig().getName(),
                    jobId,
                    current,
                    targetState));

                // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
                updateStateTimestamps(targetState);

                runningJobStateIMap.set(jobId, targetState);
                return true;
            } else {
                return false;
            }
        }
    }

    public PassiveCompletableFuture<JobStatus> getJobEndCompletableFuture() {
        return new PassiveCompletableFuture<>(jobEndFuture);
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

    public void neverNeedRestore() {
        this.needRestore = false;
    }
}
