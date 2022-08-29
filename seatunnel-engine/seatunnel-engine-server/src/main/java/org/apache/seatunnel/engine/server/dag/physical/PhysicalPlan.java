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
import org.apache.seatunnel.engine.core.job.PipelineState;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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

    private AtomicReference<JobStatus> jobStatus = new AtomicReference<>();

    private final JobImmutableInformation jobImmutableInformation;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    /**
     * when job status turn to end, complete this future. And then the waitForCompleteByPhysicalPlan
     * in {@link org.apache.seatunnel.engine.server.scheduler.JobScheduler} whenComplete method will be called.
     */
    private final CompletableFuture<JobStatus> jobEndFuture;

    /**
     * This future only can completion by the {@link SubPlan } subPlanFuture.
     * When subPlanFuture completed, this NonCompletableFuture's whenComplete method will be called.
     */
    private List<PassiveCompletableFuture<PipelineState>> waitForCompleteBySubPlan;

    private final ExecutorService executorService;

    private final String jobFullName;

    public PhysicalPlan(@NonNull List<SubPlan> pipelineList,
                        @NonNull ExecutorService executorService,
                        @NonNull JobImmutableInformation jobImmutableInformation,
                        long initializationTimestamp) {
        this.executorService = executorService;
        this.jobImmutableInformation = jobImmutableInformation;
        stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.jobStatus.set(JobStatus.CREATED);
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();
        this.jobEndFuture = new CompletableFuture<>();
        this.pipelineList = pipelineList;
        if (pipelineList.isEmpty()) {
            throw new UnknownPhysicalPlanException("The physical plan didn't have any can execute pipeline");
        }
        this.jobFullName = String.format("Job %s (%s)", jobImmutableInformation.getJobConfig().getName(),
            jobImmutableInformation.getJobId());
    }

    public void initStateFuture() {
        pipelineList.stream().forEach(subPlan -> {
            PassiveCompletableFuture<PipelineState> future = subPlan.initStateFuture();
            future.whenComplete((v, t) -> {
                // We need not handle t, Because we will not return t from Pipeline
                if (PipelineState.CANCELED.equals(v)) {
                    canceledPipelineNum.incrementAndGet();
                } else if (PipelineState.FAILED.equals(v)) {
                    LOGGER.severe("Pipeline Failed, Begin to cancel other pipelines in this job.");
                    failedPipelineNum.incrementAndGet();
                    cancelJob();
                } else if (!PipelineState.FINISHED.equals(v)) {
                    LOGGER.severe(
                        "Pipeline Failed with Unknown PipelineState, Begin to cancel other pipelines in this job.");
                    failedPipelineNum.incrementAndGet();
                    cancelJob();
                }

                if (finishedPipelineNum.incrementAndGet() == this.pipelineList.size()) {
                    if (failedPipelineNum.get() > 0) {
                        updateJobState(JobStatus.FAILING);
                    } else if (canceledPipelineNum.get() > 0) {
                        turnToEndState(JobStatus.CANCELED);
                    } else {
                        turnToEndState(JobStatus.FINISHED);
                    }
                    jobEndFuture.complete(jobStatus.get());
                }
            });
        });
    }

    public void cancelJob() {
        if (!updateJobState(JobStatus.CREATED, JobStatus.CANCELED)) {
            // may be running, failing, failed, canceling , canceled, finished
            if (updateJobState(JobStatus.RUNNING, JobStatus.CANCELLING)) {
                cancelRunningJob();
            } else {
                LOGGER.info(
                    String.format("%s in a non cancellable state: %s, skip cancel", jobFullName, jobStatus.get()));
            }
        }
    }

    private void cancelRunningJob() {
        List<CompletableFuture<Void>> collect = pipelineList.stream().map(pipeline -> {
            if (!pipeline.getPipelineState().get().isEndState() &&
                !PipelineState.CANCELING.equals(pipeline.getPipelineState().get())) {
                CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                    pipeline.cancelPipeline();
                    return null;
                });
                return future;
            }
            return null;
        }).filter(x -> x != null).collect(Collectors.toList());

        try {
            CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                collect.toArray(new CompletableFuture[collect.size()]));
            voidCompletableFuture.get();
        } catch (Exception e) {
            LOGGER.severe(
                String.format("%s cancel error with exception: %s", jobFullName, ExceptionUtils.getMessage(e)));
        }
    }

    public List<SubPlan> getPipelineList() {
        return pipelineList;
    }

    public boolean turnToRunning() {
        return updateJobState(JobStatus.CREATED, JobStatus.RUNNING);
    }

    private void turnToEndState(@NonNull JobStatus endState) {
        // consistency check
        if (jobStatus.get().isEndState()) {
            String message = "Job is trying to leave terminal state " + jobStatus.get();
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (!endState.isEndState()) {
            String message = "Need a end state, not " + endState;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        jobStatus.set(endState);
        stateTimestamps[endState.ordinal()] = System.currentTimeMillis();
    }

    public boolean updateJobState(@NonNull JobStatus targetState) {
        return updateJobState(jobStatus.get(), targetState);
    }

    public boolean updateJobState(@NonNull JobStatus current, @NonNull JobStatus targetState) {
        // consistency check
        if (current.isEndState()) {
            String message = "Job is trying to leave terminal state " + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        // now do the actual state transition
        if (jobStatus.compareAndSet(current, targetState)) {
            LOGGER.info(String.format("Job %s (%s) turn from state %s to %s.",
                jobImmutableInformation.getJobConfig().getName(),
                jobImmutableInformation.getJobId(),
                current,
                targetState));

            stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
            return true;
        } else {
            return false;
        }
    }

    public PassiveCompletableFuture<JobStatus> getJobEndCompletableFuture() {
        return new PassiveCompletableFuture<>(jobEndFuture);
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }

    public JobStatus getJobStatus() {
        return jobStatus.get();
    }

    public String getJobFullName() {
        return jobFullName;
    }
}
