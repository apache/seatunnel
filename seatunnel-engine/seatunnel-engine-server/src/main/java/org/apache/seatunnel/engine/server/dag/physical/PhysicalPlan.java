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

import org.apache.seatunnel.engine.common.utils.NonCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineState;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
    private final NonCompletableFuture<PipelineState>[] waitForCompleteBySubPlan;

    private final ExecutorService executorService;

    public PhysicalPlan(@NonNull List<SubPlan> pipelineList,
                        @NonNull ExecutorService executorService,
                        @NonNull JobImmutableInformation jobImmutableInformation,
                        long initializationTimestamp,
                        @NonNull NonCompletableFuture<PipelineState>[] waitForCompleteBySubPlan) {
        this.executorService = executorService;
        this.jobImmutableInformation = jobImmutableInformation;
        stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.jobStatus.set(JobStatus.CREATED);
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();
        this.jobEndFuture = new CompletableFuture<JobStatus>();
        this.waitForCompleteBySubPlan = waitForCompleteBySubPlan;
        this.pipelineList = pipelineList;
        if (pipelineList.isEmpty()) {
            throw new UnknownPhysicalPlanException("The physical plan didn't have any can execute pipeline");
        }
        Arrays.stream(this.waitForCompleteBySubPlan).forEach(x -> {
            x.whenComplete((v, t) -> {
                if (PipelineState.CANCELED.equals(v)) {
                    canceledPipelineNum.incrementAndGet();
                } else if (PipelineState.FAILED.equals(v)) {
                    failedPipelineNum.incrementAndGet();
                }

                if (finishedPipelineNum.incrementAndGet() == this.pipelineList.size()) {
                    if (failedPipelineNum.get() > 0) {
                        jobStatus.set(JobStatus.FAILING);
                    } else if (canceledPipelineNum.get() > 0) {
                        jobStatus.set(JobStatus.CANCELED);
                    } else {
                        jobStatus.set(JobStatus.FINISHED);
                    }
                    jobEndFuture.complete(jobStatus.get());
                }
            });
        });
    }

    public List<SubPlan> getPipelineList() {
        return pipelineList;
    }

    public void turnToRunning() {
        if (!updateJobState(JobStatus.CREATED, JobStatus.RUNNING)) {
            throw new IllegalStateException(
                "Job may only be scheduled from state " + JobStatus.CREATED);
        }
    }

    public boolean updateJobState(JobStatus current, JobStatus targetState) {
        // consistency check
        if (current.isEndState()) {
            String message = "Job is trying to leave terminal state " + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        // now do the actual state transition
        if (jobStatus.get() == current) {
            jobStatus.set(targetState);
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

    public CompletableFuture<JobStatus> getJobEndCompletableFuture() {
        return this.jobEndFuture;
    }
}
