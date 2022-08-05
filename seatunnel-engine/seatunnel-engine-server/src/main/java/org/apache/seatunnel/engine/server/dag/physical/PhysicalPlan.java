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

import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class PhysicalPlan {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalPlan.class);

    private List<SubPlan> plans;

    private int finishedPipelineNum;

    private JobStatus jobStatus = JobStatus.CREATED;

    private final JobImmutableInformation jobImmutableInformation;

    /**
     * The currently executed tasks, for callbacks.
     */
    private final List<>

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    /**
     * when job status turn to end, complete this future.
     */
    private final CompletableFuture<JobStatus> jobEndFuture = new CompletableFuture<>();



    public PhysicalPlan(@NonNull List<SubPlan> plans,
                        @NonNull JobImmutableInformation jobImmutableInformation,
                        long initializationTimestamp) {
        this.plans = plans;
        this.jobImmutableInformation = jobImmutableInformation;
        stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();
    }

    public PhysicalPlan(@NonNull JobImmutableInformation jobImmutableInformation, long initializationTimestamp) {
        this.jobImmutableInformation = jobImmutableInformation;
        stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();
        this.currentExecutionMap = new LinkedHashMap<>();
    }

    public List<SubPlan> getPlans() {
        return plans;
    }

    public void pipelineFinished() {
        finishedPipelineNum++;
        if (finishedPipelineNum == plans.size()) {
            // check whether we are still in "RUNNING" and trigger the final cleanup
            if (jobStatus == JobStatus.RUNNING) {

                if (updateJobState(JobStatus.RUNNING, JobStatus.FINISHED)) {
                    LOGGER.info("Job {}({}) finished", jobInformation.getJobName(), jobInformation.getJobId());
                    jobEndFuture.complete(jobStatus);
                }
            }
        }
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
        if (jobStatus == current) {
            jobStatus = targetState;
            LOGGER.info(String.format("Job {} ({}) turn from state {} to {}.",
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

    public void executionTaskFinish() {
        final int currFinishedNum = ++numFinishedExecutionTask;
        if (currFinishedNum == tasks.size()) {

            // check whether we are still in "RUNNING" and trigger the final cleanup
            if (jobStatus == JobStatus.RUNNING) {

                if (updateJobState(JobStatus.RUNNING, JobStatus.FINISHED)) {
                    LOG.info("Job {}({}) finished", jobInformation.getJobName(), jobInformation.getJobId());
                    jobEndFuture.complete(jobStatus);
                }
            }
        }
    }

    public void setPlans(List<SubPlan> plans) {
        this.plans = plans;
    }
}
