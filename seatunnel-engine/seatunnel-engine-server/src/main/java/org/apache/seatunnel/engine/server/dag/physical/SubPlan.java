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
import org.apache.seatunnel.engine.core.job.PipelineState;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SubPlan {
    private static final ILogger LOGGER = Logger.getLogger(SubPlan.class);

    private final List<PhysicalVertex> physicalVertexList;

    private final List<PhysicalVertex> coordinatorVertexList;

    private final int pipelineIndex;

    private final int totalPipelineNum;

    private final JobImmutableInformation jobImmutableInformation;

    private AtomicInteger finishedTaskNum = new AtomicInteger(0);

    private AtomicInteger canceledTaskNum = new AtomicInteger(0);

    private AtomicInteger failedTaskNum = new AtomicInteger(0);

    private AtomicReference<PipelineState> pipelineState = new AtomicReference<>();

    private final String pipelineFullName;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * pipeline transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    /**
     * Complete this future when this sub plan complete. When this future completed, the waitForCompleteBySubPlan in {@link PhysicalPlan }
     * whenComplete method will be called.
     */
    private final CompletableFuture<PipelineState> pipelineFuture;

    /**
     * This future only can completion by the {@link PhysicalVertex } taskFuture.
     * When the taskFuture in {@link PhysicalVertex} completed, The NonCompletableFuture's whenComplete method will be called
     */
    private final NonCompletableFuture<TaskExecutionState>[] waitForCompleteByPhysicalVertex;

    public SubPlan(int pipelineIndex,
                   int totalPipelineNum,
                   long initializationTimestamp,
                   @NonNull List<PhysicalVertex> physicalVertexList,
                   @NonNull List<PhysicalVertex> coordinatorVertexList,
                   @NonNull CompletableFuture<PipelineState> pipelineFuture,
                   @NonNull NonCompletableFuture<TaskExecutionState>[] waitForCompleteByPhysicalVertex,
                   @NonNull JobImmutableInformation jobImmutableInformation) {
        this.pipelineIndex = pipelineIndex;
        this.pipelineFuture = pipelineFuture;
        this.totalPipelineNum = totalPipelineNum;
        this.physicalVertexList = physicalVertexList;
        this.coordinatorVertexList = coordinatorVertexList;
        this.waitForCompleteByPhysicalVertex = waitForCompleteByPhysicalVertex;
        stateTimestamps = new long[PipelineState.values().length];
        this.stateTimestamps[PipelineState.INITIALIZING.ordinal()] = initializationTimestamp;
        this.pipelineState.set(PipelineState.CREATED);
        this.stateTimestamps[PipelineState.CREATED.ordinal()] = System.currentTimeMillis();
        this.jobImmutableInformation = jobImmutableInformation;
        this.pipelineFullName = String.format(
            "Job %s (%s), Pipeline: [(%d/%d)]",
            jobImmutableInformation.getJobConfig().getName(),
            jobImmutableInformation.getJobId(),
            pipelineIndex + 1,
            totalPipelineNum);

        Arrays.stream(this.waitForCompleteByPhysicalVertex).forEach(x -> {
            x.whenComplete((v, t) -> {
                // We need not handle t, Because we will not return t from PhysicalVertex
                if (ExecutionState.CANCELED.equals(v.getExecutionState())) {
                    canceledTaskNum.incrementAndGet();
                } else if (ExecutionState.FAILED.equals(v.getExecutionState())) {
                    LOGGER.severe("Task Failed, Begin to cancel other tasks in this pipeline.");
                    cancelPipeline().whenComplete((v1, t1) -> {
                        LOGGER.severe("Cancel other tasks complete");
                        failedTaskNum.incrementAndGet();
                    });
                } else if (!ExecutionState.FINISHED.equals(v.getExecutionState())) {
                    LOGGER.severe(
                        "Task Failed with Unknown ExecutionState, Begin to cancel other tasks in this pipeline.");
                    cancelPipeline().whenComplete((v1, t1) -> {
                        LOGGER.severe("Cancel other tasks complete");
                        failedTaskNum.incrementAndGet();
                    });
                }

                if (finishedTaskNum.incrementAndGet() == (physicalVertexList.size() + coordinatorVertexList.size())) {
                    if (failedTaskNum.get() > 0) {
                        updatePipelineState(PipelineState.FAILED);
                        LOGGER.info(String.format("%s end with state FAILED", this.pipelineFullName));
                    } else if (canceledTaskNum.get() > 0) {
                        updatePipelineState(PipelineState.CANCELED);
                        LOGGER.info(String.format("%s end with state CANCELED", this.pipelineFullName));
                    } else {
                        updatePipelineState(PipelineState.FINISHED);
                        LOGGER.info(String.format("%s end with state FINISHED", this.pipelineFullName));
                    }
                    pipelineFuture.complete(pipelineState.get());
                }
            });
        });
    }

    public boolean updatePipelineState(@NonNull PipelineState targetState) {
        return updatePipelineState(pipelineState.get(), targetState);
    }

    public boolean updatePipelineState(@NonNull PipelineState current, @NonNull PipelineState targetState) {
        // consistency check
        if (current.isEndState()) {
            String message = "Pipeline is trying to leave terminal state " + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (PipelineState.SCHEDULED.equals(targetState) && !PipelineState.CREATED.equals(current)) {
            String message = "Only [CREATED] pipeline can turn to [SCHEDULED]" + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (PipelineState.DEPLOYING.equals(targetState) && !PipelineState.SCHEDULED.equals(current)) {
            String message = "Only [SCHEDULED] pipeline can turn to [DEPLOYING]" + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (PipelineState.RUNNING.equals(targetState) && !PipelineState.DEPLOYING.equals(current)) {
            String message = "Only [DEPLOYING] pipeline can turn to [RUNNING]" + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        // now do the actual state transition
        if (pipelineState.get() == current) {
            pipelineState.set(targetState);
            LOGGER.info(String.format("%s turn from state %s to %s.",
                pipelineFullName,
                current,
                targetState));

            stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
            return true;
        } else {
            return false;
        }
    }

    public NonCompletableFuture<Void> cancelPipeline() {
        CompletableFuture<Void> cancelFuture = CompletableFuture.supplyAsync(() -> {
            // TODO Implement cancel tasks in pipeline.
            return null;
        });

        cancelFuture.complete(null);
        return new NonCompletableFuture<>(cancelFuture);
    }

    public void failedWithNoEnoughResource() {
        LOGGER.severe(String.format("%s failed with have no enough resource to run.", this.getPipelineFullName()));
        updatePipelineState(PipelineState.SCHEDULED, PipelineState.FAILED);
        pipelineFuture.complete(PipelineState.FAILED);
    }

    public int getPipelineIndex() {
        return pipelineIndex;
    }

    public List<PhysicalVertex> getPhysicalVertexList() {
        return physicalVertexList;
    }

    public List<PhysicalVertex> getCoordinatorVertexList() {
        return coordinatorVertexList;
    }

    public String getPipelineFullName() {
        return pipelineFullName;
    }
}
