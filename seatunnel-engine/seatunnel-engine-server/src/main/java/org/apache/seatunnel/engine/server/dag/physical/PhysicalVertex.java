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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.NonCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.operation.DeployTaskOperation;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import com.hazelcast.cluster.Address;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel
 * Each {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some PhysicalVertex.
 * And the number of PhysicalVertex equals the {@link ExecutionVertex#getParallelism()}.
 */
public class PhysicalVertex {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalVertex.class);

    private final long physicalVertexId;

    /**
     * the index of PhysicalVertex
     */
    private final int subTaskGroupIndex;

    private final String taskFullName;

    private final int parallelism;

    private final TaskGroupDefaultImpl taskGroup;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final int pipelineIndex;

    private final int totalPipelineNum;

    private final Set<URL> pluginJarsUrls;

    private AtomicReference<ExecutionState> executionState = new AtomicReference<>();

    /**
     * When PhysicalVertex status turn to end, complete this future. And then the waitForCompleteByPhysicalVertex
     * in {@link SubPlan} whenComplete method will be called.
     */
    private final CompletableFuture<TaskExecutionState> taskFuture;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * task transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    /**
     * This future only can completion by the task run in {@link com.hazelcast.spi.impl.executionservice.ExecutionService }
     */
    private NonCompletableFuture<TaskExecutionState> waitForCompleteByExecutionService;

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    private final NodeEngine nodeEngine;

    public PhysicalVertex(long physicalVertexId,
                          int subTaskGroupIndex,
                          @NonNull ExecutorService executorService,
                          int parallelism,
                          @NonNull TaskGroupDefaultImpl taskGroup,
                          @NonNull CompletableFuture<TaskExecutionState> taskFuture,
                          @NonNull FlakeIdGenerator flakeIdGenerator,
                          int pipelineIndex,
                          int totalPipelineNum,
                          Set<URL> pluginJarsUrls,
                          @NonNull JobImmutableInformation jobImmutableInformation,
                          long initializationTimestamp,
                          @NonNull NodeEngine nodeEngine) {
        this.physicalVertexId = physicalVertexId;
        this.subTaskGroupIndex = subTaskGroupIndex;
        this.executorService = executorService;
        this.parallelism = parallelism;
        this.taskGroup = taskGroup;
        this.flakeIdGenerator = flakeIdGenerator;
        this.pipelineIndex = pipelineIndex;
        this.totalPipelineNum = totalPipelineNum;
        this.pluginJarsUrls = pluginJarsUrls;
        this.jobImmutableInformation = jobImmutableInformation;
        this.initializationTimestamp = initializationTimestamp;
        stateTimestamps = new long[ExecutionState.values().length];
        this.stateTimestamps[ExecutionState.INITIALIZING.ordinal()] = initializationTimestamp;
        this.executionState.set(ExecutionState.CREATED);
        this.stateTimestamps[ExecutionState.CREATED.ordinal()] = System.currentTimeMillis();
        this.nodeEngine = nodeEngine;
        this.taskFullName =
            String.format(
                "Job %s (%s), Pipeline: [(%d/%d)], task: [%s (%d/%d)]",
                jobImmutableInformation.getJobConfig().getName(),
                jobImmutableInformation.getJobId(),
                pipelineIndex + 1,
                totalPipelineNum,
                taskGroup.getTaskGroupName(),
                subTaskGroupIndex + 1,
                parallelism);
        this.taskFuture = taskFuture;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    // This method must not throw an exception
    public void deploy(@NonNull Address address) {

        TaskGroupImmutableInformation taskGroupImmutableInformation =
                new TaskGroupImmutableInformation(flakeIdGenerator.newId(),
                        nodeEngine.getSerializationService().toData(this.taskGroup),
                        this.pluginJarsUrls);

        try {
            waitForCompleteByExecutionService = new NonCompletableFuture<>(
                    nodeEngine.getOperationService().createInvocationBuilder(Constant.SEATUNNEL_SERVICE_NAME,
                                    new DeployTaskOperation(nodeEngine.getSerializationService().toData(taskGroupImmutableInformation)),
                                    address)
                            .invoke());
        } catch (Throwable th) {
            LOGGER.severe(String.format("%s deploy error with Exception: %s",
                    this.taskFullName,
                    ExceptionUtils.getMessage(th)));
            updateTaskState(ExecutionState.DEPLOYING, ExecutionState.FAILED);
            taskFuture.complete(
                    new TaskExecutionState(taskGroupImmutableInformation.getExecutionId(), ExecutionState.FAILED, null));
        }

        updateTaskState(ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        waitForCompleteByExecutionService.whenComplete((v, t) -> {
            try {
                if (t != null) {
                    LOGGER.severe("An unexpected error occurred while the task was running", t);
                    taskFuture.completeExceptionally(t);
                } else {
                    updateTaskState(executionState.get(), v.getExecutionState());
                    if (v.getThrowable() != null) {
                        LOGGER.severe(String.format("%s end with state %s and Exception: %s",
                                this.taskFullName,
                                v.getExecutionState(),
                                ExceptionUtils.getMessage(v.getThrowable())));
                    } else {
                        LOGGER.severe(String.format("%s end with state %s",
                                this.taskFullName,
                                v.getExecutionState()));
                    }
                    taskFuture.complete(v);
                }
            } catch (Throwable th) {
                LOGGER.severe(
                    String.format("%s end with Exception: %s", this.taskFullName, ExceptionUtils.getMessage(th)));
                updateTaskState(ExecutionState.RUNNING, ExecutionState.FAILED);
                v = new TaskExecutionState(v.getTaskExecutionId(), ExecutionState.FAILED, null);
                taskFuture.complete(v);
            }
        });
    }

    public long getPhysicalVertexId() {
        return physicalVertexId;
    }

    public boolean updateTaskState(@NonNull ExecutionState current, @NonNull ExecutionState targetState) {
        // consistency check
        if (current.isEndState()) {
            String message = "Task is trying to leave terminal state " + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (ExecutionState.SCHEDULED.equals(targetState) && !ExecutionState.CREATED.equals(current)) {
            String message = "Only [CREATED] task can turn to [SCHEDULED]" + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (ExecutionState.DEPLOYING.equals(targetState) && !ExecutionState.SCHEDULED.equals(current)) {
            String message = "Only [SCHEDULED] task can turn to [DEPLOYING]" + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (ExecutionState.RUNNING.equals(targetState) && !ExecutionState.DEPLOYING.equals(current)) {
            String message = "Only [DEPLOYING] task can turn to [RUNNING]" + current;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        // now do the actual state transition
        if (executionState.get() == current) {
            executionState.set(targetState);
            LOGGER.info(String.format("%s turn from state %s to %s.",
                taskFullName,
                current,
                targetState));

            stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
            return true;
        } else {
            return false;
        }
    }

    public TaskGroupDefaultImpl getTaskGroup() {
        return taskGroup;
    }
}
