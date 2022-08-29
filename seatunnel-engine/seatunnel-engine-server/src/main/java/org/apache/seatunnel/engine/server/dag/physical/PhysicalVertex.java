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
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.CancelTaskOperation;
import org.apache.seatunnel.engine.server.task.operation.DeployTaskOperation;

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
import java.util.function.Function;

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

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    private final NodeEngine nodeEngine;

    private Address currentExecutionAddress;

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
                pipelineIndex,
                totalPipelineNum,
                taskGroup.getTaskGroupName(),
                subTaskGroupIndex + 1,
                parallelism);
        this.taskFuture = taskFuture;
    }

    public void deployOnMaster() {
        currentExecutionAddress = nodeEngine.getMasterAddress();
        deployInternal(taskGroupImmutableInformation -> {
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            return new PassiveCompletableFuture<>(server.getTaskExecutionService()
                .deployTask(taskGroupImmutableInformation));
        });
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    // This method must not throw an exception
    public void deploy(@NonNull SlotProfile slotProfile) {
        currentExecutionAddress = slotProfile.getWorker();
        deployInternal(taskGroupImmutableInformation -> new PassiveCompletableFuture<>(
            nodeEngine.getOperationService().createInvocationBuilder(Constant.SEATUNNEL_SERVICE_NAME,
                    new DeployTaskOperation(slotProfile,
                        nodeEngine.getSerializationService().toData(taskGroupImmutableInformation)),
                    slotProfile.getWorker())
                .invoke()));
    }

    private void deployInternal(Function<TaskGroupImmutableInformation, PassiveCompletableFuture<TaskExecutionState>> deployMethod) {
        TaskGroupImmutableInformation taskGroupImmutableInformation = getTaskGroupImmutableInformation();
        PassiveCompletableFuture<TaskExecutionState> completeFuture;
        try {
            if (ExecutionState.DEPLOYING.equals(executionState.get())) {
                completeFuture = deployMethod.apply(taskGroupImmutableInformation);
                // may be canceling
                if (!updateTaskState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
                    // If we found the task state turned to CANCELING after deployed to TaskExecutionService. We need
                    // notice the TaskExecutionService to cancel this task.
                    noticeTaskExecutionServiceCancel();
                    if (ExecutionState.CANCELING.equals(this.getExecutionState().get())) {
                        turnToEndState(ExecutionState.CANCELED);
                        taskFuture.complete(
                            new TaskExecutionState(this.physicalVertexId, ExecutionState.CANCELED, null));
                    } else {
                        turnToEndState(ExecutionState.FAILED);
                        taskFuture.complete(new TaskExecutionState(this.physicalVertexId, ExecutionState.FAILED,
                            new JobException(String.format("%s turn to a unexpected state: %s, make it Failed",
                                this.getTaskFullName(), executionState.get()))));
                    }
                }
                monitorTask(taskGroupImmutableInformation.getExecutionId(), completeFuture);
            } else if (ExecutionState.CANCELING.equals(this.getExecutionState().get())) {
                turnToEndState(ExecutionState.CANCELED);
                taskFuture.complete(new TaskExecutionState(this.physicalVertexId, executionState.get(), null));
            } else {
                turnToEndState(ExecutionState.FAILED);
                taskFuture.complete(new TaskExecutionState(this.physicalVertexId, executionState.get(),
                    new JobException(String.format("%s turn to a unexpected state", jobImmutableInformation.getJobId()))));
            }
        } catch (Throwable th) {
            failedByException(th);
        }
    }

    private void failedByException(Throwable th) {
        LOGGER.severe(String.format("%s deploy error with Exception: %s",
            this.taskFullName,
            ExceptionUtils.getMessage(th)));
        turnToEndState(ExecutionState.FAILED);
        taskFuture.complete(
            new TaskExecutionState(this.physicalVertexId, ExecutionState.FAILED, th));
    }

    private TaskGroupImmutableInformation getTaskGroupImmutableInformation() {
        return new TaskGroupImmutableInformation(flakeIdGenerator.newId(),
                nodeEngine.getSerializationService().toData(this.taskGroup),
                this.pluginJarsUrls);
    }

    /**
     * @param completeFuture This future only can completion by the task run in
     *                       {@link com.hazelcast.spi.impl.executionservice.ExecutionService }
     */
    private void monitorTask(long executionId, PassiveCompletableFuture<TaskExecutionState> completeFuture) {
        completeFuture.whenComplete((v, t) -> {
            try {
                if (t != null) {
                    LOGGER.severe("An unexpected error occurred while the task was running", t);
                    taskFuture.complete(
                            new TaskExecutionState(executionId, ExecutionState.FAILED,
                                    t));
                } else {
                    turnToEndState(v.getExecutionState());
                    if (v.getThrowable() != null) {
                        LOGGER.severe(String.format("%s end with state %s and Exception: %s",
                            this.taskFullName,
                            v.getExecutionState(),
                            ExceptionUtils.getMessage(v.getThrowable())));
                    } else {
                        LOGGER.info(String.format("%s end with state %s",
                            this.taskFullName,
                            v.getExecutionState()));
                    }
                    taskFuture.complete(v);
                }
            } catch (Throwable th) {
                LOGGER.severe(
                    String.format("%s end with Exception: %s", this.taskFullName, ExceptionUtils.getMessage(th)));
                turnToEndState(ExecutionState.FAILED);
                v = new TaskExecutionState(v.getTaskExecutionId(), ExecutionState.FAILED, th);
                taskFuture.complete(v);
            }
        });
    }

    public long getPhysicalVertexId() {
        return physicalVertexId;
    }

    private void turnToEndState(@NonNull ExecutionState endState) {
        // consistency check
        if (executionState.get().isEndState()) {
            String message = "Task is trying to leave terminal state " + executionState.get();
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        if (!endState.isEndState()) {
            String message = "Need a end state, not " + endState;
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        LOGGER.info(String.format("%s turn to end state %s.",
            taskFullName,
            endState));
        executionState.set(endState);
        stateTimestamps[endState.ordinal()] = System.currentTimeMillis();
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
        if (executionState.compareAndSet(current, targetState)) {
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

    public void cancel() {
        if (updateTaskState(ExecutionState.CREATED, ExecutionState.CANCELED) ||
            updateTaskState(ExecutionState.SCHEDULED, ExecutionState.CANCELED)) {
            taskFuture.complete(new TaskExecutionState(this.physicalVertexId, ExecutionState.CANCELED, null));
        } else if (updateTaskState(ExecutionState.DEPLOYING, ExecutionState.CANCELING)) {
            // do nothing, because even if task is deployed to TaskExecutionService, we can do the cancel in deploy method
        } else if (updateTaskState(ExecutionState.RUNNING, ExecutionState.CANCELING)) {
            noticeTaskExecutionServiceCancel();
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void noticeTaskExecutionServiceCancel() {
        int i = 0;
        // In order not to generate uncontrolled tasks, We will try again until the taskFuture is completed
        while (!taskFuture.isDone()) {
            try {
                i++;
                nodeEngine.getOperationService().createInvocationBuilder(Constant.SEATUNNEL_SERVICE_NAME,
                        new CancelTaskOperation(taskGroup.getId()),
                        currentExecutionAddress)
                    .invoke().get();
                return;
            } catch (Exception e) {
                LOGGER.warning(String.format("%s cancel failed with Exception: %s, retry %s", this.getTaskFullName(),
                    ExceptionUtils.getMessage(e), i));
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public AtomicReference<ExecutionState> getExecutionState() {
        return executionState;
    }

    public String getTaskFullName() {
        return taskFullName;
    }
}
