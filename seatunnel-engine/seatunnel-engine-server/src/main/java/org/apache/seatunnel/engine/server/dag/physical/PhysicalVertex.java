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
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.CancelTaskOperation;
import org.apache.seatunnel.engine.server.task.operation.DeployTaskOperation;

import com.hazelcast.cluster.Address;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel
 * Each {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some PhysicalVertex.
 * And the number of PhysicalVertex equals the {@link ExecutionVertex#getParallelism()}.
 */
public class PhysicalVertex {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalVertex.class);

    private final TaskGroupLocation taskGroupLocation;

    /**
     * the index of PhysicalVertex
     */
    private final int subTaskGroupIndex;

    private final String taskFullName;

    private final int parallelism;

    private final TaskGroupDefaultImpl taskGroup;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final int pipelineId;

    private final int totalPipelineNum;

    private final Set<URL> pluginJarsUrls;

    private final IMap<Object, Object> runningJobStateIMap;

    /**
     * When PhysicalVertex status turn to end, complete this future. And then the waitForCompleteByPhysicalVertex
     * in {@link SubPlan} whenComplete method will be called.
     */
    private CompletableFuture<TaskExecutionState> taskFuture;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * task transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    private final NodeEngine nodeEngine;

    private TaskGroupImmutableInformation taskGroupImmutableInformation;

    private JobMaster jobMaster;

    public PhysicalVertex(int subTaskGroupIndex,
                          @NonNull ExecutorService executorService,
                          int parallelism,
                          @NonNull TaskGroupDefaultImpl taskGroup,
                          @NonNull FlakeIdGenerator flakeIdGenerator,
                          int pipelineId,
                          int totalPipelineNum,
                          Set<URL> pluginJarsUrls,
                          @NonNull JobImmutableInformation jobImmutableInformation,
                          long initializationTimestamp,
                          @NonNull NodeEngine nodeEngine,
                          @NonNull IMap runningJobStateIMap,
                          @NonNull IMap runningJobStateTimestampsIMap) {
        this.taskGroupLocation = taskGroup.getTaskGroupLocation();
        this.subTaskGroupIndex = subTaskGroupIndex;
        this.executorService = executorService;
        this.parallelism = parallelism;
        this.taskGroup = taskGroup;
        this.flakeIdGenerator = flakeIdGenerator;
        this.pipelineId = pipelineId;
        this.totalPipelineNum = totalPipelineNum;
        this.pluginJarsUrls = pluginJarsUrls;
        this.jobImmutableInformation = jobImmutableInformation;
        this.initializationTimestamp = initializationTimestamp;

        Long[] stateTimestamps = new Long[ExecutionState.values().length];
        if (runningJobStateTimestampsIMap.get(taskGroup.getTaskGroupLocation()) == null) {
            stateTimestamps[ExecutionState.INITIALIZING.ordinal()] = initializationTimestamp;
            runningJobStateTimestampsIMap.put(taskGroup.getTaskGroupLocation(), stateTimestamps);

        }

        if (runningJobStateIMap.get(taskGroupLocation) == null) {
            // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
            stateTimestamps[ExecutionState.CREATED.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsIMap.put(taskGroupLocation, stateTimestamps);

            runningJobStateIMap.put(taskGroupLocation, ExecutionState.CREATED);
        }

        this.nodeEngine = nodeEngine;
        if (LOGGER.isFineEnabled() || LOGGER.isFinestEnabled()) {
            this.taskFullName =
                String.format(
                    "Job %s (%s), Pipeline: [(%d/%d)], task: [%s (%d/%d)], taskGroupLocation: [%s]",
                    jobImmutableInformation.getJobConfig().getName(),
                    jobImmutableInformation.getJobId(),
                    pipelineId,
                    totalPipelineNum,
                    taskGroup.getTaskGroupName(),
                    subTaskGroupIndex + 1,
                    parallelism,
                    taskGroupLocation);
        } else {
            this.taskFullName =
                String.format(
                    "Job %s (%s), Pipeline: [(%d/%d)], task: [%s (%d/%d)]",
                    jobImmutableInformation.getJobConfig().getName(),
                    jobImmutableInformation.getJobId(),
                    pipelineId,
                    totalPipelineNum,
                    taskGroup.getTaskGroupName(),
                    subTaskGroupIndex + 1,
                    parallelism);
        }

        this.taskFuture = new CompletableFuture<>();

        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
    }

    public PassiveCompletableFuture<TaskExecutionState> initStateFuture() {
        this.taskFuture = new CompletableFuture<>();
        ExecutionState executionState = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
        if (executionState != null) {
            LOGGER.info(
                String.format("The task %s is in state %s when init state future", taskFullName, executionState));
        }
        // If the task state is CANCELING we need call noticeTaskExecutionServiceCancel().
        if (ExecutionState.CANCELING.equals(executionState)) {
            noticeTaskExecutionServiceCancel();
        } else if (executionState.isEndState()) {
            this.taskFuture.complete(new TaskExecutionState(taskGroupLocation, executionState, null));
        }
        return new PassiveCompletableFuture<>(this.taskFuture);
    }

    private void deployOnLocal(@NonNull SlotProfile slotProfile) {
        deployInternal(taskGroupImmutableInformation -> {
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            server.getSlotService().getSlotContext(slotProfile)
                .getTaskExecutionService().deployTask(taskGroupImmutableInformation);
        });
    }

    private void deployOnRemote(@NonNull SlotProfile slotProfile) {
        deployInternal(taskGroupImmutableInformation -> {
            try {
                nodeEngine.getOperationService().createInvocationBuilder(Constant.SEATUNNEL_SERVICE_NAME,
                        new DeployTaskOperation(slotProfile,
                            nodeEngine.getSerializationService().toData(taskGroupImmutableInformation)),
                        slotProfile.getWorker())
                    .invoke().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    // This method must not throw an exception
    public void deploy(@NonNull SlotProfile slotProfile) {
        try {
            if (slotProfile.getWorker().equals(nodeEngine.getThisAddress())) {
                deployOnLocal(slotProfile);
            } else {
                deployOnRemote(slotProfile);
            }
        } catch (Throwable th) {
            failedByException(th);
        }
    }

    private void deployInternal(Consumer<TaskGroupImmutableInformation> taskGroupConsumer) {
        TaskGroupImmutableInformation taskGroupImmutableInformation = getTaskGroupImmutableInformation();
        synchronized (this) {
            ExecutionState currentState = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
            if (ExecutionState.DEPLOYING.equals(currentState)) {
                taskGroupConsumer.accept(taskGroupImmutableInformation);
                updateTaskState(ExecutionState.DEPLOYING, ExecutionState.RUNNING);
            }
        }
    }

    private void failedByException(Throwable th) {
        LOGGER.severe(String.format("%s deploy error with Exception: %s",
            this.taskFullName,
            ExceptionUtils.getMessage(th)));
        turnToEndState(ExecutionState.FAILED);
        taskFuture.complete(
            new TaskExecutionState(this.taskGroupLocation, ExecutionState.FAILED, th));
    }

    private TaskGroupImmutableInformation getTaskGroupImmutableInformation() {
        return new TaskGroupImmutableInformation(flakeIdGenerator.newId(),
            nodeEngine.getSerializationService().toData(this.taskGroup),
            this.pluginJarsUrls);
    }

    private boolean turnToEndState(@NonNull ExecutionState endState) {
        synchronized (this) {
            // consistency check
            ExecutionState currentState = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
            if (currentState.isEndState()) {
                String message = String.format("Task %s is already in terminal state %s", taskFullName, currentState);
                LOGGER.warning(message);
                return false;
            }
            if (!endState.isEndState()) {
                String message =
                    String.format("Turn task %s state to end state need gave a end state, not %s", taskFullName,
                        endState);
                LOGGER.warning(message);
                return false;
            }

            updateStateTimestamps(endState);
            runningJobStateIMap.set(taskGroupLocation, endState);
            LOGGER.info(String.format("%s turn to end state %s.",
                taskFullName,
                endState));
            return true;
        }
    }

    public boolean updateTaskState(@NonNull ExecutionState current, @NonNull ExecutionState targetState) {
        synchronized (this) {
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
            if (current.equals(runningJobStateIMap.get(taskGroupLocation))) {
                updateStateTimestamps(targetState);
                runningJobStateIMap.set(taskGroupLocation, targetState);
                LOGGER.info(String.format("%s turn from state %s to %s.",
                    taskFullName,
                    current,
                    targetState));
                return true;
            } else {
                return false;
            }
        }
    }

    public TaskGroupDefaultImpl getTaskGroup() {
        return taskGroup;
    }

    public void cancel() {
        if (updateTaskState(ExecutionState.CREATED, ExecutionState.CANCELED) ||
            updateTaskState(ExecutionState.SCHEDULED, ExecutionState.CANCELED) ||
            updateTaskState(ExecutionState.DEPLOYING, ExecutionState.CANCELED)) {
            taskFuture.complete(new TaskExecutionState(this.taskGroupLocation, ExecutionState.CANCELED, null));
        } else if (updateTaskState(ExecutionState.RUNNING, ExecutionState.CANCELING)) {
            noticeTaskExecutionServiceCancel();
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void noticeTaskExecutionServiceCancel() {
        int i = 0;
        // In order not to generate uncontrolled tasks, We will try again until the taskFuture is completed
        while (!taskFuture.isDone() && nodeEngine.getClusterService().getMember(getCurrentExecutionAddress()) != null) {
            try {
                i++;
                LOGGER.info(
                    String.format("Send cancel %s operator to member %s", taskFullName, getCurrentExecutionAddress()));
                nodeEngine.getOperationService().createInvocationBuilder(Constant.SEATUNNEL_SERVICE_NAME,
                        new CancelTaskOperation(taskGroupLocation),
                        getCurrentExecutionAddress())
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

    private void updateStateTimestamps(@NonNull ExecutionState targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(taskGroupLocation);
        stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsIMap.set(taskGroupLocation, stateTimestamps);

    }

    public ExecutionState getExecutionState() {
        return (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
    }

    private void resetExecutionState() {
        synchronized (this) {
            ExecutionState executionState = getExecutionState();
            if (!executionState.isEndState()) {
                String message =
                    String.format("%s reset state failed, only end state can be reset, current is %s",
                        getTaskFullName(),
                        executionState);
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }
            updateStateTimestamps(ExecutionState.CREATED);
            runningJobStateIMap.set(taskGroupLocation, ExecutionState.CREATED);
        }
    }

    public void reset() {
        resetExecutionState();
    }

    public String getTaskFullName() {
        return taskFullName;
    }

    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        if (!turnToEndState(taskExecutionState.getExecutionState())) {
            return;
        }
        if (taskExecutionState.getThrowable() != null) {
            LOGGER.severe(String.format("%s end with state %s and Exception: %s",
                this.taskFullName,
                taskExecutionState.getExecutionState(),
                ExceptionUtils.getMessage(taskExecutionState.getThrowable())));
        } else {
            LOGGER.info(String.format("%s end with state %s",
                this.taskFullName,
                taskExecutionState.getExecutionState()));
        }
        taskFuture.complete(taskExecutionState);
    }

    public Address getCurrentExecutionAddress() {
        return jobMaster.getOwnedSlotProfiles(taskGroupLocation).getWorker();
    }

    public TaskGroupLocation getTaskGroupLocation() {
        return taskGroupLocation;
    }

    public void setJobMaster(JobMaster jobMaster) {
        this.jobMaster = jobMaster;
    }
}
