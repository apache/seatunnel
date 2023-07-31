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
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.CancelTaskOperation;
import org.apache.seatunnel.engine.server.task.operation.CheckTaskGroupIsExecutingOperation;
import org.apache.seatunnel.engine.server.task.operation.DeployTaskOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel Each
 * {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some
 * PhysicalVertex. And the number of PhysicalVertex equals the {@link
 * ExecutionVertex#getParallelism()}.
 */
public class PhysicalVertex {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalVertex.class);

    private final TaskGroupLocation taskGroupLocation;

    private final String taskFullName;

    private final TaskGroupDefaultImpl taskGroup;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final Set<URL> pluginJarsUrls;

    private final IMap<Object, Object> runningJobStateIMap;

    /**
     * When PhysicalVertex status turn to end, complete this future. And then the
     * waitForCompleteByPhysicalVertex in {@link SubPlan} whenComplete method will be called.
     */
    private CompletableFuture<TaskExecutionState> taskFuture;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the task
     * transitioned into a certain state. The index into this array is the ordinal of the enum
     * value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    private final NodeEngine nodeEngine;

    private JobMaster jobMaster;

    private volatile ExecutionState currExecutionState = ExecutionState.CREATED;

    public PhysicalVertex(
            int subTaskGroupIndex,
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
        this.executorService = executorService;
        this.taskGroup = taskGroup;
        this.flakeIdGenerator = flakeIdGenerator;
        this.pluginJarsUrls = pluginJarsUrls;

        Long[] stateTimestamps = new Long[ExecutionState.values().length];
        if (runningJobStateTimestampsIMap.get(taskGroup.getTaskGroupLocation()) == null) {
            stateTimestamps[ExecutionState.INITIALIZING.ordinal()] = initializationTimestamp;
            runningJobStateTimestampsIMap.put(taskGroup.getTaskGroupLocation(), stateTimestamps);
        }

        if (runningJobStateIMap.get(taskGroupLocation) == null) {
            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            stateTimestamps[ExecutionState.CREATED.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsIMap.put(taskGroupLocation, stateTimestamps);

            runningJobStateIMap.put(taskGroupLocation, ExecutionState.CREATED);
        }

        this.currExecutionState = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);

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
        this.currExecutionState = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
        if (currExecutionState != null) {
            LOGGER.info(
                    String.format(
                            "The task %s is in state %s when init state future",
                            taskFullName, currExecutionState));
        }
        // if the task state is RUNNING
        // We need to check the real running status of Task from taskExecutionServer.
        // Because the state may be RUNNING when the cluster is restarted, but the Task no longer
        // exists.
        if (ExecutionState.RUNNING.equals(currExecutionState)) {
            if (!checkTaskGroupIsExecuting(taskGroupLocation)) {
                updateTaskState(ExecutionState.RUNNING, ExecutionState.FAILED);
                this.taskFuture.complete(
                        new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED));
            }
        }
        // If the task state is CANCELING we need call noticeTaskExecutionServiceCancel().
        else if (ExecutionState.CANCELING.equals(currExecutionState)) {
            noticeTaskExecutionServiceCancel();
        } else if (currExecutionState.isEndState()) {
            this.taskFuture.complete(new TaskExecutionState(taskGroupLocation, currExecutionState));
        }
        return new PassiveCompletableFuture<>(this.taskFuture);
    }

    private boolean checkTaskGroupIsExecuting(TaskGroupLocation taskGroupLocation) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        SlotProfile slotProfile =
                getOwnedSlotProfilesByTaskGroup(taskGroupLocation, ownedSlotProfilesIMap);
        if (null != slotProfile) {
            Address worker = slotProfile.getWorker();
            List<Address> members =
                    nodeEngine.getClusterService().getMembers().stream()
                            .map(Member::getAddress)
                            .collect(Collectors.toList());
            if (!members.contains(worker)) {
                LOGGER.warning(
                        "The node:"
                                + worker.toString()
                                + " running the taskGroup "
                                + taskGroupLocation
                                + " no longer exists, return false.");
                return false;
            }
            InvocationFuture<Object> invoke =
                    nodeEngine
                            .getOperationService()
                            .createInvocationBuilder(
                                    SeaTunnelServer.SERVICE_NAME,
                                    new CheckTaskGroupIsExecutingOperation(taskGroupLocation),
                                    worker)
                            .invoke();
            try {
                return (Boolean) invoke.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.warning(
                        "Execution of CheckTaskGroupIsExecutingOperation "
                                + taskGroupLocation
                                + " failed, checkTaskGroupIsExecuting return false. ",
                        e);
            }
        }
        return false;
    }

    private SlotProfile getOwnedSlotProfilesByTaskGroup(
            TaskGroupLocation taskGroupLocation,
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap) {
        PipelineLocation pipelineLocation = taskGroupLocation.getPipelineLocation();
        try {
            return ownedSlotProfilesIMap.get(pipelineLocation).get(taskGroupLocation);
        } catch (NullPointerException ignore) {
        }
        return null;
    }

    private TaskDeployState deployOnLocal(@NonNull SlotProfile slotProfile) throws Exception {
        return deployInternal(
                taskGroupImmutableInformation -> {
                    SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
                    return server.getSlotService()
                            .getSlotContext(slotProfile)
                            .getTaskExecutionService()
                            .deployTask(taskGroupImmutableInformation);
                });
    }

    private TaskDeployState deployOnRemote(@NonNull SlotProfile slotProfile) {
        return deployInternal(
                taskGroupImmutableInformation -> {
                    try {
                        return (TaskDeployState)
                                NodeEngineUtil.sendOperationToMemberNode(
                                                nodeEngine,
                                                new DeployTaskOperation(
                                                        slotProfile,
                                                        nodeEngine
                                                                .getSerializationService()
                                                                .toData(
                                                                        taskGroupImmutableInformation)),
                                                slotProfile.getWorker())
                                        .get();
                    } catch (Exception e) {
                        if (getExecutionState().isEndState()) {
                            LOGGER.warning(ExceptionUtils.getMessage(e));
                            LOGGER.warning(
                                    String.format(
                                            "%s deploy error, but the state is already in end state %s, skip this error",
                                            getTaskFullName(), currExecutionState));
                            return TaskDeployState.success();
                        } else {
                            return TaskDeployState.failed(e);
                        }
                    }
                });
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    // This method must not throw an exception
    public TaskDeployState deploy(@NonNull SlotProfile slotProfile) {
        try {
            if (slotProfile.getWorker().equals(nodeEngine.getThisAddress())) {
                return deployOnLocal(slotProfile);
            } else {
                return deployOnRemote(slotProfile);
            }
        } catch (Throwable th) {
            failedByException(th);
            return TaskDeployState.failed(th);
        }
    }

    private TaskDeployState deployInternal(
            Function<TaskGroupImmutableInformation, TaskDeployState> taskGroupConsumer) {
        TaskGroupImmutableInformation taskGroupImmutableInformation =
                getTaskGroupImmutableInformation();
        synchronized (this) {
            if (ExecutionState.DEPLOYING.equals(currExecutionState)) {
                TaskDeployState state = taskGroupConsumer.apply(taskGroupImmutableInformation);
                updateTaskState(ExecutionState.DEPLOYING, ExecutionState.RUNNING);
                return state;
            }
            return TaskDeployState.success();
        }
    }

    private void failedByException(Throwable th) {
        LOGGER.severe(
                String.format(
                        "%s deploy error with Exception: %s",
                        this.taskFullName, ExceptionUtils.getMessage(th)));
        turnToEndState(ExecutionState.FAILED);
        taskFuture.complete(
                new TaskExecutionState(this.taskGroupLocation, ExecutionState.FAILED, th));
    }

    private TaskGroupImmutableInformation getTaskGroupImmutableInformation() {
        return new TaskGroupImmutableInformation(
                flakeIdGenerator.newId(),
                nodeEngine.getSerializationService().toData(this.taskGroup),
                this.pluginJarsUrls);
    }

    private boolean turnToEndState(@NonNull ExecutionState endState) {
        synchronized (this) {
            if (!endState.isEndState()) {
                String message =
                        String.format(
                                "Turn task %s state to end state need gave a end state, not %s",
                                taskFullName, endState);
                LOGGER.warning(message);
                return false;
            }
            // consistency check
            if (currExecutionState.equals(endState)) {
                return true;
            }
            if (currExecutionState.isEndState()) {
                String message =
                        String.format(
                                "Task %s is already in terminal state %s",
                                taskFullName, currExecutionState);
                LOGGER.warning(message);
                return false;
            }

            try {
                RetryUtils.retryWithException(
                        () -> {
                            updateStateTimestamps(endState);
                            runningJobStateIMap.set(taskGroupLocation, endState);
                            return null;
                        },
                        new RetryUtils.RetryMaterial(
                                Constant.OPERATION_RETRY_TIME,
                                true,
                                exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                                Constant.OPERATION_RETRY_SLEEP));
            } catch (Exception e) {
                LOGGER.warning(ExceptionUtils.getMessage(e));
                // If master/worker node done, The job will restore and fix the state from
                // TaskExecutionService
                LOGGER.warning(
                        String.format(
                                "Set %s state %s to Imap failed, skip.",
                                getTaskFullName(), endState));
            }
            this.currExecutionState = endState;
            LOGGER.info(String.format("%s turn to end state %s.", taskFullName, endState));
            return true;
        }
    }

    public boolean updateTaskState(
            @NonNull ExecutionState current, @NonNull ExecutionState targetState) {
        synchronized (this) {
            LOGGER.info(
                    String.format(
                            "Try to update the task %s state from %s to %s",
                            taskFullName, current, targetState));
            // consistency check
            if (current.isEndState()) {
                String message = "Task is trying to leave terminal state " + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (ExecutionState.SCHEDULED.equals(targetState)
                    && !ExecutionState.CREATED.equals(current)) {
                String message = "Only [CREATED] task can turn to [SCHEDULED]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (ExecutionState.DEPLOYING.equals(targetState)
                    && !ExecutionState.SCHEDULED.equals(current)) {
                String message = "Only [SCHEDULED] task can turn to [DEPLOYING]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (ExecutionState.RUNNING.equals(targetState)
                    && !ExecutionState.DEPLOYING.equals(current)) {
                String message = "Only [DEPLOYING] task can turn to [RUNNING]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // now do the actual state transition
            if (current.equals(currExecutionState)) {
                try {
                    RetryUtils.retryWithException(
                            () -> {
                                updateStateTimestamps(targetState);
                                runningJobStateIMap.set(taskGroupLocation, targetState);
                                return null;
                            },
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    exception ->
                                            ExceptionUtil.isOperationNeedRetryException(exception),
                                    Constant.OPERATION_RETRY_SLEEP));
                } catch (Exception e) {
                    LOGGER.warning(ExceptionUtils.getMessage(e));
                    // If master/worker node done, The job will restore and fix the state from
                    // TaskExecutionService
                    LOGGER.warning(
                            String.format(
                                    "Set %s state %s to Imap failed, skip.",
                                    getTaskFullName(), targetState));
                }
                this.currExecutionState = targetState;
                LOGGER.info(
                        String.format(
                                "%s turn from state %s to %s.",
                                taskFullName, current, targetState));
                return true;
            } else {
                LOGGER.warning(
                        String.format(
                                "The task %s state in Imap is %s, not equals expected state %s",
                                taskFullName, currExecutionState, current));
                return false;
            }
        }
    }

    public void cancel() {
        if (updateTaskState(ExecutionState.CREATED, ExecutionState.CANCELED)
                || updateTaskState(ExecutionState.SCHEDULED, ExecutionState.CANCELED)
                || updateTaskState(ExecutionState.DEPLOYING, ExecutionState.CANCELED)) {
            taskFuture.complete(
                    new TaskExecutionState(this.taskGroupLocation, ExecutionState.CANCELED));
        } else if (updateTaskState(ExecutionState.RUNNING, ExecutionState.CANCELING)) {
            noticeTaskExecutionServiceCancel();
        } else if (ExecutionState.CANCELING.equals(runningJobStateIMap.get(taskGroupLocation))) {
            noticeTaskExecutionServiceCancel();
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void noticeTaskExecutionServiceCancel() {
        // Check whether the node exists, and whether the Task on the node exists. If there is no
        // direct update state
        if (!checkTaskGroupIsExecuting(taskGroupLocation)) {
            updateTaskState(ExecutionState.CANCELING, ExecutionState.CANCELED);
            taskFuture.complete(
                    new TaskExecutionState(this.taskGroupLocation, ExecutionState.CANCELED));
            return;
        }
        int i = 0;
        // In order not to generate uncontrolled tasks, We will try again until the taskFuture is
        // completed
        Address executionAddress;
        while (!taskFuture.isDone()
                && nodeEngine
                                .getClusterService()
                                .getMember(executionAddress = getCurrentExecutionAddress())
                        != null
                && i < Constant.OPERATION_RETRY_TIME) {
            try {
                i++;
                LOGGER.info(
                        String.format(
                                "Send cancel %s operator to member %s",
                                taskFullName, executionAddress));
                nodeEngine
                        .getOperationService()
                        .createInvocationBuilder(
                                Constant.SEATUNNEL_SERVICE_NAME,
                                new CancelTaskOperation(taskGroupLocation),
                                executionAddress)
                        .invoke()
                        .get();
                return;
            } catch (Exception e) {
                LOGGER.warning(
                        String.format(
                                "%s cancel failed with Exception: %s, retry %s",
                                this.getTaskFullName(), ExceptionUtils.getMessage(e), i));
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private void updateStateTimestamps(@NonNull ExecutionState targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update
        // runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(taskGroupLocation);
        stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsIMap.set(taskGroupLocation, stateTimestamps);
    }

    public ExecutionState getExecutionState() {
        return currExecutionState;
    }

    private void resetExecutionState() {
        synchronized (this) {
            ExecutionState executionState = getExecutionState();
            if (!executionState.isEndState()) {
                String message =
                        String.format(
                                "%s reset state failed, only end state can be reset, current is %s",
                                getTaskFullName(), executionState);
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }
            try {
                RetryUtils.retryWithException(
                        () -> {
                            updateStateTimestamps(ExecutionState.CREATED);
                            runningJobStateIMap.set(taskGroupLocation, ExecutionState.CREATED);
                            return null;
                        },
                        new RetryUtils.RetryMaterial(
                                Constant.OPERATION_RETRY_TIME,
                                true,
                                exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                                Constant.OPERATION_RETRY_SLEEP));
            } catch (Exception e) {
                LOGGER.warning(ExceptionUtils.getMessage(e));
                // If master/worker node done, The job will restore and fix the state from
                // TaskExecutionService
                LOGGER.warning(
                        String.format(
                                "Set %s state %s to Imap failed, skip.",
                                getTaskFullName(), ExecutionState.CREATED));
            }
            this.currExecutionState = ExecutionState.CREATED;
            LOGGER.info(
                    String.format("%s turn to state %s.", taskFullName, ExecutionState.CREATED));
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
        if (StringUtils.isNotEmpty(taskExecutionState.getThrowableMsg())) {
            LOGGER.severe(
                    String.format(
                            "%s end with state %s and Exception: %s",
                            this.taskFullName,
                            taskExecutionState.getExecutionState(),
                            taskExecutionState.getThrowableMsg()));
        } else {
            LOGGER.info(
                    String.format(
                            "%s end with state %s",
                            this.taskFullName, taskExecutionState.getExecutionState()));
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
