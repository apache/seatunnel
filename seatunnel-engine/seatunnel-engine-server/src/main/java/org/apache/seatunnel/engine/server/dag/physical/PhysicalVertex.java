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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.exception.TaskGroupDeployException;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.CancelTaskOperation;
import org.apache.seatunnel.engine.server.task.operation.CheckTaskGroupIsExecutingOperation;
import org.apache.seatunnel.engine.server.task.operation.DeployTaskOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel Each
 * {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some
 * PhysicalVertex. And the number of PhysicalVertex equals the {@link
 * ExecutionVertex#getParallelism()}.
 */
@Slf4j
public class PhysicalVertex {

    /**
     * Matches the exact failure message emitted when a deployed worker leaves the cluster. The
     * message may be flattened by ExceptionUtils, so it can appear as the message of an exception
     * stack-trace line instead of as a bare string.
     */
    private static final Pattern DEPLOYED_NODE_OFFLINE_ERROR_PATTERN =
            Pattern.compile(
                    "(?m)^(?:(?:Caused by: )?[\\w.$]+(?:Exception|Error): )?"
                            + "The taskGroup\\([^\\r\\n]+\\) deployed node\\([^\\r\\n]+\\) offline\\r?$");

    private final TaskGroupLocation taskGroupLocation;

    private final String taskFullName;

    private final TaskGroupDefaultImpl taskGroup;

    private final FlakeIdGenerator flakeIdGenerator;

    private final List<Set<URL>> pluginJarsUrls;

    // List<Set<URL>> pluginJarsUrls is a collection of paths stored on the engine for all connector
    // Jar
    // packages and third-party Jar packages that the connector relies on.
    // All storage paths come from the unique identifier obtained after uploading the Jar package
    // through the client.
    // Set<ConnectorJarIdentifier> represents the set of the unique identifier of a Jar package
    // file,
    // which contains more information about the Jar package file, including the name of the
    // connector plugin using the current Jar, the type of the current Jar package, and so on.
    // TODO: Only use List<Set<ConnectorJarIdentifier>>to save more information about the Jar
    // package,
    // including the storage path of the Jar package on the server.
    private final List<Set<ConnectorJarIdentifier>> connectorJarIdentifiers;

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

    private volatile ExecutionState currExecutionState;

    public volatile boolean isRunning = false;

    /** The error throw by physicalVertex, should be set when physicalVertex throw error. */
    private AtomicReference<String> errorByPhysicalVertex = new AtomicReference<>();

    public PhysicalVertex(
            int subTaskGroupIndex,
            int parallelism,
            @NonNull TaskGroupDefaultImpl taskGroup,
            @NonNull FlakeIdGenerator flakeIdGenerator,
            int pipelineId,
            int totalPipelineNum,
            List<Set<URL>> pluginJarsUrls,
            List<Set<ConnectorJarIdentifier>> connectorJarIdentifiers,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull NodeEngine nodeEngine,
            @NonNull IMap<Object, Object> runningJobStateIMap,
            @NonNull IMap<Object, Long[]> runningJobStateTimestampsIMap) {
        this.taskGroupLocation = taskGroup.getTaskGroupLocation();
        this.taskGroup = taskGroup;
        this.flakeIdGenerator = flakeIdGenerator;
        this.pluginJarsUrls = pluginJarsUrls;
        this.connectorJarIdentifiers = connectorJarIdentifiers;

        this.currExecutionState =
                DistributedStateTransition.initialize(
                        runningJobStateIMap,
                        taskGroupLocation,
                        ExecutionState.CREATED,
                        ExecutionState.class,
                        runningJobStateTimestampsIMap,
                        ExecutionState.values().length,
                        ExecutionState.INITIALIZING.ordinal(),
                        initializationTimestamp,
                        ExecutionState.CREATED.ordinal());

        this.nodeEngine = nodeEngine;
        this.taskFullName =
                String.format(
                        "Job (%s), Pipeline: [(%d/%d)], task: [%s (%d/%d)], taskGroupLocation: [%s]",
                        jobImmutableInformation.getJobId(),
                        pipelineId,
                        totalPipelineNum,
                        taskGroup.getTaskGroupName(),
                        subTaskGroupIndex + 1,
                        parallelism,
                        taskGroupLocation);

        this.taskFuture = new CompletableFuture<>();

        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
    }

    public PassiveCompletableFuture<TaskExecutionState> initStateFuture() {
        this.taskFuture = new CompletableFuture<>();
        this.currExecutionState = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
        if (currExecutionState != null) {
            log.info(
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
                updateTaskState(ExecutionState.FAILING);
            }
        } else if (ExecutionState.DEPLOYING.equals(currExecutionState)) {
            if (!checkTaskGroupIsExecuting(taskGroupLocation)) {
                updateTaskState(ExecutionState.FAILING);
            }
        }
        return new PassiveCompletableFuture<>(this.taskFuture);
    }

    public void restoreExecutionState() {
        startPhysicalVertex();
        stateProcess();
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
                log.warn(
                        "The node:{} running the taskGroup {} no longer exists, return false.",
                        worker.toString(),
                        taskGroupLocation);
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
                log.error(
                        String.format(
                                "Execution of CheckTaskGroupIsExecutingOperation %s failed, checkTaskGroupIsExecuting return false. ",
                                taskGroupLocation),
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
                            log.warn(ExceptionUtils.getMessage(e));
                            log.warn(
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

    public void makeTaskGroupDeploy() {
        updateTaskState(ExecutionState.DEPLOYING);
    }

    // This method must not throw an exception
    public TaskDeployState deploy(@NonNull SlotProfile slotProfile) {
        try {
            if (slotProfile.getWorker().equals(nodeEngine.getThisAddress())) {
                return deployOnLocal(slotProfile);
            } else {
                return deployOnRemote(slotProfile);
            }
        } catch (Throwable th) {
            return TaskDeployState.failed(th);
        }
    }

    private TaskDeployState deployInternal(
            Function<TaskGroupImmutableInformation, TaskDeployState> taskGroupConsumer) {
        TaskGroupImmutableInformation taskGroupImmutableInformation =
                getTaskGroupImmutableInformation();
        TaskDeployState state = taskGroupConsumer.apply(taskGroupImmutableInformation);
        updateTaskState(ExecutionState.RUNNING);
        return state;
    }

    @VisibleForTesting
    public TaskGroupImmutableInformation getTaskGroupImmutableInformation() {
        List<Data> tasksData =
                this.taskGroup.getTasks().stream()
                        .map(task -> (Data) nodeEngine.getSerializationService().toData(task))
                        .collect(Collectors.toList());
        return new TaskGroupImmutableInformation(
                this.taskGroup.getTaskGroupLocation().getJobId(),
                flakeIdGenerator.newId(),
                this.taskGroup.getTaskGroupType(),
                this.taskGroup.getTaskGroupLocation(),
                this.taskGroup.getTaskGroupName(),
                tasksData,
                this.pluginJarsUrls,
                this.connectorJarIdentifiers);
    }

    @VisibleForTesting
    public TaskGroup getTaskGroup() {
        return taskGroup;
    }

    public synchronized void updateTaskState(@NonNull ExecutionState targetState) {
        try {
            DistributedStateTransition.Result<ExecutionState> transitionResult =
                    RetryUtils.retryWithException(
                            () ->
                                    DistributedStateTransition.transition(
                                            runningJobStateIMap,
                                            taskGroupLocation,
                                            currExecutionState,
                                            ExecutionState.CREATED,
                                            targetState,
                                            ExecutionState.class,
                                            ExecutionState::isEndState,
                                            this::canPersistState,
                                            this::lockMissingStatePersistence,
                                            this::unlockMissingStatePersistence,
                                            runningJobStateTimestampsIMap,
                                            ExecutionState.values().length,
                                            targetState.ordinal()),
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    ExceptionUtil::isOperationNeedRetryException,
                                    Constant.OPERATION_RETRY_SLEEP));
            ExecutionState current = transitionResult.getPreviousState();
            if (transitionResult.isStateEntryMissing()) {
                log.warn(
                        "{} state entry missing from distributed map (possibly due to node "
                                + "removal during scaling down), using local state {} as fallback, "
                                + "target state: {}",
                        taskFullName,
                        current,
                        targetState);
            }
            if (transitionResult.isPersistenceBlocked()) {
                log.info(
                        "{} task state persistence is blocked by its generation fence",
                        taskFullName);
                return;
            }
            if (!transitionResult.isTransitioned()) {
                ExecutionState localState = this.currExecutionState;
                this.currExecutionState = transitionResult.getCurrentState();
                log.info(
                        "{} did not transition to {} because distributed state {} won the race",
                        taskFullName,
                        targetState,
                        transitionResult.getCurrentState());
                if (localState != transitionResult.getCurrentState()) {
                    stateProcess();
                }
                return;
            }
            this.currExecutionState = targetState;
            log.info(
                    String.format(
                            "%s turned from state %s to %s.", taskFullName, current, targetState));
            stateProcess();
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
            if (!targetState.equals(ExecutionState.FAILING)) {
                makeTaskGroupFailing(e);
            }
        }
    }

    /**
     * Checks whether this task's exact generation still owns distributed persistence.
     *
     * <p>A false result fences both ordinary transitions and terminal-state resets.
     */
    private boolean canPersistState() {
        return jobMaster == null || jobMaster.isStatePersistenceAllowed();
    }

    /**
     * Locks the cleanup fence only when a transition is about to recreate a missing state key.
     *
     * <p>Existing task-state updates remain synchronized only by their own state-key lock.
     */
    private void lockMissingStatePersistence() {
        if (jobMaster != null) {
            jobMaster.lockStatePersistenceFence();
        }
    }

    /**
     * Releases the missing-key cleanup fence after the state-key lock has been released.
     *
     * <p>This release order preserves the cleanup-to-state lock hierarchy.
     */
    private void unlockMissingStatePersistence() {
        if (jobMaster != null) {
            jobMaster.unlockStatePersistenceFence();
        }
    }

    public synchronized void cancel() {
        if (!getExecutionState().isEndState()) {
            updateTaskState(ExecutionState.CANCELING);
        }
    }

    private void noticeTaskExecutionServiceCancel() {
        // Check whether the node exists, and whether the Task on the node exists. If there is no
        // direct update state
        if (!checkTaskGroupIsExecuting(taskGroupLocation)) {
            updateTaskState(ExecutionState.CANCELED);
            return;
        }
        int i = 0;
        // In order not to generate uncontrolled tasks, We will try again until the taskFuture is
        // completed
        Address executionAddress = getCurrentExecutionAddress();
        while (!taskFuture.isDone()
                && executionAddress != null
                && nodeEngine.getClusterService().getMember(executionAddress) != null) {
            try {
                i++;
                log.info(
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
                log.warn(
                        String.format(
                                "%s cancel failed with Exception: %s, retry %s",
                                this.getTaskFullName(), ExceptionUtils.getMessage(e), i));
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            executionAddress = getCurrentExecutionAddress();
        }

        if (!taskFuture.isDone() && ExecutionState.CANCELING.equals(getExecutionState())) {
            log.warn(
                    "{} cancel did not receive a terminal callback before member {} became unavailable, mark task as CANCELED locally.",
                    taskFullName,
                    executionAddress);
            updateTaskState(ExecutionState.CANCELED);
        }
    }

    public ExecutionState getExecutionState() {
        return currExecutionState;
    }

    /**
     * Resets a terminal task without allowing an obsolete generation to recreate its keys.
     *
     * <p>The local cache advances only after the timestamp-first distributed reset succeeds.
     */
    private void resetExecutionState() {
        synchronized (this) {
            try {
                DistributedStateTransition.Result<ExecutionState> result =
                        RetryUtils.retryWithException(
                                () ->
                                        DistributedStateTransition.reset(
                                                runningJobStateIMap,
                                                taskGroupLocation,
                                                ExecutionState.CREATED,
                                                ExecutionState.class,
                                                ExecutionState::isEndState,
                                                this::canPersistState,
                                                runningJobStateTimestampsIMap,
                                                ExecutionState.values().length,
                                                ExecutionState.CREATED.ordinal()),
                                new RetryUtils.RetryMaterial(
                                        Constant.OPERATION_RETRY_TIME,
                                        true,
                                        ExceptionUtil::isOperationNeedRetryException,
                                        Constant.OPERATION_RETRY_SLEEP));
                if (result.isPersistenceBlocked()) {
                    log.info(
                            "Skip resetting {} because its generation no longer owns distributed state",
                            taskFullName);
                    return;
                }
                errorByPhysicalVertex = new AtomicReference<>();
                this.currExecutionState = result.getCurrentState();
                log.info(
                        "{} turned from state {} to {}.",
                        taskFullName,
                        result.getPreviousState(),
                        result.getCurrentState());
            } catch (Exception e) {
                log.warn(ExceptionUtils.getMessage(e));
                log.warn(
                        String.format(
                                "Reset %s state to %s in IMap failed, keep the current state.",
                                getTaskFullName(), ExecutionState.CREATED));
            }
        }
    }

    public void reset() {
        resetExecutionState();
    }

    public String getTaskFullName() {
        return taskFullName;
    }

    public void updateStateByExecutionService(TaskExecutionState taskExecutionState) {
        if (!taskExecutionState.getExecutionState().isEndState()) {
            throw new SeaTunnelEngineException(
                    String.format(
                            "The state must be end state from ExecutionService, can not be %s",
                            taskExecutionState.getExecutionState()));
        }
        errorByPhysicalVertex.compareAndSet(null, taskExecutionState.getThrowableMsg());
        updateTaskState(taskExecutionState.getExecutionState());
    }

    public synchronized void forceStop() {
        ExecutionState executionState = getExecutionState();
        if (executionState == null || executionState.isEndState()) {
            return;
        }
        noticeTaskExecutionServiceCancel();
        if (!taskFuture.isDone()) {
            updateTaskState(ExecutionState.CANCELED);
        }
    }

    public Address getCurrentExecutionAddress() {
        SlotProfile ownedSlotProfiles = jobMaster.getOwnedSlotProfiles(taskGroupLocation);
        if (ownedSlotProfiles == null) {
            return null;
        }
        return ownedSlotProfiles.getWorker();
    }

    public TaskGroupLocation getTaskGroupLocation() {
        return taskGroupLocation;
    }

    public void setJobMaster(JobMaster jobMaster) {
        this.jobMaster = jobMaster;
    }

    public void startPhysicalVertex() {
        isRunning = true;
        log.info(String.format("%s state process is start", taskFullName));
    }

    public void stopPhysicalVertex() {
        isRunning = false;
        log.info(String.format("%s state process is stopped", taskFullName));
    }

    public synchronized void stateProcess() {
        if (!isRunning) {
            log.warn(String.format("%s state process is not start", taskFullName));
            return;
        }
        switch (getExecutionState()) {
            case INITIALIZING:
            case CREATED:
            case RUNNING:
                break;
            case DEPLOYING:
                TaskDeployState deployState =
                        deploy(jobMaster.getOwnedSlotProfiles(taskGroupLocation));
                if (!deployState.isSuccess()) {
                    makeTaskGroupFailing(
                            new TaskGroupDeployException(deployState.getThrowableMsg()));
                } else {
                    updateTaskState(ExecutionState.RUNNING);
                }
                break;
            case FAILING:
                updateTaskState(ExecutionState.FAILED);
                break;
            case CANCELING:
                noticeTaskExecutionServiceCancel();
                break;
            case CANCELED:
                stopPhysicalVertex();
                taskFuture.complete(
                        new TaskExecutionState(
                                taskGroupLocation,
                                ExecutionState.CANCELED,
                                errorByPhysicalVertex.get()));
                return;
            case FAILED:
                stopPhysicalVertex();
                String errorMsg = errorByPhysicalVertex.get();
                if (isDeployedNodeOfflineFailure(errorMsg)) {
                    log.warn(
                            String.format(
                                    "%s end with state %s due to node offline: %s",
                                    this.taskFullName, ExecutionState.FAILED, errorMsg));
                } else {
                    log.error(
                            String.format(
                                    "%s end with state %s and Exception: %s",
                                    this.taskFullName, ExecutionState.FAILED, errorMsg));
                }
                taskFuture.complete(
                        new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED, errorMsg));
                return;
            case FINISHED:
                stopPhysicalVertex();
                taskFuture.complete(
                        new TaskExecutionState(
                                taskGroupLocation,
                                ExecutionState.FINISHED,
                                errorByPhysicalVertex.get()));
                return;
            default:
                throw new IllegalArgumentException(
                        "Unknown TaskGroup State: " + getExecutionState());
        }
    }

    public void makeTaskGroupFailing(Throwable err) {
        errorByPhysicalVertex.compareAndSet(null, ExceptionUtils.getMessage(err));
        updateTaskState(ExecutionState.FAILING);
    }

    /**
     * Uses the coordinator's exact offline-node message template so only expected scale-down
     * failures are downgraded to warn logs.
     */
    @VisibleForTesting
    static boolean isDeployedNodeOfflineFailure(String errorMsg) {
        return errorMsg != null && DEPLOYED_NODE_OFFLINE_ERROR_PATTERN.matcher(errorMsg).find();
    }
}
