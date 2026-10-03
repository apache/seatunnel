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
import org.apache.seatunnel.engine.server.CoordinatorService;
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
import java.util.stream.Collectors;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel Each
 * {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some
 * PhysicalVertex. And the number of PhysicalVertex equals the {@link
 * ExecutionVertex#getParallelism()}.
 */
@Slf4j
public class PhysicalVertex {

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

    /**
     * The failure recorded for this physical vertex, installed first-write-wins. The failure
     * message and its coordinator-classified graceful member-removal flag are kept in one immutable
     * holder so they are always written and read as a single unit: writing them as two independent
     * atomics allowed a concurrent caller to re-tag another caller's already-recorded failure with
     * its own classification, logging a genuine failure at the wrong level.
     */
    private AtomicReference<FailureClassification> failureClassificationByPhysicalVertex =
            new AtomicReference<>();

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
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap) {
        this.taskGroupLocation = taskGroup.getTaskGroupLocation();
        this.taskGroup = taskGroup;
        this.flakeIdGenerator = flakeIdGenerator;
        this.pluginJarsUrls = pluginJarsUrls;
        this.connectorJarIdentifiers = connectorJarIdentifiers;

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

    /**
     * Checks whether a task group believed to be running is still executing on its assigned worker
     * during master-failover restoration. When that worker has already left the cluster, the
     * failure is classified here, before any membership callback runs, using the same marker rules
     * as {@link CoordinatorService#failedTaskOnMemberRemoved}. Package-visible so the restore-path
     * classification can be exercised directly by tests.
     */
    @VisibleForTesting
    boolean checkTaskGroupIsExecuting(TaskGroupLocation taskGroupLocation) {
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
                recordMemberRemovedFailure(
                        failureClassificationByPhysicalVertex,
                        taskGroupLocation,
                        worker,
                        getGracefulMemberRemovalMarker(worker),
                        System.currentTimeMillis());
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

    /**
     * Reads the one-time graceful-removal marker while restoring a task whose worker has already
     * left the cluster. This recovery path may run before the new master receives its membership
     * callback, so it must preserve the same classification as the callback path.
     */
    private Long getGracefulMemberRemovalMarker(Address lostAddress) {
        try {
            return nodeEngine
                    .getHazelcastInstance()
                    .<Address, Long>getMap(Constant.IMAP_GRACEFUL_MEMBER_REMOVAL)
                    .get(lostAddress);
        } catch (Exception e) {
            log.debug("Unable to read graceful member-removal marker for {}", lostAddress, e);
            return null;
        }
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
            ExecutionState current = (ExecutionState) runningJobStateIMap.get(taskGroupLocation);
            if (current == null) {
                log.warn(
                        "{} current state is null, skip transition to {}. Task execution location: {}",
                        taskFullName,
                        targetState,
                        taskGroupLocation);
                return;
            }
            log.debug(
                    String.format(
                            "Try to update the task %s state from %s to %s",
                            taskFullName, current, targetState));

            if (current.equals(targetState)) {
                log.info(
                        "{} current state equals target state: {}, skip",
                        taskFullName,
                        targetState);
                return;
            }

            // consistency check
            if (current.isEndState()) {
                String message = "Task is trying to leave terminal state " + current;
                log.error(message);
                return;
            }

            // now do the actual state transition
            RetryUtils.retryWithException(
                    () -> {
                        updateStateTimestamps(targetState);
                        if (runningJobStateIMap.get(taskGroupLocation) != null) {
                            runningJobStateIMap.set(taskGroupLocation, targetState);
                        }
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            ExceptionUtil::isOperationNeedRetryException,
                            Constant.OPERATION_RETRY_SLEEP));
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

    private void updateStateTimestamps(@NonNull ExecutionState targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update
        // runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(taskGroupLocation);
        if (stateTimestamps == null) {
            log.warn(
                    "{} state timestamps have already been cleaned, skip persisting transition to {}",
                    taskFullName,
                    targetState);
            return;
        }
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
                log.error(message);
                throw new IllegalStateException(message);
            }
            try {
                RetryUtils.retryWithException(
                        () -> {
                            updateStateTimestamps(ExecutionState.CREATED);
                            runningJobStateIMap.set(taskGroupLocation, ExecutionState.CREATED);
                            // reset the recorded failure classification
                            failureClassificationByPhysicalVertex = new AtomicReference<>();
                            return null;
                        },
                        new RetryUtils.RetryMaterial(
                                Constant.OPERATION_RETRY_TIME,
                                true,
                                ExceptionUtil::isOperationNeedRetryException,
                                Constant.OPERATION_RETRY_SLEEP));
            } catch (Exception e) {
                log.warn(ExceptionUtils.getMessage(e));
                // If master/worker node done, The job will restore and fix the state from
                // TaskExecutionService
                log.warn(
                        String.format(
                                "Set %s state %s to Imap failed, skip.",
                                getTaskFullName(), ExecutionState.CREATED));
            }
            this.currExecutionState = ExecutionState.CREATED;
            log.info(String.format("%s turn to state %s.", taskFullName, ExecutionState.CREATED));
        }
    }

    public void reset() {
        resetExecutionState();
    }

    public String getTaskFullName() {
        return taskFullName;
    }

    public void updateStateByExecutionService(TaskExecutionState taskExecutionState) {
        updateStateByExecutionService(taskExecutionState, false);
    }

    /**
     * Receives a coordinator-classified graceful member-removal flag without changing the task
     * state's serialized payload.
     */
    public void updateStateByExecutionService(
            TaskExecutionState taskExecutionState, boolean gracefulMemberRemovalFailure) {
        if (!taskExecutionState.getExecutionState().isEndState()) {
            throw new SeaTunnelEngineException(
                    String.format(
                            "The state must be end state from ExecutionService, can not be %s",
                            taskExecutionState.getExecutionState()));
        }
        recordFailureClassification(
                failureClassificationByPhysicalVertex,
                taskExecutionState.getThrowableMsg(),
                gracefulMemberRemovalFailure);
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
                                getRecordedFailureMessage()));
                return;
            case FAILED:
                stopPhysicalVertex();
                // Read the recorded failure once so the message and its graceful classification
                // always come from the same write and can never be observed as a torn pair.
                FailureClassification failureClassification =
                        failureClassificationByPhysicalVertex.get();
                String errorMsg =
                        failureClassification == null
                                ? null
                                : failureClassification.getErrorMessage();
                boolean gracefulMemberRemovalFailure =
                        failureClassification != null
                                && failureClassification.isGracefulMemberRemovalFailure();
                if (shouldLogFailureAsWarn(gracefulMemberRemovalFailure)) {
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
                                getRecordedFailureMessage()));
                return;
            default:
                throw new IllegalArgumentException(
                        "Unknown TaskGroup State: " + getExecutionState());
        }
    }

    public void makeTaskGroupFailing(Throwable err) {
        recordFailureClassification(
                failureClassificationByPhysicalVertex, ExceptionUtils.getMessage(err), false);
        updateTaskState(ExecutionState.FAILING);
    }

    /**
     * Returns the message of the recorded failure classification, or {@code null} when no failure
     * has been recorded for this physical vertex.
     */
    private String getRecordedFailureMessage() {
        FailureClassification failureClassification = failureClassificationByPhysicalVertex.get();
        return failureClassification == null ? null : failureClassification.getErrorMessage();
    }

    /**
     * Only coordinator-classified graceful member removals should be downgraded to warn logs. Every
     * other failure keeps the pre-existing error level, so an unproven departure (crash, kill,
     * partition) is never hidden as routine scale-down noise.
     */
    @VisibleForTesting
    static boolean shouldLogFailureAsWarn(boolean gracefulMemberRemovalFailure) {
        return gracefulMemberRemovalFailure;
    }

    /**
     * Installs a failure message and its graceful member-removal classification into the slot as
     * one atomic, first-write-wins unit. Later callers never override an already-recorded failure,
     * so a concurrent node-offline classification can not re-tag a genuine failure as graceful (or
     * strip the graceful flag from a recorded offline failure). A {@code null} message never claims
     * the slot, preserving the pre-existing first-write-wins behavior where a message-less caller
     * left the slot claimable by a later caller that actually carries a failure message.
     */
    @VisibleForTesting
    static void recordFailureClassification(
            AtomicReference<FailureClassification> failureClassificationSlot,
            String errorMessage,
            boolean gracefulMemberRemovalFailure) {
        if (errorMessage == null) {
            return;
        }
        failureClassificationSlot.compareAndSet(
                null, new FailureClassification(errorMessage, gracefulMemberRemovalFailure));
    }

    /**
     * Records the same structured member-removal failure emitted after a membership callback. It is
     * used by the master failover recovery path, where a task can be found on a worker that has
     * already left before the callback is processed. The message and the TTL rule are deliberately
     * not re-implemented here: both are delegated to the coordinator's shared helpers so this
     * second consumer of the graceful-removal marker can never drift from the callback path.
     */
    @VisibleForTesting
    static void recordMemberRemovedFailure(
            AtomicReference<FailureClassification> failureClassificationSlot,
            TaskGroupLocation taskGroupLocation,
            Address lostAddress,
            Long markedAt,
            long nowMillis) {
        recordFailureClassification(
                failureClassificationSlot,
                CoordinatorService.buildMemberRemovedOfflineMessage(taskGroupLocation, lostAddress),
                CoordinatorService.isGracefulMemberRemovalMarkerValid(markedAt, nowMillis));
    }

    /**
     * Immutable pairing of a failure message and the coordinator-classified graceful member-removal
     * flag. The pair must always travel together: keeping them in two independent fields with
     * mismatched write discipline (first-write-wins message, last-write-wins flag) allowed a
     * genuine concurrent failure to inherit another caller's classification and be logged at the
     * wrong level.
     */
    @VisibleForTesting
    static final class FailureClassification {

        /**
         * The failure message recorded for this physical vertex. It is stored next to its
         * classification in this immutable holder so a reader can never pair the message of one
         * write with the graceful flag of another.
         */
        private final String errorMessage;

        /**
         * Whether the coordinator classified the recorded failure as caused by a graceful member
         * removal, which is the only case that downgrades the failure log level to warn.
         */
        private final boolean gracefulMemberRemovalFailure;

        private FailureClassification(String errorMessage, boolean gracefulMemberRemovalFailure) {
            this.errorMessage = errorMessage;
            this.gracefulMemberRemovalFailure = gracefulMemberRemovalFailure;
        }

        String getErrorMessage() {
            return errorMessage;
        }

        boolean isGracefulMemberRemovalFailure() {
            return gracefulMemberRemovalFailure;
        }
    }
}
