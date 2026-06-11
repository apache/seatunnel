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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.Checkpoint;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointEndOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.NotifyTaskRestoreOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.NotifyTaskStartOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneakyThrow;
import static org.apache.seatunnel.engine.core.checkpoint.CheckpointType.CHECKPOINT_TYPE;
import static org.apache.seatunnel.engine.core.checkpoint.CheckpointType.SAVEPOINT_TYPE;
import static org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan.COORDINATOR_INDEX;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;

/**
 * Used to coordinate all checkpoints of a pipeline.
 *
 * <p>Generate and coordinate {@link Checkpoint} with a checkpoint plan
 */
public class CheckpointCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final long jobId;

    private final int pipelineId;

    private final CheckpointManager checkpointManager;

    private final CheckpointStorage checkpointStorage;

    @Getter private final CheckpointIDCounter checkpointIdCounter;

    private final transient Serializer serializer;

    /**
     * All tasks in this pipeline. <br>
     * key: the task id; <br>
     * value: the parallelism of the task;
     */
    private final Map<Long, Integer> pipelineTasks;

    private final Map<Long, SeaTunnelTaskState> pipelineTaskStatus;

    private final CheckpointPlan plan;

    /**
     * Tracks source (starting) subtasks that have finished emitting data and are ready to close,
     * but are waiting for the final checkpoint barrier before actual termination.
     *
     * <p>This collection is used to determine when all starting subtasks have reached a safe close
     * point so that a final checkpoint can be triggered.
     */
    private final Set<TaskLocation> readyToCloseStartingTask;

    /**
     * Tracks source subtasks that are currently in an idle (no-data) state and have indicated
     * readiness to close.
     *
     * <p>Idle tasks may not actively emit records but must still participate in checkpoint
     * coordination to ensure consistent termination semantics.
     */
    private final Set<TaskLocation> readyToCloseIdleTask;

    /**
     * Tracks idle subtasks that have fully completed their close procedure.
     *
     * <p>This collection is used as a coordination barrier to ensure all idle tasks are properly
     * shut down before the checkpoint coordinator transitions to a completed or suspended state.
     */
    @Getter private final Set<TaskLocation> closedIdleTask;

    private final ConcurrentHashMap<Long, PendingCheckpoint> pendingCheckpoints;

    private final ArrayDeque<String> completedCheckpointIds;

    private volatile CompletedCheckpoint latestCompletedCheckpoint = null;

    private final CheckpointConfig coordinatorConfig;

    private transient ScheduledExecutorService scheduler;

    private final AtomicLong latestTriggerTimestamp = new AtomicLong(0);

    private final AtomicInteger pendingCounter = new AtomicInteger(0);

    /**
     * Indicates whether a schema change operation (e.g., DDL event) is currently in progress.
     *
     * <p>When set to {@code true}, normal periodic checkpoint triggering is temporarily suspended
     * to ensure a dedicated schema-change checkpoint is completed before applying structural
     * modifications.
     */
    private final AtomicBoolean schemaChanging = new AtomicBoolean(false);

    private final Object lock = new Object();

    /** Flag marking the coordinator as shut down (not accepting any messages anymore). */
    private volatile boolean shutdown;

    /**
     * Marks whether all pipeline subtasks have reached the READY state.
     *
     * <p>This flag is set exactly once during a checkpoint cycle to prevent duplicate invocations
     * of the task-start notification logic and redundant checkpoint scheduling.
     */
    private final AtomicBoolean isAllTaskReady = new AtomicBoolean(false);

    private final ExecutorService executorService;

    private CompletableFuture<CheckpointCoordinatorState> checkpointCoordinatorFuture;

    private AtomicReference<String> errorByPhysicalVertex = new AtomicReference<>();

    private final IMap<Object, Object> runningJobStateIMap;

    private final CheckpointMonitorService checkpointMonitorService;

    // save pending checkpoint for savepoint, to make sure the different savepoint request can be
    // processed with one savepoint operation in the same time.
    private PendingCheckpoint savepointPendingCheckpoint;

    private final String checkpointStateImapKey;

    private final String readyToCloseImapKey;

    @SneakyThrows
    public CheckpointCoordinator(
            CheckpointManager manager,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig,
            long jobId,
            CheckpointPlan plan,
            CheckpointIDCounter checkpointIdCounter,
            PipelineState pipelineState,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            boolean isStartWithSavePoint,
            CheckpointMonitorService checkpointMonitorService) {

        this.executorService = executorService;
        this.checkpointManager = manager;
        this.checkpointStorage = checkpointStorage;
        this.jobId = jobId;
        this.pipelineId = plan.getPipelineId();
        this.checkpointStateImapKey = "checkpoint_state_" + jobId + "_" + pipelineId;
        this.readyToCloseImapKey = checkpointStateImapKey + "_ready_to_close";
        this.runningJobStateIMap = runningJobStateIMap;
        this.plan = plan;
        this.coordinatorConfig = checkpointConfig;
        this.checkpointMonitorService = checkpointMonitorService;
        this.pendingCheckpoints = new ConcurrentHashMap<>();
        this.completedCheckpointIds =
                new ArrayDeque<>(coordinatorConfig.getStorage().getMaxRetainedCheckpoints() + 1);
        this.scheduler =
                Executors.newScheduledThreadPool(
                        2,
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setName(
                                    String.format(
                                            "checkpoint-coordinator-%s/%s", pipelineId, jobId));
                            return thread;
                        });
        ((ScheduledThreadPoolExecutor) this.scheduler).setRemoveOnCancelPolicy(true);
        this.scheduler = MDCTracer.tracing(scheduler);
        this.serializer = new ProtoStuffSerializer();
        this.pipelineTasks = getPipelineTasks(plan.getPipelineSubtasks());
        this.pipelineTaskStatus = new ConcurrentHashMap<>();
        this.checkpointIdCounter = checkpointIdCounter;
        this.readyToCloseStartingTask = new CopyOnWriteArraySet<>();
        this.readyToCloseIdleTask = new CopyOnWriteArraySet<>();
        this.closedIdleTask = new CopyOnWriteArraySet<>();

        LOG.info(
                "Create CheckpointCoordinator, job id: {}, pipeline id: {}, plan: {}",
                jobId,
                pipelineId,
                plan);
        if (pipelineState != null) {
            this.latestCompletedCheckpoint =
                    serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);
            this.latestCompletedCheckpoint.setRestored(true);
            LOG.info(
                    "Restore checkpoint, job id: {}, pipeline id: {}, checkpoint id: {}, data: {} ",
                    jobId,
                    pipelineId,
                    latestCompletedCheckpoint.getCheckpointId(),
                    latestCompletedCheckpoint);
        }
        this.checkpointCoordinatorFuture = new CompletableFuture();

        // For job restore from master node active switch
        CheckpointCoordinatorStatus checkpointCoordinatorStatus =
                (CheckpointCoordinatorStatus) runningJobStateIMap.get(checkpointStateImapKey);

        // This is not a new job
        if (isStartWithSavePoint) {
            updateStatus(CheckpointCoordinatorStatus.RUNNING);
            return;
        }

        // If checkpointCoordinatorStatus is not null it means this CheckpointCoordinator is created
        // by job restore from master node active switch
        if (checkpointCoordinatorStatus != null) {
            if (checkpointCoordinatorStatus.isEndState()) {
                this.checkpointCoordinatorFuture.complete(
                        new CheckpointCoordinatorState(checkpointCoordinatorStatus, null));
            } else {
                updateStatus(CheckpointCoordinatorStatus.RUNNING);
            }
        }
    }

    public int getPipelineId() {
        return pipelineId;
    }

    // --------------------------------------------------------------------------------------------
    // The start step of the coordinator
    // --------------------------------------------------------------------------------------------
    /**
     * Entry point for handling task status reports sent by running tasks.
     *
     * <p>This method updates the internal task status map and triggers the appropriate state
     * transition in the checkpoint lifecycle.
     *
     * <p>Status mappings:
     *
     * <ul>
     *   <li>{@code WAITING_RESTORE} → invokes {@link #restoreTaskState(TaskLocation)}
     *   <li>{@code READY_START} → invokes {@link #allTaskReady()}
     * </ul>
     *
     * @param operation the task status report containing task location and status
     */
    protected void reportedTask(TaskReportStatusOperation operation) {
        pipelineTaskStatus.put(operation.getLocation().getTaskID(), operation.getStatus());
        CompletableFuture.runAsync(
                        () -> {
                            switch (operation.getStatus()) {
                                case WAITING_RESTORE:
                                    restoreTaskState(operation.getLocation());
                                    break;
                                case READY_START:
                                    allTaskReady();
                                    break;
                                default:
                                    break;
                            }
                        },
                        executorService)
                .exceptionally(
                        error -> {
                            handleCoordinatorError(
                                    "task running failed",
                                    error,
                                    CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
                            return null;
                        });
    }

    @VisibleForTesting
    public void handleCoordinatorError(String message, Throwable e, CheckpointCloseReason reason) {
        LOG.error(message, e);
        handleCoordinatorError(reason, e);
    }

    private void handleCoordinatorError(CheckpointCloseReason reason, Throwable e) {
        CheckpointException checkpointException = new CheckpointException(reason, e);
        errorByPhysicalVertex.compareAndSet(null, ExceptionUtils.getMessage(checkpointException));

        if (checkpointCoordinatorFuture.isDone()) {
            return;
        }
        updateStatus(CheckpointCoordinatorStatus.FAILED);
        checkpointCoordinatorFuture.complete(
                new CheckpointCoordinatorState(
                        CheckpointCoordinatorStatus.FAILED, errorByPhysicalVertex.get()));
        checkpointManager.handleCheckpointError(pipelineId, false);
        // we should wait the checkpoint manager handle the error to cancel other task by use
        // checkpoint coordinator thread pool. So we killed the thread pool at the end of this
        // method to avoid the thread be interrupted before handle checkpoint error finished.
        cleanPendingCheckpoint(reason);
    }

    /**
     * Restores the execution state of the specified task from the latest successfully completed
     * checkpoint.
     *
     * <p>This method reconstructs the relevant {@link ActionSubtaskState} instances based on the
     * saved {@link ActionState} information. It supports both coordinator-level and subtask-level
     * state recovery.
     *
     * <p>The restoration respects the current task parallelism and ensures that only relevant
     * subtask states are reassigned to the recovering task.
     *
     * <p>If no checkpoint is available or a corresponding action state cannot be found, the method
     * safely skips restoration for that entry.
     *
     * @param taskLocation identifies the task whose state should be restored
     */
    private void restoreTaskState(TaskLocation taskLocation) {
        List<ActionSubtaskState> states = new ArrayList<>();
        if (latestCompletedCheckpoint != null) {
            if (!latestCompletedCheckpoint.isRestored()) {
                latestCompletedCheckpoint.setRestored(true);
            }
            final Integer currentParallelism = pipelineTasks.get(taskLocation.getTaskVertexId());
            plan.getSubtaskActions()
                    .get(taskLocation)
                    .forEach(
                            tuple -> {
                                ActionState actionState =
                                        latestCompletedCheckpoint.getTaskStates().get(tuple.f0());
                                if (actionState == null) {
                                    LOG.info(
                                            "Not found task({}) state for key({})",
                                            taskLocation,
                                            tuple.f0());
                                    return;
                                }
                                if (COORDINATOR_INDEX.equals(tuple.f1())) {
                                    states.add(actionState.getCoordinatorState());
                                    return;
                                }
                                for (int i = tuple.f1();
                                        i < actionState.getParallelism();
                                        i += currentParallelism) {
                                    ActionSubtaskState subtaskState =
                                            actionState.getSubtaskStates().get(i);
                                    if (subtaskState != null) {
                                        states.add(subtaskState);
                                    }
                                }
                            });
        }
        checkpointManager
                .sendOperationToMemberNode(new NotifyTaskRestoreOperation(taskLocation, states))
                .join();
    }

    /**
     * Verifies whether all pipeline tasks are ready to start and triggers the next phase of
     * execution if conditions are satisfied.
     *
     * <p>This method checks that:
     *
     * <ul>
     *   <li>All subtasks have reported their status
     *   <li>All reported statuses are {@code READY_START}
     * </ul>
     *
     * <p>If all tasks are ready, it performs the following actions:
     *
     * <ul>
     *   <li>Ensures the operation executes only once using an atomic guard
     *   <li>Notifies all tasks to start execution
     *   <li>Marks the latest completed checkpoint as completed
     *   <li>Schedules periodic checkpoint triggering if checkpointing is enabled
     * </ul>
     *
     * <p>This method plays a central role in the checkpoint coordinator's state machine by
     * synchronizing the transition from task initialization to active execution.
     */
    private void allTaskReady() {
        if (pipelineTaskStatus.size() != plan.getPipelineSubtasks().size()) {
            return;
        }
        for (SeaTunnelTaskState status : pipelineTaskStatus.values()) {
            if (READY_START != status) {
                return;
            }
        }
        if (!isAllTaskReady.compareAndSet(false, true)) {
            LOG.info("all task already ready, skip notify task start");
            return;
        }
        InvocationFuture<?>[] futures = notifyTaskStart();
        CompletableFuture.allOf(futures).join();
        if (!notifyCompleted(latestCompletedCheckpoint)) {
            return;
        }
        if (coordinatorConfig.isCheckpointEnable()) {
            LOG.info("checkpoint is enabled, start schedule trigger pending checkpoint.");
            scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
        } else {
            LOG.info(
                    "checkpoint is disabled, because in batch mode and 'checkpoint.interval' of env is missing.");
        }
    }

    @VisibleForTesting
    protected boolean notifyCompleted(CompletedCheckpoint completedCheckpoint) {
        if (completedCheckpoint != null) {
            try {
                LOG.info(
                        "start notify checkpoint completed, job id: {}, pipeline id: {}, checkpoint id: {}.",
                        completedCheckpoint.getJobId(),
                        completedCheckpoint.getPipelineId(),
                        completedCheckpoint.getCheckpointId());
                InvocationFuture<?>[] invocationFutures =
                        notifyCheckpointCompleted(completedCheckpoint);
                CompletableFuture.allOf(invocationFutures).join();
                // Execution to this point means that all notifyCheckpointCompleted have been
                // completed
                InvocationFuture<?>[] invocationFuturesForEnd =
                        notifyCheckpointEnd(completedCheckpoint);
                CompletableFuture.allOf(invocationFuturesForEnd).join();
            } catch (Throwable e) {
                handleCoordinatorError(
                        "notify checkpoint completed failed",
                        e,
                        CheckpointCloseReason.CHECKPOINT_NOTIFY_COMPLETE_FAILED);
                return false;
            }
        }
        return true;
    }

    /**
     * Sends a start notification to all pipeline subtasks.
     *
     * <p>This method iterates over all subtasks defined in the execution plan and creates a {@code
     * NotifyTaskStartOperation} for each one. The operation is then dispatched to the corresponding
     * member node via the {@code checkpointManager}.
     *
     * <p>The start notification is sent asynchronously, and an array of {@code InvocationFuture}
     * objects is returned. Each future represents the remote invocation result for a specific
     * subtask.
     *
     * <p>The caller may wait for all returned futures to complete (for example, using {@code
     * CompletableFuture.allOf(...)}) in order to ensure that all subtasks have successfully started
     * before proceeding with further actions such as triggering checkpoints.
     *
     * @return an array of {@code InvocationFuture} instances corresponding to the asynchronous
     *     start operations for each pipeline subtask
     */
    public InvocationFuture<?>[] notifyTaskStart() {
        return plan.getPipelineSubtasks().stream()
                .map(NotifyTaskStartOperation::new)
                .map(checkpointManager::sendOperationToMemberNode)
                .toArray(InvocationFuture[]::new);
    }

    public void reportCheckpointErrorFromTask(String errorMsg) {
        handleCoordinatorError(
                "report error from task",
                new SeaTunnelException(errorMsg),
                CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
    }

    private void scheduleTriggerPendingCheckpoint(long delayMills) {
        scheduleTriggerPendingCheckpoint(CHECKPOINT_TYPE, delayMills);
    }

    @VisibleForTesting
    protected void scheduleTriggerPendingCheckpoint(
            CheckpointType checkpointType, long delayMills) {
        scheduler.schedule(
                () -> tryTriggerPendingCheckpoint(checkpointType),
                delayMills,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Marks a starting task as ready to close and checks whether a final checkpoint should be
     * triggered.
     *
     * <p>This method is invoked when a starting subtask reaches a state where it can be safely
     * closed. The task location is recorded in the {@code readyToCloseStartingTask} set. Once all
     * starting subtasks defined in the execution plan have reported readiness, a final checkpoint
     * of type {@code COMPLETED_POINT_TYPE} is triggered.
     *
     * <p>The final checkpoint ensures that all state is fully persisted before the job transitions
     * to a terminal state, providing consistency and fault tolerance guarantees.
     *
     * @param taskLocation the location metadata of the task that is ready to close
     */
    protected void readyToClose(TaskLocation taskLocation) {
        readyToCloseStartingTask.add(taskLocation);
        updateReadyToCloseStartingTask();
        if (readyToCloseStartingTask.size() == plan.getStartingSubtasks().size()) {
            tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
        }
    }

    private Set<TaskLocation> loadReadyToCloseStartingTask() {
        try {
            Object stored = runningJobStateIMap.get(readyToCloseImapKey);
            if (stored instanceof Set) {
                Set<TaskLocation> result = (Set<TaskLocation>) stored;
                LOG.info(
                        "Loaded readyToCloseStartingTask from IMap, job id: {}, pipeline id: {}, value: {}",
                        jobId,
                        pipelineId,
                        result);
                return result;
            }
            return null;
        } catch (Exception e) {
            LOG.error(
                    "Failed to load readyToCloseStartingTask from IMap, job id: {}, pipeline id: {}.",
                    jobId,
                    pipelineId,
                    e);
            throw new RuntimeException(
                    "Failed to load readyToCloseStartingTask from IMap, key: "
                            + readyToCloseImapKey,
                    e);
        }
    }

    private void updateReadyToCloseStartingTask() {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        runningJobStateIMap.compute(
                                readyToCloseImapKey,
                                (k, exist) -> {
                                    Set<TaskLocation> merged =
                                            exist instanceof Set
                                                    ? new HashSet<>((Set<TaskLocation>) exist)
                                                    : new HashSet<>();
                                    merged.addAll(readyToCloseStartingTask);
                                    return merged;
                                });
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            ExceptionUtil::isOperationNeedRetryException,
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOG.error(
                    "Failed to persist readyToCloseStartingTask to IMap after retries, key: {}."
                            + " Failing the job to avoid an unrecoverable stuck state on master failover.",
                    readyToCloseImapKey,
                    e);
            throw new RuntimeException(
                    "Failed to persist readyToCloseStartingTask to IMap, key: "
                            + readyToCloseImapKey,
                    e);
        }
    }

    protected void readyToCloseIdleTask(TaskLocation taskLocation) {
        if (plan.getStartingSubtasks().contains(taskLocation)) {
            throw new UnsupportedOperationException("Unsupported close starting task");
        }

        LOG.info(
                "Received close idle task, task id: {}, pipeline id: {}, job id: {}, detail: {}",
                taskLocation.getTaskID(),
                taskLocation.getPipelineId(),
                taskLocation.getJobId(),
                taskLocation);
        synchronized (readyToCloseIdleTask) {
            if (readyToCloseIdleTask.contains(taskLocation)
                    || closedIdleTask.contains(taskLocation)) {
                LOG.warn(
                        "task already in closed, task id: {}, pipeline id: {}, job id: {}, detail: {}",
                        taskLocation.getTaskID(),
                        taskLocation.getPipelineId(),
                        taskLocation.getJobId(),
                        taskLocation);
                return;
            }

            List<TaskLocation> subTaskList = new ArrayList<>();
            for (TaskLocation subTask : plan.getPipelineSubtasks()) {
                if (subTask.getTaskGroupLocation().equals(taskLocation.getTaskGroupLocation())) {
                    // close all subtask in the same task group
                    subTaskList.add(subTask);
                    LOG.info(
                            "Add task to prepare close list, task id: {}, pipeline id: {}, job id: {}",
                            subTask.getTaskID(),
                            subTask.getPipelineId(),
                            subTask.getJobId());
                }
            }
            readyToCloseIdleTask.addAll(subTaskList);
        }
    }

    protected void completedCloseIdleTask(TaskLocation taskLocation) {
        synchronized (readyToCloseIdleTask) {
            if (readyToCloseIdleTask.contains(taskLocation)) {
                readyToCloseIdleTask.remove(taskLocation);
                closedIdleTask.add(taskLocation);
                LOG.info(
                        "Completed close task, task id: {}, pipeline id: {}, job id: {}",
                        taskLocation.getTaskID(),
                        taskLocation.getPipelineId(),
                        taskLocation.getJobId());
            }
        }
    }

    protected void restoreCoordinator(boolean alreadyStarted) {
        LOG.info("received restore CheckpointCoordinator with alreadyStarted: {}", alreadyStarted);
        errorByPhysicalVertex = new AtomicReference<>();
        checkpointCoordinatorFuture = new CompletableFuture<>();
        updateStatus(CheckpointCoordinatorStatus.RUNNING);

        Set<TaskLocation> restoredReadyToClose = loadReadyToCloseStartingTask();

        cleanPendingCheckpoint(CheckpointCloseReason.CHECKPOINT_COORDINATOR_RESET);
        shutdown = false;

        if (restoredReadyToClose != null && !restoredReadyToClose.isEmpty()) {
            readyToCloseStartingTask.addAll(restoredReadyToClose);
            LOG.info(
                    "Restored readyToCloseStartingTask, restored count: {}, "
                            + "total starting subtasks: {}, job id: {}, pipeline id: {}",
                    readyToCloseStartingTask.size(),
                    plan.getStartingSubtasks().size(),
                    jobId,
                    pipelineId);
        }

        if (alreadyStarted) {
            isAllTaskReady.set(true);
            if (!notifyCompleted(latestCompletedCheckpoint)) {
                return;
            }
            if (readyToCloseStartingTask.size() == plan.getStartingSubtasks().size()) {
                // All sources already finished before failover; complete the job now.
                tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
            } else {
                tryTriggerPendingCheckpoint(CHECKPOINT_TYPE);
            }
        } else {
            isAllTaskReady.set(false);
        }
    }
    /**
     * Attempts to trigger a pending checkpoint based on the given checkpoint type.
     *
     * <p>This method enforces several preconditions before initiating a checkpoint:
     *
     * <ul>
     *   <li>The current thread must not be interrupted
     *   <li>All pipeline tasks must be in READY state
     *   <li>The configured checkpoint interval must have elapsed (except for final or schema change
     *       checkpoints)
     * </ul>
     *
     * <p>If the minimum interval has not yet passed, the checkpoint trigger will be rescheduled for
     * the remaining delay time.
     *
     * <p>This mechanism ensures stable and controlled checkpoint scheduling, preventing excessive
     * checkpoint triggering while maintaining data consistency.
     *
     * @param checkpointType the type of checkpoint to trigger, which determines whether interval
     *     constraints should be applied
     */
    protected void tryTriggerPendingCheckpoint(CheckpointType checkpointType) {
        if (Thread.currentThread().isInterrupted()) {
            LOG.warn("currentThread already be interrupted, skip trigger checkpoint");
            return;
        }
        final long currentTimestamp = Instant.now().toEpochMilli();
        if (checkpointType.notFinalCheckpoint() && checkpointType.notSchemaChangeCheckpoint()) {
            if (!isAllTaskReady.get()) {
                LOG.info("Not all tasks are ready, skipping checkpoint trigger");
                return;
            }
            long interval = currentTimestamp - latestTriggerTimestamp.get();
            if (interval <= 0) {
                LOG.error(
                        "The time on your server may not be incremental which can lead checkpoint to stop. "
                                + "The latestTriggerTimestamp: ({}), but the currentTimestamp: ({})",
                        latestTriggerTimestamp.get(),
                        currentTimestamp);
            }
            if (interval < coordinatorConfig.getCheckpointInterval()) {
                LOG.info(
                        "skip trigger checkpoint "
                                + "because the last trigger timestamp is ({}) and current timestamp is ({}), "
                                + "the interval is less than config.",
                        latestTriggerTimestamp.get(),
                        currentTimestamp);
                scheduleTriggerPendingCheckpoint(
                        checkpointType, coordinatorConfig.getCheckpointInterval() - interval);
                return;
            }

            if (latestCompletedCheckpoint != null
                    && coordinatorConfig.getCheckpointMinPause() != -1) {
                long lastCompletedTime = latestCompletedCheckpoint.getCompletedTimestamp();
                long timeSinceLastCompleted = currentTimestamp - lastCompletedTime;
                if (timeSinceLastCompleted < coordinatorConfig.getCheckpointMinPause()) {
                    long minPauseDelay =
                            coordinatorConfig.getCheckpointMinPause() - timeSinceLastCompleted;
                    LOG.info(
                            "skip trigger checkpoint "
                                    + "because the last completed timestamp is {} and current timestamp is {}, "
                                    + "the time since completion ({} ms) is less than min-pause ({} ms).",
                            lastCompletedTime,
                            currentTimestamp,
                            timeSinceLastCompleted,
                            coordinatorConfig.getCheckpointMinPause());
                    scheduleTriggerPendingCheckpoint(checkpointType, minPauseDelay);
                    return;
                }
            }
        }
        synchronized (lock) {
            if (isCompleted() || isShutdown()) {
                LOG.warn(
                        "can't trigger checkpoint with type: {}, because checkpoint coordinator"
                                + " already have last completed checkpoint: ({}) or shutdown ({}).",
                        checkpointType,
                        latestCompletedCheckpoint != null
                                ? latestCompletedCheckpoint.getCheckpointType()
                                : "null",
                        shutdown);
                return;
            }

            if (schemaChanging.get() && checkpointType.isGeneralCheckpoint()) {
                LOG.info("skip trigger generic-checkpoint because schema change in progress");
                return;
            }

            if (pendingCounter.get() > 0) {
                scheduleTriggerPendingCheckpoint(checkpointType, 500L);
                LOG.debug("skip trigger checkpoint because there is already a pending checkpoint.");
                return;
            }

            CompletableFuture<PendingCheckpoint> pendingCheckpoint =
                    createPendingCheckpoint(currentTimestamp, checkpointType);
            startTriggerPendingCheckpoint(pendingCheckpoint);
            // if checkpoint type are final type, we don't need to trigger next checkpoint
            if (checkpointType.notFinalCheckpoint() && checkpointType.notSchemaChangeCheckpoint()) {
                scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
            } else {
                LOG.info(
                        "skip schedule trigger checkpoint because checkpoint type is {}",
                        checkpointType);
            }
        }
    }

    private boolean isShutdown() {
        return shutdown;
    }

    public static Map<Long, Integer> getPipelineTasks(Set<TaskLocation> pipelineSubtasks) {
        return pipelineSubtasks.stream()
                .collect(Collectors.groupingBy(TaskLocation::getTaskVertexId, Collectors.toList()))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
    }

    @SneakyThrows
    public PassiveCompletableFuture<CompletedCheckpoint> startSavepoint() {
        LOG.info("start save point for job id: {}.", jobId);
        if (shutdown || isCompleted()) {
            return completableFutureWithError(
                    CheckpointCloseReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }
        if (!isAllTaskReady.get()) {
            return completableFutureWithError(
                    CheckpointCloseReason.TASK_NOT_ALL_READY_WHEN_SAVEPOINT);
        }
        if (savepointPendingCheckpoint != null
                && !savepointPendingCheckpoint.getCompletableFuture().isDone()) {
            return savepointPendingCheckpoint.getCompletableFuture();
        }
        CompletableFuture<PendingCheckpoint> savepoint;
        synchronized (lock) {
            while (pendingCounter.get() > 0 && !shutdown) {
                Thread.sleep(500);
            }
            if (shutdown || isCompleted()) {
                return completableFutureWithError(
                        CheckpointCloseReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
            }
            savepoint = createPendingCheckpoint(Instant.now().toEpochMilli(), SAVEPOINT_TYPE);
            startTriggerPendingCheckpoint(savepoint);
        }
        savepointPendingCheckpoint = savepoint.join();
        LOG.info(
                "save point checkpoint is created, job id: {}, pipeline id: {}, checkpoint id: {}.",
                jobId,
                pipelineId,
                savepointPendingCheckpoint.getCheckpointId());
        return savepointPendingCheckpoint.getCompletableFuture();
    }

    private PassiveCompletableFuture<CompletedCheckpoint> completableFutureWithError(
            CheckpointCloseReason closeReason) {
        CompletableFuture<CompletedCheckpoint> future = new CompletableFuture<>();
        future.completeExceptionally(new CheckpointException(closeReason));
        return new PassiveCompletableFuture<>(future);
    }

    private void startTriggerPendingCheckpoint(
            CompletableFuture<PendingCheckpoint> pendingCompletableFuture) {
        pendingCompletableFuture.thenAccept(
                pendingCheckpoint -> {
                    LOG.info(
                            "wait checkpoint id: {} completed.",
                            pendingCheckpoint.getCheckpointId());
                    PassiveCompletableFuture<CompletedCheckpoint> completableFuture =
                            pendingCheckpoint.getCompletableFuture();
                    completableFuture.whenCompleteAsync(
                            (completedCheckpoint, error) -> {
                                if (error != null) {
                                    handleCoordinatorError(
                                            "trigger checkpoint failed",
                                            error,
                                            CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
                                } else if (completedCheckpoint != null) {
                                    try {
                                        completePendingCheckpoint(completedCheckpoint);
                                    } catch (Throwable e) {
                                        handleCoordinatorError(
                                                "complete checkpoint failed",
                                                e,
                                                CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
                                    }
                                } else {
                                    LOG.info(
                                            "skip this checkpoint cause by completedCheckpoint is null");
                                }
                            },
                            executorService);

                    // Trigger the barrier and wait for all tasks to ACK
                    LOG.debug("trigger checkpoint barrier {}", pendingCheckpoint.getInfo());
                    CompletableFuture<InvocationFuture<?>[]> completableFutureArray =
                            CompletableFuture.supplyAsync(
                                            () ->
                                                    new CheckpointBarrier(
                                                            pendingCheckpoint.getCheckpointId(),
                                                            pendingCheckpoint
                                                                    .getCheckpointTimestamp(),
                                                            pendingCheckpoint.getCheckpointType(),
                                                            new HashSet<>(readyToCloseIdleTask),
                                                            new HashSet<>(closedIdleTask)),
                                            executorService)
                                    .thenApplyAsync(this::triggerCheckpoint, executorService);

                    try {
                        CompletableFuture.allOf(completableFutureArray).get();
                    } catch (InterruptedException e) {
                        handleCoordinatorError(
                                "triggering checkpoint barrier has been interrupted",
                                e,
                                CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
                        return;
                    } catch (Exception e) {
                        handleCoordinatorError(
                                "triggering checkpoint barrier failed",
                                e,
                                CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
                        return;
                    }
                    if (coordinatorConfig.isCheckpointEnable()) {
                        LOG.debug(
                                "Start a scheduled task to prevent checkpoint timeouts for barrier {}",
                                pendingCheckpoint.getInfo());
                        long checkpointTimeout = coordinatorConfig.getCheckpointTimeout();
                        if (pendingCheckpoint.getCheckpointType().isSchemaChangeAfterCheckpoint()) {
                            checkpointTimeout =
                                    coordinatorConfig.getSchemaChangeCheckpointTimeout();
                        }
                        pendingCheckpoint.setCheckpointTimeOutFuture(
                                scheduler.schedule(
                                        () -> {
                                            // If any task is not acked within the checkpoint
                                            // timeout
                                            if (pendingCheckpoints.get(
                                                                    pendingCheckpoint
                                                                            .getCheckpointId())
                                                            != null
                                                    && !pendingCheckpoint.isFullyAcknowledged()) {
                                                LOG.info(
                                                        "timeout checkpoint: {}",
                                                        pendingCheckpoint.getInfo());
                                                handleCoordinatorError(
                                                        CheckpointCloseReason.CHECKPOINT_EXPIRED,
                                                        null);
                                            }
                                        },
                                        checkpointTimeout,
                                        TimeUnit.MILLISECONDS));
                    }
                });
        pendingCounter.incrementAndGet();
    }

    private CompletableFuture<PendingCheckpoint> createPendingCheckpoint(
            long triggerTimestamp, CheckpointType checkpointType) {
        synchronized (lock) {
            CompletableFuture<Long> idFuture;
            if (checkpointType.notCompletedCheckpoint()) {
                idFuture =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        // this must happen outside the coordinator-wide lock,
                                        // because it communicates with external services
                                        // (in HA mode) and may block for a while.
                                        return checkpointIdCounter.getAndIncrement();
                                    } catch (Throwable e) {
                                        handleCoordinatorError(
                                                "get checkpoint id failed",
                                                e,
                                                CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR);
                                        throw new CompletionException(e);
                                    }
                                },
                                executorService);
            } else {
                idFuture =
                        CompletableFuture.supplyAsync(
                                () -> Barrier.PREPARE_CLOSE_BARRIER_ID, executorService);
            }
            return triggerPendingCheckpoint(triggerTimestamp, idFuture, checkpointType);
        }
    }

    private CompletableFuture<PendingCheckpoint> triggerPendingCheckpoint(
            long triggerTimestamp,
            CompletableFuture<Long> idFuture,
            CheckpointType checkpointType) {
        if (!Thread.holdsLock(lock)) {
            throw new RuntimeException(
                    String.format(
                            "Unsafe invoke, the current thread[%s] has not acquired the lock[%s].",
                            Thread.currentThread().getName(), this.lock.toString()));
        }

        latestTriggerTimestamp.set(triggerTimestamp);
        return idFuture.thenApplyAsync(
                        checkpointId ->
                                new PendingCheckpoint(
                                        this.jobId,
                                        this.plan.getPipelineId(),
                                        checkpointId,
                                        triggerTimestamp,
                                        checkpointType,
                                        getNotYetAcknowledgedTasks(),
                                        getTaskStatistics(),
                                        getActionStates()),
                        executorService)
                .thenApplyAsync(
                        pendingCheckpoint -> {
                            pendingCheckpoints.put(
                                    pendingCheckpoint.getCheckpointId(), pendingCheckpoint);
                            if (checkpointMonitorService != null) {
                                checkpointMonitorService.onCheckpointTriggered(
                                        jobId,
                                        plan.getPipelineId(),
                                        pendingCheckpoint.getCheckpointId(),
                                        pendingCheckpoint.getCheckpointType(),
                                        pendingCheckpoint.getCheckpointTimestamp(),
                                        pendingCheckpoint.getTotalSubtasks());
                            }
                            return pendingCheckpoint;
                        },
                        executorService);
    }

    private Set<Long> getNotYetAcknowledgedTasks() {
        return plan.getPipelineSubtasks().stream()
                .filter(e -> !closedIdleTask.contains(e))
                .map(TaskLocation::getTaskID)
                .collect(Collectors.toCollection(CopyOnWriteArraySet::new));
    }

    private Map<ActionStateKey, ActionState> getActionStates() {
        Map<ActionStateKey, Integer> pipelineActions = new HashMap<>(plan.getPipelineActions());
        Set<ActionStateKey> closedActionKeys =
                plan.getSubtaskActions().entrySet().stream()
                        .filter(
                                entry ->
                                        SeaTunnelTaskState.CLOSED.equals(
                                                this.pipelineTaskStatus.get(
                                                        entry.getKey().getTaskID())))
                        .flatMap(entry -> entry.getValue().stream().map(Tuple2::f0))
                        .collect(Collectors.toSet());
        pipelineActions.keySet().removeAll(closedActionKeys);

        return pipelineActions.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new ActionState(entry.getKey(), entry.getValue())));
    }

    private Map<Long, TaskStatistics> getTaskStatistics() {
        Map<Long, Integer> tasks = new HashMap<>(this.pipelineTasks);
        for (Long taskId : this.pipelineTasks.keySet()) {
            if (SeaTunnelTaskState.CLOSED.equals(this.pipelineTaskStatus.get(taskId))) {
                tasks.remove(taskId);
            }
        }
        return tasks.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new TaskStatistics(entry.getKey(), entry.getValue())));
    }

    public InvocationFuture<?>[] triggerCheckpoint(CheckpointBarrier checkpointBarrier) {
        return plan.getStartingSubtasks().stream()
                .filter(
                        taskLocation ->
                                !SeaTunnelTaskState.CLOSED.equals(
                                        this.pipelineTaskStatus.get(taskLocation.getTaskID())))
                .map(
                        taskLocation ->
                                new CheckpointBarrierTriggerOperation(
                                        checkpointBarrier, taskLocation))
                .map(checkpointManager::sendOperationToMemberNode)
                .toArray(InvocationFuture[]::new);
    }

    /**
     * Cleans and aborts all pending checkpoints due to the given close reason.
     *
     * <p>This method forcefully terminates all in-progress {@code PendingCheckpoint}s and resets
     * the internal coordinator state. It is typically invoked when the checkpoint coordinator is
     * shutting down, resetting, or when the job reaches a terminal state.
     *
     * <p>The cleanup process includes:
     *
     * <ul>
     *   <li>Marking the coordinator as shutdown
     *   <li>Resetting task readiness state
     *   <li>Aborting all pending checkpoints with the provided {@code CheckpointCloseReason}
     *   <li>Notifying the {@code CheckpointMonitorService} about checkpoint failures (except when
     *       caused by a coordinator reset)
     *   <li>Clearing all internal tracking structures
     *   <li>Resetting counters and schema change flags
     *   <li>Stopping and recreating the scheduler thread pool
     * </ul>
     *
     * <p>If the close reason is {@code CHECKPOINT_COORDINATOR_RESET}, the monitor service will
     * clear all in-progress checkpoint metadata without reporting them as failures.
     *
     * <p>This method ensures that no residual checkpoint state remains in memory and that the
     * coordinator is ready for a clean restart if necessary.
     *
     * @param closedReason the reason why pending checkpoints are being closed; determines how
     *     monitoring and cleanup are handled
     */
    protected void cleanPendingCheckpoint(CheckpointCloseReason closedReason) {
        shutdown = true;
        isAllTaskReady.set(false);
        synchronized (lock) {
            LOG.info("start clean pending checkpoint cause {}", closedReason.message());
            if (!pendingCheckpoints.isEmpty()) {
                pendingCheckpoints
                        .values()
                        .forEach(
                                pendingCheckpoint -> {
                                    if (checkpointMonitorService != null
                                            && closedReason
                                                    != CheckpointCloseReason
                                                            .CHECKPOINT_COORDINATOR_RESET) {
                                        checkpointMonitorService.onCheckpointFailed(
                                                jobId,
                                                plan.getPipelineId(),
                                                pendingCheckpoint.getCheckpointId(),
                                                pendingCheckpoint.getCheckpointType(),
                                                closedReason,
                                                null,
                                                pendingCheckpoint.getCheckpointTimestamp());
                                    }
                                    pendingCheckpoint.abortCheckpoint(closedReason, null);
                                });
                // TODO: clear related future & scheduler task
                pendingCheckpoints.clear();
            }
            pipelineTaskStatus.clear();
            readyToCloseStartingTask.clear();
            readyToCloseIdleTask.clear();
            closedIdleTask.clear();
            pendingCounter.set(0);
            schemaChanging.set(false);
            // Only remove the persisted ready-to-close state when the coordinator truly ends
            // (completed/failed/cancelled). During a reset (master failover), the IMap entry
            // must be preserved so restoreCoordinator() can recover from it.
            if (closedReason != CheckpointCloseReason.CHECKPOINT_COORDINATOR_RESET) {
                runningJobStateIMap.remove(readyToCloseImapKey);
            }
            scheduler.shutdownNow();
            scheduler =
                    Executors.newScheduledThreadPool(
                            2,
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setName(
                                        String.format(
                                                "checkpoint-coordinator-%s/%s", pipelineId, jobId));
                                return thread;
                            });
        }
        if (checkpointMonitorService != null
                && closedReason == CheckpointCloseReason.CHECKPOINT_COORDINATOR_RESET) {
            checkpointMonitorService.clearInProgress(jobId, pipelineId);
        }
    }
    /**
     * Processes a checkpoint acknowledgment from a task.
     *
     * <p>This method is invoked when a task successfully completes its checkpoint operation and
     * sends back an acknowledgment along with its state.
     *
     * <p>The coordinator performs the following actions:
     *
     * <ul>
     *   <li>Validates the existence of the corresponding {@link PendingCheckpoint}
     *   <li>Registers the task acknowledgment and associated states
     *   <li>Updates subtask execution status (e.g., RUNNING or SAVEPOINT_PREPARE_CLOSE)
     *   <li>Notifies the checkpoint monitor service, if available
     *   <li>Handles prepare-close logic for non-final checkpoints
     * </ul>
     *
     * <p>If the checkpoint has already been completed or discarded, the acknowledgment is safely
     * ignored.
     *
     * @param ackOperation the acknowledgment operation containing the checkpoint barrier, task
     *     location, and state snapshot
     */
    protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        final long checkpointId = ackOperation.getBarrier().getId();
        final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        if (pendingCheckpoint == null) {
            LOG.info("skip already ack checkpoint id: {}", checkpointId);
            return;
        }
        TaskLocation location = ackOperation.getTaskLocation();
        LOG.debug(
                "task ack, task id: {}, pipeline id: {}, job id: {}, barrier: {}",
                location.getTaskID(),
                location.getPipelineId(),
                location.getJobId(),
                ackOperation.getBarrier().toString());

        pendingCheckpoint.acknowledgeTask(
                location,
                ackOperation.getStates(),
                pendingCheckpoint.getCheckpointType().isSavepoint()
                        ? SubtaskStatus.SAVEPOINT_PREPARE_CLOSE
                        : SubtaskStatus.RUNNING);

        if (checkpointMonitorService != null) {
            checkpointMonitorService.onCheckpointAcknowledge(
                    jobId,
                    plan.getPipelineId(),
                    pendingCheckpoint.getCheckpointId(),
                    pendingCheckpoint.getAcknowledgedSubtasks(),
                    pendingCheckpoint.getTotalSubtasks());
        }

        if (ackOperation.getBarrier().getCheckpointType().notFinalCheckpoint()
                && ackOperation.getBarrier().prepareClose(location)) {
            completedCloseIdleTask(location);
        }
    }

    /**
     * Completes a pending checkpoint after all required task acknowledgements have been received.
     *
     * <p>This method performs the finalization logic of a {@code PendingCheckpoint}, converting it
     * into a {@code CompletedCheckpoint}. The operation includes:
     *
     * <ul>
     *   <li>Logging checkpoint completion metadata (duration, trigger time, completion time)
     *   <li>Persisting serialized checkpoint state into the configured storage (when applicable)
     *   <li>Applying retention policy and deleting old checkpoints if necessary
     *   <li>Updating the latest completed checkpoint reference
     *   <li>Notifying monitoring services about checkpoint completion
     *   <li>Cleaning up internal pending checkpoint structures
     *   <li>Transitioning coordinator state if the job has finished
     * </ul>
     *
     * <p>The method is {@code synchronized} to ensure thread safety, since checkpoint completion
     * may be triggered concurrently by multiple task acknowledgements.
     *
     * <p>If the checkpoint type is not marked as a completed-only checkpoint (e.g., not a final
     * checkpoint marker), its serialized state will be stored in the configured {@code
     * CheckpointStorage}. The retention mechanism removes older checkpoints based on {@code
     * maxRetainedCheckpoints}.
     *
     * <p>If the job execution is determined to be fully completed after this checkpoint, the
     * coordinator transitions to:
     *
     * <ul>
     *   <li>{@code SUSPEND} state if the checkpoint is a savepoint
     *   <li>{@code FINISHED} state otherwise
     * </ul>
     *
     * @param completedCheckpoint the fully acknowledged checkpoint to finalize and persist; must
     *     not be {@code null}
     * @throws RuntimeException if checkpoint state serialization or storage fails
     */
    public synchronized void completePendingCheckpoint(CompletedCheckpoint completedCheckpoint) {
        LOG.debug(
                "pending checkpoint completed, job id: {}, pipeline id: {}, checkpoint id: {}, "
                        + "cost: {}, trigger: {}, completed: {}",
                completedCheckpoint.getJobId(),
                completedCheckpoint.getPipelineId(),
                completedCheckpoint.getCheckpointId(),
                completedCheckpoint.getCompletedTimestamp()
                        - completedCheckpoint.getCheckpointTimestamp(),
                completedCheckpoint.getCheckpointTimestamp(),
                completedCheckpoint.getCompletedTimestamp());
        final long checkpointId = completedCheckpoint.getCheckpointId();
        completedCheckpointIds.addLast(String.valueOf(completedCheckpoint.getCheckpointId()));
        try {
            if (completedCheckpoint.getCheckpointType().notCompletedCheckpoint()) {
                byte[] states = serializer.serialize(completedCheckpoint);
                checkpointStorage.storeCheckPoint(
                        PipelineState.builder()
                                .checkpointId(checkpointId)
                                .jobId(String.valueOf(jobId))
                                .pipelineId(pipelineId)
                                .states(states)
                                .build());
            }
            if (completedCheckpointIds.size()
                                    % coordinatorConfig.getStorage().getMaxRetainedCheckpoints()
                            == 0
                    && completedCheckpointIds.size()
                                    / coordinatorConfig.getStorage().getMaxRetainedCheckpoints()
                            > 1) {
                List<String> needDeleteCheckpointId = new ArrayList<>();
                for (int i = 0;
                        i < coordinatorConfig.getStorage().getMaxRetainedCheckpoints();
                        i++) {
                    needDeleteCheckpointId.add(completedCheckpointIds.removeFirst());
                }
                checkpointStorage.deleteCheckpoint(
                        String.valueOf(completedCheckpoint.getJobId()),
                        String.valueOf(completedCheckpoint.getPipelineId()),
                        needDeleteCheckpointId);
            }
        } catch (Throwable e) {
            LOG.error("store checkpoint states failed.", e);
            sneakyThrow(e);
        }
        LOG.info(
                "pending checkpoint notify finished, job id: {}, pipeline id: {}, checkpoint id: {}!",
                completedCheckpoint.getJobId(),
                completedCheckpoint.getPipelineId(),
                completedCheckpoint.getCheckpointId());
        latestCompletedCheckpoint = completedCheckpoint;
        if (checkpointMonitorService != null) {
            long stateSize = CheckpointMonitorService.calculateStateSize(completedCheckpoint);
            checkpointMonitorService.onCheckpointCompleted(completedCheckpoint, stateSize);
        }
        if (!notifyCompleted(completedCheckpoint)) {
            return;
        }
        PendingCheckpoint pendingCheckpoint = pendingCheckpoints.remove(checkpointId);
        if (pendingCheckpoint != null) {
            pendingCheckpoint.abortCheckpointTimeoutFutureWhenIsCompleted();
        }
        pendingCounter.decrementAndGet();

        if (isCompleted()) {
            cleanPendingCheckpoint(CheckpointCloseReason.CHECKPOINT_COORDINATOR_COMPLETED);
            if (latestCompletedCheckpoint.getCheckpointType().isSavepoint()) {
                updateStatus(CheckpointCoordinatorStatus.SUSPEND);
                checkpointCoordinatorFuture.complete(
                        new CheckpointCoordinatorState(CheckpointCoordinatorStatus.SUSPEND, null));
            } else {
                updateStatus(CheckpointCoordinatorStatus.FINISHED);
                checkpointCoordinatorFuture.complete(
                        new CheckpointCoordinatorState(CheckpointCoordinatorStatus.FINISHED, null));
            }
        }
    }

    public InvocationFuture<?>[] notifyCheckpointCompleted(CompletedCheckpoint checkpoint) {
        if (checkpoint.getCheckpointType().isSchemaChangeAfterCheckpoint()) {
            completeSchemaChangeAfterCheckpoint(checkpoint);
        }
        return plan.getPipelineSubtasks().stream()
                .map(
                        taskLocation ->
                                new CheckpointFinishedOperation(
                                        taskLocation, checkpoint.getCheckpointId(), true))
                .map(checkpointManager::sendOperationToMemberNode)
                .toArray(InvocationFuture[]::new);
    }

    public InvocationFuture<?>[] notifyCheckpointEnd(CompletedCheckpoint checkpoint) {
        if (checkpoint.getCheckpointType().isSchemaChangeCheckpoint()) {
            return plan.getPipelineSubtasks().stream()
                    .map(
                            taskLocation ->
                                    new CheckpointEndOperation(
                                            taskLocation, checkpoint.getCheckpointId(), true))
                    .map(checkpointManager::sendOperationToMemberNode)
                    .toArray(InvocationFuture[]::new);
        }
        return new InvocationFuture[0];
    }

    public boolean isCompleted() {
        if (latestCompletedCheckpoint == null) {
            return false;
        }
        return latestCompletedCheckpoint.getCheckpointType().isFinalCheckpoint()
                && !latestCompletedCheckpoint.isRestored();
    }

    public boolean isNoErrorCompleted() {
        if (latestCompletedCheckpoint == null) {
            return false;
        }
        CheckpointCoordinatorStatus status =
                (CheckpointCoordinatorStatus) runningJobStateIMap.get(checkpointStateImapKey);
        return latestCompletedCheckpoint.getCheckpointType().isFinalCheckpoint()
                && (status.equals(CheckpointCoordinatorStatus.FINISHED)
                        || status.equals(CheckpointCoordinatorStatus.SUSPEND))
                && !latestCompletedCheckpoint.isRestored();
    }

    public boolean isEndOfSavePoint() {
        if (latestCompletedCheckpoint == null) {
            return false;
        }
        return latestCompletedCheckpoint.getCheckpointType().isSavepoint();
    }

    public PassiveCompletableFuture<CheckpointCoordinatorState>
            waitCheckpointCoordinatorComplete() {
        return new PassiveCompletableFuture<>(checkpointCoordinatorFuture);
    }

    public PassiveCompletableFuture<CheckpointCoordinatorState> cancelCheckpoint() {
        // checkpoint maybe already failed before all tasks complete.
        if (checkpointCoordinatorFuture.isDone()) {
            return new PassiveCompletableFuture<>(checkpointCoordinatorFuture);
        }
        cleanPendingCheckpoint(CheckpointCloseReason.PIPELINE_END);
        updateStatus(CheckpointCoordinatorStatus.CANCELED);
        CheckpointCoordinatorState checkpointCoordinatorState =
                new CheckpointCoordinatorState(CheckpointCoordinatorStatus.CANCELED, null);
        checkpointCoordinatorFuture.complete(checkpointCoordinatorState);
        return new PassiveCompletableFuture<>(checkpointCoordinatorFuture);
    }

    private synchronized void updateStatus(@NonNull CheckpointCoordinatorStatus targetStatus) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        Object currentStatus = runningJobStateIMap.get(checkpointStateImapKey);
                        if (currentStatus == null) {
                            LOG.warn(
                                    String.format(
                                            "%s has already been cleaned, skip persisting transition to %s",
                                            checkpointStateImapKey, targetStatus));
                            return null;
                        }
                        LOG.info(
                                "Turn {} state from {} to {}",
                                checkpointStateImapKey,
                                currentStatus,
                                targetStatus);
                        runningJobStateIMap.set(checkpointStateImapKey, targetStatus);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            ExceptionUtil::isOperationNeedRetryException,
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOG.warn(
                    "Set {} state {} to IMap failed, skip do it",
                    checkpointStateImapKey,
                    targetStatus);
        }
    }

    /**
     * Schedules a schema-change-before checkpoint if no schema change is currently in progress.
     *
     * <p>This method ensures that a dedicated checkpoint of type {@code
     * SCHEMA_CHANGE_BEFORE_POINT_TYPE} is triggered before applying a schema change. It uses an
     * atomic flag ({@code schemaChanging}) to guarantee that only one schema-change checkpoint is
     * scheduled at a time.
     *
     * <p>When invoked:
     *
     * <ul>
     *   <li>If no schema change is in progress, general checkpoint triggering is effectively paused
     *       and a schema-change-before checkpoint is scheduled immediately.
     *   <li>If a schema change checkpoint is already scheduled or in progress, the method logs a
     *       warning and does nothing.
     * </ul>
     *
     * <p>This mechanism guarantees state consistency and durability before modifying the pipeline
     * schema, preventing inconsistencies between operator state and structural changes.
     */
    protected void scheduleSchemaChangeBeforeCheckpoint() {
        if (schemaChanging.compareAndSet(false, true)) {
            LOG.info(
                    "stop trigger general-checkpoint "
                            + "because schema change in progress, job id: {}, pipeline id: {}.",
                    jobId,
                    pipelineId);
            LOG.info(
                    "schedule schema-change-before checkpoint, job id: {}, pipeline id: {}.",
                    jobId,
                    pipelineId);
            scheduleTriggerPendingCheckpoint(CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE, 0);
        } else {
            LOG.warn(
                    "schema-change-before checkpoint is already scheduled, job id: {}, pipeline id: {}.",
                    jobId,
                    pipelineId);
        }
    }

    protected void scheduleSchemaChangeAfterCheckpoint() {
        if (schemaChanging.get()) {
            LOG.info(
                    "schedule schema-change-after checkpoint, job id: {}, pipeline id: {}.",
                    jobId,
                    pipelineId);
            scheduleTriggerPendingCheckpoint(CheckpointType.SCHEMA_CHANGE_AFTER_POINT_TYPE, 0);
        } else {
            LOG.warn(
                    "schema-change-after checkpoint is already scheduled, job id: {}, pipeline id: {}.",
                    jobId,
                    pipelineId);
        }
    }

    protected void completeSchemaChangeAfterCheckpoint(CompletedCheckpoint checkpoint) {
        if (schemaChanging.compareAndSet(true, false)) {
            LOG.info(
                    "completed schema-change-after checkpoint, job id: {}, pipeline id: {}, "
                            + "checkpoint id: {}.",
                    jobId,
                    pipelineId,
                    checkpoint.getCheckpointId());
            LOG.info(
                    "recover trigger general-checkpoint, job id: {}, pipeline id: {}, "
                            + "checkpoint id: {}.",
                    jobId,
                    pipelineId,
                    checkpoint.getCheckpointId());
            scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
        } else {
            throw new IllegalStateException(
                    String.format(
                            "schema-change-after checkpoint is already completed, "
                                    + "job id: %s, pipeline id: %s, checkpoint id: %s.",
                            jobId, pipelineId, checkpoint.getCheckpointId()));
        }
    }

    public String getCheckpointStateImapKey() {
        return checkpointStateImapKey;
    }

    public String getReadyToCloseImapKey() {
        return readyToCloseImapKey;
    }

    /** Only for test */
    @VisibleForTesting
    public PendingCheckpoint getSavepointPendingCheckpoint() {
        return savepointPendingCheckpoint;
    }

    @VisibleForTesting
    public Map<Long, SeaTunnelTaskState> getPipelineTaskStatus() {
        return pipelineTaskStatus;
    }
}
