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

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
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

    private final Set<TaskLocation> readyToCloseStartingTask;
    private final Set<TaskLocation> readyToCloseIdleTask;
    @Getter private final Set<TaskLocation> closedIdleTask;
    private final ConcurrentHashMap<Long, PendingCheckpoint> pendingCheckpoints;

    private final ArrayDeque<String> completedCheckpointIds;

    private volatile CompletedCheckpoint latestCompletedCheckpoint = null;

    private final CheckpointConfig coordinatorConfig;

    private transient ScheduledExecutorService scheduler;

    private final AtomicLong latestTriggerTimestamp = new AtomicLong(0);

    private final AtomicInteger pendingCounter = new AtomicInteger(0);

    private final AtomicBoolean schemaChanging = new AtomicBoolean(false);

    private final Object lock = new Object();

    /** Flag marking the coordinator as shut down (not accepting any messages anymore). */
    private volatile boolean shutdown;

    private volatile boolean isAllTaskReady = false;

    private final ExecutorService executorService;

    private CompletableFuture<CheckpointCoordinatorState> checkpointCoordinatorFuture;

    private AtomicReference<String> errorByPhysicalVertex = new AtomicReference<>();

    private final IMap<Object, Object> runningJobStateIMap;

    // save pending checkpoint for savepoint, to make sure the different savepoint request can be
    // processed with one savepoint operation in the same time.
    private PendingCheckpoint savepointPendingCheckpoint;

    private final String checkpointStateImapKey;

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
            boolean isStartWithSavePoint) {

        this.executorService = executorService;
        this.checkpointManager = manager;
        this.checkpointStorage = checkpointStorage;
        this.jobId = jobId;
        this.pipelineId = plan.getPipelineId();
        this.checkpointStateImapKey = "checkpoint_state_" + jobId + "_" + pipelineId;
        this.runningJobStateIMap = runningJobStateIMap;
        this.plan = plan;
        this.coordinatorConfig = checkpointConfig;
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
                "Create CheckpointCoordinator for job({}@{}) with plan({})",
                pipelineId,
                jobId,
                plan);
        if (pipelineState != null) {
            this.latestCompletedCheckpoint =
                    serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);
            this.latestCompletedCheckpoint.setRestored(true);
            LOG.info(
                    "Restore job({}@{}) with checkpoint({}), data: {}",
                    pipelineId,
                    jobId,
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
        cleanPendingCheckpoint(reason);
        updateStatus(CheckpointCoordinatorStatus.FAILED);
        checkpointCoordinatorFuture.complete(
                new CheckpointCoordinatorState(
                        CheckpointCoordinatorStatus.FAILED, errorByPhysicalVertex.get()));
        checkpointManager.handleCheckpointError(pipelineId, false);
    }

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

    private void allTaskReady() {
        if (pipelineTaskStatus.size() != plan.getPipelineSubtasks().size()) {
            return;
        }
        for (SeaTunnelTaskState status : pipelineTaskStatus.values()) {
            if (READY_START != status) {
                return;
            }
        }
        isAllTaskReady = true;
        InvocationFuture<?>[] futures = notifyTaskStart();
        CompletableFuture.allOf(futures).join();
        notifyCompleted(latestCompletedCheckpoint);
        if (coordinatorConfig.isCheckpointEnable()) {
            LOG.info("checkpoint is enabled, start schedule trigger pending checkpoint.");
            scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
        } else {
            LOG.info(
                    "checkpoint is disabled, because in batch mode and 'checkpoint.interval' of env is missing.");
        }
    }

    private void notifyCompleted(CompletedCheckpoint completedCheckpoint) {
        if (completedCheckpoint != null) {
            try {
                LOG.info(
                        "start notify checkpoint completed, job id: {}, pipeline id: {}, checkpoint id:{}",
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
            }
        }
    }

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

    private void scheduleTriggerPendingCheckpoint(CheckpointType checkpointType, long delayMills) {
        scheduler.schedule(
                () -> tryTriggerPendingCheckpoint(checkpointType),
                delayMills,
                TimeUnit.MILLISECONDS);
    }

    protected void readyToClose(TaskLocation taskLocation) {
        readyToCloseStartingTask.add(taskLocation);
        if (readyToCloseStartingTask.size() == plan.getStartingSubtasks().size()) {
            tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
        }
    }

    protected void readyToCloseIdleTask(TaskLocation taskLocation) {
        if (plan.getStartingSubtasks().contains(taskLocation)) {
            throw new UnsupportedOperationException("Unsupported close starting task");
        }

        LOG.info(
                "Received close idle task[{}]({}/{}). {}",
                taskLocation.getTaskID(),
                taskLocation.getPipelineId(),
                taskLocation.getJobId(),
                taskLocation);
        synchronized (readyToCloseIdleTask) {
            if (readyToCloseIdleTask.contains(taskLocation)
                    || closedIdleTask.contains(taskLocation)) {
                LOG.warn(
                        "task[{}]({}/{}) already in closed. {}",
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
                            "Add task[{}]({}/{}) to prepare close list",
                            subTask.getTaskID(),
                            subTask.getPipelineId(),
                            subTask.getJobId());
                }
            }
            readyToCloseIdleTask.addAll(subTaskList);
            tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);
        }
    }

    protected void completedCloseIdleTask(TaskLocation taskLocation) {
        synchronized (readyToCloseIdleTask) {
            if (readyToCloseIdleTask.contains(taskLocation)) {
                readyToCloseIdleTask.remove(taskLocation);
                closedIdleTask.add(taskLocation);
                LOG.info(
                        "Completed close task[{}]({}/{})",
                        taskLocation.getTaskID(),
                        taskLocation.getPipelineId(),
                        taskLocation.getJobId());
            }
        }
    }

    protected void restoreCoordinator(boolean alreadyStarted) {
        LOG.info("received restore CheckpointCoordinator with alreadyStarted= " + alreadyStarted);
        errorByPhysicalVertex = new AtomicReference<>();
        checkpointCoordinatorFuture = new CompletableFuture<>();
        updateStatus(CheckpointCoordinatorStatus.RUNNING);
        cleanPendingCheckpoint(CheckpointCloseReason.CHECKPOINT_COORDINATOR_RESET);
        shutdown = false;
        if (alreadyStarted) {
            isAllTaskReady = true;
            notifyCompleted(latestCompletedCheckpoint);
            tryTriggerPendingCheckpoint(CHECKPOINT_TYPE);
        } else {
            isAllTaskReady = false;
        }
    }

    protected void tryTriggerPendingCheckpoint(CheckpointType checkpointType) {
        if (Thread.currentThread().isInterrupted()) {
            LOG.warn("currentThread already be interrupted, skip trigger checkpoint");
            return;
        }
        final long currentTimestamp = Instant.now().toEpochMilli();
        if (checkpointType.notFinalCheckpoint() && checkpointType.notSchemaChangeCheckpoint()) {
            long diffFromLastTimestamp = currentTimestamp - latestTriggerTimestamp.get();
            if (diffFromLastTimestamp <= 0) {
                LOG.error(
                        "The time on your server may not be incremental which can lead checkpoint to stop. The latestTriggerTimestamp: ({}), but the currentTimestamp: ({})",
                        latestTriggerTimestamp.get(),
                        currentTimestamp);
            }
            if (diffFromLastTimestamp < coordinatorConfig.getCheckpointInterval()
                    || !isAllTaskReady) {
                return;
            }
        }
        synchronized (lock) {
            if (isCompleted() || isShutdown()) {
                LOG.warn(
                        String.format(
                                "can't trigger checkpoint with type: %s, because checkpoint coordinator already have last completed checkpoint: (%s) or shutdown (%b).",
                                checkpointType,
                                latestCompletedCheckpoint != null
                                        ? latestCompletedCheckpoint.getCheckpointType()
                                        : "null",
                                shutdown));
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
        LOG.info(String.format("Start save point for Job (%s)", jobId));
        if (shutdown || isCompleted()) {
            return completableFutureWithError(
                    CheckpointCloseReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }
        if (!isAllTaskReady) {
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
                String.format(
                        "The save point checkpointId is %s",
                        savepointPendingCheckpoint.getCheckpointId()));
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
                    LOG.info("wait checkpoint completed: " + pendingCheckpoint.getCheckpointId());
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
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        LOG.error(ExceptionUtils.getMessage(e));
                        return;
                    }
                    if (coordinatorConfig.isCheckpointEnable()) {
                        LOG.debug(
                                "Start a scheduled task to prevent checkpoint timeouts for barrier "
                                        + pendingCheckpoint.getInfo());
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
                                                        "timeout checkpoint: "
                                                                + pendingCheckpoint.getInfo());
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
        // TODO: some tasks have completed and will not submit state again.
        return plan.getPipelineActions().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new ActionState(entry.getKey(), entry.getValue())));
    }

    private Map<Long, TaskStatistics> getTaskStatistics() {
        // TODO: some tasks have completed and don't need to be ack
        return this.pipelineTasks.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new TaskStatistics(entry.getKey(), entry.getValue())));
    }

    public InvocationFuture<?>[] triggerCheckpoint(CheckpointBarrier checkpointBarrier) {
        // TODO: some tasks have completed and don't need to trigger
        return plan.getStartingSubtasks().stream()
                .map(
                        taskLocation ->
                                new CheckpointBarrierTriggerOperation(
                                        checkpointBarrier, taskLocation))
                .map(checkpointManager::sendOperationToMemberNode)
                .toArray(InvocationFuture[]::new);
    }

    protected void cleanPendingCheckpoint(CheckpointCloseReason closedReason) {
        shutdown = true;
        isAllTaskReady = false;
        synchronized (lock) {
            LOG.info("start clean pending checkpoint cause {}", closedReason.message());
            if (!pendingCheckpoints.isEmpty()) {
                pendingCheckpoints
                        .values()
                        .forEach(
                                pendingCheckpoint ->
                                        pendingCheckpoint.abortCheckpoint(closedReason, null));
                // TODO: clear related future & scheduler task
                pendingCheckpoints.clear();
            }
            pipelineTaskStatus.clear();
            readyToCloseStartingTask.clear();
            readyToCloseIdleTask.clear();
            closedIdleTask.clear();
            pendingCounter.set(0);
            schemaChanging.set(false);
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
    }

    protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        final long checkpointId = ackOperation.getBarrier().getId();
        final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        if (pendingCheckpoint == null) {
            LOG.info("skip already ack checkpoint " + checkpointId);
            return;
        }
        TaskLocation location = ackOperation.getTaskLocation();
        LOG.debug(
                "task[{}]({}/{}) ack. {}",
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

        if (ackOperation.getBarrier().getCheckpointType().notFinalCheckpoint()
                && ackOperation.getBarrier().prepareClose(location)) {
            completedCloseIdleTask(location);
        }
    }

    public synchronized void completePendingCheckpoint(CompletedCheckpoint completedCheckpoint) {
        LOG.debug(
                "pending checkpoint({}/{}@{}) completed! cost: {}, trigger: {}, completed: {}",
                completedCheckpoint.getCheckpointId(),
                completedCheckpoint.getPipelineId(),
                completedCheckpoint.getJobId(),
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
                "pending checkpoint({}/{}@{}) notify finished!",
                completedCheckpoint.getCheckpointId(),
                completedCheckpoint.getPipelineId(),
                completedCheckpoint.getJobId());
        latestCompletedCheckpoint = completedCheckpoint;
        notifyCompleted(completedCheckpoint);
        pendingCheckpoints.remove(checkpointId).abortCheckpointTimeoutFutureWhenIsCompleted();
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
                        LOG.info(
                                String.format(
                                        "Turn %s state from %s to %s",
                                        checkpointStateImapKey,
                                        runningJobStateIMap.get(checkpointStateImapKey),
                                        targetStatus));
                        runningJobStateIMap.set(checkpointStateImapKey, targetStatus);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOG.warn(
                    String.format(
                            "Set %s state %s to IMap failed, skip do it",
                            checkpointStateImapKey, targetStatus));
        }
    }

    protected void scheduleSchemaChangeBeforeCheckpoint() {
        if (schemaChanging.compareAndSet(false, true)) {
            LOG.info(
                    "stop trigger general-checkpoint({}@{}) because schema change in progress.",
                    pipelineId,
                    jobId);
            LOG.info("schedule schema-change-before checkpoint({}@{}).", pipelineId, jobId);
            scheduleTriggerPendingCheckpoint(CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE, 0);
        } else {
            LOG.warn(
                    "schema-change-before checkpoint({}@{}) is already scheduled.",
                    pipelineId,
                    jobId);
        }
    }

    protected void scheduleSchemaChangeAfterCheckpoint() {
        if (schemaChanging.get()) {
            LOG.info("schedule schema-change-after checkpoint({}@{}).", pipelineId, jobId);
            scheduleTriggerPendingCheckpoint(CheckpointType.SCHEMA_CHANGE_AFTER_POINT_TYPE, 0);
        } else {
            LOG.warn(
                    "schema-change-after checkpoint({}@{}) is already scheduled.",
                    pipelineId,
                    jobId);
        }
    }

    protected void completeSchemaChangeAfterCheckpoint(CompletedCheckpoint checkpoint) {
        if (schemaChanging.compareAndSet(true, false)) {
            LOG.info(
                    "completed schema-change-after checkpoint({}/{}@{}).",
                    checkpoint.getCheckpointId(),
                    pipelineId,
                    jobId);
            LOG.info(
                    "recover trigger general-checkpoint({}/{}@{}).",
                    checkpoint.getCheckpointId(),
                    pipelineId,
                    jobId);
            scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
        } else {
            throw new IllegalStateException(
                    String.format(
                            "schema-change-after checkpoint(%s/%s@%s) is already completed.",
                            checkpoint.getCheckpointId(), pipelineId, jobId));
        }
    }

    /** Only for test */
    @VisibleForTesting
    public PendingCheckpoint getSavepointPendingCheckpoint() {
        return savepointPendingCheckpoint;
    }
}
