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

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneakyThrow;
import static org.apache.seatunnel.engine.core.checkpoint.CheckpointType.CHECKPOINT_TYPE;
import static org.apache.seatunnel.engine.core.checkpoint.CheckpointType.COMPLETED_POINT_TYPE;
import static org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan.COORDINATOR_INDEX;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.checkpoint.storage.common.Serializer;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.Checkpoint;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.NotifyTaskRestoreOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.NotifyTaskStartOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Used to coordinate all checkpoints of a pipeline.
 * <p>
 * Generate and coordinate {@link Checkpoint} with a checkpoint plan
 * </p>
 */
public class CheckpointCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final long jobId;

    private final int pipelineId;

    private final CheckpointManager checkpointManager;

    private final CheckpointStorage checkpointStorage;

    @Getter
    private final CheckpointIDCounter checkpointIdCounter;

    private final transient Serializer serializer;

    /**
     * All tasks in this pipeline.
     * <br> key: the task id;
     * <br> value: the parallelism of the task;
     */
    private final Map<Long, Integer> pipelineTasks;

    private final Map<Long, SeaTunnelTaskState> pipelineTaskStatus;

    private final CheckpointPlan plan;

    private final Set<TaskLocation> readyToCloseStartingTask;
    private final ConcurrentHashMap<Long, PendingCheckpoint> pendingCheckpoints;

    private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    private volatile CompletedCheckpoint latestCompletedCheckpoint = null;

    private final CheckpointConfig coordinatorConfig;

    private int tolerableFailureCheckpoints;
    private transient ScheduledExecutorService scheduler;

    private final AtomicLong latestTriggerTimestamp = new AtomicLong(0);

    private final AtomicInteger pendingCounter = new AtomicInteger(0);

    private final Object lock = new Object();

    /** Flag marking the coordinator as shut down (not accepting any messages any more). */
    private volatile boolean shutdown;

    @Setter
    private volatile boolean isAllTaskReady = false;

    @SneakyThrows
    public CheckpointCoordinator(CheckpointManager manager,
                                 CheckpointStorage checkpointStorage,
                                 CheckpointConfig checkpointConfig,
                                 long jobId,
                                 CheckpointPlan plan,
                                 CheckpointIDCounter checkpointIdCounter,
                                 PipelineState pipelineState) {

        this.checkpointManager = manager;
        this.checkpointStorage = checkpointStorage;
        this.jobId = jobId;
        this.pipelineId = plan.getPipelineId();
        this.plan = plan;
        this.coordinatorConfig = checkpointConfig;
        this.tolerableFailureCheckpoints = coordinatorConfig.getTolerableFailureCheckpoints();
        this.pendingCheckpoints = new ConcurrentHashMap<>();
        this.completedCheckpoints = new ArrayDeque<>(coordinatorConfig.getStorage().getMaxRetainedCheckpoints() + 1);
        this.scheduler = Executors.newScheduledThreadPool(
            1, runnable -> {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                thread.setName(String.format("checkpoint-coordinator-%s/%s", pipelineId, jobId));
                return thread;
            });
        this.serializer = new ProtoStuffSerializer();
        this.pipelineTasks = getPipelineTasks(plan.getPipelineSubtasks());
        this.pipelineTaskStatus = new ConcurrentHashMap<>();
        this.checkpointIdCounter = checkpointIdCounter;
        this.readyToCloseStartingTask = new CopyOnWriteArraySet<>();
        if (pipelineState != null) {
            this.latestCompletedCheckpoint = serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);
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
        CompletableFuture.runAsync(() -> {
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
        });
    }

    private void restoreTaskState(TaskLocation taskLocation) {
        List<ActionSubtaskState> states = new ArrayList<>();
        if (latestCompletedCheckpoint != null) {
            final Integer currentParallelism = pipelineTasks.get(taskLocation.getTaskVertexId());
            plan.getSubtaskActions().get(taskLocation)
                .forEach(tuple -> {
                    ActionState actionState = latestCompletedCheckpoint.getTaskStates().get(tuple.f0());
                    if (actionState == null) {
                        return;
                    }
                    if (COORDINATOR_INDEX.equals(tuple.f1())) {
                        states.add(actionState.getCoordinatorState());
                        return;
                    }
                    for (int i = tuple.f1(); i < actionState.getParallelism(); i += currentParallelism) {
                        states.add(actionState.getSubtaskStates().get(i));
                    }
                });
        }
        checkpointManager.sendOperationToMemberNode(new NotifyTaskRestoreOperation(taskLocation, states));
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
        scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
    }

    public InvocationFuture<?>[] notifyTaskStart() {
        return plan.getPipelineSubtasks()
            .stream()
            .map(NotifyTaskStartOperation::new)
            .map(checkpointManager::sendOperationToMemberNode)
            .toArray(InvocationFuture[]::new);
    }

    private void scheduleTriggerPendingCheckpoint(long delayMills) {
        scheduler.schedule(() -> tryTriggerPendingCheckpoint(), delayMills, TimeUnit.MILLISECONDS);
    }

    protected void tryTriggerPendingCheckpoint() {
        tryTriggerPendingCheckpoint(CHECKPOINT_TYPE);
    }

    protected void readyToClose(TaskLocation taskLocation) {
        readyToCloseStartingTask.add(taskLocation);
        if (readyToCloseStartingTask.size() == plan.getStartingSubtasks().size()) {
            tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
        }
    }

    protected void tryTriggerPendingCheckpoint(CheckpointType checkpointType) {
        synchronized (lock) {
            if (isCompleted() || isShutdown()) {
                LOG.warn(String.format("can't trigger checkpoint with type: %s, because checkpoint coordinator already have completed checkpoint", checkpointType));
                return;
            }
            final long currentTimestamp = Instant.now().toEpochMilli();
            if (checkpointType.equals(CHECKPOINT_TYPE)) {
                if (currentTimestamp - latestTriggerTimestamp.get() < coordinatorConfig.getCheckpointInterval() ||
                    pendingCounter.get() >= coordinatorConfig.getMaxConcurrentCheckpoints() || !isAllTaskReady) {
                    return;
                }
            } else {
                shutdown = true;
                waitingPendingCheckpointDone();
            }
            CompletableFuture<PendingCheckpoint> pendingCheckpoint = createPendingCheckpoint(currentTimestamp, checkpointType);
            startTriggerPendingCheckpoint(pendingCheckpoint);
            pendingCounter.incrementAndGet();
            scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void waitingPendingCheckpointDone() {
        while (pendingCounter.get() != 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new SeaTunnelEngineException(e);
            }
        }
    }

    private boolean canTriggered() {
        return !isCompleted() && !isShutdown();
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public static Map<Long, Integer> getPipelineTasks(Set<TaskLocation> pipelineSubtasks) {
        return pipelineSubtasks.stream()
            .collect(Collectors.groupingBy(TaskLocation::getTaskVertexId, Collectors.toList()))
            .entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
    }

    public PassiveCompletableFuture<CompletedCheckpoint> startSavepoint() {
        CompletableFuture<PendingCheckpoint> savepoint = createPendingCheckpoint(Instant.now().toEpochMilli(), CheckpointType.SAVEPOINT_TYPE);
        startTriggerPendingCheckpoint(savepoint);
        return savepoint.join().getCompletableFuture();
    }

    private void startTriggerPendingCheckpoint(CompletableFuture<PendingCheckpoint> pendingCompletableFuture) {
        pendingCompletableFuture.thenAcceptAsync(pendingCheckpoint -> {
            LOG.info("wait checkpoint completed: " + pendingCheckpoint.getCheckpointId());
            PassiveCompletableFuture<CompletedCheckpoint> completableFuture = pendingCheckpoint.getCompletableFuture();
            completableFuture.thenAcceptAsync(this::completePendingCheckpoint);

            // Trigger the barrier and wait for all tasks to ACK
            LOG.debug("trigger checkpoint barrier {}/{}/{}, {}", pendingCheckpoint.getJobId(), pendingCheckpoint.getPipelineId(), pendingCheckpoint.getCheckpointId(), pendingCheckpoint.getCheckpointType());
            CompletableFuture<InvocationFuture<?>[]> completableFutureArray = CompletableFuture.supplyAsync(() ->
                    new CheckpointBarrier(pendingCheckpoint.getCheckpointId(),
                        pendingCheckpoint.getCheckpointTimestamp(),
                        pendingCheckpoint.getCheckpointType()))
                .thenApplyAsync(this::triggerCheckpoint);

            try {
                CompletableFuture.allOf(completableFutureArray).get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                LOG.error(ExceptionUtils.getMessage(e));
                return;
            }

            LOG.info("Start a scheduled task to prevent checkpoint timeouts");
            scheduler.schedule(() -> {
                    // If any task is not acked within the checkpoint timeout
                    if (pendingCheckpoints.get(pendingCheckpoint.getCheckpointId()) != null && !pendingCheckpoint.isFullyAcknowledged()) {
                        if (tolerableFailureCheckpoints-- <= 0) {
                            cleanPendingCheckpoint(CheckpointFailureReason.CHECKPOINT_EXPIRED);
                            checkpointManager.handleCheckpointTimeout(pipelineId);
                        }
                    }
                }, coordinatorConfig.getCheckpointTimeout(),
                TimeUnit.MILLISECONDS
            );
        });
    }

    CompletableFuture<PendingCheckpoint> createPendingCheckpoint(long triggerTimestamp, CheckpointType checkpointType) {
        synchronized (lock) {
            CompletableFuture<Long> idFuture;
            if (!checkpointType.equals(COMPLETED_POINT_TYPE)) {
                idFuture = CompletableFuture.supplyAsync(() -> {
                    try {
                        // this must happen outside the coordinator-wide lock,
                        // because it communicates with external services
                        // (in HA mode) and may block for a while.
                        return checkpointIdCounter.getAndIncrement();
                    } catch (Throwable e) {
                        throw new CompletionException(e);
                    }
                });
            } else {
                idFuture = CompletableFuture.supplyAsync(() -> Barrier.PREPARE_CLOSE_BARRIER_ID);
            }
            return triggerPendingCheckpoint(triggerTimestamp, idFuture, checkpointType);
        }
    }

    CompletableFuture<PendingCheckpoint> triggerPendingCheckpoint(long triggerTimestamp, CompletableFuture<Long> idFuture, CheckpointType checkpointType) {
        assert Thread.holdsLock(lock);
        latestTriggerTimestamp.set(triggerTimestamp);
        return idFuture.thenApplyAsync(checkpointId ->
            new PendingCheckpoint(this.jobId,
                this.plan.getPipelineId(),
                checkpointId,
                triggerTimestamp,
                checkpointType,
                getNotYetAcknowledgedTasks(),
                getTaskStatistics(),
                getActionStates())
        ).thenApplyAsync(pendingCheckpoint -> {
            pendingCheckpoints.put(pendingCheckpoint.getCheckpointId(), pendingCheckpoint);
            return pendingCheckpoint;
        });
    }

    private Set<Long> getNotYetAcknowledgedTasks() {
        // TODO: some tasks have completed and don't need to be ack
        Set<Long> set = new CopyOnWriteArraySet<>();
        Set<Long> threadUnsafe = plan.getPipelineSubtasks()
            .stream().map(TaskLocation::getTaskID)
            .collect(Collectors.toSet());
        set.addAll(threadUnsafe);
        return set;
    }

    private Map<Long, ActionState> getActionStates() {
        // TODO: some tasks have completed and will not submit state again.
        return plan.getPipelineActions().entrySet()
            .stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new ActionState(String.valueOf(entry.getKey()), entry.getValue())));
    }

    private Map<Long, TaskStatistics> getTaskStatistics() {
        // TODO: some tasks have completed and don't need to be ack
        return this.pipelineTasks.entrySet()
            .stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new TaskStatistics(entry.getKey(), entry.getValue())));
    }

    public InvocationFuture<?>[] triggerCheckpoint(CheckpointBarrier checkpointBarrier) {
        // TODO: some tasks have completed and don't need to trigger
        return plan.getStartingSubtasks()
            .stream()
            .map(taskLocation -> new CheckpointBarrierTriggerOperation(checkpointBarrier, taskLocation))
            .map(checkpointManager::sendOperationToMemberNode)
            .toArray(InvocationFuture[]::new);
    }

    protected void cleanPendingCheckpoint(CheckpointFailureReason failureReason) {
        synchronized (lock) {
            pendingCheckpoints.values().forEach(pendingCheckpoint ->
                pendingCheckpoint.abortCheckpoint(failureReason, null)
            );
            // TODO: clear related future & scheduler task
            pendingCheckpoints.clear();
            pendingCounter.set(0);
            scheduler.shutdownNow();
            scheduler = Executors.newScheduledThreadPool(
                1, runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setDaemon(true);
                    thread.setName(String.format("checkpoint-coordinator-%s/%s", pipelineId, jobId));
                    return thread;
                });
            isAllTaskReady = false;
        }
    }

    protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        final long checkpointId = ackOperation.getBarrier().getId();
        final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        TaskLocation location = ackOperation.getTaskLocation();
        LOG.info("task[{}]({}/{}) ack. {}", location.getTaskID(), location.getPipelineId(), location.getJobId(), ackOperation.getBarrier().toString());
        if (ackOperation.getBarrier().getCheckpointType() == COMPLETED_POINT_TYPE) {
            synchronized (lock) {
                if (pendingCheckpoints.get(Barrier.PREPARE_CLOSE_BARRIER_ID) == null) {
                    CompletableFuture<PendingCheckpoint> future = triggerPendingCheckpoint(
                        Instant.now().toEpochMilli(),
                        CompletableFuture.completedFuture(Barrier.PREPARE_CLOSE_BARRIER_ID),
                        COMPLETED_POINT_TYPE);
                    startTriggerPendingCheckpoint(future);
                    future.join();
                }
            }
            pendingCheckpoints.values().parallelStream()
                .forEach(cp -> cp.acknowledgeTask(ackOperation.getTaskLocation(), ackOperation.getStates(), SubtaskStatus.AUTO_PREPARE_CLOSE));
            return;
        } else if (pendingCheckpoint == null) {
            LOG.info("job: {}, pipeline: {}, the checkpoint({}) don't exist.", jobId, pipelineId, checkpointId);
            return;
        }

        pendingCheckpoint.acknowledgeTask(location, ackOperation.getStates(),
            CheckpointType.SAVEPOINT_TYPE == pendingCheckpoint.getCheckpointType() ?
                SubtaskStatus.SAVEPOINT_PREPARE_CLOSE :
                SubtaskStatus.RUNNING);
    }

    public void completePendingCheckpoint(CompletedCheckpoint completedCheckpoint) {
        LOG.debug("pending checkpoint({}/{}@{}) completed! cost: {}, trigger: {}, completed: {}",
                completedCheckpoint.getCheckpointId(), completedCheckpoint.getPipelineId(), completedCheckpoint.getJobId(),
                completedCheckpoint.getCompletedTimestamp() - completedCheckpoint.getCheckpointTimestamp(), completedCheckpoint.getCheckpointTimestamp(), completedCheckpoint.getCompletedTimestamp());
        pendingCounter.decrementAndGet();
        final long checkpointId = completedCheckpoint.getCheckpointId();
        pendingCheckpoints.remove(checkpointId);
        if (pendingCheckpoints.size() + 1 == coordinatorConfig.getMaxConcurrentCheckpoints()) {
            // latest checkpoint completed time > checkpoint interval
            tryTriggerPendingCheckpoint();
        }
        completedCheckpoints.addLast(completedCheckpoint);
        try {
            byte[] states = serializer.serialize(completedCheckpoint);
            checkpointStorage.storeCheckPoint(PipelineState.builder()
                .checkpointId(checkpointId)
                .jobId(String.valueOf(jobId))
                .pipelineId(pipelineId)
                .states(states)
                .build());
            if (completedCheckpoints.size() > coordinatorConfig.getStorage().getMaxRetainedCheckpoints()) {
                CompletedCheckpoint superfluous = completedCheckpoints.removeFirst();
                checkpointStorage.deleteCheckpoint(
                    String.valueOf(superfluous.getJobId()),
                    String.valueOf(superfluous.getPipelineId()),
                    String.valueOf(superfluous.getCheckpointId()));
            }
        } catch (IOException | CheckpointStorageException e) {
            sneakyThrow(e);
        }
        LOG.info("pending checkpoint({}/{}@{}) notify finished!", completedCheckpoint.getCheckpointId(), completedCheckpoint.getPipelineId(), completedCheckpoint.getJobId());
        InvocationFuture<?>[] invocationFutures = notifyCheckpointCompleted(checkpointId);
        CompletableFuture.allOf(invocationFutures).join();
        // TODO: notifyCheckpointCompleted fail
        latestCompletedCheckpoint = completedCheckpoint;
        if (isCompleted()) {
            cleanPendingCheckpoint(CheckpointFailureReason.CHECKPOINT_COORDINATOR_COMPLETED);
        }
    }

    public InvocationFuture<?>[] notifyCheckpointCompleted(long checkpointId) {
        return plan.getPipelineSubtasks()
            .stream()
            .map(taskLocation -> new CheckpointFinishedOperation(taskLocation, checkpointId, true))
            .map(checkpointManager::sendOperationToMemberNode)
            .toArray(InvocationFuture[]::new);
    }

    public boolean isCompleted() {
        if (latestCompletedCheckpoint == null) {
            return false;
        }
        return latestCompletedCheckpoint.getCheckpointType() == COMPLETED_POINT_TYPE;
    }
}
