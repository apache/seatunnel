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
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.checkpoint.storage.common.Serializer;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
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

    private final CheckpointStorageConfiguration storageConfig;

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

    private final ConcurrentHashMap<Long, PendingCheckpoint> pendingCheckpoints;

    private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    private CompletedCheckpoint latestCompletedCheckpoint;

    private final CheckpointCoordinatorConfiguration coordinatorConfig;

    private int tolerableFailureCheckpoints;
    private final transient ScheduledExecutorService scheduler;

    private final AtomicLong latestTriggerTimestamp = new AtomicLong(0);

    private final AtomicInteger pendingCounter = new AtomicInteger(0);

    private final Object lock = new Object();

    private final Object autoSavepointLock = new Object();
    public CheckpointCoordinator(CheckpointManager manager,
                                 CheckpointStorage checkpointStorage,
                                 CheckpointStorageConfiguration storageConfig,
                                 long jobId,
                                 CheckpointPlan plan,
                                 CheckpointCoordinatorConfiguration coordinatorConfig) {

        this.checkpointManager = manager;
        this.checkpointStorage = checkpointStorage;
        this.storageConfig = storageConfig;
        this.jobId = jobId;
        this.pipelineId = plan.getPipelineId();
        this.plan = plan;
        this.coordinatorConfig = coordinatorConfig;
        this.latestCompletedCheckpoint = plan.getRestoredCheckpoint();
        this.tolerableFailureCheckpoints = coordinatorConfig.getTolerableFailureCheckpoints();
        this.pendingCheckpoints = new ConcurrentHashMap<>();
        this.completedCheckpoints = new ArrayDeque<>(storageConfig.getMaxRetainedCheckpoints() + 1);
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
        // TODO: IDCounter SPI
        this.checkpointIdCounter = new StandaloneCheckpointIDCounter();
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
            final ActionState actionState = latestCompletedCheckpoint.getTaskStates().get(taskLocation.getTaskVertexId());
            for (int i = taskLocation.getTaskIndex(); i < actionState.getParallelism(); i += currentParallelism) {
                states.add(actionState.getSubtaskStates()[i]);
            }
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
        scheduler.schedule(this::tryTriggerPendingCheckpoint, delayMills, TimeUnit.MILLISECONDS);
    }

    private void tryTriggerPendingCheckpoint() {
        synchronized (lock) {
            final long currentTimestamp = Instant.now().toEpochMilli();
            if (currentTimestamp - latestTriggerTimestamp.get() >= coordinatorConfig.getCheckpointInterval() &&
                pendingCounter.get() < coordinatorConfig.getMaxConcurrentCheckpoints()) {
                CompletableFuture<PendingCheckpoint> pendingCheckpoint = createPendingCheckpoint(currentTimestamp, CheckpointType.CHECKPOINT_TYPE);
                startTriggerPendingCheckpoint(pendingCheckpoint);
                pendingCounter.incrementAndGet();
                scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
            }
        }
    }

    public static Map<Long, Integer> getPipelineTasks(Set<TaskLocation> pipelineSubtasks) {
        return pipelineSubtasks.stream()
            .collect(Collectors.groupingBy(TaskLocation::getTaskVertexId, Collectors.toList()))
            .entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
    }

    public PassiveCompletableFuture<PendingCheckpoint> startSavepoint() {
        CompletableFuture<PendingCheckpoint> savepoint = createPendingCheckpoint(Instant.now().toEpochMilli(), CheckpointType.SAVEPOINT_TYPE);
        startTriggerPendingCheckpoint(savepoint);
        return new PassiveCompletableFuture<>(savepoint);
    }

    private void startTriggerPendingCheckpoint(CompletableFuture<PendingCheckpoint> pendingCompletableFuture) {
        // Trigger the barrier and wait for all tasks to ACK
        pendingCompletableFuture.thenAcceptAsync(pendingCheckpoint -> {
            if (CheckpointType.AUTO_SAVEPOINT_TYPE != pendingCheckpoint.getCheckpointType()) {
                LOG.debug("trigger checkpoint barrier" + pendingCheckpoint);
                CompletableFuture.supplyAsync(() ->
                        new CheckpointBarrier(pendingCheckpoint.getCheckpointId(),
                            pendingCheckpoint.getCheckpointTimestamp(),
                            pendingCheckpoint.getCheckpointType()))
                    .thenApplyAsync(this::triggerCheckpoint)
                    .thenApplyAsync(invocationFutures -> CompletableFuture.allOf(invocationFutures).join());
            }
            LOG.debug("wait checkpoint completed: " + pendingCheckpoint);
            pendingCheckpoint.getCompletableFuture()
                .thenAcceptAsync(this::completePendingCheckpoint);
        });

        // If any task is not acked within the checkpoint timeout
        pendingCompletableFuture.thenAcceptAsync(pendingCheckpoint -> {
            LOG.debug("Start a scheduled task to prevent checkpoint timeouts");
            scheduler.schedule(() -> {
                    if (pendingCheckpoints.get(pendingCheckpoint.getCheckpointId()) != null && !pendingCheckpoint.isFullyAcknowledged()) {
                        if (tolerableFailureCheckpoints-- <= 0) {
                            cleanPendingCheckpoint();
                            // TODO: notify job master to restore the pipeline.
                        }
                    }
                }, coordinatorConfig.getCheckpointTimeout(),
                TimeUnit.MILLISECONDS
            );
        });
    }

    CompletableFuture<PendingCheckpoint> createPendingCheckpoint(long triggerTimestamp, CheckpointType checkpointType) {
        CompletableFuture<Long> idFuture = CompletableFuture.supplyAsync(() -> {
            try {
                // this must happen outside the coordinator-wide lock,
                // because it communicates with external services
                // (in HA mode) and may block for a while.
                return checkpointIdCounter.getAndIncrement();
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        });
        return createPendingCheckpoint(triggerTimestamp, idFuture, checkpointType);
    }

    CompletableFuture<PendingCheckpoint> createPendingCheckpoint(long triggerTimestamp, CompletableFuture<Long> idFuture, CheckpointType checkpointType) {
        latestTriggerTimestamp.set(triggerTimestamp);
        CompletableFuture<PendingCheckpoint> completableFuture = new CompletableFuture<>();
        return idFuture.thenApplyAsync(checkpointId ->
            new PendingCheckpoint(this.jobId,
                this.plan.getPipelineId(),
                checkpointId,
                triggerTimestamp,
                checkpointType,
                getNotYetAcknowledgedTasks(),
                getTaskStatistics(),
                getActionStates(),
                completableFuture)
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

    protected void cleanPendingCheckpoint() {
        // TODO: clear related future & scheduler task
        pendingCheckpoints.clear();
    }

    protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        final long checkpointId = ackOperation.getBarrier().getId();
        final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        TaskLocation location = ackOperation.getTaskLocation();
        LOG.debug("task[{}]({}/{}) ack. {}", location.getTaskID(), location.getPipelineId(), location.getJobId(), ackOperation.getBarrier().toString());
        if (checkpointId == Barrier.PREPARE_CLOSE_BARRIER_ID) {
            synchronized (autoSavepointLock) {
                if (pendingCheckpoints.get(checkpointId) == null) {
                    CompletableFuture<PendingCheckpoint> future = createPendingCheckpoint(
                        Instant.now().toEpochMilli(),
                        CompletableFuture.completedFuture(Barrier.PREPARE_CLOSE_BARRIER_ID),
                        CheckpointType.AUTO_SAVEPOINT_TYPE);
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

    public void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) {
        LOG.info("pending checkpoint({}/{}@{}) completed!", pendingCheckpoint.getCheckpointId(), pendingCheckpoint.getPipelineId(), pendingCheckpoint.getJobId());
        pendingCounter.decrementAndGet();
        final long checkpointId = pendingCheckpoint.getCheckpointId();
        CompletedCheckpoint completedCheckpoint = pendingCheckpoint.toCompletedCheckpoint();
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
            if (completedCheckpoints.size() > storageConfig.getMaxRetainedCheckpoints()) {
                CompletedCheckpoint superfluous = completedCheckpoints.removeFirst();
                checkpointStorage.deleteCheckpoint(
                    String.valueOf(superfluous.getJobId()),
                    String.valueOf(superfluous.getPipelineId()),
                    String.valueOf(superfluous.getCheckpointId()));
            }
        } catch (IOException | CheckpointStorageException e) {
            sneakyThrow(e);
        }
        InvocationFuture<?>[] invocationFutures = notifyCheckpointCompleted(checkpointId);
        CompletableFuture.allOf(invocationFutures).join();
        // TODO: notifyCheckpointCompleted fail
        latestCompletedCheckpoint = completedCheckpoint;
    }

    public InvocationFuture<?>[] notifyCheckpointCompleted(long checkpointId) {
        return plan.getPipelineSubtasks()
            .stream()
            .map(taskLocation -> new CheckpointFinishedOperation(taskLocation, checkpointId, true))
            .map(checkpointManager::sendOperationToMemberNode)
            .toArray(InvocationFuture[]::new);
    }
}
