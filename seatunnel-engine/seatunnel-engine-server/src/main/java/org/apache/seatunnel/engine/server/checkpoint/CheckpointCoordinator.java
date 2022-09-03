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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    private final LinkedHashMap<Long, PendingCheckpoint> pendingCheckpoints;

    private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    private final CheckpointCoordinatorConfiguration coordinatorConfig;

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
        this.pendingCheckpoints = new LinkedHashMap<>();
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
        this.pipelineTaskStatus = plan.getPipelineSubtasks()
            .stream()
            .map(TaskLocation::getTaskID)
            .collect(Collectors.toMap(id -> id, id -> SeaTunnelTaskState.CREATED));

        // TODO: IDCounter SPI
        this.checkpointIdCounter = new StandaloneCheckpointIDCounter();
        scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
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
        pendingCompletableFuture.thenAcceptAsync(pendingCheckpoint -> {
            if (CheckpointType.AUTO_SAVEPOINT_TYPE != pendingCheckpoint.getCheckpointType()) {
                CompletableFuture.supplyAsync(() ->
                        new CheckpointBarrier(pendingCheckpoint.getCheckpointId(),
                            pendingCheckpoint.getCheckpointTimestamp(),
                            pendingCheckpoint.getCheckpointType()))
                    .thenApplyAsync(this::triggerCheckpoint)
                    .thenApplyAsync(invocationFutures -> CompletableFuture.allOf(invocationFutures).join());
            }

            pendingCheckpoint.getCompletableFuture()
                .thenAcceptAsync(this::completePendingCheckpoint);
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
        ).thenApplyAsync(pendingCheckpoint -> pendingCheckpoints.put(pendingCheckpoint.getCheckpointId(), pendingCheckpoint));
    }

    private Set<Long> getNotYetAcknowledgedTasks() {
        // TODO: some tasks have completed and don't need to be ack
        return plan.getPipelineSubtasks()
            .stream().map(TaskLocation::getTaskID)
            .collect(Collectors.toSet());
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
        return plan.getStartingSubtasks()
            .stream()
            .map(taskLocation -> new CheckpointBarrierTriggerOperation(checkpointBarrier, taskLocation))
            .map(checkpointManager::triggerCheckpoint)
            .toArray(InvocationFuture[]::new);
    }

    protected void reportedTask(TaskReportStatusOperation reportStatusOperation) {
        pipelineTaskStatus.put(reportStatusOperation.getLocation().getTaskID(), reportStatusOperation.getStatus());

    }

    protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        final long checkpointId = ackOperation.getBarrier().getId();
        final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        if (pendingCheckpoint == null) {
            LOG.debug("job: {}, pipeline: {}, the checkpoint({}) don't exist.", jobId, pipelineId, checkpointId);
            return;
        } else if (checkpointId == Barrier.PREPARE_CLOSE_BARRIER_ID) {
            PendingCheckpoint autoSavepoint;
            synchronized (autoSavepointLock) {
                autoSavepoint = pendingCheckpoints.get(checkpointId);
                if (autoSavepoint == null) {
                    CompletableFuture<PendingCheckpoint> future = createPendingCheckpoint(
                        Instant.now().toEpochMilli(),
                        CompletableFuture.completedFuture(Barrier.PREPARE_CLOSE_BARRIER_ID),
                        CheckpointType.AUTO_SAVEPOINT_TYPE);
                    future.join();
                    autoSavepoint = pendingCheckpoints.get(checkpointId);
                }
            }
            autoSavepoint.acknowledgeTask(ackOperation.getTaskLocation(), ackOperation.getStates(), SubtaskStatus.AUTO_PREPARE_CLOSE);
            return;
        }

        pendingCheckpoint.acknowledgeTask(ackOperation.getTaskLocation(), ackOperation.getStates(),
            CheckpointType.SAVEPOINT_TYPE == pendingCheckpoint.getCheckpointType() ?
                SubtaskStatus.SAVEPOINT_PREPARE_CLOSE :
                SubtaskStatus.RUNNING);
    }

    public void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) {
        pendingCounter.decrementAndGet();
        final long checkpointId = pendingCheckpoint.getCheckpointId();
        InvocationFuture<?>[] invocationFutures = notifyCheckpointCompleted(checkpointId);
        CompletableFuture.allOf(invocationFutures).join();
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
    }

    public InvocationFuture<?>[] notifyCheckpointCompleted(long checkpointId) {
        return plan.getPipelineSubtasks()
            .stream()
            .map(taskLocation -> new CheckpointFinishedOperation(checkpointId, taskLocation, true))
            .map(checkpointManager::notifyCheckpointFinished)
            .toArray(InvocationFuture[]::new);
    }
}
