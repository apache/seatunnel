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

import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageUnavailableException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.job.Job;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeAfterCheckpointOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeBeforeCheckpointOperation;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;
import org.apache.seatunnel.engine.server.utils.CheckpointRestoreUtils;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to manage all checkpoints for a job.
 *
 * <p>Maintain the life cycle of the {@link CheckpointCoordinator} through the {@link
 * CheckpointPlan} and the status of the job.
 */
@Slf4j
public class CheckpointManager {

    private final Long jobId;

    private final NodeEngine nodeEngine;

    /**
     * key: the pipeline id of the job; <br>
     * value: the checkpoint coordinator of the pipeline;
     */
    private final Map<Integer, CheckpointCoordinator> coordinatorMap;

    private final CheckpointStorage checkpointStorage;

    private final CheckpointConfig checkpointConfig;

    private final JobMaster jobMaster;

    private final CheckpointMonitorService checkpointMonitorService;

    private final Serializer serializer = new ProtoStuffSerializer();

    public CheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            NodeEngine nodeEngine,
            JobMaster jobMaster,
            Map<Integer, CheckpointPlan> checkpointPlanMap,
            CheckpointConfig checkpointConfig,
            CheckpointStorage checkpointStorage,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            SeaTunnelEngineContext engineContext,
            CheckpointMonitorService checkpointMonitorService) {
        this(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                false,
                nodeEngine,
                jobMaster,
                checkpointPlanMap,
                checkpointConfig,
                checkpointStorage,
                executorService,
                runningJobStateIMap,
                engineContext,
                checkpointMonitorService);
    }

    /**
     * @param masterFailoverRestore true when restoring after a master switch/restart; prefer this
     *     job's latest completed checkpoint, falling back to the requested source only when absent.
     */
    public CheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            boolean masterFailoverRestore,
            NodeEngine nodeEngine,
            JobMaster jobMaster,
            Map<Integer, CheckpointPlan> checkpointPlanMap,
            CheckpointConfig checkpointConfig,
            CheckpointStorage checkpointStorage,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            SeaTunnelEngineContext engineContext,
            CheckpointMonitorService checkpointMonitorService) {
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.jobMaster = jobMaster;
        this.checkpointStorage = checkpointStorage;
        this.checkpointConfig = checkpointConfig;
        this.checkpointMonitorService = checkpointMonitorService;
        CounterStateStore<String> checkpointCounterStore =
                engineContext.getStateStores().checkpointCounterStore();

        Map<Integer, PipelineState> recoveryStates = new HashMap<>();
        if (checkpointConfig.isCheckpointEnable() && isRestoreJob) {
            // Read and validate every pipeline before constructing coordinators or changing
            // counters.
            long deadline =
                    System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(
                                    checkpointConfig.getCheckpointTimeout());
            try {
                for (CheckpointPlan plan : checkpointPlanMap.values()) {
                    PipelineState state =
                            masterFailoverRestore
                                    ? latestRecoveryStateWithRetry(
                                            jobId, plan.getPipelineId(), null, deadline)
                                    : null;
                    if (state == null && restoreSourceJobId != null) {
                        state =
                                masterFailoverRestore
                                        ? latestRecoveryStateWithRetry(
                                                restoreSourceJobId,
                                                plan.getPipelineId(),
                                                restoreMode,
                                                deadline)
                                        : getLatestCheckpointStateByType(
                                                String.valueOf(restoreSourceJobId),
                                                String.valueOf(plan.getPipelineId()),
                                                restoreMode);
                    }
                    if (state != null) {
                        recoveryStates.put(plan.getPipelineId(), state);
                    }
                }
            } catch (Exception failure) {
                ExceptionUtil.sneakyThrow(failure);
            }
        }
        if (jobMaster != null) {
            // Order effectful construction against coordinator retirement and explicit
            // cancellation.
            synchronized (jobMaster.getCoordinatorService()) {
                try {
                    if (masterFailoverRestore) {
                        checkRecoveryActive();
                    } else {
                        jobMaster.checkInitializationActive();
                    }
                } catch (InterruptedException retired) {
                    ExceptionUtil.sneakyThrow(retired);
                }
                this.coordinatorMap =
                        createCoordinators(
                                checkpointPlanMap,
                                recoveryStates,
                                checkpointCounterStore,
                                executorService,
                                runningJobStateIMap,
                                isRestoreJob,
                                restoreMode);
            }
        } else {
            this.coordinatorMap =
                    createCoordinators(
                            checkpointPlanMap,
                            recoveryStates,
                            checkpointCounterStore,
                            executorService,
                            runningJobStateIMap,
                            isRestoreJob,
                            restoreMode);
        }
    }

    private Map<Integer, CheckpointCoordinator> createCoordinators(
            Map<Integer, CheckpointPlan> checkpointPlanMap,
            Map<Integer, PipelineState> recoveryStates,
            CounterStateStore<String> checkpointCounterStore,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            boolean isRestoreJob,
            RestoreMode restoreMode) {
        return MDCTracer.tracing(checkpointPlanMap.values().parallelStream())
                .map(
                        plan -> {
                            StateStoreCheckpointIDCounter idCounter =
                                    new StateStoreCheckpointIDCounter(
                                            jobId, plan.getPipelineId(), checkpointCounterStore);
                            try {
                                idCounter.start();
                                PipelineState pipelineState =
                                        recoveryStates.get(plan.getPipelineId());
                                if (pipelineState != null) {
                                    long checkpointId = pipelineState.getCheckpointId();
                                    idCounter.setCount(Math.max(idCounter.get(), checkpointId + 1));
                                    log.info(
                                            "pipeline({}) restore with {} on checkpointId({})",
                                            plan.getPipelineId(),
                                            restoreMode,
                                            checkpointId);
                                }
                                return new CheckpointCoordinator(
                                        this,
                                        checkpointStorage,
                                        checkpointConfig,
                                        jobId,
                                        plan,
                                        idCounter,
                                        pipelineState,
                                        executorService,
                                        runningJobStateIMap,
                                        isRestoreJob,
                                        checkpointMonitorService);
                            } catch (Exception e) {
                                ExceptionUtil.sneakyThrow(e);
                            }
                            throw new RuntimeException("Never throw here.");
                        })
                .collect(
                        Collectors.toMap(
                                CheckpointCoordinator::getPipelineId, Function.identity()));
    }

    /**
     * Retries only an explicitly unavailable initial read; cancellation/demotion ends admission.
     */
    private PipelineState latestRecoveryStateWithRetry(
            long sourceJobId, int pipelineId, RestoreMode mode, long deadline) throws Exception {
        CheckpointStorageUnavailableException lastFailure = null;
        while (true) {
            checkRecoveryActive();
            if (lastFailure != null && System.nanoTime() >= deadline) {
                throw lastFailure;
            }
            try {
                PipelineState state = latestRecoveryState(sourceJobId, pipelineId, mode);
                checkRecoveryActive();
                return state;
            } catch (CheckpointStorageUnavailableException unavailable) {
                lastFailure = unavailable;
                checkRecoveryActive();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw unavailable;
                }
                log.warn(
                        "Checkpoint storage unavailable during master takeover for job {}, retrying recovery read",
                        jobId);
                TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(500)));
            }
        }
    }

    /**
     * A retired coordinator or terminal job must not publish recovery state after a blocked read.
     */
    private void checkRecoveryActive() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Checkpoint recovery interrupted");
        }
        if (jobMaster != null) {
            jobMaster.checkRecoveryActive();
            Object state = jobMaster.getRunningJobStateIMap().get(jobId);
            if (state == null || state instanceof JobStatus && ((JobStatus) state).isEndState()) {
                throw new InterruptedException("Job no longer requires master takeover recovery");
            }
        }
    }

    /**
     * Validates every retained entry before selecting a checkpoint. Only a successful empty lookup
     * permits the caller to fall back from the current job to the original restore source; storage
     * failures and corrupt/foreign entries must never be interpreted as an empty history.
     */
    private PipelineState latestRecoveryState(long sourceJobId, int pipelineId, RestoreMode mode)
            throws Exception {
        List<PipelineState> states =
                checkpointStorage.getCheckpointsByJobIdAndPipelineId(
                        String.valueOf(sourceJobId), String.valueOf(pipelineId));
        for (PipelineState state : states) {
            if (!String.valueOf(sourceJobId).equals(state.getJobId())
                    || pipelineId != state.getPipelineId()) {
                throw new IllegalStateException(
                        "Checkpoint identity does not match the recovering pipeline");
            }
            deserializeCheckpoint(state);
        }
        return states.stream()
                .filter(
                        state ->
                                mode == null
                                        || !mode.isRestore()
                                        || CheckpointRestoreUtils.matchesRestoreCheckpointType(
                                                deserializeCheckpoint(state).getCheckpointType(),
                                                mode))
                .max(Comparator.comparingLong(PipelineState::getCheckpointId))
                .orElse(null);
    }

    private PipelineState getLatestCheckpointStateByType(
            String sourceJobId, String pipelineId, RestoreMode restoreMode) throws Exception {
        if (restoreMode == null || !restoreMode.isRestore()) {
            return checkpointStorage.getLatestCheckpointByJobIdAndPipelineId(
                    sourceJobId, pipelineId);
        }
        List<PipelineState> pipelineStates =
                checkpointStorage.getCheckpointsByJobIdAndPipelineId(sourceJobId, pipelineId);
        return pipelineStates.stream()
                .filter(
                        state ->
                                CheckpointRestoreUtils.matchesRestoreCheckpointType(
                                        deserializeCheckpoint(state).getCheckpointType(),
                                        restoreMode))
                .max(Comparator.comparingLong(PipelineState::getCheckpointId))
                .orElse(null);
    }

    private CompletedCheckpoint deserializeCheckpoint(PipelineState pipelineState) {
        try {
            return serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Trigger savepoints and return futures that complete when the pending savepoint checkpoints
     * are acknowledged.
     */
    @SuppressWarnings("unchecked")
    public PassiveCompletableFuture<CompletedCheckpoint>[] triggerSavePoints() {
        return MDCTracer.tracing(coordinatorMap.values().parallelStream())
                .map(CheckpointCoordinator::startSavepoint)
                .toArray(PassiveCompletableFuture[]::new);
    }

    /**
     * Trigger savepoints and return futures that complete after every checkpoint coordinator
     * reaches a stable terminal state.
     */
    @SuppressWarnings("unchecked")
    public PassiveCompletableFuture<CheckpointCoordinatorState>[]
            triggerSavePointsAndWaitComplete() {
        return MDCTracer.tracing(coordinatorMap.values().parallelStream())
                .map(CheckpointCoordinator::startSavepointAndWaitComplete)
                .toArray(PassiveCompletableFuture[]::new);
    }

    public void reportedPipelineRunning(int pipelineId, boolean alreadyStarted) {
        log.debug(
                "reported pipeline running stack: {}",
                Arrays.toString(Thread.currentThread().getStackTrace()));
        getCheckpointCoordinator(pipelineId).restoreCoordinator(alreadyStarted);
        if (!alreadyStarted && checkpointMonitorService != null) {
            checkpointMonitorService.onPipelineRestored(jobId, pipelineId);
        }
    }

    protected void handleCheckpointError(int pipelineId, boolean neverRestore) {
        jobMaster.handleCheckpointError(pipelineId, neverRestore);
    }

    private CheckpointCoordinator getCheckpointCoordinator(TaskLocation taskLocation) {
        return getCheckpointCoordinator(taskLocation.getPipelineId());
    }

    public void reportCheckpointErrorFromTask(TaskLocation taskLocation, String errorMsg) {
        getCheckpointCoordinator(taskLocation).reportCheckpointErrorFromTask(errorMsg);
    }

    public CheckpointCoordinator getCheckpointCoordinator(int pipelineId) {
        CheckpointCoordinator coordinator = coordinatorMap.get(pipelineId);
        if (coordinator == null) {
            throw new RuntimeException(
                    String.format("The checkpoint coordinator(%s) don't exist", pipelineId));
        }
        return coordinator;
    }

    /**
     * Called by the {@link Task}. <br>
     * used by Task to report the {@link SeaTunnelTaskState} of the state machine.
     */
    public void reportedTask(TaskReportStatusOperation reportStatusOperation) {
        // task address may change during restore.
        log.debug(
                "reported task({}) status {}",
                reportStatusOperation.getLocation().getTaskID(),
                reportStatusOperation.getStatus());
        getCheckpointCoordinator(reportStatusOperation.getLocation())
                .reportedTask(reportStatusOperation);
    }

    /**
     * Called by the {@link SourceSplitEnumeratorTask}. <br>
     * used by SourceSplitEnumeratorTask to tell CheckpointCoordinator pipeline will trigger close
     * barrier by SourceSplitEnumeratorTask.
     */
    public void readyToClose(TaskLocation taskLocation) {
        getCheckpointCoordinator(taskLocation).readyToClose(taskLocation);
    }

    /**
     * Called by the {@link SourceSplitEnumeratorTask}. <br>
     * used by SourceSplitEnumeratorTask to tell CheckpointCoordinator pipeline will trigger close
     * barrier of idle task by SourceSplitEnumeratorTask.
     */
    public void readyToCloseIdleTask(TaskLocation taskLocation) {
        getCheckpointCoordinator(taskLocation).readyToCloseIdleTask(taskLocation);
    }

    /**
     * Called by the JobMaster. <br>
     * Listen to the {@link PipelineStatus} of the {@link Pipeline}, which is used to shut down the
     * running {@link CheckpointIDCounter} at the end of the pipeline.
     */
    public CompletableFuture<Void> listenPipeline(int pipelineId, PipelineStatus pipelineStatus) {
        return getCheckpointCoordinator(pipelineId)
                .getCheckpointIdCounter()
                .shutdown(pipelineStatus);
    }

    /**
     * Called by the JobMaster. <br>
     * Listen to the {@link JobStatus} of the {@link Job}.
     */
    public void clearCheckpointIfNeed(JobStatus jobStatus) {
        if (checkpointConfig.isCheckpointEnable()
                && (jobStatus == JobStatus.FINISHED || jobStatus == JobStatus.CANCELED)
                && !isSavePointEnd()) {
            if (jobStatus == JobStatus.CANCELED && checkpointConfig.isRetainAfterJobCancelled()) {
                log.info(
                        "Job {} has retain-after-job-cancelled enabled, retaining checkpoint data",
                        jobId);
            } else {
                checkpointStorage.deleteCheckpoint(jobId + "");
            }
        }
        if (checkpointMonitorService != null
                && (jobStatus == JobStatus.FINISHED || jobStatus == JobStatus.CANCELED)) {
            checkpointMonitorService.cleanupJob(jobId);
        }
    }

    /**
     * Called by the JobMaster. <br>
     * Returns whether the pipeline has completed; No need to deploy/restore the {@link SubPlan} if
     * the pipeline has been completed;
     */
    public boolean isCompletedPipeline(int pipelineId) {
        return getCheckpointCoordinator(pipelineId).isNoErrorCompleted();
    }

    /**
     * Called by the {@link Task}. <br>
     * used for the ack of the checkpoint, including the state snapshot of all {@link Action} within
     * the {@link Task}.
     */
    public void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        log.debug("checkpoint manager received ack {}", ackOperation.getTaskLocation());
        CheckpointCoordinator coordinator =
                getCheckpointCoordinator(ackOperation.getTaskLocation());
        if (coordinator.isCompleted()) {
            log.info(
                    "The checkpoint coordinator({}) is completed",
                    ackOperation.getTaskLocation().getPipelineId());
            return;
        }
        coordinator.acknowledgeTask(ackOperation);
    }

    public void triggerSchemaChangeBeforeCheckpoint(
            TriggerSchemaChangeBeforeCheckpointOperation operation) {
        log.debug(
                "checkpoint manager received schema-change-before checkpoint operation {}",
                operation.getTaskLocation());
        CheckpointCoordinator coordinator = getCheckpointCoordinator(operation.getTaskLocation());
        if (coordinator.isCompleted()) {
            log.info(
                    "The checkpoint coordinator({}) is completed",
                    operation.getTaskLocation().getPipelineId());
            return;
        }

        coordinator.scheduleSchemaChangeBeforeCheckpoint();
    }

    public void triggerSchemaChangeAfterCheckpoint(
            TriggerSchemaChangeAfterCheckpointOperation operation) {
        log.debug(
                "checkpoint manager received schema-change-after checkpoint operation {}",
                operation.getTaskLocation());
        CheckpointCoordinator coordinator = getCheckpointCoordinator(operation.getTaskLocation());
        if (coordinator.isCompleted()) {
            log.info(
                    "The checkpoint coordinator({}) is completed",
                    operation.getTaskLocation().getPipelineId());
            return;
        }

        coordinator.scheduleSchemaChangeAfterCheckpoint();
    }

    public boolean isSavePointEnd() {
        return coordinatorMap.values().stream()
                .map(CheckpointCoordinator::isEndOfSavePoint)
                .reduce((v1, v2) -> v1 && v2)
                .orElse(false);
    }

    public boolean isPipelineSavePointEnd(PipelineLocation pipelineLocation) {
        return coordinatorMap.get(pipelineLocation.getPipelineId()).isEndOfSavePoint();
    }

    protected InvocationFuture<?> sendOperationToMemberNode(TaskOperation operation) {
        log.debug(
                "Send Operation : "
                        + operation.getClass().getSimpleName()
                        + " to "
                        + jobMaster.queryTaskGroupAddress(
                                operation.getTaskLocation().getTaskGroupLocation())
                        + " for task group:"
                        + operation.getTaskLocation().getTaskGroupLocation());
        return NodeEngineUtil.sendOperationToMemberNode(
                nodeEngine,
                operation,
                jobMaster.queryTaskGroupAddress(
                        operation.getTaskLocation().getTaskGroupLocation()));
    }

    /**
     * Call By JobMaster If all the tasks canceled or some task failed, JobMaster will call this
     * method to cancel checkpoint coordinator.
     *
     * @param pipelineId
     * @return
     */
    public PassiveCompletableFuture<CheckpointCoordinatorState> cancelCheckpoint(int pipelineId) {
        return getCheckpointCoordinator(pipelineId).cancelCheckpoint();
    }

    /**
     * Call By JobMaster If all the tasks is finished, JobMaster will call this method to wait
     * checkpoint coordinator complete.
     *
     * @param pipelineId
     * @return
     */
    public PassiveCompletableFuture<CheckpointCoordinatorState> waitCheckpointCoordinatorComplete(
            int pipelineId) {
        return getCheckpointCoordinator(pipelineId).waitCheckpointCoordinatorComplete();
    }
}
