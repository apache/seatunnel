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
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointData;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointHandle;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointWriter;
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
import org.apache.seatunnel.engine.server.savepoint.serialization.SavepointReaderRegistry;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;
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

    /** Savepoint bundle capability; null when the storage plugin does not support it. */
    private final SavepointStorage savepointStorage;

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
            SavepointStorage savepointStorage,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            SeaTunnelEngineContext engineContext,
            CheckpointMonitorService checkpointMonitorService) {
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.jobMaster = jobMaster;
        this.checkpointStorage = checkpointStorage;
        this.savepointStorage = savepointStorage;
        this.checkpointConfig = checkpointConfig;
        this.checkpointMonitorService = checkpointMonitorService;
        CounterStateStore<String> checkpointCounterStore =
                engineContext.getStateStores().checkpointCounterStore();

        this.coordinatorMap =
                MDCTracer.tracing(checkpointPlanMap.values().parallelStream())
                        .map(
                                plan -> {
                                    StateStoreCheckpointIDCounter idCounter =
                                            new StateStoreCheckpointIDCounter(
                                                    jobId,
                                                    plan.getPipelineId(),
                                                    checkpointCounterStore);
                                    try {
                                        idCounter.start();
                                        PipelineState pipelineState = null;
                                        if (checkpointConfig.isCheckpointEnable()
                                                && isRestoreJob
                                                && restoreSourceJobId != null) {
                                            pipelineState =
                                                    getLatestCheckpointStateByType(
                                                            String.valueOf(restoreSourceJobId),
                                                            String.valueOf(plan.getPipelineId()),
                                                            restoreMode);
                                            if (pipelineState != null) {
                                                long checkpointId = pipelineState.getCheckpointId();
                                                idCounter.setCount(checkpointId + 1);
                                                log.info(
                                                        "pipeline({}) restore with {} on checkpointId({})",
                                                        plan.getPipelineId(),
                                                        restoreMode,
                                                        checkpointId);
                                            }
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

    public SavepointStorage getSavepointStorage() {
        return savepointStorage;
    }

    private PipelineState getLatestCheckpointStateByType(
            String sourceJobId, String pipelineId, RestoreMode restoreMode) throws Exception {
        if (restoreMode == RestoreMode.SAVEPOINT && savepointStorage != null) {
            PipelineState bundleState = getPipelineStateFromLatestBundle(sourceJobId, pipelineId);
            if (bundleState != null) {
                return bundleState;
            }
            // No bundle: fall through to the legacy CP-directory scan (best-effort).
            log.warn(
                    "No savepoint bundle found for job {}, fall back to legacy checkpoint-directory restore",
                    sourceJobId);
        }
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

    /**
     * Reads the newest completed savepoint bundle of the restore source job and returns the
     * requested pipeline payload normalized to the legacy runtime byte layout.
     */
    private PipelineState getPipelineStateFromLatestBundle(String sourceJobId, String pipelineId)
            throws Exception {
        List<SavepointHandle> handles = savepointStorage.listCompletedSavepoints(sourceJobId);
        if (handles.isEmpty()) {
            return null;
        }
        SavepointHandle newest = handles.get(0);
        SavepointData data = savepointStorage.readSavepoint(sourceJobId, newest.getSavepointId());
        Map<Integer, byte[]> payloads = new HashMap<>();
        data.getPipelineStates().forEach((pid, state) -> payloads.put(pid, state.getStates()));
        Map<Integer, CompletedCheckpoint> restored =
                SavepointReaderRegistry.forVersion(data.getMeta()).read(data.getMeta(), payloads);
        PipelineState pipelineState = data.getPipelineStates().get(Integer.valueOf(pipelineId));
        CompletedCheckpoint checkpoint = restored.get(Integer.valueOf(pipelineId));
        if (pipelineState == null || checkpoint == null) {
            throw new RuntimeException(
                    String.format(
                            "Savepoint bundle %s of job %s has no payload for pipeline %s",
                            newest.getSavepointId(), sourceJobId, pipelineId));
        }
        // engine-wire payload -> runtime CompletedCheckpoint -> legacy byte layout expected by
        // CheckpointCoordinator.
        pipelineState.setStates(serializer.serialize(checkpoint));
        return pipelineState;
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
     * reaches a stable terminal state. All coordinators write into the same savepoint attempt via
     * the given writer (engine-wire v1 payloads); a null writer keeps the legacy behavior.
     */
    @SuppressWarnings("unchecked")
    public PassiveCompletableFuture<CheckpointCoordinatorState>[] triggerSavePointsAndWaitComplete(
            SavepointWriter writer) {
        coordinatorMap.values().forEach(coordinator -> coordinator.setSavepointWriter(writer));
        return triggerSavePointsAndWaitComplete();
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

    /** Pipeline ids of the job; a savepoint bundle must cover exactly these pipelines. */
    public Set<Integer> getPipelineIds() {
        return coordinatorMap.keySet();
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
