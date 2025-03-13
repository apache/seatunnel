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
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.job.Job;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeAfterCheckpointOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeBeforeCheckpointOperation;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;
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

    private final JobMaster jobMaster;

    private final ExecutorService executorService;

    public CheckpointManager(
            long jobId,
            boolean isStartWithSavePoint,
            NodeEngine nodeEngine,
            JobMaster jobMaster,
            Map<Integer, CheckpointPlan> checkpointPlanMap,
            CheckpointConfig checkpointConfig,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap)
            throws CheckpointStorageException {
        this.executorService = executorService;
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.jobMaster = jobMaster;
        this.checkpointStorage =
                FactoryUtil.discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                CheckpointStorageFactory.class,
                                checkpointConfig.getStorage().getStorage())
                        .create(checkpointConfig.getStorage().getStoragePluginConfig());
        this.coordinatorMap =
                MDCTracer.tracing(checkpointPlanMap.values().parallelStream())
                        .map(
                                plan -> {
                                    IMapCheckpointIDCounter idCounter =
                                            new IMapCheckpointIDCounter(
                                                    jobId, plan.getPipelineId(), nodeEngine);
                                    try {
                                        idCounter.start();
                                        PipelineState pipelineState = null;
                                        if (isStartWithSavePoint) {
                                            pipelineState =
                                                    checkpointStorage
                                                            .getLatestCheckpointByJobIdAndPipelineId(
                                                                    String.valueOf(jobId),
                                                                    String.valueOf(
                                                                            plan.getPipelineId()));
                                            if (pipelineState != null) {
                                                long checkpointId = pipelineState.getCheckpointId();
                                                idCounter.setCount(checkpointId + 1);
                                                log.info(
                                                        "pipeline({}) start with savePoint on checkPointId({})",
                                                        plan.getPipelineId(),
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
                                                isStartWithSavePoint);
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
     * Called by the JobMaster, actually triggered by the user. <br>
     * After the savepoint is triggered, it will cause the job to stop automatically.
     */
    @SuppressWarnings("unchecked")
    public PassiveCompletableFuture<CompletedCheckpoint>[] triggerSavePoints() {
        return MDCTracer.tracing(coordinatorMap.values().parallelStream())
                .map(CheckpointCoordinator::startSavepoint)
                .toArray(PassiveCompletableFuture[]::new);
    }

    public void reportedPipelineRunning(int pipelineId, boolean alreadyStarted) {
        log.debug(
                "reported pipeline running stack: {}",
                Arrays.toString(Thread.currentThread().getStackTrace()));
        getCheckpointCoordinator(pipelineId).restoreCoordinator(alreadyStarted);
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

    @VisibleForTesting
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
        if ((jobStatus == JobStatus.FINISHED || jobStatus == JobStatus.CANCELED)
                && !isSavePointEnd()) {
            checkpointStorage.deleteCheckpoint(jobId + "");
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
                "Sead Operation : "
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
