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

import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.job.Job;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to manage all checkpoints for a job.
 * <p>
 * Maintain the life cycle of the {@link CheckpointCoordinator} through the {@link CheckpointPlan} and the status of the job.
 * </p>
 */
@Slf4j
public class CheckpointManager {

    private final Long jobId;

    private final NodeEngine nodeEngine;

    /**
     * key: the pipeline id of the job;
     * <br> value: the checkpoint coordinator of the pipeline;
     */
    private final Map<Integer, CheckpointCoordinator> coordinatorMap;

    private final CheckpointStorage checkpointStorage;

    private final JobMaster jobMaster;

    public CheckpointManager(long jobId,
                             NodeEngine nodeEngine,
                             JobMaster jobMaster,
                             Map<Integer, CheckpointPlan> checkpointPlanMap,
                             CheckpointConfig checkpointConfig) throws CheckpointStorageException {
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.jobMaster = jobMaster;
        this.checkpointStorage =
            FactoryUtil.discoverFactory(Thread.currentThread().getContextClassLoader(), CheckpointStorageFactory.class,
                    checkpointConfig.getStorage().getStorage())
                .create(new ConcurrentHashMap<>());
        IMap<Integer, Long> checkpointIdMap =
            nodeEngine.getHazelcastInstance().getMap(String.format("checkpoint-id-%d", jobId));
        this.coordinatorMap = checkpointPlanMap.values().parallelStream()
            .map(plan -> {
                IMapCheckpointIDCounter idCounter = new IMapCheckpointIDCounter(plan.getPipelineId(), checkpointIdMap);
                try {
                    idCounter.start();
                    // TODO: support savepoint
                    PipelineState pipelineState = null;
                    if (idCounter.get() != CheckpointIDCounter.INITIAL_CHECKPOINT_ID) {
                        pipelineState =
                            checkpointStorage.getCheckpoint(String.valueOf(jobId), String.valueOf(plan.getPipelineId()),
                                String.valueOf(idCounter.get() - 1));
                    }
                    return new CheckpointCoordinator(this,
                        checkpointStorage,
                        checkpointConfig,
                        jobId,
                        plan,
                        idCounter,
                        pipelineState);
                } catch (Exception e) {
                    ExceptionUtil.sneakyThrow(e);
                }
                throw new RuntimeException("Never throw here.");
            }).collect(Collectors.toMap(CheckpointCoordinator::getPipelineId, Function.identity()));
    }

    /**
     * Called by the JobMaster, actually triggered by the user.
     * <br> After the savepoint is triggered, it will cause the job to stop automatically.
     */
    @SuppressWarnings("unchecked")
    public PassiveCompletableFuture<CompletedCheckpoint>[] triggerSavepoints() {
        return coordinatorMap.values()
            .parallelStream()
            .map(CheckpointCoordinator::startSavepoint)
            .toArray(PassiveCompletableFuture[]::new);
    }

    /**
     * Called by the JobMaster, actually triggered by the user.
     * <br> After the savepoint is triggered, it will cause the pipeline to stop automatically.
     */
    public PassiveCompletableFuture<CompletedCheckpoint> triggerSavepoint(int pipelineId) {
        return getCheckpointCoordinator(pipelineId).startSavepoint();
    }

    public void reportedPipelineRunning(int pipelineId) {
        getCheckpointCoordinator(pipelineId).setAllTaskReady(true);
        getCheckpointCoordinator(pipelineId).tryTriggerPendingCheckpoint();
    }

    protected void handleCheckpointTimeout(int pipelineId) {
        jobMaster.handleCheckpointTimeout(pipelineId);
    }

    private CheckpointCoordinator getCheckpointCoordinator(TaskLocation taskLocation) {
        return getCheckpointCoordinator(taskLocation.getPipelineId());
    }

    private CheckpointCoordinator getCheckpointCoordinator(int pipelineId) {
        CheckpointCoordinator coordinator = coordinatorMap.get(pipelineId);
        if (coordinator == null) {
            throw new RuntimeException(String.format("The checkpoint coordinator(%s) don't exist", pipelineId));
        }
        return coordinator;
    }

    /**
     * Called by the {@link Task}.
     * <br> used by Task to report the {@link SeaTunnelTaskState} of the state machine.
     */
    public void reportedTask(TaskReportStatusOperation reportStatusOperation) {
        // task address may change during restore.
        log.debug("reported task({}) status{}", reportStatusOperation.getLocation().getTaskID(),
            reportStatusOperation.getStatus());
        getCheckpointCoordinator(reportStatusOperation.getLocation()).reportedTask(reportStatusOperation);
    }

    /**
     * Called by the {@link SourceSplitEnumeratorTask}.
     * <br> used by SourceSplitEnumeratorTask to tell CheckpointCoordinator pipeline will trigger close barrier by SourceSplitEnumeratorTask.
     */
    public void readyToClose(TaskLocation taskLocation) {
        getCheckpointCoordinator(taskLocation).readyToClose(taskLocation);
    }

    /**
     * Called by the JobMaster.
     * <br> Listen to the {@link PipelineStatus} of the {@link SubPlan}, which is used to cancel the running {@link PendingCheckpoint} when the SubPlan is abnormal.
     */
    public CompletableFuture<Void> listenPipelineRetry(int pipelineId, PipelineStatus pipelineStatus) {
        getCheckpointCoordinator(pipelineId).cleanPendingCheckpoint(CheckpointFailureReason.PIPELINE_END);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Called by the JobMaster.
     * <br> Listen to the {@link PipelineStatus} of the {@link Pipeline}, which is used to shut down the running {@link CheckpointIDCounter} at the end of the pipeline.
     */
    public CompletableFuture<Void> listenPipeline(int pipelineId, PipelineStatus pipelineStatus) {
        return getCheckpointCoordinator(pipelineId).getCheckpointIdCounter().shutdown(pipelineStatus);
    }

    /**
     * Called by the JobMaster.
     * <br> Listen to the {@link JobStatus} of the {@link Job}.
     */
    public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
        if (jobStatus == JobStatus.FINISHED) {
            checkpointStorage.deleteCheckpoint(jobId + "");
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Called by the JobMaster.
     * <br> Returns whether the pipeline has completed; No need to deploy/restore the {@link SubPlan} if the pipeline has been completed;
     */
    public boolean isCompletedPipeline(int pipelineId) {
        return getCheckpointCoordinator(pipelineId).isCompleted();
    }

    /**
     * Called by the {@link Task}.
     * <br> used for the ack of the checkpoint, including the state snapshot of all {@link Action} within the {@link Task}.
     */
    public void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        CheckpointCoordinator coordinator = getCheckpointCoordinator(ackOperation.getTaskLocation());
        if (coordinator.isCompleted()) {
            log.info("The checkpoint coordinator({}) is completed", ackOperation.getTaskLocation().getPipelineId());
            return;
        }
        coordinator.acknowledgeTask(ackOperation);
    }

    protected InvocationFuture<?> sendOperationToMemberNode(TaskOperation operation) {
        return NodeEngineUtil.sendOperationToMemberNode(nodeEngine, operation,
            jobMaster.queryTaskGroupAddress(operation.getTaskLocation().getTaskGroupLocation().getTaskGroupId()));
    }
}
