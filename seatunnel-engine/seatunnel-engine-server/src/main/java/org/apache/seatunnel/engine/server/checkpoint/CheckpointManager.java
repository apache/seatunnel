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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to manage all checkpoints for a job.
 * <p>
 * Maintain the life cycle of the {@link CheckpointCoordinator} through the {@link CheckpointPlan} and the status of the job.
 * </p>
 */
public class CheckpointManager {

    private final Long jobId;

    private final NodeEngine nodeEngine;

    private final Map<Long, Address> subtaskWithAddresses;

    /**
     * key: the pipeline id of the job;
     * <br> value: the checkpoint plan of the pipeline;
     */
    private final Map<Integer, CheckpointPlan> checkpointPlanMap;

    /**
     * key: the pipeline id of the job;
     * <br> value: the checkpoint coordinator of the pipeline;
     */
    private final Map<Integer, CheckpointCoordinator> coordinatorMap;

    private final CheckpointStorage checkpointStorage;
    private final CheckpointCoordinatorConfiguration coordinatorConfig;

    public CheckpointManager(long jobId,
                             NodeEngine nodeEngine,
                             Map<Integer, CheckpointPlan> checkpointPlanMap,
                             CheckpointCoordinatorConfiguration coordinatorConfig,
                             CheckpointStorageConfiguration storageConfig) throws CheckpointStorageException {
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.checkpointPlanMap = checkpointPlanMap;
        this.coordinatorConfig = coordinatorConfig;
        this.coordinatorMap = new HashMap<>(checkpointPlanMap.size());
        this.subtaskWithAddresses = new HashMap<>();
        this.checkpointStorage = FactoryUtil.discoverFactory(Thread.currentThread().getContextClassLoader(), CheckpointStorageFactory.class, storageConfig.getStorage())
            .create(new HashMap<>());
    }

    /**
     * Called by the JobMaster, actually triggered by the user.
     * <br> After the savepoint is triggered, it will cause the job to stop automatically.
     */
    @SuppressWarnings("unchecked")
    public PassiveCompletableFuture<PendingCheckpoint>[] triggerSavepoints() {
        return coordinatorMap.values()
            .parallelStream()
            .map(CheckpointCoordinator::startSavepoint)
            .toArray(PassiveCompletableFuture[]::new);
    }

    /**
     * Called by the JobMaster, actually triggered by the user.
     * <br> After the savepoint is triggered, it will cause the pipeline to stop automatically.
     */
    public PassiveCompletableFuture<PendingCheckpoint> triggerSavepoint(int pipelineId) {
        return coordinatorMap.get(pipelineId).startSavepoint();
    }

    private CheckpointCoordinator getCheckpointCoordinator(TaskLocation taskLocation) {
        return coordinatorMap.get(taskLocation.getPipelineId());
    }

    /**
     * Called by the {@link Task}.
     * <br> used by Task to report the {@link SeaTunnelTaskState} of the state machine.
     */
    public void reportedTask(TaskReportStatusOperation reportStatusOperation, Address address) {
        // task address may change during restore.
        subtaskWithAddresses.put(reportStatusOperation.getLocation().getTaskID(), address);
        getCheckpointCoordinator(reportStatusOperation.getLocation()).reportedTask(reportStatusOperation);
    }

    /**
     * Called by the JobMaster.
     * <br> Listen to the {@link ExecutionState} of the {@link TaskGroup}, which is used to cancel the running {@link PendingCheckpoint} when the task group is abnormal.
     */
    public void listenTaskGroup(TaskGroupLocation groupLocation, ExecutionState executionState) {
        if (jobId != groupLocation.getJobId()) {
            return;
        }
        switch (executionState) {
            case FAILED:
            case CANCELED:
            case CANCELING:
                coordinatorMap.get(groupLocation.getPipelineId()).cleanPendingCheckpoint();
                return;
            default:
        }
    }

    /**
     * Called by the {@link Task}.
     * <br> used for the ack of the checkpoint, including the state snapshot of all {@link Action} within the {@link Task}.
     */
    public void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        getCheckpointCoordinator(ackOperation.getTaskLocation()).acknowledgeTask(ackOperation);
    }

    protected InvocationFuture<?> sendOperationToMemberNode(TaskOperation operation) {
        return NodeEngineUtil.sendOperationToMemberNode(nodeEngine, operation, subtaskWithAddresses.get(operation.getTaskLocation().getTaskID()));
    }
}
