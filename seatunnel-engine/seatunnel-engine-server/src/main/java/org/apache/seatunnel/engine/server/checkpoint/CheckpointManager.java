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

import org.apache.seatunnel.engine.server.NodeEngineUtil;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointTriggerOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.TaskInfo;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Used to manage all checkpoints for a job.
 * <p>
 * Maintain the life cycle of the {@link CheckpointCoordinator} through the {@link CheckpointPlan} and the status of the job.
 * </p>
 */
public class CheckpointManager {

    private final Long jobId;

    private final NodeEngine nodeEngine;

    private final Map<Long, Integer> subtaskWithPipelines;

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

    private final CheckpointCoordinatorConfiguration config;

    public CheckpointManager(long jobId, NodeEngine nodeEngine, Map<Integer, CheckpointPlan> checkpointPlanMap, CheckpointCoordinatorConfiguration config) {
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.checkpointPlanMap = checkpointPlanMap;
        this.config = config;
        this.coordinatorMap = new HashMap<>(checkpointPlanMap.size());
        this.subtaskWithPipelines = checkpointPlanMap.values().stream().flatMap(plan -> plan.getPipelineTaskIds().keySet().stream().map(taskId -> Tuple2.tuple2(taskId, plan.getPipelineId()))).collect(Collectors.toMap(Tuple2::f0, Tuple2::f1));
        this.subtaskWithAddresses = new HashMap<>(subtaskWithPipelines.size());
    }

    private int getPipelineId(long subtaskId) {
        return subtaskWithPipelines.get(subtaskId);
    }

    private CheckpointCoordinator getCheckpointCoordinator(TaskInfo taskInfo) {
        return coordinatorMap.get(getPipelineId(taskInfo.getSubtaskId()));
    }

    public void acknowledgeTask(TaskAcknowledgeOperation ackOperation, Address address) {
        getCheckpointCoordinator(ackOperation.getTaskInfo()).acknowledgeTask(ackOperation);
        subtaskWithAddresses.putIfAbsent(ackOperation.getTaskInfo().getSubtaskId(), address);
    }

    public void taskCompleted(TaskInfo taskInfo) {
        getCheckpointCoordinator(taskInfo).taskCompleted(taskInfo);
    }

    public InvocationFuture<?> triggerCheckpoint(CheckpointTriggerOperation operation) {
        return NodeEngineUtil.sendOperationToMasterNode(nodeEngine, operation);
    }

    public InvocationFuture<?> notifyCheckpointFinished(CheckpointFinishedOperation operation) {
        return NodeEngineUtil.sendOperationToMemberNode(nodeEngine, operation, subtaskWithAddresses.get(operation.getTaskInfo().getSubtaskId()));
    }
}
