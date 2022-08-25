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

import org.apache.seatunnel.engine.core.checkpoint.Checkpoint;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.TaskInfo;

import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private final long pipelineId;

    private final CheckpointManager checkpointManager;

    private final CheckpointPlan plan;

    private final Map<Long, PendingCheckpoint> pendingCheckpoints;

    private final Map<Long, CompletedCheckpoint> completedCheckpoints;

    private final CheckpointCoordinatorConfiguration config;

    public CheckpointCoordinator(CheckpointManager manager,
                                 long jobId,
                                 CheckpointPlan plan,
                                 CheckpointCoordinatorConfiguration config) {
        this.checkpointManager = manager;
        this.jobId = jobId;
        this.pipelineId = plan.getPipelineId();
        this.plan = plan;
        this.config = config;
        this.pendingCheckpoints = new LinkedHashMap<>();
        this.completedCheckpoints = new LinkedHashMap<>();
    }

    protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
        final long checkpointId = ackOperation.getCheckpointId();
        final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        if (pendingCheckpoint == null) {
            LOG.debug("job: {}, pipeline: {}, the checkpoint({}) don't exist.", jobId, pipelineId, checkpointId);
            return;
        }
        pendingCheckpoint.acknowledgeTask(ackOperation.getTaskInfo(), ackOperation.getStates());
        if (pendingCheckpoint.isFullyAcknowledged()) {
            CompletedCheckpoint completedCheckpoint = completePendingCheckpoint(pendingCheckpoint);
            pendingCheckpoints.remove(checkpointId);
            completedCheckpoints.put(checkpointId, completedCheckpoint);
        }
    }

    public CompletedCheckpoint completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) {
        notifyCheckpointCompleted(pendingCheckpoint.getCheckpointId());
        return new CompletedCheckpoint(
            jobId,
            pipelineId,
            pendingCheckpoint.getCheckpointId(),
            pendingCheckpoint.getCheckpointTimestamp(),
            System.currentTimeMillis(),
            pendingCheckpoint.getTaskStates(),
            pendingCheckpoint.getTaskStatistics());
    }

    public List<InvocationFuture> notifyCheckpointCompleted(long checkpointId) {
        return plan.getPipelineTaskIds()
            .entrySet()
            .stream()
            .map(entry ->
                new CheckpointFinishedOperation(checkpointId,
                    new TaskInfo(jobId, entry.getValue(), entry.getKey()),
                    true)
            ).map(checkpointManager::notifyCheckpointFinished)
            .collect(Collectors.toList());
    }

    protected void taskCompleted(TaskInfo taskInfo) {
        pendingCheckpoints.values()
            .forEach(cp -> cp.taskCompleted(taskInfo));
    }
}
