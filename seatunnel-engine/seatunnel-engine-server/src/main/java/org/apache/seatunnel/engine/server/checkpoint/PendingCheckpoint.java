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

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.Checkpoint;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
public class PendingCheckpoint implements Checkpoint {

    private final long jobId;

    private final int pipelineId;

    private final long checkpointId;

    private final long triggerTimestamp;

    private final CheckpointType checkpointType;

    private final Set<Long> notYetAcknowledgedTasks;

    private final Map<Long, TaskStatistics> taskStatistics;

    private final Map<Long, ActionState> actionStates;

    private final CompletableFuture<PendingCheckpoint> completableFuture;

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public int getPipelineId() {
        return this.pipelineId;
    }

    @Override
    public long getJobId() {
        return this.jobId;
    }

    @Override
    public long getCheckpointTimestamp() {
        return this.triggerTimestamp;
    }

    @Override
    public CheckpointType getCheckpointType() {
        return this.checkpointType;
    }

    protected Map<Long, TaskStatistics> getTaskStatistics() {
        return taskStatistics;
    }

    protected Map<Long, ActionState> getActionStates() {
        return actionStates;
    }

    public PassiveCompletableFuture<PendingCheckpoint> getCompletableFuture() {
        return new PassiveCompletableFuture<>(completableFuture);
    }

    public void acknowledgeTask(TaskLocation taskLocation, List<ActionSubtaskState> states, SubtaskStatus subtaskStatus) {
        boolean exist = notYetAcknowledgedTasks.remove(taskLocation.getTaskID());
        if (!exist) {
            return;
        }
        TaskStatistics statistics = taskStatistics.get(taskLocation.getTaskVertexId());

        long stateSize = 0;
        for (ActionSubtaskState state : states) {
            ActionState actionState = actionStates.get(state.getActionId());
            if (actionState == null) {
                return;
            }
            stateSize += state.getState().stream().map(s -> s.length).count();
            actionState.reportState(state.getIndex(), state);
        }
        statistics.reportSubtaskStatistics(new SubtaskStatistics(
            taskLocation.getTaskIndex(),
            Instant.now().toEpochMilli(),
            stateSize,
            subtaskStatus));

        if (isFullyAcknowledged()) {
            completableFuture.complete(this);
        }
    }

    protected boolean isFullyAcknowledged() {
        return notYetAcknowledgedTasks.size() == 0;
    }

    public CompletedCheckpoint toCompletedCheckpoint() {
        return new CompletedCheckpoint(
            jobId,
            pipelineId,
            checkpointId,
            triggerTimestamp,
            checkpointType,
            System.currentTimeMillis(),
            actionStates,
            taskStatistics);
    }
}
