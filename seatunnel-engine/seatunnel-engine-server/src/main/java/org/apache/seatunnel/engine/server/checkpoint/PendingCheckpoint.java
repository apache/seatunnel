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
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.Checkpoint;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Nullable;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

public class PendingCheckpoint implements Checkpoint {
    private static final Logger LOG = LoggerFactory.getLogger(PendingCheckpoint.class);
    private final long jobId;

    private final int pipelineId;

    private final long checkpointId;

    private final long triggerTimestamp;

    private final CheckpointType checkpointType;

    private final Set<Long> notYetAcknowledgedTasks;

    private final Map<Long, TaskStatistics> taskStatistics;

    private final Map<ActionStateKey, ActionState> actionStates;

    private final CompletableFuture<CompletedCheckpoint> completableFuture;

    @Getter private CheckpointException failureCause;

    @Setter ScheduledFuture<?> checkpointTimeOutFuture;

    public PendingCheckpoint(
            long jobId,
            int pipelineId,
            long checkpointId,
            long triggerTimestamp,
            CheckpointType checkpointType,
            Set<Long> notYetAcknowledgedTasks,
            Map<Long, TaskStatistics> taskStatistics,
            Map<ActionStateKey, ActionState> actionStates) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.checkpointId = checkpointId;
        this.triggerTimestamp = triggerTimestamp;
        this.checkpointType = checkpointType;
        this.notYetAcknowledgedTasks = notYetAcknowledgedTasks;
        this.taskStatistics = taskStatistics;
        this.actionStates = actionStates;
        this.completableFuture = new CompletableFuture<>();
    }

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

    protected Map<ActionStateKey, ActionState> getActionStates() {
        return actionStates;
    }

    public PassiveCompletableFuture<CompletedCheckpoint> getCompletableFuture() {
        return new PassiveCompletableFuture<>(completableFuture);
    }

    public void acknowledgeTask(
            TaskLocation taskLocation,
            List<ActionSubtaskState> states,
            SubtaskStatus subtaskStatus) {
        LOG.debug("acknowledgeTask states [{}]", states);
        boolean exist = notYetAcknowledgedTasks.remove(taskLocation.getTaskID());
        if (!exist) {
            return;
        }
        TaskStatistics statistics = taskStatistics.get(taskLocation.getTaskVertexId());

        long stateSize = 0;
        for (ActionSubtaskState state : states) {
            ActionState actionState = actionStates.get(state.getStateKey());
            if (actionState == null) {
                continue;
            }
            stateSize +=
                    state.getState().stream().filter(Objects::nonNull).map(s -> s.length).count();
            actionState.reportState(state.getIndex(), state);
        }
        statistics.reportSubtaskStatistics(
                new SubtaskStatistics(
                        taskLocation.getTaskIndex(),
                        Instant.now().toEpochMilli(),
                        stateSize,
                        subtaskStatus));

        if (isFullyAcknowledged()) {
            LOG.debug("checkpoint is full ack!");
            completableFuture.complete(toCompletedCheckpoint());
        }
    }

    protected boolean isFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty();
    }

    private CompletedCheckpoint toCompletedCheckpoint() {
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

    public void abortCheckpoint(CheckpointCloseReason closedReason, @Nullable Throwable cause) {
        if (closedReason.equals(CheckpointCloseReason.CHECKPOINT_COORDINATOR_RESET)
                || closedReason.equals(CheckpointCloseReason.PIPELINE_END)) {
            completableFuture.complete(null);
        } else {
            this.failureCause = new CheckpointException(closedReason, cause);
            completableFuture.completeExceptionally(failureCause);
        }
    }

    // Avoid memory leak in ScheduledThreadPoolExecutor due to overly long timeout settings causing
    // numerous completed checkpoints to remain
    public void abortCheckpointTimeoutFutureWhenIsCompleted() {
        if (checkpointTimeOutFuture == null) {
            return;
        }
        checkpointTimeOutFuture.cancel(false);
    }

    public String getInfo() {
        return String.format(
                "%s/%s/%s, %s",
                this.getJobId(),
                this.getPipelineId(),
                this.getCheckpointId(),
                this.getCheckpointType());
    }
}
