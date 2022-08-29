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
import org.apache.seatunnel.engine.server.execution.TaskInfo;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PendingCheckpoint implements Checkpoint {

    private final long jobId;

    private final int pipelineId;

    private final long checkpointId;

    private final long triggerTimestamp;

    private final Set<Long> notYetAcknowledgedTasks;

    private final Map<Long, TaskStatistics> taskStatistics;

    private final Map<Long, TaskState> taskStates;

    public PendingCheckpoint(long jobId,
                             int pipelineId,
                             long checkpointId,
                             long triggerTimestamp,
                             Set<Long> notYetAcknowledgedTasks,
                             Map<Long, TaskState> taskStates,
                             Map<Long, Integer> allVertices) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.checkpointId = checkpointId;
        this.triggerTimestamp = triggerTimestamp;
        this.notYetAcknowledgedTasks = notYetAcknowledgedTasks;
        this.taskStates = taskStates;
        this.taskStatistics = allVertices.entrySet()
            .stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new TaskStatistics(entry.getKey(), entry.getValue())));
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

    protected Map<Long, TaskStatistics> getTaskStatistics() {
        return taskStatistics;
    }

    protected Map<Long, TaskState> getTaskStates() {
        return taskStates;
    }

    public void acknowledgeTask(TaskInfo taskInfo, byte[] states) {
        notYetAcknowledgedTasks.remove(taskInfo.getSubtaskId());
        TaskStatistics statistics = taskStatistics.get(taskInfo.getJobVertexId());
        statistics.reportSubtaskStatistics(new SubtaskStatistics(
            taskInfo.getIndex(),
            System.currentTimeMillis(),
            states.length));
        TaskState taskState = taskStates.get(taskInfo.getJobVertexId());
        if (taskState == null) {
            return;
        }
        taskState.reportState(taskInfo.getIndex(), states);
    }

    public void taskCompleted(TaskInfo taskInfo) {
        taskStatistics.get(taskInfo.getJobVertexId())
            .completed(taskInfo.getIndex());
    }

    public boolean isFullyAcknowledged() {
        return notYetAcknowledgedTasks.size() == 0;
    }
}
