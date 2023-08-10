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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

public class CompletedCheckpoint implements Checkpoint, Serializable {
    private static final long serialVersionUID = 1L;
    private final long jobId;

    private final int pipelineId;

    private final long checkpointId;

    private final long triggerTimestamp;

    private final CheckpointType checkpointType;

    private final long completedTimestamp;

    private final Map<ActionStateKey, ActionState> taskStates;

    private final Map<Long, TaskStatistics> taskStatistics;

    @Getter @Setter private boolean isRestored = false;

    public CompletedCheckpoint(
            long jobId,
            int pipelineId,
            long checkpointId,
            long triggerTimestamp,
            CheckpointType checkpointType,
            long completedTimestamp,
            Map<ActionStateKey, ActionState> taskStates,
            Map<Long, TaskStatistics> taskStatistics) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.checkpointId = checkpointId;
        this.triggerTimestamp = triggerTimestamp;
        this.checkpointType = checkpointType;
        this.completedTimestamp = completedTimestamp;
        this.taskStates = taskStates;
        this.taskStatistics = taskStatistics;
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

    public long getCompletedTimestamp() {
        return completedTimestamp;
    }

    public Map<ActionStateKey, ActionState> getTaskStates() {
        return taskStates;
    }

    public Map<Long, TaskStatistics> getTaskStatistics() {
        return taskStatistics;
    }
}
