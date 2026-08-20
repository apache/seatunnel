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

import io.protostuff.Tag;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@ToString
public class CompletedCheckpoint implements Checkpoint, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Legacy protostuff field number. Keep this tag stable because completed checkpoint bytes are
     * persisted in checkpoint storage and must remain readable after new fields are added.
     */
    @Tag(1)
    private final long jobId;

    /** Legacy protostuff field number for the pipeline identity. */
    @Tag(2)
    private final int pipelineId;

    /** Legacy protostuff field number for the checkpoint sequence. */
    @Tag(3)
    private final long checkpointId;

    /** Legacy protostuff field number for the trigger time. */
    @Tag(4)
    private final long triggerTimestamp;

    /** Legacy protostuff field number for the checkpoint type. */
    @Tag(5)
    private final CheckpointType checkpointType;

    /** Legacy protostuff field number for the completion time. */
    @Tag(6)
    private final long completedTimestamp;

    /** Legacy protostuff field number for task action states. */
    @Tag(7)
    private final Map<ActionStateKey, ActionState> taskStates;

    /** Legacy protostuff field number for per-task checkpoint statistics. */
    @Tag(8)
    private final Map<Long, TaskStatistics> taskStatistics;

    /**
     * New opt-in checkpoint intent field. It intentionally uses a tag after all legacy fields so
     * old SeaTunnel versions can ignore it when a normal checkpoint is written in the raw legacy
     * format.
     */
    @Tag(10)
    private final CheckpointIntent checkpointIntent;

    /** Legacy protostuff field number for restore bookkeeping. */
    @Tag(9)
    @Getter
    @Setter
    private volatile boolean isRestored = false;

    public CompletedCheckpoint(
            long jobId,
            int pipelineId,
            long checkpointId,
            long triggerTimestamp,
            CheckpointType checkpointType,
            long completedTimestamp,
            Map<ActionStateKey, ActionState> taskStates,
            Map<Long, TaskStatistics> taskStatistics) {
        this(
                jobId,
                pipelineId,
                checkpointId,
                triggerTimestamp,
                checkpointType,
                completedTimestamp,
                taskStates,
                taskStatistics,
                CheckpointIntent.normal(jobId, pipelineId, checkpointId));
    }

    public CompletedCheckpoint(
            long jobId,
            int pipelineId,
            long checkpointId,
            long triggerTimestamp,
            CheckpointType checkpointType,
            long completedTimestamp,
            Map<ActionStateKey, ActionState> taskStates,
            Map<Long, TaskStatistics> taskStatistics,
            CheckpointIntent checkpointIntent) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.checkpointId = checkpointId;
        this.triggerTimestamp = triggerTimestamp;
        this.checkpointType = checkpointType;
        this.completedTimestamp = completedTimestamp;
        this.taskStates = taskStates;
        this.taskStatistics = taskStatistics;
        this.checkpointIntent = checkpointIntent;
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

    public CheckpointIntent getCheckpointIntent() {
        return checkpointIntent == null
                ? CheckpointIntent.normal(jobId, pipelineId, checkpointId)
                : checkpointIntent;
    }
}
