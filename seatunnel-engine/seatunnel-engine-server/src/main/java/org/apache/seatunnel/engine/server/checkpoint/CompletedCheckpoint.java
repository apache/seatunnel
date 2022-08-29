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

import java.io.Serializable;
import java.util.Map;

public class CompletedCheckpoint implements Checkpoint, Serializable {
    private static final long serialVersionUID = 1L;
    private final long jobId;

    private final long pipelineId;

    private final long checkpointId;

    private final long triggerTimestamp;

    private final long completedTimestamp;

    private final Map<Long, TaskState> taskStates;

    private final Map<Long, TaskStatistics> taskStatistics;

    public CompletedCheckpoint(long jobId,
                               long pipelineId,
                               long checkpointId,
                               long triggerTimestamp,
                               long completedTimestamp,
                               Map<Long, TaskState> taskStates,
                               Map<Long, TaskStatistics> taskStatistics) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.checkpointId = checkpointId;
        this.triggerTimestamp = triggerTimestamp;
        this.completedTimestamp = completedTimestamp;
        this.taskStates = taskStates;
        this.taskStatistics = taskStatistics;
    }

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public long getPipelineId() {
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
}
