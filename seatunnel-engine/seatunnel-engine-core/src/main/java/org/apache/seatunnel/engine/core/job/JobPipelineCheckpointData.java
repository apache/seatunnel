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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
public class JobPipelineCheckpointData implements Serializable {
    private long jobId;
    private int pipelineId;
    private long checkpointId;
    private long triggerTimestamp;
    private CheckpointType checkpointType;
    private long completedTimestamp;
    private Map<String, ActionState> taskStates;

    @Tolerate
    public JobPipelineCheckpointData() {}

    @Data
    @AllArgsConstructor
    public static class ActionState implements Serializable {
        private List<byte[]> coordinatorState;
        private List<ActionSubtaskState> subtaskState;
    }

    @Data
    @AllArgsConstructor
    public static class ActionSubtaskState implements Serializable {
        private final int index;
        private final List<byte[]> state;
    }
}
