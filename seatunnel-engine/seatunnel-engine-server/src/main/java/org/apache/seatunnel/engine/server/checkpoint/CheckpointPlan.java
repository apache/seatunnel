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

import org.apache.seatunnel.engine.server.execution.TaskLocation;

import com.hazelcast.jet.datamodel.Tuple2;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/** checkpoint plan info */
@Getter
@Builder(builderClassName = "Builder")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CheckpointPlan {

    public static final Integer COORDINATOR_INDEX = -1;

    private final int pipelineId;

    /** All task locations of the pipeline. */
    private final Set<TaskLocation> pipelineSubtasks;

    /** All starting task of a pipeline. */
    private final Set<TaskLocation> startingSubtasks;

    /**
     * All actions in this pipeline. <br>
     * key: the action state key; <br>
     * value: the parallelism of the action;
     */
    private final Map<ActionStateKey, Integer> pipelineActions;

    /**
     * <br>
     * key: the subtask locations; <br>
     * value: all actions in this subtask; f0: action state key, f1: action index;
     */
    private final Map<TaskLocation, Set<Tuple2<ActionStateKey, Integer>>> subtaskActions;

    public static final class Builder {
        private final Set<TaskLocation> pipelineSubtasks = new CopyOnWriteArraySet<>();
        private final Set<TaskLocation> startingSubtasks = new CopyOnWriteArraySet<>();
        private final Map<ActionStateKey, Integer> pipelineActions = new ConcurrentHashMap<>();

        private final Map<TaskLocation, Set<Tuple2<ActionStateKey, Integer>>> subtaskActions =
                new ConcurrentHashMap<>();

        private Builder() {}

        public Builder pipelineSubtasks(Set<TaskLocation> pipelineTaskIds) {
            this.pipelineSubtasks.addAll(pipelineTaskIds);
            return this;
        }

        public Builder startingSubtasks(Set<TaskLocation> startingVertices) {
            this.startingSubtasks.addAll(startingVertices);
            return this;
        }

        public Builder pipelineActions(Map<ActionStateKey, Integer> pipelineActions) {
            this.pipelineActions.putAll(pipelineActions);
            return this;
        }

        public Builder subtaskActions(
                Map<TaskLocation, Set<Tuple2<ActionStateKey, Integer>>> subtaskActions) {
            this.subtaskActions.putAll(subtaskActions);
            return this;
        }
    }
}
