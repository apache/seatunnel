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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.server.task.TaskGroupInfo;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.List;

public class SubPlan {
    private static final ILogger LOGGER = Logger.getLogger(SubPlan.class);

    private final List<TaskGroupInfo> tasks;

    private final List<TaskGroupInfo> coordinatorTasks;

    private int currentFinishedVertexNum;

    private final int pipelineIndex;

    private final PhysicalPlan physicalPlan;

    public SubPlan(int pipelineIndex, @NonNull List<TaskGroupInfo> tasks,
                   @NonNull List<TaskGroupInfo> coordinatorTasks, @NonNull PhysicalPlan physicalPlan) {
        this.pipelineIndex = pipelineIndex;
        this.tasks = tasks;
        this.coordinatorTasks = coordinatorTasks;
        this.physicalPlan = physicalPlan;
    }

    public List<TaskGroupInfo> getTasks() {
        return tasks;
    }

    public List<TaskGroupInfo> getCoordinatorTasks() {
        return coordinatorTasks;
    }

    public void vertexFinished() {
        currentFinishedVertexNum++;
        if (currentFinishedVertexNum == (tasks.size() + coordinatorTasks.size())) {
            LOGGER.info(String.format("Pipeline finished {}/{}", pipelineIndex + 1, physicalPlan.getPlans().size()));
            physicalPlan.pipelineFinished();
        }
    }
}
