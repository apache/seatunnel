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

import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.TaskGroup;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel
 * Each {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some PhysicalVertex.
 * And the number of PhysicalVertex equals the {@link ExecutionVertex#getParallelism()}.
 * <p>
 * When PhysicalVertex is schedule, It will create a {@link PhysicalExecutionVertex} and call the {@link PhysicalExecutionVertex#deploy()}
 * method. If a TaskGroup failed, PhysicalVertex will create a new PhysicalExecutionVertex and deploy it to retry the TaskGroup.
 */
public class PhysicalVertex {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalVertex.class);

    /**
     * the index of PhysicalVertex
     */
    private final int subTaskGroupIndex;

    private final String taskNameWithSubtask;

    private final int parallelism;

    private PhysicalExecutionVertex currentExecutionVertex; // this field must never be null

    private final TaskGroup taskGroup;

    private final PhysicalPlan physicalPlan;

    public PhysicalVertex(int subTaskGroupIndex, int parallelism, @NonNull TaskGroup taskGroup, @NonNull PhysicalPlan physicalPlan) {
        this.subTaskGroupIndex = subTaskGroupIndex;
        this.parallelism = parallelism;
        this.taskGroup = taskGroup;
        this.taskNameWithSubtask =
            String.format(
                "%s (%d/%d)",
                taskGroup.getTaskGroupName(),
                subTaskGroupIndex + 1,
                parallelism);

        this.currentExecutionVertex = new PhysicalExecutionVertex();
        this.physicalPlan = physicalPlan;
        this.physicalPlan.addExecution(currentExecutionVertex);
    }

    public void deploy() throws JobException {
        currentExecutionVertex.deploy();
    }

    public void executionFinished() {
        LOGGER.info(String.format("The SubTask {} ({x}/{}) finished", taskGroup.getTaskGroupName(), subTaskGroupIndex + 1,
            parallelism));
        executionTask.executionVertexFinished();
    }
}
