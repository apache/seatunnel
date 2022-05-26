/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.scheduler;

import org.apache.seatunnel.engine.execution.TaskExecution;
import org.apache.seatunnel.engine.executionplan.ExecutionPlan;
import org.apache.seatunnel.engine.executionplan.ExecutionSubTask;
import org.apache.seatunnel.engine.executionplan.ExecutionTask;
import org.apache.seatunnel.engine.task.TaskExecutionState;

import java.util.List;

public class StreamScheduler implements SchedulerStrategy {
    private ExecutionPlan executionPlan;

    public StreamScheduler(ExecutionPlan baseExecutionPlan) {
        this.executionPlan = baseExecutionPlan;
    }

    @Override
    public void startScheduling(TaskExecution taskExecution) {
        List<ExecutionTask> tasks = executionPlan.getExecutionTasks();
        requestAllSlotAndScheduler(tasks, taskExecution);
    }

    private void requestAllSlotAndScheduler(List<ExecutionTask> tasks, TaskExecution taskExecution) {
        // TODO The committed only after all the task resources have been requested
        for (ExecutionTask executionTask : tasks) {
            for (ExecutionSubTask subTask : executionTask.getSubTasks()) {
                subTask.deploy(taskExecution);
            }
        }
    }

    @Override
    public boolean updateExecutionState(TaskExecutionState state) {
        return executionPlan.updateExecutionState(state);
    }
}
