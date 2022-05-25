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

import org.apache.seatunnel.engine.api.common.JobStatus;
import org.apache.seatunnel.engine.execution.TaskExecution;
import org.apache.seatunnel.engine.executionplan.ExecutionPlan;
import org.apache.seatunnel.engine.executionplan.ExecutionSubTask;
import org.apache.seatunnel.engine.executionplan.ExecutionTask;
import org.apache.seatunnel.engine.task.TaskExecutionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BatchScheduler implements SchedulerStrategy {

    public final Logger logger = LoggerFactory.getLogger(getClass());

    private ExecutionPlan executionPlan;

    public BatchScheduler(ExecutionPlan baseExecutionPlan) {
        this.executionPlan = baseExecutionPlan;
    }

    @Override
    public void startScheduling(TaskExecution taskExecution) {
        CompletableFuture<JobStatus> jobEndCompletableFuture = executionPlan.getJobEndCompletableFuture();
        executionPlan.turnToRunning();
        List<ExecutionTask> tasks = executionPlan.getExecutionTasks();
        schedulerOneByOne(tasks, taskExecution);
        try {
            jobEndCompletableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e1) {
            throw new RuntimeException(e1);
        }
    }

    private void schedulerOneByOne(List<ExecutionTask> tasks, TaskExecution taskExecution) {
        for (ExecutionTask executionTask : tasks) {
            for (ExecutionSubTask subTask : executionTask.getSubTasks()) {
                subTask.getCurrentExecution().turnState(org.apache.seatunnel.engine.executionplan.ExecutionState.SCHEDULED);
                // TODO request task resource to run
                subTask.deploy(taskExecution);
            }
        }
    }

    @Override
    public boolean updateExecutionState(TaskExecutionState state) {
        return executionPlan.updateExecutionState(state);
    }
}
