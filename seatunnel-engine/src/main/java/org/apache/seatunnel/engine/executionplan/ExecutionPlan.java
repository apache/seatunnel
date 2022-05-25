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

package org.apache.seatunnel.engine.executionplan;

import org.apache.seatunnel.engine.api.common.JobStatus;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.logicalplan.LogicalTask;
import org.apache.seatunnel.engine.task.TaskExecutionState;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public interface ExecutionPlan {
    ScheduledExecutorService getExecutorService();

    List<ExecutionTask> getExecutionTasks();

    Configuration getConfiguration();

    JobInformation getJobInformation();

    void initExecutionTasks(List<LogicalTask> logicalTasks);

    void turnToRunning();

    boolean updateJobState(JobStatus current, JobStatus newState);

    boolean updateExecutionState(TaskExecutionState state);

    CompletableFuture<JobStatus> getJobEndCompletableFuture();

    void removeExecution(Execution execExecution);

    void addExecution(Execution execution);

    void executionTaskFinish();
}
