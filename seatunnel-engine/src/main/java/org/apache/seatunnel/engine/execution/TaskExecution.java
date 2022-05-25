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

package org.apache.seatunnel.engine.execution;

import org.apache.seatunnel.engine.api.checkpoint.StateBackend;
import org.apache.seatunnel.engine.api.common.JobID;
import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.ExecutionState;
import org.apache.seatunnel.engine.jobmanager.JobMaster;
import org.apache.seatunnel.engine.task.BatchTask;
import org.apache.seatunnel.engine.task.StreamTask;
import org.apache.seatunnel.engine.task.Task;
import org.apache.seatunnel.engine.task.TaskExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TaskExecution {
    protected final Logger logger = LoggerFactory.getLogger(TaskExecution.class);

    private ExecutorService executorService;

    private StateBackend stateBackend;

    private Map<JobID, Future<?>> submitJobMap;

    private Map<JobID, JobMaster> jobMasterMap;

    public TaskExecution(ExecutorService executorService, StateBackend stateBackend) {
        this.executorService = executorService;
        this.stateBackend = stateBackend;
        submitJobMap = new ConcurrentHashMap<>();
        jobMasterMap = new ConcurrentHashMap<>();
    }

    public void submit(TaskInfo taskInfo) {
        JobID jobId = taskInfo.jobInformation.getJobId();

        //INITIALIZING
        updateExecutionState(jobId, taskInfo.executionID, ExecutionState.INITIALIZING, null);
        Task task = buildTask(taskInfo, stateBackend);
        //RUNNING
        updateExecutionState(jobId, taskInfo.executionID, ExecutionState.RUNNING, null);

        task.beforeRun();

        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.runAsync(task, executorService);

        voidCompletableFuture.whenComplete((r, e) -> {
            if (null == e) {
                //FINISHED
                updateExecutionState(jobId, taskInfo.executionID, ExecutionState.FINISHED, null);

            } else {
                //FAILED
                updateExecutionState(jobId, taskInfo.executionID, ExecutionState.FAILED, e);

            }
        });

        submitJobMap.put(jobId, voidCompletableFuture);

    }

    public long aliveTaskSize() {
        return submitJobMap.entrySet().stream().filter(x -> !x.getValue().isDone()).count();
    }

    private Task buildTask(TaskInfo taskInfo, StateBackend stateBackend) {
        return Boundedness.BOUNDED.equals(taskInfo.boundedness) ?
                new BatchTask(
                        taskInfo.jobInformation,
                        taskInfo.executionID,
                        taskInfo.taskId,
                        taskInfo.sourceReader,
                        taskInfo.transformations,
                        taskInfo.sinkWriter
                ) :
                new StreamTask(
                        taskInfo.jobInformation,
                        taskInfo.executionID,
                        taskInfo.taskId,
                        taskInfo.sourceReader,
                        taskInfo.transformations,
                        taskInfo.sinkWriter,
                        stateBackend
                );
    }

    public void registerJobMaster(JobID jobID, JobMaster jobMaster) {
        this.jobMasterMap.put(jobID, jobMaster);
    }

    private void updateExecutionState(JobID jobID, ExecutionId executionID, ExecutionState executionState, Throwable throwable) {
        logger.info("executionID: {}  , state change to  {}", executionID, executionState.toString());
        if (jobMasterMap.containsKey(jobID)) {
            JobMaster jobMaster = jobMasterMap.get(jobID);
            jobMaster.updateExecutionState(new TaskExecutionState(
                    executionID,
                    executionState,
                    throwable
            ));
        } else {
            logger.error("JobID: {}, can not find jobMaster ", jobID);
        }
    }
}
