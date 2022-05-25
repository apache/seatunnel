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

import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.transform.Transformation;
import org.apache.seatunnel.engine.execution.TaskExecution;

import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.Executor;

public class ExecutionSubTask {

    private static final Logger LOG = BaseExecutionPlan.LOG;

    private ExecutionTask executionTask;

    private final int subTaskIndex;

    private List<Transformation> transformations;

    private SinkWriter sinkWriter;

    private SourceReader sourceReader;

    private final String taskNameWithSubtask;

    private Execution currentExecution; // this field must never be null

    public ExecutionSubTask(Executor executor, ExecutionTask executionTask, int subTaskIndex, SourceReader sourceReader, List<Transformation> transformations, SinkWriter sinkWriter) {
        this.executionTask = executionTask;
        this.subTaskIndex = subTaskIndex;
        this.transformations = transformations;
        this.sinkWriter = sinkWriter;
        this.sourceReader = sourceReader;

        this.taskNameWithSubtask =
            String.format(
                "%s (%d/%d)",
                executionTask.getTaskName(),
                subTaskIndex + 1,
                executionTask.getParallelism());

        this.currentExecution = new Execution(executor, this, 1);
        this.executionTask.getExecutionPlan().addExecution(this.currentExecution);
    }

    public void deploy(TaskExecution taskExecution) {
        try {
            currentExecution.deploy(taskExecution);
        } catch (JobException e) {
            e.printStackTrace();
        }
    }

    public String getTaskNameWithSubtask() {
        return taskNameWithSubtask;
    }

    public ExecutionTask getExecutionTask() {
        return executionTask;
    }

    public int getSubTaskIndex() {
        return subTaskIndex;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public SinkWriter getSinkWriter() {
        return sinkWriter;
    }

    public SourceReader getSourceReader() {
        return sourceReader;
    }

    public Execution getCurrentExecution() {
        return currentExecution;
    }

    public void executionFinished(Execution execution) {
        LOG.info("The SubTask {} ({}/{}) finished", executionTask.getTaskName(), subTaskIndex + 1, executionTask.getParallelism());
        executionTask.executionTaskFinished();
    }
}
