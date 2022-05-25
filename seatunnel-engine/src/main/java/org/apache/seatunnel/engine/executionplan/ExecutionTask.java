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

import org.apache.seatunnel.engine.api.sink.Sink;
import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.source.Source;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.transform.Transformation;

import org.slf4j.Logger;

import java.util.List;

public class ExecutionTask {

    private static final Logger LOG = BaseExecutionPlan.LOG;

    private final ExecutionPlan executionPlan;

    private final String taskName;

    private final int parallelism;

    private int numSubTaskFinished = 0;

    private final Source source;

    private final List<Transformation> transformations;

    private final Sink sink;

    private ExecutionSubTask[] subTasks;

    public ExecutionTask(String taskName,
                         int parallelism,
                         Source source,
                         List<Transformation> transformations,
                         Sink sink,
                         ExecutionPlan executionPlan) {
        this.taskName = taskName;
        this.parallelism = parallelism;
        this.source = source;
        this.transformations = transformations;
        this.sink = sink;
        this.executionPlan = executionPlan;

        init();
    }

    protected void init() {
        subTasks = new ExecutionSubTask[parallelism];
        for (int i = 0; i < parallelism; i++) {
            SourceReader reader = source.createSourceReader(i, parallelism);
            SinkWriter sinkWriter = sink.createWriter(i, parallelism);

            ExecutionSubTask executionSubTask = new ExecutionSubTask(executionPlan.getExecutorService(), this, i, reader, transformations, sinkWriter);
            subTasks[i] = executionSubTask;
        }
    }

    public String getTaskName() {
        return taskName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getNumSubTaskFinished() {
        return numSubTaskFinished;
    }

    public ExecutionSubTask[] getSubTasks() {
        return subTasks;
    }

    public ExecutionPlan getExecutionPlan() {
        return executionPlan;
    }

    public Source getSource() {
        return source;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public Sink getSink() {
        return sink;
    }

    void executionTaskFinished() {
        numSubTaskFinished++;
        if (numSubTaskFinished == parallelism) {
            LOG.info("The Task {} finished", taskName);
            executionPlan.executionTaskFinish();
        }
    }
}
