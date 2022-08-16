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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import lombok.NonNull;

import java.net.URL;
import java.util.Set;

public abstract class AbstractTask implements Task {
    private static final long serialVersionUID = -2524701323779523718L;

    protected TaskExecutionContext executionContext;
    protected final long jobID;

    protected final Long vertexId;

    protected final TaskLocation taskID;

    protected Progress progress;

    public AbstractTask(long jobID, TaskLocation taskID, Long vertexId) {
        this.taskID = taskID;
        this.jobID = jobID;
        this.vertexId = vertexId;
        this.progress = new Progress();
    }

    public abstract Set<URL> getJarsUrl();

    @Override
    public void setTaskExecutionContext(TaskExecutionContext taskExecutionContext) {
        this.executionContext = taskExecutionContext;
    }

    public TaskExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public void init() throws Exception {
        progress.start();
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        return progress.toState();
    }

    @NonNull
    @Override
    public Long getTaskID() {
        return taskID.getTaskID();
    }

    @Override
    public Long getJobId() {
        return jobID;
    }

    @Override
    public String getVertexKey() {
        return vertexId.toString();
    }
}
