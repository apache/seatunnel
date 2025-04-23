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

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;

public abstract class AbstractTask implements Task {
    private static final long serialVersionUID = -2524701323779523718L;

    protected TaskExecutionContext executionContext;
    protected final long jobID;
    protected final TaskLocation taskLocation;
    protected volatile CompletableFuture<Void> restoreComplete;
    protected volatile boolean startCalled;
    protected volatile boolean closeCalled;
    protected volatile boolean prepareCloseStatus;

    protected AtomicLong prepareCloseBarrierId;

    protected Progress progress;

    public AbstractTask(long jobID, TaskLocation taskLocation) {
        this.taskLocation = taskLocation;
        this.jobID = jobID;
        this.progress = new Progress();
        this.startCalled = false;
        this.closeCalled = false;
        this.prepareCloseStatus = false;
        this.prepareCloseBarrierId = new AtomicLong(-1);
    }

    public abstract Set<URL> getJarsUrl();

    public abstract Set<ConnectorJarIdentifier> getConnectorPluginJars();

    @Override
    public void setTaskExecutionContext(TaskExecutionContext taskExecutionContext) {
        this.executionContext = taskExecutionContext;
    }

    @Override
    public TaskExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public void init() throws Exception {
        this.restoreComplete = new CompletableFuture<>();
        progress.start();
    }

    @NonNull @Override
    public abstract ProgressState call() throws Exception;

    public TaskLocation getTaskLocation() {
        return this.taskLocation;
    }

    @NonNull @Override
    public Long getTaskID() {
        return taskLocation.getTaskID();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!restoreComplete.isDone()) {
                restoreComplete.cancel(true);
            }
        } catch (Exception ignore) {
        }
    }

    protected void reportTaskStatus(SeaTunnelTaskState status) {
        getExecutionContext()
                .sendToMaster(new TaskReportStatusOperation(taskLocation, status))
                .join();
    }

    public static <T> List<byte[]> serializeStates(Serializer<T> serializer, List<T> states) {
        return states.stream()
                .map(state -> sneaky(() -> serializer.serialize(state)))
                .collect(Collectors.toList());
    }

    public void startCall() {
        startCalled = true;
    }

    public void tryClose(long checkpointId) {
        if (prepareCloseStatus && prepareCloseBarrierId.get() == checkpointId) {
            closeCall();
        }
    }

    public void closeCall() {
        closeCalled = true;
    }
}
