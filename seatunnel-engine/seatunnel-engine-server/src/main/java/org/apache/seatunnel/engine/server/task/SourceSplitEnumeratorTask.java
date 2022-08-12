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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.context.SeaTunnelSplitEnumeratorContext;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SourceSplitEnumeratorTask.class);

    private static final long serialVersionUID = -3713701594297977775L;

    private final SourceAction<?, SplitT, ?> source;
    private SourceSplitEnumerator<?, ?> enumerator;
    private SeaTunnelSplitEnumeratorContext<SplitT> context;
    private Map<TaskLocation, Address> taskMemberMapping;
    private Map<Long, TaskLocation> taskIDToTaskLocationMapping;

    @Override
    public void init() throws Exception {
        super.init();
        LOGGER.info("starting seatunnel source split enumerator task, source name: " + source.getName());
        context = new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this);
        enumerator = this.source.getSource().createEnumerator(context);
        taskMemberMapping = new ConcurrentHashMap<>();
        taskIDToTaskLocationMapping = new ConcurrentHashMap<>();
        enumerator.open();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
    }

    public SourceSplitEnumeratorTask(long jobID, TaskLocation taskID, SourceAction<?, SplitT, ?> source) {
        super(jobID, taskID);
        this.source = source;
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        return super.call();
    }

    public void receivedReader(TaskLocation readerId, Address memberAddr) {
        this.addTaskMemberMapping(readerId, memberAddr);
        enumerator.registerReader((int) readerId.getTaskID());
    }

    public void requestSplit(long taskID) {
        enumerator.handleSplitRequest((int) taskID);
    }

    public void addTaskMemberMapping(TaskLocation taskID, Address memberAddr) {
        taskMemberMapping.put(taskID, memberAddr);
        taskIDToTaskLocationMapping.put(taskID.getTaskID(), taskID);
    }

    public Address getTaskMemberAddr(long taskID) {
        return taskMemberMapping.get(taskIDToTaskLocationMapping.get(taskID));
    }

    public void removeTaskMemberMapping(long taskID) {
        TaskLocation taskLocation = taskIDToTaskLocationMapping.get(taskID);
        taskMemberMapping.remove(taskLocation);
        taskIDToTaskLocationMapping.remove(taskID);
    }

    public Set<Long> getRegisteredReaders() {
        return taskMemberMapping.keySet().stream().map(TaskLocation::getTaskID).collect(Collectors.toSet());
    }

    private void noMoreElement(int taskID) {
        enumerator.handleSplitRequest(taskID);
    }

    @Override
    public void receivedMessage(Object message) {
        // TODO custom message
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }
}
