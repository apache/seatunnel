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
import org.apache.seatunnel.engine.server.execution.TaskInfo;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SourceSplitEnumeratorTask.class);

    private static final long serialVersionUID = -3713701594297977775L;

    private final SourceAction<?, SplitT, ?> source;
    private SourceSplitEnumerator<?, ?> enumerator;
    private int maxReaderSize;
    private AtomicInteger unfinishedReaders;
    private Map<TaskInfo, Address> taskMemberMapping;
    private Map<Integer, TaskInfo> subtaskIdToTaskInfoMap;

    private CompletableFuture<ProgressState> future;

    @Override
    public void init() throws Exception {
        super.init();
        future = new CompletableFuture<>();
        LOGGER.info("starting seatunnel source split enumerator task, source name: " + source.getName());
        SeaTunnelSplitEnumeratorContext<SplitT> context = new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this);
        enumerator = this.source.getSource().createEnumerator(context);
        taskMemberMapping = new ConcurrentHashMap<>();
        subtaskIdToTaskInfoMap = new ConcurrentHashMap<>();
        maxReaderSize = source.getParallelism();
        unfinishedReaders = new AtomicInteger(maxReaderSize);
        enumerator.open();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
        future.complete(progress.done().toState());
    }

    public SourceSplitEnumeratorTask(TaskInfo taskInfo, SourceAction<?, SplitT, ?> source) {
        super(taskInfo);
        this.source = source;
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        if (maxReaderSize == taskMemberMapping.size()) {
            LOGGER.info("received enough reader, starting enumerator...");
            enumerator.run();
            return future.get();
        } else {
            return progress.toState();
        }
    }

    public void receivedReader(TaskInfo readerTaskInfo, Address memberAddr) {
        LOGGER.info("received reader register, " + readerTaskInfo.toBasicLog());
        this.addTaskMemberMapping(readerTaskInfo, memberAddr);
        enumerator.registerReader(readerTaskInfo.getIndex());
    }

    public void requestSplit(long subtaskId) {
        enumerator.handleSplitRequest((int) subtaskId);
    }

    public void addTaskMemberMapping(TaskInfo taskInfo, Address memberAddr) {
        taskMemberMapping.put(taskInfo, memberAddr);
        subtaskIdToTaskInfoMap.put(taskInfo.getIndex(), taskInfo);
    }

    public Address getTaskMemberAddr(int subtaskId) {
        return taskMemberMapping.get(subtaskIdToTaskInfoMap.get(subtaskId));
    }

    public TaskInfo getTaskInfo(int subtaskId) {
        return subtaskIdToTaskInfoMap.get(subtaskId);
    }

    public void readerFinished(long taskInfo) {
        removeTaskMemberMapping(taskInfo);
        if (unfinishedReaders.decrementAndGet() == 0) {
            try {
                this.close();
            } catch (Exception e) {
                throw new TaskRuntimeException(e);
            }
        }
    }

    public void removeTaskMemberMapping(long taskInfo) {
        TaskInfo taskInfo = subtaskIdToTaskInfoMap.get(taskInfo);
        taskMemberMapping.remove(taskInfo);
        subtaskIdToTaskInfoMap.remove(taskInfo);
    }

    public Set<Long> getRegisteredReaders() {
        return taskMemberMapping.keySet().stream().map(TaskInfo::getTaskInfo).collect(Collectors.toSet());
    }

    private void noMoreElement(int taskInfo) {
        enumerator.handleSplitRequest(taskInfo);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }
}
