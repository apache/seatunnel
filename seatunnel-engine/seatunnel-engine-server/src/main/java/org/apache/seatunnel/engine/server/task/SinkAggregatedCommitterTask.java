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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.execution.WorkerTaskLocation;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SinkAggregatedCommitterTask<AggregatedCommitInfoT> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SinkAggregatedCommitterTask.class);
    private static final long serialVersionUID = 5906594537520393503L;
    private final SinkAction<?, ?, ?, AggregatedCommitInfoT> sink;
    private final int maxWriterSize;

    private final SinkAggregatedCommitter<?, AggregatedCommitInfoT> aggregatedCommitter;

    private Map<Long, Address> writerAddressMap;

    private Map<Long, List<AggregatedCommitInfoT>> checkpointCommitInfoMap;
    private Map<Long, Map<Long, Long>> alreadyReceivedCommitInfo;
    private Object closeLock;
    private CompletableFuture<Void> completableFuture;

    public SinkAggregatedCommitterTask(long jobID, TaskLocation taskID, SinkAction<?, ?, ?, AggregatedCommitInfoT> sink,
                                       SinkAggregatedCommitter<?, AggregatedCommitInfoT> aggregatedCommitter) {
        super(jobID, taskID);
        this.sink = sink;
        this.aggregatedCommitter = aggregatedCommitter;
        this.maxWriterSize = sink.getParallelism();
    }

    @Override
    public void init() throws Exception {
        super.init();
        this.closeLock = new Object();
        this.alreadyReceivedCommitInfo = new ConcurrentHashMap<>();
        this.writerAddressMap = new ConcurrentHashMap<>();
        this.checkpointCommitInfoMap = new ConcurrentHashMap<>();
        this.completableFuture = new CompletableFuture<>();
        LOGGER.info("starting seatunnel sink aggregated committer task, sink name: " + sink.getName());
    }

    public void receivedWriterRegister(WorkerTaskLocation writerID, Address address) {
        this.writerAddressMap.put(writerID.getTaskID(), address);
    }

    public void receivedWriterUnregister(WorkerTaskLocation writerID) {
        this.writerAddressMap.remove(writerID.getTaskID());
        if (writerAddressMap.isEmpty()) {
            try {
                this.close();
            } catch (IOException e) {
                LOGGER.severe("aggregated committer close failed", e);
                throw new TaskRuntimeException(e);
            }
        }
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        if (completableFuture.isDone()) {
            completableFuture.get();
        }
        return progress.toState();
    }

    @Override
    public void close() throws IOException {
        synchronized (closeLock) {
            aggregatedCommitter.close();
            progress.done();
            completableFuture.complete(null);
        }
    }

    public void receivedWriterCommitInfo(long checkpointID, long subTaskId,
                                         AggregatedCommitInfoT[] commitInfos) {
        checkpointCommitInfoMap.computeIfAbsent(checkpointID, id -> new CopyOnWriteArrayList<>());
        alreadyReceivedCommitInfo.computeIfAbsent(checkpointID, id -> new ConcurrentHashMap<>());

        checkpointCommitInfoMap.get(checkpointID).addAll(Arrays.asList(commitInfos));
        Map<Long, Long> alreadyReceived = alreadyReceivedCommitInfo.get(checkpointID);
        alreadyReceived.put(subTaskId, subTaskId);
        if (alreadyReceived.size() == maxWriterSize) {
            try {
                synchronized (closeLock) {
                    aggregatedCommitter.commit(checkpointCommitInfoMap.get(checkpointID));
                }
                checkpointCommitInfoMap.remove(checkpointID);
                alreadyReceivedCommitInfo.remove(checkpointID);
            } catch (IOException e) {
                LOGGER.severe("aggregated committer commit failed, checkpointID: " + checkpointID, e);
                throw new TaskRuntimeException(e);
            }
        }

    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(sink.getJarUrls());
    }
}
