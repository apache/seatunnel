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

import static org.apache.seatunnel.engine.server.utils.ExceptionUtil.sneakyThrow;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.statemachine.CommitterState;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class SinkAggregatedCommitterTask<AggregatedCommitInfoT> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SinkAggregatedCommitterTask.class);
    private static final long serialVersionUID = 5906594537520393503L;

    private CommitterState currState;
    private final SinkAction<?, ?, ?, AggregatedCommitInfoT> sink;
    private final int maxWriterSize;

    private final SinkAggregatedCommitter<?, AggregatedCommitInfoT> aggregatedCommitter;

    private transient Serializer<AggregatedCommitInfoT> aggregatedCommitInfoSerializer;
    private Map<Long, Address> writerAddressMap;

    private Map<Long, List<AggregatedCommitInfoT>> checkpointCommitInfoMap;

    private transient org.apache.seatunnel.engine.checkpoint.storage.common.Serializer protoStuffSerializer;
    private Map<Long, Integer> checkpointBarrierCounter;
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
        currState = CommitterState.INIT;
        this.closeLock = new Object();
        this.alreadyReceivedCommitInfo = new ConcurrentHashMap<>();
        this.writerAddressMap = new ConcurrentHashMap<>();
        this.checkpointCommitInfoMap = new ConcurrentHashMap<>();
        this.completableFuture = new CompletableFuture<>();
        this.aggregatedCommitInfoSerializer = sink.getSink().getAggregatedCommitInfoSerializer().get();
        this.protoStuffSerializer = new ProtoStuffSerializer();
        LOGGER.info("starting seatunnel sink aggregated committer task, sink name: " + sink.getName());
    }

    public void receivedWriterRegister(TaskLocation writerID, Address address) {
        this.writerAddressMap.put(writerID.getTaskID(), address);
    }

    public void receivedWriterUnregister(TaskLocation writerID) {
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
        stateProcess();
        return progress.toState();
    }

    protected void stateProcess() throws Exception {
        switch (currState) {
            case INIT:
                reportReadyRestore();
                currState = CommitterState.WAITING_RESTORE;
                break;
            case WAITING_RESTORE:
                if (restoreComplete) {
                    reportReadyStart();
                    currState = CommitterState.READY_START;
                }
                break;
            case READY_START:
                if (startCalled) {
                    currState = CommitterState.STARTING;
                }
                break;
            case STARTING:
                currState = CommitterState.RUNNING;
                break;
            case RUNNING:
                if (prepareCloseStatus) {
                    currState = CommitterState.PREPARE_CLOSE;
                }
                break;
            case PREPARE_CLOSE:
                if (closeCalled) {
                    currState = CommitterState.CLOSED;
                }
                break;
            case CLOSED:
                this.close();
                return;
            // TODO support cancel by outside
            case CANCELLING:
                this.close();
                currState = CommitterState.CANCELED;
                return;
            default:
                throw new IllegalArgumentException("Unknown Enumerator State: " + currState);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (closeLock) {
            aggregatedCommitter.close();
            progress.done();
            completableFuture.complete(null);
        }
    }

    @Override
    public void triggerCheckpoint(CheckpointBarrier barrier) throws Exception {
        Integer count = checkpointBarrierCounter.compute(barrier.getId(), (id, num) -> num == null ? 1 : ++num);
        if (count != maxWriterSize) {
            return;
        }
        List<byte[]> states = checkpointCommitInfoMap.get(barrier.getId())
            .stream()
            .map(info -> {
                try {
                    return aggregatedCommitInfoSerializer.serialize(info);
                } catch (IOException e) {
                    sneakyThrow(e);
                }
                // This method wouldn't be executed.
                throw new RuntimeException("Never throw here.");
            }).collect(Collectors.toList());
        this.getExecutionContext().sendToMaster(new TaskAcknowledgeOperation(barrier.getId(), this.taskLocation,
            Collections.singletonList(new ActionSubtaskState(sink.getId(), -1, protoStuffSerializer.serialize(states)))));
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

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        aggregatedCommitter.commit(checkpointCommitInfoMap.get(checkpointId));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        aggregatedCommitter.abort(checkpointCommitInfoMap.get(checkpointId));
    }
}
