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

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CANCELED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CLOSED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.INIT;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.PREPARE_CLOSE;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.RUNNING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.STARTING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.WAITING_RESTORE;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class SinkAggregatedCommitterTask<CommandInfoT, AggregatedCommitInfoT> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SinkAggregatedCommitterTask.class);
    private static final long serialVersionUID = 5906594537520393503L;

    private SeaTunnelTaskState currState;
    private final SinkAction<?, ?, CommandInfoT, AggregatedCommitInfoT> sink;
    private final int maxWriterSize;

    private final SinkAggregatedCommitter<CommandInfoT, AggregatedCommitInfoT> aggregatedCommitter;

    private transient Serializer<AggregatedCommitInfoT> aggregatedCommitInfoSerializer;
    private Map<Long, Address> writerAddressMap;

    private ConcurrentMap<Long, List<CommandInfoT>> commitInfoCache;

    private ConcurrentMap<Long, List<AggregatedCommitInfoT>> checkpointCommitInfoMap;

    private Map<Long, Integer> checkpointBarrierCounter;
    private CompletableFuture<Void> completableFuture;

    private volatile boolean receivedSinkWriter;

    public SinkAggregatedCommitterTask(long jobID, TaskLocation taskID, SinkAction<?, ?, CommandInfoT, AggregatedCommitInfoT> sink,
                                       SinkAggregatedCommitter<CommandInfoT, AggregatedCommitInfoT> aggregatedCommitter) {
        super(jobID, taskID);
        this.sink = sink;
        this.aggregatedCommitter = aggregatedCommitter;
        this.maxWriterSize = sink.getParallelism();
        this.receivedSinkWriter = false;
    }

    @Override
    public void init() throws Exception {
        super.init();
        currState = INIT;
        this.checkpointBarrierCounter = new ConcurrentHashMap<>();
        this.commitInfoCache = new ConcurrentHashMap<>();
        this.writerAddressMap = new ConcurrentHashMap<>();
        this.checkpointCommitInfoMap = new ConcurrentHashMap<>();
        this.completableFuture = new CompletableFuture<>();
        this.aggregatedCommitInfoSerializer = sink.getSink().getAggregatedCommitInfoSerializer().get();
        LOGGER.info("starting seatunnel sink aggregated committer task, sink name: " + sink.getName());
    }

    public void receivedWriterRegister(TaskLocation writerID, Address address) {
        this.writerAddressMap.put(writerID.getTaskID(), address);
        if (maxWriterSize <= writerAddressMap.size()) {
            receivedSinkWriter = true;
        }
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        stateProcess();
        return progress.toState();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    protected void stateProcess() throws Exception {
        switch (currState) {
            case INIT:
                currState = WAITING_RESTORE;
                reportTaskStatus(WAITING_RESTORE);
                break;
            case WAITING_RESTORE:
                if (restoreComplete) {
                    currState = READY_START;
                    reportTaskStatus(READY_START);
                }
                break;
            case READY_START:
                if (startCalled) {
                    currState = STARTING;
                }
                break;
            case STARTING:
                if (receivedSinkWriter) {
                    currState = RUNNING;
                }
                break;
            case RUNNING:
                if (prepareCloseStatus) {
                    currState = PREPARE_CLOSE;
                } else {
                    Thread.sleep(100);
                }
                break;
            case PREPARE_CLOSE:
                if (closeCalled) {
                    currState = CLOSED;
                } else {
                    Thread.sleep(100);
                }
                break;
            case CLOSED:
                this.close();
                return;
            // TODO support cancel by outside
            case CANCELLING:
                this.close();
                currState = CANCELED;
                return;
            default:
                throw new IllegalArgumentException("Unknown Enumerator State: " + currState);
        }
    }

    @Override
    public void close() throws IOException {
        aggregatedCommitter.close();
        progress.done();
        completableFuture.complete(null);
    }

    @Override
    public void triggerBarrier(Barrier barrier) throws Exception {
        Integer count = checkpointBarrierCounter.compute(barrier.getId(), (id, num) -> num == null ? 1 : ++num);
        if (count != maxWriterSize) {
            return;
        }
        if (barrier.prepareClose()) {
            this.prepareCloseStatus = true;
            this.prepareCloseBarrierId.set(barrier.getId());
        }
        if (barrier.snapshot()) {
            if (commitInfoCache.containsKey(barrier.getId())) {
                AggregatedCommitInfoT aggregatedCommitInfoT = aggregatedCommitter.combine(commitInfoCache.get(barrier.getId()));
                checkpointCommitInfoMap.put(barrier.getId(), Collections.singletonList(aggregatedCommitInfoT));
            }
            List<byte[]> states = serializeStates(aggregatedCommitInfoSerializer, checkpointCommitInfoMap.getOrDefault(barrier.getId(), Collections.emptyList()));
            this.getExecutionContext().sendToMaster(new TaskAcknowledgeOperation(this.taskLocation, (CheckpointBarrier) barrier,
                Collections.singletonList(new ActionSubtaskState(sink.getId(), -1, states))));
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        List<AggregatedCommitInfoT> aggregatedCommitInfos = actionStateList.stream()
            .map(ActionSubtaskState::getState)
            .flatMap(Collection::stream)
            .map(bytes -> sneaky(() -> aggregatedCommitInfoSerializer.deserialize(bytes)))
            .collect(Collectors.toList());
        aggregatedCommitter.commit(aggregatedCommitInfos);
        restoreComplete = true;
    }

    public void receivedWriterCommitInfo(long checkpointID,
                                         CommandInfoT commitInfos) {
        commitInfoCache.computeIfAbsent(checkpointID, id -> new CopyOnWriteArrayList<>());
        commitInfoCache.get(checkpointID).add(commitInfos);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(sink.getJarUrls());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        List<AggregatedCommitInfoT> aggregatedCommitInfo = new ArrayList<>();
        checkpointCommitInfoMap.forEach((key, value) -> {
            if (key > checkpointId) {
                return;
            }
            aggregatedCommitInfo.addAll(value);
            checkpointCommitInfoMap.remove(key);
        });
        aggregatedCommitter.commit(aggregatedCommitInfo);
        tryClose(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        aggregatedCommitter.abort(checkpointCommitInfoMap.get(checkpointId));
        checkpointCommitInfoMap.remove(checkpointId);
        tryClose(checkpointId);
    }
}
