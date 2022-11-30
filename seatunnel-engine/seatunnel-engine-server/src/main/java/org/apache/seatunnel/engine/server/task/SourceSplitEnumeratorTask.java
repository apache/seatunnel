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
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.PREPARE_CLOSE;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.RUNNING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.STARTING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.WAITING_RESTORE;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.context.SeaTunnelSplitEnumeratorContext;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.operation.source.LastCheckpointNotifyOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SourceSplitEnumeratorTask.class);

    private static final long serialVersionUID = -3713701594297977775L;

    private final SourceAction<?, SplitT, Serializable> source;
    private SourceSplitEnumerator<SplitT, Serializable> enumerator;
    private SeaTunnelSplitEnumeratorContext<SplitT> enumeratorContext;

    private Serializer<Serializable> enumeratorStateSerializer;

    private int maxReaderSize;
    private Set<Long> unfinishedReaders;
    private Map<TaskLocation, Address> taskMemberMapping;
    private Map<Long, TaskLocation> taskIDToTaskLocationMapping;
    private Map<Integer, TaskLocation> taskIndexToTaskLocationMapping;

    private volatile SeaTunnelTaskState currState;

    private volatile boolean readerRegisterComplete;

    @Override
    public void init() throws Exception {
        currState = SeaTunnelTaskState.INIT;
        super.init();
        readerRegisterComplete = false;
        LOGGER.info("starting seatunnel source split enumerator task, source name: " + source.getName());
        enumeratorContext = new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this);
        enumeratorStateSerializer = this.source.getSource().getEnumeratorStateSerializer();
        taskMemberMapping = new ConcurrentHashMap<>();
        taskIDToTaskLocationMapping = new ConcurrentHashMap<>();
        taskIndexToTaskLocationMapping = new ConcurrentHashMap<>();
        maxReaderSize = source.getParallelism();
        unfinishedReaders = new CopyOnWriteArraySet<>();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
        progress.done();
    }

    @SuppressWarnings("unchecked")
    public SourceSplitEnumeratorTask(long jobID, TaskLocation taskID, SourceAction<?, SplitT, ?> source) {
        super(jobID, taskID);
        this.source = (SourceAction<?, SplitT, Serializable>) source;
        this.currState = SeaTunnelTaskState.CREATED;
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        stateProcess();
        return progress.toState();
    }

    @Override
    public void triggerBarrier(Barrier barrier) throws Exception {
        if (barrier.prepareClose()) {
            this.currState = PREPARE_CLOSE;
            this.prepareCloseBarrierId.set(barrier.getId());
        }
        final long barrierId = barrier.getId();
        Serializable snapshotState = null;
        synchronized (enumeratorContext) {
            if (barrier.snapshot()) {
                snapshotState = enumerator.snapshotState(barrierId);
            }
            sendToAllReader(location -> new BarrierFlowOperation(barrier, location));
        }
        if (barrier.snapshot()) {
            byte[] serialize = enumeratorStateSerializer.serialize(snapshotState);
            this.getExecutionContext().sendToMaster(new TaskAcknowledgeOperation(this.taskLocation, (CheckpointBarrier) barrier,
                Collections.singletonList(new ActionSubtaskState(source.getId(), -1, Collections.singletonList(serialize)))));
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        Optional<Serializable> state = actionStateList.stream()
            .map(ActionSubtaskState::getState)
            .flatMap(Collection::stream)
            .map(bytes -> sneaky(() -> enumeratorStateSerializer.deserialize(bytes)))
            .findFirst();
        if (state.isPresent()) {
            this.enumerator = this.source.getSource()
                .restoreEnumerator(enumeratorContext, state.get());
        } else {
            this.enumerator = this.source.getSource()
                .createEnumerator(enumeratorContext);
        }
        restoreComplete = true;
    }

    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        enumerator.addSplitsBack(splits, subtaskId);
    }

    public void receivedReader(TaskLocation readerId, Address memberAddr) {
        LOGGER.info("received reader register, readerID: " + readerId);
        this.addTaskMemberMapping(readerId, memberAddr);
        enumerator.registerReader(readerId.getTaskIndex());
        if (maxReaderSize == taskMemberMapping.size()) {
            readerRegisterComplete = true;
        }
    }

    public void requestSplit(long taskID) {
        enumerator.handleSplitRequest((int) taskID);
    }

    public void addTaskMemberMapping(TaskLocation taskID, Address memberAdder) {
        taskMemberMapping.put(taskID, memberAdder);
        taskIDToTaskLocationMapping.put(taskID.getTaskID(), taskID);
        taskIndexToTaskLocationMapping.put(taskID.getTaskIndex(), taskID);
        unfinishedReaders.add(taskID.getTaskID());
    }

    public Address getTaskMemberAddress(long taskID) {
        return taskMemberMapping.get(taskIDToTaskLocationMapping.get(taskID));
    }

    public TaskLocation getTaskMemberLocation(long taskID) {
        return taskIDToTaskLocationMapping.get(taskID);
    }

    public Address getTaskMemberAddressByIndex(int taskIndex) {
        return taskMemberMapping.get(taskIndexToTaskLocationMapping.get(taskIndex));
    }

    public TaskLocation getTaskMemberLocationByIndex(int taskIndex) {
        return taskIndexToTaskLocationMapping.get(taskIndex);
    }

    public void readerFinished(long taskID) {
        unfinishedReaders.remove(taskID);
        if (unfinishedReaders.isEmpty()) {
            prepareCloseStatus = true;
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void stateProcess() throws Exception {
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
                if (startCalled && readerRegisterComplete) {
                    currState = STARTING;
                    enumerator.open();
                }
                break;
            case STARTING:
                currState = RUNNING;
                LOGGER.info("received enough reader, starting enumerator...");
                enumerator.run();
                break;
            case RUNNING:
                // The reader closes automatically after reading
                if (prepareCloseStatus) {
                    this.getExecutionContext().sendToMaster(new LastCheckpointNotifyOperation(jobID, taskLocation));
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

    public Set<Integer> getRegisteredReaders() {
        return taskMemberMapping.keySet().stream().map(TaskLocation::getTaskIndex).collect(Collectors.toSet());
    }

    private void sendToAllReader(Function<TaskLocation, Operation> function) {
        List<InvocationFuture<?>> futures = new ArrayList<>();
        taskMemberMapping.forEach((location, address) ->
            futures.add(this.getExecutionContext().sendToMember(function.apply(location), address)));
        futures.forEach(InvocationFuture::join);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        enumerator.notifyCheckpointComplete(checkpointId);
        if (currState == PREPARE_CLOSE && prepareCloseBarrierId.get() == checkpointId) {
            closeCall();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        enumerator.notifyCheckpointAborted(checkpointId);
        if (currState == PREPARE_CLOSE && prepareCloseBarrierId.get() == checkpointId) {
            closeCall();
        }
    }
}
