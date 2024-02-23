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
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
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
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CANCELED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CLOSED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.PREPARE_CLOSE;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.RUNNING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.STARTING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.WAITING_RESTORE;

@Slf4j
public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final long serialVersionUID = -3713701594297977775L;

    private final SourceAction<?, SplitT, Serializable> source;
    private SourceSplitEnumerator<SplitT, Serializable> enumerator;
    private SeaTunnelSplitEnumeratorContext<SplitT> enumeratorContext;

    private Serializer<Serializable> enumeratorStateSerializer;
    private Serializer<SplitT> splitSerializer;

    private int maxReaderSize;
    private Set<Long> unfinishedReaders;
    private Map<TaskLocation, Address> taskMemberMapping;
    private Map<Long, TaskLocation> taskIDToTaskLocationMapping;
    private Map<Integer, TaskLocation> taskIndexToTaskLocationMapping;

    private volatile SeaTunnelTaskState currState;

    private volatile boolean readerRegisterComplete;

    private volatile boolean prepareCloseTriggered;

    @Override
    public void init() throws Exception {
        currState = SeaTunnelTaskState.INIT;
        super.init();
        readerRegisterComplete = false;
        log.info(
                "starting seatunnel source split enumerator task, source name: "
                        + source.getName());
        enumeratorContext =
                new SeaTunnelSplitEnumeratorContext<>(
                        this.source.getParallelism(), this, getMetricsContext());
        enumeratorStateSerializer = this.source.getSource().getEnumeratorStateSerializer();
        splitSerializer = this.source.getSource().getSplitSerializer();
        taskMemberMapping = new ConcurrentHashMap<>();
        taskIDToTaskLocationMapping = new ConcurrentHashMap<>();
        taskIndexToTaskLocationMapping = new ConcurrentHashMap<>();
        maxReaderSize = source.getParallelism();
        unfinishedReaders = new CopyOnWriteArraySet<>();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (enumerator != null) {
            enumerator.close();
        }
        progress.done();
    }

    @SuppressWarnings("unchecked")
    public SourceSplitEnumeratorTask(
            long jobID, TaskLocation taskID, SourceAction<?, SplitT, ?> source) {
        super(jobID, taskID);
        this.source = (SourceAction<?, SplitT, Serializable>) source;
        this.currState = SeaTunnelTaskState.CREATED;
    }

    @NonNull @Override
    public ProgressState call() throws Exception {
        stateProcess();
        return progress.toState();
    }

    @Override
    public void triggerBarrier(Barrier barrier) throws Exception {
        long startTime = System.currentTimeMillis();

        log.debug("split enumer trigger barrier [{}]", barrier);
        if (barrier.prepareClose()) {
            this.prepareCloseTriggered = true;
            this.prepareCloseBarrierId.set(barrier.getId());
        }
        final long barrierId = barrier.getId();
        Serializable snapshotState = null;
        byte[] serialize = null;
        // Do not modify this lock object, as it is also used in the SourceSplitEnumerator.
        synchronized (enumeratorContext) {
            if (barrier.snapshot()) {
                snapshotState = enumerator.snapshotState(barrierId);
                serialize = enumeratorStateSerializer.serialize(snapshotState);
            }
            log.debug("source split enumerator send state [{}] to master", snapshotState);
            sendToAllReader(location -> new BarrierFlowOperation(barrier, location));
        }
        if (barrier.snapshot()) {
            this.getExecutionContext()
                    .sendToMaster(
                            new TaskAcknowledgeOperation(
                                    this.taskLocation,
                                    (CheckpointBarrier) barrier,
                                    Collections.singletonList(
                                            new ActionSubtaskState(
                                                    ActionStateKey.of(source),
                                                    -1,
                                                    Collections.singletonList(serialize)))))
                    .join();
        }

        log.debug(
                "trigger barrier [{}] finished, cost {}ms. taskLocation [{}]",
                barrier.getId(),
                System.currentTimeMillis() - startTime,
                taskLocation);
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        log.debug("restoreState for split enumerator [{}]", actionStateList);
        Optional<Serializable> state =
                actionStateList.stream()
                        .map(ActionSubtaskState::getState)
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .map(bytes -> sneaky(() -> enumeratorStateSerializer.deserialize(bytes)))
                        .findFirst();
        if (state.isPresent()) {
            this.enumerator =
                    this.source.getSource().restoreEnumerator(enumeratorContext, state.get());
        } else {
            this.enumerator = this.source.getSource().createEnumerator(enumeratorContext);
        }
        restoreComplete.complete(null);
        log.debug("restoreState split enumerator [{}] finished", actionStateList);
    }

    public Serializer<SplitT> getSplitSerializer() throws ExecutionException, InterruptedException {
        // Because the splitSerializer is initialized in the init method, it's necessary to wait for
        // the Enumerator to finish initializing.
        getEnumerator();
        return splitSerializer;
    }

    public void addSplitsBack(List<SplitT> splits, int subtaskId)
            throws ExecutionException, InterruptedException {
        getEnumerator().addSplitsBack(splits, subtaskId);
    }

    public void receivedReader(TaskLocation readerId, Address memberAddr)
            throws InterruptedException, ExecutionException {
        log.info("received reader register, readerID: " + readerId);

        SourceSplitEnumerator<SplitT, Serializable> enumerator = getEnumerator();
        this.addTaskMemberMapping(readerId, memberAddr);
        enumerator.registerReader(readerId.getTaskIndex());
        int taskSize = taskMemberMapping.size();
        if (maxReaderSize == taskSize) {
            readerRegisterComplete = true;
            log.debug(String.format("reader register complete, current task size %d", taskSize));
        } else {
            log.debug(
                    String.format(
                            "current task size %d, need size %d to complete register",
                            taskSize, maxReaderSize));
        }
    }

    public void requestSplit(long taskIndex) throws ExecutionException, InterruptedException {
        getEnumerator().handleSplitRequest((int) taskIndex);
    }

    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent)
            throws ExecutionException, InterruptedException {
        getEnumerator().handleSourceEvent(subtaskId, sourceEvent);
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

    private SourceSplitEnumerator<SplitT, Serializable> getEnumerator()
            throws InterruptedException, ExecutionException {
        // (restoreComplete == null) means that the Task has not yet executed Init, so we need to
        // wait.
        while (null == restoreComplete) {
            log.warn("Task init is not complete, try to get it again after 200 ms");
            Thread.sleep(200);
        }
        restoreComplete.get();
        return enumerator;
    }

    public void readerFinished(long taskID) {
        unfinishedReaders.remove(taskID);
        if (unfinishedReaders.isEmpty()) {
            prepareCloseStatus = true;
        }
    }

    private void stateProcess() throws Exception {
        switch (currState) {
            case INIT:
                currState = WAITING_RESTORE;
                reportTaskStatus(WAITING_RESTORE);
                break;
            case WAITING_RESTORE:
                if (restoreComplete.isDone()) {
                    currState = READY_START;
                    reportTaskStatus(READY_START);
                } else {
                    Thread.sleep(100);
                }
                break;
            case READY_START:
                if (startCalled && readerRegisterComplete) {
                    currState = STARTING;
                    enumerator.open();
                } else {
                    Thread.sleep(100);
                }
                break;
            case STARTING:
                currState = RUNNING;
                log.info("received enough reader, starting enumerator...");
                enumerator.run();
                break;
            case RUNNING:
                // The reader closes automatically after reading
                if (prepareCloseStatus) {
                    this.getExecutionContext()
                            .sendToMaster(new LastCheckpointNotifyOperation(jobID, taskLocation));
                    currState = PREPARE_CLOSE;
                } else if (prepareCloseTriggered) {
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
        return taskMemberMapping.keySet().stream()
                .map(TaskLocation::getTaskIndex)
                .collect(Collectors.toSet());
    }

    private void sendToAllReader(Function<TaskLocation, Operation> function) {
        List<InvocationFuture<?>> futures = new ArrayList<>();
        taskMemberMapping.forEach(
                (location, address) -> {
                    log.debug(
                            "split enumerator send to read--size: {}, location: {}, address: {}",
                            taskMemberMapping.size(),
                            location,
                            address.toString());
                    futures.add(
                            this.getExecutionContext()
                                    .sendToMember(function.apply(location), address));
                });
        futures.forEach(InvocationFuture::join);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }

    @Override
    public Set<ConnectorJarIdentifier> getConnectorPluginJars() {
        return new HashSet<>(source.getConnectorJarIdentifiers());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        getEnumerator().notifyCheckpointComplete(checkpointId);
        if (prepareCloseBarrierId.get() == checkpointId) {
            closeCall();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        getEnumerator().notifyCheckpointAborted(checkpointId);
        if (prepareCloseBarrierId.get() == checkpointId) {
            closeCall();
        }
    }
}
