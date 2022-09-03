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

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.utils.ConsumerWithException;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SinkConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.UnknownFlowException;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.PartitionTransformSinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.PartitionTransformSourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.TransformFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateQueue;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public abstract class SeaTunnelTask extends AbstractTask {

    private static final long serialVersionUID = 2604309561613784425L;

    protected volatile SeaTunnelTaskState currState;
    private final Flow executionFlow;

    protected FlowLifeCycle startFlowLifeCycle;

    protected List<FlowLifeCycle> allCycles;

    protected List<OneInputFlowLifeCycle<Record<?>>> outputs;

    protected List<CompletableFuture<Void>> flowFutures;

    protected final Map<Long, List<ActionSubtaskState>> checkpointStates = new HashMap<>();

    private final Map<Long, Integer> cycleAcks = new HashMap<>();

    protected int indexID;

    private TaskGroup taskBelongGroup;

    public SeaTunnelTask(long jobID, TaskLocation taskID, int indexID, Flow executionFlow) {
        super(jobID, taskID);
        this.indexID = indexID;
        this.executionFlow = executionFlow;
        this.currState = SeaTunnelTaskState.CREATED;
    }

    @Override
    public void init() throws Exception {
        super.init();
        this.currState = SeaTunnelTaskState.INIT;
        flowFutures = new ArrayList<>();
        allCycles = new ArrayList<>();
        startFlowLifeCycle = convertFlowToActionLifeCycle(executionFlow);
        for (FlowLifeCycle cycle : allCycles) {
            cycle.init();
        }
        CompletableFuture.allOf(flowFutures.toArray(new CompletableFuture[0])).whenComplete((s, e) -> closeCalled = true);
    }

    protected void stateProcess() throws Exception {
        switch (currState) {
            case INIT:
                currState = WAITING_RESTORE;
                reportTaskStatus(WAITING_RESTORE);
                break;
            case WAITING_RESTORE:
                if (restoreComplete) {
                    currState = READY_START;
                    reportReadyStart();
                }
                break;
            case READY_START:
                if (startCalled) {
                    currState = STARTING;
                }
                break;
            case STARTING:
                currState = RUNNING;
                break;
            case RUNNING:
                collect();
                if (prepareCloseStatus) {
                    currState = PREPARE_CLOSE;
                }
                break;
            case PREPARE_CLOSE:
                if (closeCalled) {
                    currState = CLOSED;
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

    public void setTaskGroup(TaskGroup group) {
        this.taskBelongGroup = group;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private FlowLifeCycle convertFlowToActionLifeCycle(@NonNull Flow flow) throws Exception {

        FlowLifeCycle lifeCycle;
        List<OneInputFlowLifeCycle<Record<?>>> flowLifeCycles = new ArrayList<>();
        if (!flow.getNext().isEmpty()) {
            for (Flow f : executionFlow.getNext()) {
                flowLifeCycles.add((OneInputFlowLifeCycle<Record<?>>) convertFlowToActionLifeCycle(f));
            }
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        flowFutures.add(completableFuture);
        if (flow instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow f = (PhysicalExecutionFlow) flow;
            if (f.getAction() instanceof SourceAction) {
                lifeCycle = createSourceFlowLifeCycle((SourceAction<?, ?, ?>) f.getAction(),
                    (SourceConfig) f.getConfig(), completableFuture);
                outputs = flowLifeCycles;
            } else if (f.getAction() instanceof SinkAction) {
                lifeCycle = new SinkFlowLifeCycle<>((SinkAction) f.getAction(), taskLocation, indexID, this,
                    ((SinkConfig) f.getConfig()).getCommitterTask(),
                    ((SinkConfig) f.getConfig()).isContainCommitter(), completableFuture);
            } else if (f.getAction() instanceof TransformChainAction) {
                lifeCycle =
                    new TransformFlowLifeCycle<SeaTunnelRow>((TransformChainAction) f.getAction(), this,
                        new SeaTunnelTransformCollector(flowLifeCycles), completableFuture);
            } else if (f.getAction() instanceof PartitionTransformAction) {
                // TODO use index and taskID to create ringbuffer list
                if (executionFlow.getNext().isEmpty()) {
                    lifeCycle = new PartitionTransformSinkFlowLifeCycle(this, completableFuture);
                } else {
                    lifeCycle = new PartitionTransformSourceFlowLifeCycle(this, completableFuture);
                }
            } else {
                throw new UnknownActionException(f.getAction());
            }
        } else if (flow instanceof IntermediateExecutionFlow) {
            IntermediateQueueConfig config =
                ((IntermediateExecutionFlow<IntermediateQueueConfig>) flow).getConfig();
            lifeCycle = new IntermediateQueueFlowLifeCycle(this, completableFuture,
                ((TaskGroupWithIntermediateQueue) taskBelongGroup)
                    .getBlockingQueueCache(config.getQueueID()));
            outputs = flowLifeCycles;
        } else {
            throw new UnknownFlowException(flow);
        }
        allCycles.add(lifeCycle);
        return lifeCycle;
    }

    protected abstract SourceFlowLifeCycle<?, ?> createSourceFlowLifeCycle(SourceAction<?, ?, ?> sourceAction,
                                                                           SourceConfig config,
                                                                           CompletableFuture<Void> completableFuture);

    protected abstract void collect() throws Exception;

    @Override
    public Set<URL> getJarsUrl() {
        List<Flow> now = Collections.singletonList(executionFlow);
        Set<URL> urls = new HashSet<>();
        List<Flow> next = new ArrayList<>();
        while (!now.isEmpty()) {
            next.clear();
            now.forEach(n -> {
                if (n instanceof PhysicalExecutionFlow) {
                    urls.addAll(((PhysicalExecutionFlow) n).getAction().getJarUrls());
                }
                next.addAll(n.getNext());
            });
            now = next;
        }
        return urls;
    }

    @Override
    public void close() throws IOException {
        allCycles.parallelStream().forEach(cycle -> sneaky(cycle::close));
    }

    public void ack(Barrier barrier) {
        cycleAcks.compute(barrier.getId(), (id, count) -> count == null ? 1 : ++count);
    }

    public void addState(Barrier barrier, long actionId, List<byte[]> state) {
        List<ActionSubtaskState> states = checkpointStates.computeIfAbsent(barrier.getId(), id -> new ArrayList<>());
        states.add(new ActionSubtaskState(actionId, indexID, state));
        if (cycleAcks.size() == allCycles.size()) {
            this.getExecutionContext().sendToMaster(
                new TaskAcknowledgeOperation(this.taskLocation, (CheckpointBarrier) barrier, states));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        notifyAllAction(listener -> listener.notifyCheckpointComplete(checkpointId));
        if (prepareCloseStatus) {
            closeCall();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        notifyAllAction(listener -> listener.notifyCheckpointAborted(checkpointId));
        if (prepareCloseStatus) {
            closeCall();
        }
    }

    public void notifyAllAction(ConsumerWithException<InternalCheckpointListener> consumer){
        allCycles.stream()
            .filter(cycle -> cycle instanceof InternalCheckpointListener)
            .map(cycle -> (InternalCheckpointListener) cycle)
            .forEach(listener -> sneaky(consumer, listener));
    }
}
