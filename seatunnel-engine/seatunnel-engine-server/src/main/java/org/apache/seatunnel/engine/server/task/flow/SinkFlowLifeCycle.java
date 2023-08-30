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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SinkWriterContext;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupAddressOperation;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkPrepareCommitOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkRegisterOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.cluster.Address;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;
import static org.apache.seatunnel.engine.server.task.AbstractTask.serializeStates;

@Slf4j
public class SinkFlowLifeCycle<T, CommitInfoT extends Serializable, AggregatedCommitInfoT, StateT>
        extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private final SinkAction<T, StateT, CommitInfoT, AggregatedCommitInfoT> sinkAction;
    private SinkWriter<T, CommitInfoT, StateT> writer;

    private transient Optional<Serializer<CommitInfoT>> commitInfoSerializer;
    private transient Optional<Serializer<StateT>> writerStateSerializer;

    private final int indexID;

    private final TaskLocation taskLocation;

    private Address committerTaskAddress;

    private final TaskLocation committerTaskLocation;

    private Optional<SinkCommitter<CommitInfoT>> committer;

    private Optional<CommitInfoT> lastCommitInfo;

    private MetricsContext metricsContext;

    private Counter sinkWriteCount;

    private Meter sinkWriteQPS;

    private final boolean containAggCommitter;

    public SinkFlowLifeCycle(
            SinkAction<T, StateT, CommitInfoT, AggregatedCommitInfoT> sinkAction,
            TaskLocation taskLocation,
            int indexID,
            SeaTunnelTask runningTask,
            TaskLocation committerTaskLocation,
            boolean containAggCommitter,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext) {
        super(sinkAction, runningTask, completableFuture);
        this.sinkAction = sinkAction;
        this.indexID = indexID;
        this.taskLocation = taskLocation;
        this.committerTaskLocation = committerTaskLocation;
        this.containAggCommitter = containAggCommitter;
        this.metricsContext = metricsContext;
        sinkWriteCount = metricsContext.counter(SINK_WRITE_COUNT);
        sinkWriteQPS = metricsContext.meter(SINK_WRITE_QPS);
    }

    @Override
    public void init() throws Exception {
        this.commitInfoSerializer = sinkAction.getSink().getCommitInfoSerializer();
        this.writerStateSerializer = sinkAction.getSink().getWriterStateSerializer();
        this.committer = sinkAction.getSink().createCommitter();
        this.lastCommitInfo = Optional.empty();
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (containAggCommitter) {
            committerTaskAddress = getCommitterTaskAddress();
        }
        registerCommitter();
    }

    private Address getCommitterTaskAddress() throws ExecutionException, InterruptedException {
        return (Address)
                runningTask
                        .getExecutionContext()
                        .sendToMaster(new GetTaskGroupAddressOperation(committerTaskLocation))
                        .get();
    }

    @Override
    public void close() throws IOException {
        super.close();
        writer.close();
    }

    private void registerCommitter() {
        if (containAggCommitter) {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SinkRegisterOperation(taskLocation, committerTaskLocation),
                            committerTaskAddress)
                    .join();
        }
    }

    @Override
    public void received(Record<?> record) {
        try {
            if (record.getData() instanceof Barrier) {
                long startTime = System.currentTimeMillis();

                Barrier barrier = (Barrier) record.getData();
                if (barrier.prepareClose()) {
                    prepareClose = true;
                }
                if (barrier.snapshot()) {
                    try {
                        lastCommitInfo = writer.prepareCommit();
                    } catch (Exception e) {
                        writer.abortPrepare();
                        throw e;
                    }
                    List<StateT> states = writer.snapshotState(barrier.getId());
                    if (!writerStateSerializer.isPresent()) {
                        runningTask.addState(
                                barrier, ActionStateKey.of(sinkAction), Collections.emptyList());
                    } else {
                        runningTask.addState(
                                barrier,
                                ActionStateKey.of(sinkAction),
                                serializeStates(writerStateSerializer.get(), states));
                    }
                    if (containAggCommitter) {
                        CommitInfoT commitInfoT = null;
                        if (lastCommitInfo.isPresent()) {
                            commitInfoT = lastCommitInfo.get();
                        }
                        runningTask
                                .getExecutionContext()
                                .sendToMember(
                                        new SinkPrepareCommitOperation<CommitInfoT>(
                                                barrier,
                                                committerTaskLocation,
                                                commitInfoSerializer.isPresent()
                                                        ? commitInfoSerializer
                                                                .get()
                                                                .serialize(commitInfoT)
                                                        : null),
                                        committerTaskAddress)
                                .join();
                    }
                } else {
                    if (containAggCommitter) {
                        runningTask
                                .getExecutionContext()
                                .sendToMember(
                                        new BarrierFlowOperation(barrier, committerTaskLocation),
                                        committerTaskAddress)
                                .join();
                    }
                }
                runningTask.ack(barrier);

                log.debug(
                        "trigger barrier [{}] finished, cost {}ms. taskLocation [{}]",
                        barrier.getId(),
                        System.currentTimeMillis() - startTime,
                        taskLocation);
            } else if (record.getData() instanceof SchemaChangeEvent) {
                if (prepareClose) {
                    return;
                }
                SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
                writer.applySchemaChange(event);
            } else {
                if (prepareClose) {
                    return;
                }
                writer.write((T) record.getData());
                sinkWriteCount.inc();
                sinkWriteQPS.markEvent();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (committer.isPresent() && lastCommitInfo.isPresent()) {
            committer.get().commit(Collections.singletonList(lastCommitInfo.get()));
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (committer.isPresent() && lastCommitInfo.isPresent()) {
            committer.get().abort(Collections.singletonList(lastCommitInfo.get()));
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        List<StateT> states = new ArrayList<>();
        if (writerStateSerializer.isPresent()) {
            states =
                    actionStateList.stream()
                            .map(ActionSubtaskState::getState)
                            .flatMap(Collection::stream)
                            .filter(Objects::nonNull)
                            .map(
                                    bytes ->
                                            sneaky(
                                                    () ->
                                                            writerStateSerializer
                                                                    .get()
                                                                    .deserialize(bytes)))
                            .collect(Collectors.toList());
        }
        if (states.isEmpty()) {
            this.writer =
                    sinkAction
                            .getSink()
                            .createWriter(new SinkWriterContext(indexID, metricsContext));
        } else {
            this.writer =
                    sinkAction
                            .getSink()
                            .restoreWriter(new SinkWriterContext(indexID, metricsContext), states);
        }
    }
}
