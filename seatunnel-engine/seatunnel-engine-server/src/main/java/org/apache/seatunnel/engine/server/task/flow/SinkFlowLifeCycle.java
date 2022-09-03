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

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;
import static org.apache.seatunnel.engine.server.task.AbstractTask.serializeStates;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SinkWriterContext;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkPrepareCommitOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkRegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkUnregisterOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SinkFlowLifeCycle<T, StateT> extends ActionFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private final SinkAction<T, StateT, ?, ?> sinkAction;
    private SinkWriter<T, ?, StateT> writer;

    private transient Optional<Serializer<StateT>> writerStateSerializer;

    private final int indexID;

    private final TaskLocation taskLocation;

    private final TaskLocation committerTaskID;

    private final boolean containCommitter;

    public SinkFlowLifeCycle(SinkAction<T, StateT, ?, ?> sinkAction, TaskLocation taskLocation, int indexID,
                             SeaTunnelTask runningTask, TaskLocation committerTaskID,
                             boolean containCommitter, CompletableFuture<Void> completableFuture) {
        super(sinkAction, runningTask, completableFuture);
        this.sinkAction = sinkAction;
        this.indexID = indexID;
        this.taskLocation = taskLocation;
        this.committerTaskID = committerTaskID;
        this.containCommitter = containCommitter;
    }

    @Override
    public void init() throws Exception {
        this.writerStateSerializer = sinkAction.getSink().getWriterStateSerializer();
    }

    @Override
    public void open() throws Exception {
        super.open();
        registerCommitter();
    }

    @Override
    public void close() throws IOException {
        super.close();
        writer.close();
        if (containCommitter) {
            runningTask.getExecutionContext().sendToMaster(new SinkUnregisterOperation(taskLocation,
                    committerTaskID)).join();
        }
    }

    private void registerCommitter() {
        if (containCommitter) {
            runningTask.getExecutionContext().sendToMaster(new SinkRegisterOperation(taskLocation,
                    committerTaskID)).join();
        }
    }

    @Override
    public void received(Record<?> record) {
        try {
            if (record.getData() instanceof Barrier) {
                Barrier barrier = (Barrier) record.getData();
                runningTask.ack(barrier);
                if (barrier.prepareClose()) {
                    prepareClose = true;
                }
                if (barrier.snapshot()) {
                    if (writerStateSerializer.isPresent()) {
                        runningTask.addState(barrier, sinkAction.getId(), Collections.emptyList());
                    } else {
                        List<StateT> states = writer.snapshotState(barrier.getId());
                        runningTask.addState(barrier, sinkAction.getId(), serializeStates(writerStateSerializer.get(), states));
                    }
                    // TODO: prepare commit
                    runningTask.getExecutionContext().sendToMaster(new SinkPrepareCommitOperation(barrier, taskLocation,
                        new byte[0]));
                } else {
                    runningTask.getExecutionContext().sendToMaster(new CheckpointBarrierTriggerOperation(barrier, taskLocation));
                }
            } else {
                if (prepareClose) {
                    return;
                }
                writer.write((T) record.getData());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // TODO: committer commit
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // TODO: committer abort
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        if (writerStateSerializer.isPresent()) {
            return;
        }
        List<StateT> states = actionStateList.stream()
            .map(ActionSubtaskState::getState)
            .flatMap(Collection::stream)
            .map(bytes -> sneaky(() -> writerStateSerializer.get().deserialize(bytes)))
            .collect(Collectors.toList());
        if (states.isEmpty()) {
            this.writer = sinkAction.getSink().createWriter(new SinkWriterContext(indexID));
        } else {
            this.writer = sinkAction.getSink().restoreWriter(new SinkWriterContext(indexID), states);
        }
    }
}
