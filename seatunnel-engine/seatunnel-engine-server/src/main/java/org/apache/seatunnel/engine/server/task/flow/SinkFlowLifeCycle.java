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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.execution.WorkerTaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SinkWriterContext;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkRegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkUnregisterOperation;
import org.apache.seatunnel.engine.server.task.record.ClosedSign;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SinkFlowLifeCycle<T, StateT> extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {

    private final SinkAction<T, StateT, ?, ?> sinkAction;
    private SinkWriter<T, ?, StateT> writer;

    // TODO init states
    private List<StateT> states;

    private final int indexID;

    private final WorkerTaskLocation taskID;

    private final TaskLocation committerTaskID;

    private final SeaTunnelTask runningTask;

    private final boolean containCommitter;

    public SinkFlowLifeCycle(SinkAction<T, StateT, ?, ?> sinkAction, TaskLocation taskID, int indexID,
                             SeaTunnelTask runningTask, TaskLocation committerTaskID,
                             boolean containCommitter, CompletableFuture<Void> completableFuture) {
        super(completableFuture);
        this.sinkAction = sinkAction;
        this.indexID = indexID;
        this.runningTask = runningTask;
        this.taskID = new WorkerTaskLocation(runningTask.getExecutionContext().getSlotContext().getSlotID(),
                taskID.getTaskGroupID(), taskID.getTaskID());
        this.committerTaskID = committerTaskID;
        this.containCommitter = containCommitter;
    }

    @Override
    public void init() throws Exception {
        if (states == null || states.isEmpty()) {
            this.writer = sinkAction.getSink().createWriter(new SinkWriterContext(indexID));
        } else {
            this.writer = sinkAction.getSink().restoreWriter(new SinkWriterContext(indexID), states);
        }
        states = null;
        registerCommitter();
    }

    @Override
    public void close() throws IOException {
        super.close();
        writer.close();
        if (containCommitter) {
            runningTask.getExecutionContext().sendToMaster(new SinkUnregisterOperation(taskID,
                    committerTaskID)).join();
        }
    }

    private void registerCommitter() {
        if (containCommitter) {
            runningTask.getExecutionContext().sendToMaster(new SinkRegisterOperation(taskID,
                    committerTaskID)).join();
        }
    }

    @Override
    public void received(Record<?> record) {
        // TODO maybe received barrier, need change method to support this.
        try {
            if (record.getData() instanceof ClosedSign) {
                this.close();
            } else {
                writer.write((T) record.getData());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
