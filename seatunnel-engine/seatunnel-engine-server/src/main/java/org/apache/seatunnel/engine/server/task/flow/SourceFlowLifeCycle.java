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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SourceReaderContext;
import org.apache.seatunnel.engine.server.task.operation.source.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceRegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceUnregisterOperation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> implements FlowLifeCycle {

    private static final ILogger LOGGER = Logger.getLogger(SourceFlowLifeCycle.class);

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskLocation enumeratorTaskID;
    private final SeaTunnelTask runningTask;

    private SourceReader<T, SplitT> reader;

    private final int indexID;

    private final TaskLocation currentTaskID;

    public SourceFlowLifeCycle(SourceAction<T, SplitT, ?> sourceAction, int indexID,
                               TaskLocation enumeratorTaskID, SeaTunnelTask runningTask, TaskLocation currentTaskID) {
        this.sourceAction = sourceAction;
        this.indexID = indexID;
        this.enumeratorTaskID = enumeratorTaskID;
        this.runningTask = runningTask;
        this.currentTaskID = currentTaskID;
    }

    @Override
    public void init() throws Exception {
        reader = sourceAction.getSource()
                .createReader(new SourceReaderContext(indexID, sourceAction.getSource().getBoundedness(), this));
        reader.open();
        register();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public void collect(Collector<T> collector) throws Exception {
        reader.pollNext(collector);
    }

    @Override
    public void handleMessage(Object message) throws Exception {
        // TODO which method should be run? register or requestSplit or other?
    }

    public void signalNoMoreElement() {
        // Close this reader
        try {
            runningTask.getExecutionContext().sendToMaster(new SourceUnregisterOperation(currentTaskID,
                    enumeratorTaskID)).get();
            runningTask.close();
        } catch (Exception e) {
            LOGGER.warning("source register failed ", e);
            throw new RuntimeException(e);
        }
    }

    private void register() {
        try {
            runningTask.getExecutionContext().sendToMaster(new SourceRegisterOperation(currentTaskID,
                    enumeratorTaskID)).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source register failed ", e);
            throw new RuntimeException(e);
        }
    }

    public void requestSplit() {
        try {
            runningTask.getExecutionContext().sendToMaster(new RequestSplitOperation(currentTaskID,
                    enumeratorTaskID)).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source request split failed", e);
            throw new RuntimeException(e);
        }
    }

    private void receivedSplits(List<SplitT> splits) {
        if (splits.isEmpty()) {
            reader.handleNoMoreSplits();
        } else {
            reader.addSplits(splits);
        }
    }
}
