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
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SourceReaderContext;
import org.apache.seatunnel.engine.server.task.operation.RegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.UnregisterOperation;

import java.io.IOException;
import java.util.List;

public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> implements FlowLifeCycle {

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final int enumeratorTaskID;
    private final SeaTunnelTask<?> runningTask;

    private SourceReader<T, SplitT> reader;

    private final int indexID;

    private final int currentTaskID;

    public SourceFlowLifeCycle(SourceAction<T, SplitT, ?> sourceAction, int indexID,
                               int enumeratorTaskID, SeaTunnelTask<?> runningTask, int currentTaskID) {
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
        runningTask.sendToMaster(new UnregisterOperation(currentTaskID, enumeratorTaskID));
        try {
            runningTask.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void register() {
        runningTask.sendToMaster(new RegisterOperation(currentTaskID, enumeratorTaskID));
    }

    public void requestSplit() {
        runningTask.sendToMaster(new RequestSplitOperation(currentTaskID, enumeratorTaskID));
    }

    private void receivedSplits(List<SplitT> splits) {
        if (splits.isEmpty()) {
            reader.handleNoMoreSplits();
        } else {
            reader.addSplits(splits);
        }
    }
}
