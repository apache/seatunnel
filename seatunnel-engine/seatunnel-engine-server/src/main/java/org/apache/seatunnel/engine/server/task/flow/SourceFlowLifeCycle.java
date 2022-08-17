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

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.execution.TaskInfo;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SourceReaderContext;
import org.apache.seatunnel.engine.server.task.operation.source.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceRegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceUnregisterOperation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> extends AbstractFlowLifeCycle {

    private static final ILogger LOGGER = Logger.getLogger(SourceFlowLifeCycle.class);

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskInfo enumeratorTaskInfo;
    private final SeaTunnelTask runningTask;

    private SourceReader<T, SplitT> reader;

    private final int indexID;

    private final TaskInfo currentTaskInfo;

    private SeaTunnelSourceCollector<T> collector;

    private volatile boolean closed;

    public SourceFlowLifeCycle(SourceAction<T, SplitT, ?> sourceAction,
                               TaskInfo enumeratorTaskInfo, SeaTunnelTask runningTask,
                               TaskInfo currentTaskInfo, CompletableFuture<Void> completableFuture) {
        super(completableFuture);
        this.sourceAction = sourceAction;
        this.indexID = currentTaskInfo.getIndex();
        this.enumeratorTaskInfo = enumeratorTaskInfo;
        this.runningTask = runningTask;
        this.currentTaskInfo = currentTaskInfo;
    }

    public void setCollector(SeaTunnelSourceCollector<T> collector) {
        this.collector = collector;
    }

    @Override
    public void init() throws Exception {
        this.closed = false;
        reader = sourceAction.getSource()
                .createReader(new SourceReaderContext(indexID, sourceAction.getSource().getBoundedness(), this));
        reader.open();
        register();
    }

    @Override
    public void close() throws IOException {
        super.close();
        reader.close();
    }

    public void collect() throws Exception {
        if (!closed) {
            reader.pollNext(collector);
        }
    }

    public void signalNoMoreElement() {
        // Close this reader
        try {
            this.closed = true;
            collector.close();
            runningTask.getExecutionContext().sendToMaster(new SourceUnregisterOperation(currentTaskInfo,
                    enumeratorTaskInfo)).get();
            this.close();
        } catch (Exception e) {
            LOGGER.warning("source close failed ", e);
            throw new RuntimeException(e);
        }
    }

    private void register() {
        try {
            runningTask.getExecutionContext().sendToMaster(new SourceRegisterOperation(currentTaskInfo,
                    enumeratorTaskInfo)).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source register failed ", e);
            throw new RuntimeException(e);
        }
    }

    public void requestSplit() {
        try {
            runningTask.getExecutionContext().sendToMaster(new RequestSplitOperation(currentTaskInfo,
                    enumeratorTaskInfo)).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source request split failed", e);
            throw new RuntimeException(e);
        }
    }

    public void receivedSplits(List<SplitT> splits) {
        if (splits.isEmpty()) {
            reader.handleNoMoreSplits();
        } else {
            reader.addSplits(splits);
        }
    }
}
