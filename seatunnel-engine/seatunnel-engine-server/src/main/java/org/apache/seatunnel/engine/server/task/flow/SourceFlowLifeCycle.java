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

import static org.apache.seatunnel.engine.server.utils.ExceptionUtil.sneakyThrow;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SourceReaderContext;
import org.apache.seatunnel.engine.server.task.operation.source.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceNoMoreElementOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceRegisterOperation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> extends AbstractFlowLifeCycle implements InternalCheckpointListener {

    private static final ILogger LOGGER = Logger.getLogger(SourceFlowLifeCycle.class);

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskLocation enumeratorTaskID;

    private SourceReader<T, SplitT> reader;

    private transient Serializer<SplitT> splitSerializer;

    private transient org.apache.seatunnel.engine.checkpoint.storage.common.Serializer protoStuffSerializer;
    private final int indexID;

    private final TaskLocation currentTaskID;

    private SeaTunnelSourceCollector<T> collector;

    private volatile boolean closed;

    public SourceFlowLifeCycle(SourceAction<T, SplitT, ?> sourceAction, int indexID,
                               TaskLocation enumeratorTaskID, SeaTunnelTask runningTask,
                               TaskLocation currentTaskID, CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        this.sourceAction = sourceAction;
        this.indexID = indexID;
        this.enumeratorTaskID = enumeratorTaskID;
        this.currentTaskID = currentTaskID;
    }

    public void setCollector(SeaTunnelSourceCollector<T> collector) {
        this.collector = collector;
    }

    @Override
    public void init() throws Exception {
        this.closed = false;
        this.splitSerializer = sourceAction.getSource().getSplitSerializer();
        this.protoStuffSerializer = new ProtoStuffSerializer();
        reader = sourceAction.getSource()
                .createReader(new SourceReaderContext(indexID, sourceAction.getSource().getBoundedness(), this));
        reader.open();
        register();
    }

    @Override
    public void close() throws IOException {
        reader.close();
        super.close();
    }

    public void collect() throws Exception {
        if (!closed) {
            reader.pollNext(collector);
        }
    }

    public void signalNoMoreElement() {
        // ready close this reader
        try {
            this.closed = true;
            runningTask.getExecutionContext().sendToMaster(new SourceNoMoreElementOperation(currentTaskID,
                    enumeratorTaskID)).get();
        } catch (Exception e) {
            LOGGER.warning("source close failed ", e);
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

    public void receivedSplits(List<SplitT> splits) {
        if (splits.isEmpty()) {
            reader.handleNoMoreSplits();
        } else {
            reader.addSplits(splits);
        }
    }

    public void snapshotState(long checkpointId) throws Exception {
        List<byte[]> state = reader.snapshotState(checkpointId)
            .stream()
            .map(info -> {
                try {
                    return splitSerializer.serialize(info);
                } catch (IOException e) {
                    sneakyThrow(e);
                }
                // This method wouldn't be executed.
                throw new RuntimeException("Never throw here.");
            }).collect(Collectors.toList());
        runningTask.ack(checkpointId);
        runningTask.addState(checkpointId, sourceAction.getId(),  protoStuffSerializer.serialize(state));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        reader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        reader.notifyCheckpointAborted(checkpointId);
    }
}
