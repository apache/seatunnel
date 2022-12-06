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
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SourceReaderContext;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupAddressOperation;
import org.apache.seatunnel.engine.server.task.operation.source.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.RestoredSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceNoMoreElementOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceReaderEventOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceRegisterOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> extends ActionFlowLifeCycle implements InternalCheckpointListener {

    private static final ILogger LOGGER = Logger.getLogger(SourceFlowLifeCycle.class);

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskLocation enumeratorTaskLocation;

    private Address enumeratorTaskAddress;

    private SourceReader<T, SplitT> reader;

    private transient Serializer<SplitT> splitSerializer;

    private final int indexID;

    private final TaskLocation currentTaskLocation;

    private SeaTunnelSourceCollector<T> collector;

    public SourceFlowLifeCycle(SourceAction<T, SplitT, ?> sourceAction, int indexID,
                               TaskLocation enumeratorTaskLocation, SeaTunnelTask runningTask,
                               TaskLocation currentTaskLocation, CompletableFuture<Void> completableFuture) {
        super(sourceAction, runningTask, completableFuture);
        this.sourceAction = sourceAction;
        this.indexID = indexID;
        this.enumeratorTaskLocation = enumeratorTaskLocation;
        this.currentTaskLocation = currentTaskLocation;
    }

    public void setCollector(SeaTunnelSourceCollector<T> collector) {
        this.collector = collector;
    }

    @Override
    public void init() throws Exception {
        this.splitSerializer = sourceAction.getSource().getSplitSerializer();
        this.reader = sourceAction.getSource()
                .createReader(new SourceReaderContext(indexID, sourceAction.getSource().getBoundedness(), this));
        this.enumeratorTaskAddress = getEnumeratorTaskAddress();
    }

    @Override
    public void open() throws Exception {
        reader.open();
        register();
    }

    private Address getEnumeratorTaskAddress() throws ExecutionException, InterruptedException {
        return (Address) runningTask.getExecutionContext()
            .sendToMaster(new GetTaskGroupAddressOperation(enumeratorTaskLocation)).get();
    }

    @Override
    public void close() throws IOException {
        reader.close();
        super.close();
    }

    public void collect() throws Exception {
        if (!prepareClose) {
            reader.pollNext(collector);
        }
    }

    public void signalNoMoreElement() {
        // ready close this reader
        try {
            this.prepareClose = true;
            runningTask.getExecutionContext().sendToMember(new SourceNoMoreElementOperation(currentTaskLocation,
                enumeratorTaskLocation), enumeratorTaskAddress).get();
        } catch (Exception e) {
            LOGGER.warning("source close failed ", e);
            throw new RuntimeException(e);
        }
    }

    private void register() {
        try {
            runningTask.getExecutionContext().sendToMember(new SourceRegisterOperation(currentTaskLocation,
                enumeratorTaskLocation), enumeratorTaskAddress).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source register failed ", e);
            throw new RuntimeException(e);
        }
    }

    public void requestSplit() {
        try {
            runningTask.getExecutionContext().sendToMember(new RequestSplitOperation(currentTaskLocation,
                enumeratorTaskLocation), enumeratorTaskAddress).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source request split failed", e);
            throw new RuntimeException(e);
        }
    }

    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        try {
            runningTask.getExecutionContext().sendToMember(
                new SourceReaderEventOperation(enumeratorTaskLocation, currentTaskLocation, sourceEvent),
                enumeratorTaskAddress).get();
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

    public void triggerBarrier(Barrier barrier) throws Exception {
        // Block the reader from adding barrier to the collector.
        synchronized (collector.getCheckpointLock()) {
            if (barrier.prepareClose()) {
                this.prepareClose = true;
            }
            if (barrier.snapshot()) {
                List<byte[]> states = serializeStates(splitSerializer, reader.snapshotState(barrier.getId()));
                runningTask.addState(barrier, sourceAction.getId(), states);
            }
            // ack after #addState
            runningTask.ack(barrier);
            collector.sendRecordToNext(new Record<>(barrier));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        reader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        reader.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        if (actionStateList.isEmpty()) {
            return;
        }
        List<SplitT> splits = actionStateList.stream()
            .map(ActionSubtaskState::getState)
            .flatMap(Collection::stream)
            .map(bytes -> sneaky(() -> splitSerializer.deserialize(bytes)))
            .collect(Collectors.toList());
        try {
            runningTask.getExecutionContext()
                .sendToMember(new RestoredSplitOperation(enumeratorTaskLocation, SerializationUtils.serialize(splits.toArray()), indexID),
                    enumeratorTaskAddress).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warning("source request split failed", e);
            throw new RuntimeException(e);
        }
    }
}
