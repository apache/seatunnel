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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.task.AbstractTask.serializeStates;

@Slf4j
public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> extends ActionFlowLifeCycle
        implements InternalCheckpointListener {

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskLocation enumeratorTaskLocation;

    private Address enumeratorTaskAddress;

    private SourceReader<T, SplitT> reader;

    private transient Serializer<SplitT> splitSerializer;

    private final int indexID;

    private final TaskLocation currentTaskLocation;

    private SeaTunnelSourceCollector<T> collector;

    private final MetricsContext metricsContext;

    private final AtomicReference<SchemaChangePhase> schemaChangePhase = new AtomicReference<>();

    public SourceFlowLifeCycle(
            SourceAction<T, SplitT, ?> sourceAction,
            int indexID,
            TaskLocation enumeratorTaskLocation,
            SeaTunnelTask runningTask,
            TaskLocation currentTaskLocation,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext) {
        super(sourceAction, runningTask, completableFuture);
        this.sourceAction = sourceAction;
        this.indexID = indexID;
        this.enumeratorTaskLocation = enumeratorTaskLocation;
        this.currentTaskLocation = currentTaskLocation;
        this.metricsContext = metricsContext;
    }

    public void setCollector(SeaTunnelSourceCollector<T> collector) {
        this.collector = collector;
    }

    @Override
    public void init() throws Exception {
        this.splitSerializer = sourceAction.getSource().getSplitSerializer();
        this.reader =
                sourceAction
                        .getSource()
                        .createReader(
                                new SourceReaderContext(
                                        indexID,
                                        sourceAction.getSource().getBoundedness(),
                                        this,
                                        metricsContext));
        this.enumeratorTaskAddress = getEnumeratorTaskAddress();
    }

    @Override
    public void open() throws Exception {
        reader.open();
        register();
    }

    private Address getEnumeratorTaskAddress() throws ExecutionException, InterruptedException {
        return (Address)
                runningTask
                        .getExecutionContext()
                        .sendToMaster(new GetTaskGroupAddressOperation(enumeratorTaskLocation))
                        .get();
    }

    @Override
    public void close() throws IOException {
        reader.close();
        super.close();
    }

    public void collect() throws Exception {
        if (!prepareClose) {
            if (schemaChanging()) {
                log.debug("schema is changing, stop reader collect records");

                Thread.sleep(200);
                return;
            }

            reader.pollNext(collector);
            if (collector.isEmptyThisPollNext()) {
                Thread.sleep(100);
            } else {
                collector.resetEmptyThisPollNext();
            }

            if (collector.captureSchemaChangeBeforeCheckpointSignal()) {
                if (schemaChangePhase.get() != null) {
                    throw new IllegalStateException(
                            "previous schema changes in progress, schemaChangePhase: "
                                    + schemaChangePhase.get());
                }
                runningTask.triggerSchemaChangeBeforeCheckpoint().get();
                schemaChangePhase.set(SchemaChangePhase.createBeforePhase());
                log.info("triggered schema-change-before checkpoint, stopping collect data");
            } else if (collector.captureSchemaChangeAfterCheckpointSignal()) {
                if (schemaChangePhase.get() != null) {
                    throw new IllegalStateException(
                            "previous schema changes in progress, schemaChangePhase: "
                                    + schemaChangePhase.get());
                }
                runningTask.triggerSchemaChangeAfterCheckpoint().get();
                schemaChangePhase.set(SchemaChangePhase.createAfterPhase());
                log.info("triggered schema-change-after checkpoint, stopping collect data");
            }
        } else {
            Thread.sleep(100);
        }
    }

    public void signalNoMoreElement() {
        // ready close this reader
        try {
            this.prepareClose = true;
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SourceNoMoreElementOperation(
                                    currentTaskLocation, enumeratorTaskLocation),
                            enumeratorTaskAddress)
                    .get();
        } catch (Exception e) {
            log.warn("source close failed {}", e);
            throw new RuntimeException(e);
        }
    }

    private void register() {
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SourceRegisterOperation(
                                    currentTaskLocation, enumeratorTaskLocation),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source register failed.", e);
            throw new RuntimeException(e);
        }
    }

    public void requestSplit() {
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new RequestSplitOperation(currentTaskLocation, enumeratorTaskLocation),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source request split failed.", e);
            throw new RuntimeException(e);
        }
    }

    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SourceReaderEventOperation(
                                    enumeratorTaskLocation, currentTaskLocation, sourceEvent),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source request split failed.", e);
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
        log.debug("source trigger barrier [{}]", barrier);

        long startTime = System.currentTimeMillis();

        // Block the reader from adding barrier to the collector.
        synchronized (collector.getCheckpointLock()) {
            if (barrier.prepareClose()) {
                this.prepareClose = true;
            }
            if (barrier.snapshot()) {
                List<byte[]> states =
                        serializeStates(splitSerializer, reader.snapshotState(barrier.getId()));
                runningTask.addState(barrier, ActionStateKey.of(sourceAction), states);
            }
            // ack after #addState
            runningTask.ack(barrier);
            log.debug("source ack barrier finished, taskId: [{}]", runningTask.getTaskID());
            collector.sendRecordToNext(new Record<>(barrier));
            log.debug("send record to next finished, taskId: [{}]", runningTask.getTaskID());
        }

        log.debug(
                "trigger barrier [{}] finished, cost: {}ms. taskLocation: [{}]",
                barrier.getId(),
                System.currentTimeMillis() - startTime,
                currentTaskLocation);

        CheckpointType checkpointType = ((CheckpointBarrier) barrier).getCheckpointType();
        if (schemaChanging() && checkpointType.isSchemaChangeCheckpoint()) {
            if (checkpointType.isSchemaChangeBeforeCheckpoint()
                    && schemaChangePhase.get().isBeforePhase()) {
                schemaChangePhase.get().setCheckpointId(barrier.getId());
            } else if (checkpointType.isSchemaChangeAfterCheckpoint()
                    && schemaChangePhase.get().isAfterPhase()) {
                schemaChangePhase.get().setCheckpointId(barrier.getId());
            } else {
                throw new IllegalStateException(
                        String.format(
                                "schema-change checkpoint[%s,%s] and phase[%s] is not matched",
                                barrier.getId(),
                                checkpointType,
                                schemaChangePhase.get().getPhase()));
            }
            log.info(
                    "lock checkpoint[{}] waiting for complete..., phase: [{}]",
                    barrier.getId(),
                    schemaChangePhase.get().getPhase());
        }
    }

    private boolean schemaChanging() {
        return schemaChangePhase.get() != null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        reader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        reader.notifyCheckpointAborted(checkpointId);
        if (schemaChangePhase.get() != null
                && schemaChangePhase.get().getCheckpointId() == checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "schema-change checkpoint[%s] is aborted, phase: [%s]",
                            checkpointId, schemaChangePhase.get().getPhase()));
        }
    }

    @Override
    public void notifyCheckpointEnd(long checkpointId) throws Exception {
        if (schemaChangePhase.get() != null
                && schemaChangePhase.get().getCheckpointId() == checkpointId) {
            log.info(
                    "notify schema-change checkpoint[{}] end, phase: [{}]",
                    checkpointId,
                    schemaChangePhase.get().getPhase());
            schemaChangePhase.set(null);
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        if (actionStateList.isEmpty()) {
            return;
        }
        List<byte[]> splits =
                actionStateList.stream()
                        .map(ActionSubtaskState::getState)
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new RestoredSplitOperation(enumeratorTaskLocation, splits, indexID),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source request split failed.", e);
            throw new RuntimeException(e);
        }
    }

    @Getter
    @ToString
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SchemaChangePhase implements Serializable {
        private static final String PHASE_CHANGE_BEFORE = "SCHEMA-CHANGE-BEFORE";
        private static final String PHASE_CHANGE_AFTER = "SCHEMA-CHANGE-AFTER";

        private final String phase;
        private volatile long checkpointId = -1;

        public static SchemaChangePhase createBeforePhase() {
            return new SchemaChangePhase(PHASE_CHANGE_BEFORE);
        }

        public static SchemaChangePhase createAfterPhase() {
            return new SchemaChangePhase(PHASE_CHANGE_AFTER);
        }

        public boolean isBeforePhase() {
            return PHASE_CHANGE_BEFORE.equals(phase);
        }

        public boolean isAfterPhase() {
            return PHASE_CHANGE_AFTER.equals(phase);
        }

        public void setCheckpointId(long checkpointId) {
            if (this.checkpointId != -1) {
                throw new IllegalStateException("checkpointId is already set");
            }
            this.checkpointId = checkpointId;
        }
    }
}
