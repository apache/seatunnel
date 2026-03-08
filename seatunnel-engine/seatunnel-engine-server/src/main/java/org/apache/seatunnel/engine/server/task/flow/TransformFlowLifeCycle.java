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

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelBatchTransform;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class TransformFlowLifeCycle<T> extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private static final byte SERIALIZER_PRESENT = 1;

    private static final byte SERIALIZER_ABSENT = 0;

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    private final List<BatchTransformHolder<T, ?>> batchTransforms;

    public TransformFlowLifeCycle(
            TransformChainAction<T> action,
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            CompletableFuture<Void> completableFuture) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.collector = collector;
        this.batchTransforms = collectBatchTransforms(action.getTransforms());
    }

    @Override
    public void open() throws Exception {
        super.open();
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.open();
            } catch (Exception e) {
                log.error(
                        "Open transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
    }

    @Override
    public void received(Record<?> record) {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            if (barrier.prepareClose(this.runningTask.getTaskLocation())) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                emitBatchOutputs(true);
                runningTask.addState(
                        barrier,
                        ActionStateKey.of(action),
                        snapshotBatchTransformStates(barrier.getId()));
            } else {
                emitBatchOutputs(true);
            }
            // ack after #addState
            runningTask.ack(barrier);
            collector.collect(record);
        } else if (record.getData() instanceof SchemaChangeEvent) {
            if (prepareClose) {
                return;
            }
            SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
            for (SeaTunnelTransform<T> t : transform) {
                SchemaChangeEvent eventBefore = event;
                event = t.mapSchemaChangeEvent(eventBefore);
                if (event == null) {
                    log.info(
                            "Transform[{}] filtered schema change event {}",
                            t.getPluginName(),
                            eventBefore);
                    break;
                }
                log.info(
                        "Transform[{}] input schema change event {} and output schema change event {}",
                        t.getPluginName(),
                        eventBefore,
                        event);
            }
            if (event != null) {
                collector.collect(new Record<>(event));
            }
        } else {
            if (prepareClose) {
                return;
            }
            T inputData = (T) record.getData();
            List<T> outputDataList = transform(inputData);
            if (!outputDataList.isEmpty()) {
                // todo log metrics
                for (T outputData : outputDataList) {
                    collector.collect(new Record<>(outputData));
                }
            }
        }
    }

    public List<T> transform(T inputData) {
        return applyTransforms(0, Collections.singletonList(inputData));
    }

    private List<T> applyTransforms(int startIndex, List<T> inputDataList) {
        if (transform.isEmpty()) {
            return inputDataList;
        }

        List<T> dataList = inputDataList;

        for (int i = startIndex; i < transform.size() && !dataList.isEmpty(); i++) {
            SeaTunnelTransform<T> transformer = transform.get(i);
            List<T> nextInputDataList = new ArrayList<>();
            if (transformer instanceof SeaTunnelBatchTransform) {
                SeaTunnelBatchTransform<T, ?> batchTransform =
                        (SeaTunnelBatchTransform<T, ?>) transformer;
                for (T data : dataList) {
                    batchTransform.collect(data);
                    List<T> readyOutputs = batchTransform.drainOutput();
                    if (CollectionUtils.isNotEmpty(readyOutputs)) {
                        nextInputDataList.addAll(readyOutputs);
                    }
                }
            } else if (transformer instanceof SeaTunnelFlatMapTransform) {
                SeaTunnelFlatMapTransform<T> transformDecorator =
                        (SeaTunnelFlatMapTransform<T>) transformer;
                for (T data : dataList) {
                    List<T> outputDataArray = transformDecorator.flatMap(data);
                    log.debug(
                            "Transform[{}] input row {} and output row {}",
                            transformer,
                            data,
                            outputDataArray);
                    if (CollectionUtils.isNotEmpty(outputDataArray)) {
                        nextInputDataList.addAll(outputDataArray);
                    }
                }
            } else if (transformer instanceof SeaTunnelMapTransform) {
                for (T data : dataList) {
                    SeaTunnelMapTransform<T> transformDecorator =
                            (SeaTunnelMapTransform<T>) transformer;
                    T outputData = transformDecorator.map(data);
                    log.debug(
                            "Transform[{}] input row {} and output row {}",
                            transformer,
                            data,
                            outputData);
                    if (outputData == null) {
                        log.trace("Transform[{}] filtered data row {}", transformer, data);
                        continue;
                    }
                    nextInputDataList.add(outputData);
                }
            }

            dataList = nextInputDataList;
        }

        return dataList;
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        if (actionStateList.isEmpty() || batchTransforms.isEmpty()) {
            return;
        }
        List<byte[]> stateEnvelopes =
                actionStateList.stream()
                        .map(ActionSubtaskState::getState)
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        for (int i = 0; i < batchTransforms.size() && i < stateEnvelopes.size(); i++) {
            restoreBatchTransformState(batchTransforms.get(i), stateEnvelopes.get(i));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        for (BatchTransformHolder<T, ?> batchTransform : batchTransforms) {
            batchTransform.transform.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        for (BatchTransformHolder<T, ?> batchTransform : batchTransforms) {
            batchTransform.transform.notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void close() throws IOException {
        emitBatchOutputs(true);
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.close();
            } catch (Exception e) {
                log.error(
                        "Close transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
        super.close();
    }

    private void emitBatchOutputs(boolean forceFlush) {
        for (BatchTransformHolder<T, ?> batchTransform : batchTransforms) {
            List<T> outputs =
                    forceFlush
                            ? batchTransform.transform.flush()
                            : batchTransform.transform.drainOutput();
            emitOutputs(batchTransform.transformIndex + 1, outputs);
        }
    }

    private void emitOutputs(int startIndex, List<T> outputs) {
        if (CollectionUtils.isEmpty(outputs)) {
            return;
        }
        List<T> transformedOutputs = applyTransforms(startIndex, outputs);
        for (T output : transformedOutputs) {
            collector.collect(new Record<>(output));
        }
    }

    private List<byte[]> snapshotBatchTransformStates(long checkpointId) {
        if (batchTransforms.isEmpty()) {
            return Collections.emptyList();
        }
        List<byte[]> states = new ArrayList<>(batchTransforms.size());
        for (BatchTransformHolder<T, ?> batchTransform : batchTransforms) {
            states.add(snapshotBatchTransformState(batchTransform, checkpointId));
        }
        return states;
    }

    private <StateT> byte[] snapshotBatchTransformState(
            BatchTransformHolder<T, StateT> batchTransform, long checkpointId) {
        try {
            List<StateT> states = batchTransform.transform.snapshotState(checkpointId);
            return serializeStateEnvelope(batchTransform.serializer, states);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to snapshot state for batch transform [%s]",
                            batchTransform.transform.getPluginName()),
                    e);
        }
    }

    private <StateT> byte[] serializeStateEnvelope(
            Optional<Serializer<StateT>> serializerOptional, List<StateT> states)
            throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
            if (!serializerOptional.isPresent()) {
                if (CollectionUtils.isNotEmpty(states)) {
                    throw new IllegalStateException(
                            "Batch transform returned state but no serializer was provided");
                }
                outputStream.writeByte(SERIALIZER_ABSENT);
                outputStream.writeInt(0);
                return byteArrayOutputStream.toByteArray();
            }
            outputStream.writeByte(SERIALIZER_PRESENT);
            outputStream.writeInt(states.size());
            Serializer<StateT> serializer = serializerOptional.get();
            for (StateT state : states) {
                byte[] stateBytes = serializer.serialize(state);
                if (stateBytes == null) {
                    outputStream.writeInt(-1);
                } else {
                    outputStream.writeInt(stateBytes.length);
                    outputStream.write(stateBytes);
                }
            }
            return byteArrayOutputStream.toByteArray();
        }
    }

    private <StateT> void restoreBatchTransformState(
            BatchTransformHolder<T, StateT> batchTransform, byte[] stateEnvelope) throws Exception {
        List<StateT> states = deserializeStateEnvelope(batchTransform.serializer, stateEnvelope);
        if (!states.isEmpty()) {
            batchTransform.transform.restoreState(states);
        }
    }

    private <StateT> List<StateT> deserializeStateEnvelope(
            Optional<Serializer<StateT>> serializerOptional, byte[] stateEnvelope)
            throws Exception {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(stateEnvelope);
                DataInputStream inputStream = new DataInputStream(byteArrayInputStream)) {
            byte serializerFlag = inputStream.readByte();
            int stateCount = inputStream.readInt();
            if (serializerFlag == SERIALIZER_ABSENT) {
                return Collections.emptyList();
            }
            if (!serializerOptional.isPresent()) {
                throw new IllegalStateException(
                        "Checkpoint envelope contains state but serializer is absent");
            }
            List<StateT> states = new ArrayList<>(stateCount);
            Serializer<StateT> serializer = serializerOptional.get();
            for (int i = 0; i < stateCount; i++) {
                int length = inputStream.readInt();
                if (length < 0) {
                    states.add(null);
                    continue;
                }
                byte[] stateBytes = new byte[length];
                inputStream.readFully(stateBytes);
                states.add(serializer.deserialize(stateBytes));
            }
            return states;
        }
    }

    private List<BatchTransformHolder<T, ?>> collectBatchTransforms(
            List<SeaTunnelTransform<T>> transforms) {
        List<BatchTransformHolder<T, ?>> result = new ArrayList<>();
        for (int i = 0; i < transforms.size(); i++) {
            SeaTunnelTransform<T> transform = transforms.get(i);
            if (transform instanceof SeaTunnelBatchTransform) {
                result.add(
                        new BatchTransformHolder<>(i, (SeaTunnelBatchTransform<T, ?>) transform));
            }
        }
        return result;
    }

    private static final class BatchTransformHolder<T, StateT> {
        private final int transformIndex;
        private final SeaTunnelBatchTransform<T, StateT> transform;
        private final Optional<Serializer<StateT>> serializer;

        @SuppressWarnings("unchecked")
        private BatchTransformHolder(int transformIndex, SeaTunnelBatchTransform<T, ?> transform) {
            this.transformIndex = transformIndex;
            this.transform = (SeaTunnelBatchTransform<T, StateT>) transform;
            this.serializer =
                    (Optional<Serializer<StateT>>)
                            (Optional<?>) this.transform.getStateSerializer();
        }
    }
}
