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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;

@Slf4j
public class TransformFlowLifeCycle<T> extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    /** Batch transforms that require state management */
    private final List<SeaTunnelBatchTransform<T, ?>> batchTransforms;

    /** State serializers for batch transforms */
    private final List<Optional<Serializer<?>>> stateSerializers;

    public TransformFlowLifeCycle(
            TransformChainAction<T> action,
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            CompletableFuture<Void> completableFuture) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.collector = collector;

        // Extract batch transforms and their serializers
        this.batchTransforms = new ArrayList<>();
        this.stateSerializers = new ArrayList<>();
        for (SeaTunnelTransform<T> t : transform) {
            if (t instanceof SeaTunnelBatchTransform) {
                @SuppressWarnings("unchecked")
                SeaTunnelBatchTransform<T, ?> batchTransform = (SeaTunnelBatchTransform<T, ?>) t;
                batchTransforms.add(batchTransform);
                @SuppressWarnings("unchecked")
                Optional<Serializer<?>> serializer =
                        (Optional<Serializer<?>>) (Optional<?>) batchTransform.getStateSerializer();
                stateSerializers.add(serializer);
            }
        }
        log.info(
                "TransformFlowLifeCycle initialized with {} batch transforms out of {} total transforms",
                batchTransforms.size(),
                transform.size());
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
                // Flush batch transforms before checkpoint and snapshot their states
                List<byte[]> states = snapshotBatchTransformStates(barrier.getId());
                runningTask.addState(barrier, ActionStateKey.of(action), states);
            } else {
                // For non-snapshot barriers, still flush batch transforms
                flushAllBatchTransforms();
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
        if (transform.isEmpty()) {
            return Collections.singletonList(inputData);
        }

        List<T> dataList = new ArrayList<>();
        dataList.add(inputData);

        for (SeaTunnelTransform<T> transformer : transform) {
            List<T> nextInputDataList = new ArrayList<>();
            if (transformer instanceof SeaTunnelBatchTransform) {
                // For batch transform, collect data and check if there's immediate output
                SeaTunnelBatchTransform<T, ?> batchTransform =
                        (SeaTunnelBatchTransform<T, ?>) transformer;
                for (T data : dataList) {
                    batchTransform.collect(data);
                    log.trace(
                            "BatchTransform[{}] collected row {}",
                            transformer.getPluginName(),
                            data);
                }
                // Batch transform typically doesn't output immediately
                // Data will be flushed during checkpoint or when buffer is full
                // For now, we don't add anything to nextInputDataList
                // This means batch transforms should be at the end of the chain
                // or handle their own output through flush()
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

            // If this is a batch transform, dataList becomes empty (data is buffered)
            if (transformer instanceof SeaTunnelBatchTransform) {
                dataList = Collections.emptyList();
            } else {
                dataList = nextInputDataList;
            }
        }

        return dataList;
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        if (actionStateList.isEmpty() || batchTransforms.isEmpty()) {
            log.debug("No state to restore for transform");
            return;
        }

        List<byte[]> allStates =
                actionStateList.stream()
                        .map(ActionSubtaskState::getState)
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        if (allStates.isEmpty()) {
            log.debug("No actual states found in actionStateList");
            return;
        }

        // Restore states to batch transforms
        // Each batch transform's states are stored consecutively
        int stateIndex = 0;
        for (int i = 0; i < batchTransforms.size() && stateIndex < allStates.size(); i++) {
            SeaTunnelBatchTransform<T, ?> batchTransform = batchTransforms.get(i);
            Optional<Serializer<?>> serializerOpt = stateSerializers.get(i);

            if (serializerOpt.isPresent()) {
                List<Object> states = new ArrayList<>();
                // Read states for this transform (assuming one state per transform for simplicity)
                if (stateIndex < allStates.size()) {
                    byte[] stateBytes = allStates.get(stateIndex++);
                    Object state = sneaky(() -> serializerOpt.get().deserialize(stateBytes));
                    states.add(state);
                }
                restoreBatchTransformState(batchTransform, states);
                log.info(
                        "Restored state for batch transform [{}], state count: {}",
                        batchTransform.getPluginName(),
                        states.size());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <StateT> void restoreBatchTransformState(
            SeaTunnelBatchTransform<T, StateT> batchTransform, List<Object> states)
            throws Exception {
        List<StateT> typedStates =
                states.stream().map(s -> (StateT) s).collect(Collectors.toList());
        batchTransform.restoreState(typedStates);
    }

    /**
     * Snapshot states from all batch transforms.
     *
     * @param checkpointId The checkpoint ID
     * @return Serialized states from all batch transforms
     */
    private List<byte[]> snapshotBatchTransformStates(long checkpointId) {
        if (batchTransforms.isEmpty()) {
            return Collections.emptyList();
        }

        List<byte[]> allStates = new ArrayList<>();
        for (int i = 0; i < batchTransforms.size(); i++) {
            SeaTunnelBatchTransform<T, ?> batchTransform = batchTransforms.get(i);
            Optional<Serializer<?>> serializerOpt = stateSerializers.get(i);

            try {
                // First flush the batch transform to output any buffered data
                List<T> flushedData = batchTransform.flush();
                if (CollectionUtils.isNotEmpty(flushedData)) {
                    for (T data : flushedData) {
                        collector.collect(new Record<>(data));
                    }
                    log.debug(
                            "Flushed {} records from batch transform [{}] during checkpoint",
                            flushedData.size(),
                            batchTransform.getPluginName());
                }

                // Then snapshot the state
                List<?> states = batchTransform.snapshotState(checkpointId);
                if (serializerOpt.isPresent() && CollectionUtils.isNotEmpty(states)) {
                    List<byte[]> serializedStates =
                            serializeBatchStates(serializerOpt.get(), states);
                    allStates.addAll(serializedStates);
                    log.debug(
                            "Snapshot {} states from batch transform [{}]",
                            states.size(),
                            batchTransform.getPluginName());
                }
            } catch (Exception e) {
                log.error(
                        "Failed to snapshot state for batch transform [{}]",
                        batchTransform.getPluginName(),
                        e);
                throw new RuntimeException(e);
            }
        }
        return allStates;
    }

    @SuppressWarnings("unchecked")
    private <StateT> List<byte[]> serializeBatchStates(
            Serializer<StateT> serializer, List<?> states) throws IOException {
        List<byte[]> serializedStates = new ArrayList<>();
        for (Object state : states) {
            serializedStates.add(serializer.serialize((StateT) state));
        }
        return serializedStates;
    }

    /** Flush all batch transforms without taking snapshot. */
    private void flushAllBatchTransforms() {
        for (SeaTunnelBatchTransform<T, ?> batchTransform : batchTransforms) {
            try {
                List<T> flushedData = batchTransform.flush();
                if (CollectionUtils.isNotEmpty(flushedData)) {
                    for (T data : flushedData) {
                        collector.collect(new Record<>(data));
                    }
                }
            } catch (Exception e) {
                log.error(
                        "Failed to flush batch transform [{}]", batchTransform.getPluginName(), e);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        for (SeaTunnelBatchTransform<T, ?> batchTransform : batchTransforms) {
            batchTransform.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        for (SeaTunnelBatchTransform<T, ?> batchTransform : batchTransforms) {
            batchTransform.notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void close() throws IOException {
        // Flush any remaining data in batch transforms before closing
        flushAllBatchTransforms();

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
}
