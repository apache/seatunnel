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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;
import org.apache.seatunnel.engine.server.trace.StainTraceUtils;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Executes transform operators and extends stain trace payloads across transform boundaries. */
@Slf4j
public class TransformFlowLifeCycle<T> extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>> {

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    private volatile int stainTraceMaxEntriesPerTrace = -1;
    private volatile Counter stainTraceEntriesTruncatedTotal;
    private volatile Boolean stainTracePropagateToAllSplits;

    public TransformFlowLifeCycle(
            TransformChainAction<T> action,
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            CompletableFuture<Void> completableFuture) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.collector = collector;
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

    /** Propagates barriers and schema changes, and extends stain trace payloads for row data. */
    @Override
    public void received(Record<?> record) {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            if (barrier.prepareClose(this.runningTask.getTaskLocation())) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                runningTask.addState(barrier, ActionStateKey.of(action), Collections.emptyList());
            }
            // ack after #addState
            runningTask.ack(barrier);
            collector.collect(record);
        } else if (record.getData() instanceof SchemaChangeEvent) {
            if (prepareClose) {
                return;
            }
            SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
            for (int i = 0; i < transform.size(); i++) {
                SeaTunnelTransform<T> t = transform.get(i);
                // Refresh this transform's input from upstream's post-event produced schema so
                // its catalog matches the actual row layout it will receive. Without this, each
                // transform applies ALTER to its own stale local catalog, diverging from the
                // upstream's actual output positions and breaking name-based field access (SQL
                // projections, FilterField excludes) after live ALTER ADD COLUMN.
                if (i > 0) {
                    t.setInputCatalogTables(transform.get(i - 1).getProducedCatalogTables());
                }
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
            boolean hasTracePayload =
                    inputData instanceof SeaTunnelRow
                            && StainTraceUtils.hasPayload((SeaTunnelRow) inputData);
            if (hasTracePayload) {
                SeaTunnelRow inputRow = (SeaTunnelRow) inputData;
                StainTraceUtils.appendIfPresent(
                        inputRow,
                        StainTraceStage.TRANSFORM_IN,
                        runningTask.getTaskID(),
                        System.currentTimeMillis(),
                        getStainTraceMaxEntriesPerTrace(),
                        getStainTraceEntriesTruncatedTotal());
            }
            List<T> outputDataList = transform(inputData);
            if (!outputDataList.isEmpty()) {
                // todo log metrics
                byte[] inheritedPayload = null;
                if (hasTracePayload) {
                    inheritedPayload = StainTraceUtils.getPayloadOrNull((SeaTunnelRow) inputData);
                }
                boolean propagateToAllSplits =
                        hasTracePayload
                                && inheritedPayload != null
                                && outputDataList.size() > 1
                                && isStainTracePropagateToAllSplits();
                boolean payloadInherited = false;
                for (T outputData : outputDataList) {
                    if (hasTracePayload && outputData instanceof SeaTunnelRow) {
                        SeaTunnelRow outputRow = (SeaTunnelRow) outputData;
                        if (inheritedPayload == null) {
                            StainTraceUtils.removePayload(outputRow);
                        } else if (propagateToAllSplits) {
                            StainTraceUtils.setPayload(outputRow, inheritedPayload);
                            StainTraceUtils.appendIfPresent(
                                    outputRow,
                                    StainTraceStage.TRANSFORM_OUT,
                                    runningTask.getTaskID(),
                                    System.currentTimeMillis(),
                                    getStainTraceMaxEntriesPerTrace(),
                                    getStainTraceEntriesTruncatedTotal());
                        } else if (!payloadInherited) {
                            StainTraceUtils.setPayload(outputRow, inheritedPayload);
                            StainTraceUtils.appendIfPresent(
                                    outputRow,
                                    StainTraceStage.TRANSFORM_OUT,
                                    runningTask.getTaskID(),
                                    System.currentTimeMillis(),
                                    getStainTraceMaxEntriesPerTrace(),
                                    getStainTraceEntriesTruncatedTotal());
                            payloadInherited = true;
                        } else {
                            StainTraceUtils.removePayload(outputRow);
                        }
                    }
                    collector.collect(new Record<>(outputData));
                }
            }
        }
    }

    /** Runs the configured transform chain and returns all rows produced from the current input. */
    public List<T> transform(T inputData) {
        if (transform.isEmpty()) {
            return Collections.singletonList(inputData);
        }

        List<T> dataList = new ArrayList<>();
        dataList.add(inputData);

        for (SeaTunnelTransform<T> transformer : transform) {
            List<T> nextInputDataList = new ArrayList<>();
            if (transformer instanceof SeaTunnelFlatMapTransform) {
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
        // nothing
    }

    @Override
    public void close() throws IOException {
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

    private Counter getStainTraceEntriesTruncatedTotal() {
        if (stainTraceEntriesTruncatedTotal == null) {
            synchronized (this) {
                if (stainTraceEntriesTruncatedTotal == null) {
                    stainTraceEntriesTruncatedTotal =
                            runningTask
                                    .getMetricsContext()
                                    .counter(StainTraceConstants.METRIC_ENTRIES_TRUNCATED_TOTAL);
                }
            }
        }
        return stainTraceEntriesTruncatedTotal;
    }

    private int getStainTraceMaxEntriesPerTrace() {
        if (stainTraceMaxEntriesPerTrace < 0) {
            synchronized (this) {
                if (stainTraceMaxEntriesPerTrace < 0) {
                    stainTraceMaxEntriesPerTrace =
                            runningTask
                                    .getExecutionContext()
                                    .getTaskExecutionService()
                                    .getSeaTunnelConfig()
                                    .getEngineConfig()
                                    .getStainTraceMaxEntriesPerTrace();
                }
            }
        }
        return stainTraceMaxEntriesPerTrace;
    }

    private boolean isStainTracePropagateToAllSplits() {
        if (stainTracePropagateToAllSplits == null) {
            synchronized (this) {
                if (stainTracePropagateToAllSplits == null) {
                    stainTracePropagateToAllSplits =
                            runningTask
                                    .getExecutionContext()
                                    .getTaskExecutionService()
                                    .getSeaTunnelConfig()
                                    .getEngineConfig()
                                    .isStainTracePropagateToAllSplits();
                }
            }
        }
        return stainTracePropagateToAllSplits;
    }
}
