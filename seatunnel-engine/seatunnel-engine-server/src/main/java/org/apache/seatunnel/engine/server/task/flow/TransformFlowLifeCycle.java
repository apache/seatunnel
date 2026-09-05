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
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.signal.Signal;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.error.DefaultErrorSinkWriter;
import org.apache.seatunnel.engine.server.task.error.DefaultRowErrorClassifier;
import org.apache.seatunnel.engine.server.task.error.ErrorHandler;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerConfigUtil;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerConfigUtil.StageType;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerMode;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlingFlatMapTransform;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlingMapTransform;
import org.apache.seatunnel.engine.server.task.error.ErrorSinkConfig;
import org.apache.seatunnel.engine.server.task.error.ErrorSinkRowWriter;
import org.apache.seatunnel.engine.server.task.error.LocalErrorHandlerCounter;
import org.apache.seatunnel.engine.server.task.error.RowErrorClassifier;
import org.apache.seatunnel.engine.server.task.error.StageErrorConfig;
import org.apache.seatunnel.engine.server.task.error.StateStoreErrorHandlerCounter;
import org.apache.seatunnel.engine.server.task.error.SynchronizedErrorSinkRowWriter;
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
import java.util.Map;

import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_PROCESS_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_RECORDS_OUT;

/** Executes transform operators and extends stain trace payloads across transform boundaries. */
@Slf4j
public class TransformFlowLifeCycle<T> extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    private ErrorHandler<T> errorHandler;

    private transient Counter processNs;
    private transient Counter recordsIn;
    private transient Counter recordsOut;
    private volatile Counter stainTraceEntriesTruncatedTotal;
    private volatile Boolean stainTracePropagateToAllSplits;
    private volatile int stainTraceMaxEntriesPerTrace = -1;
    private boolean dryRunSampleEnabled;
    private boolean dryRunSamplePrintData;
    private int dryRunSampleLimit;
    private int[] dryRunSampleCounts;

    public TransformFlowLifeCycle(
            TransformChainAction<T> action,
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            CompletableFuture<Void> completableFuture) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.collector = collector;
        this.dryRunSampleCounts = new int[transform.size()];
    }

    @Override
    public void open() throws Exception {
        super.open();
        // Use the task's metrics context so metrics can be reported by TaskExecutionService.
        // (TaskExecutionContext#getOrCreateMetricsContext reads from the master IMAP and may return
        // a fresh context which is not tracked/reported on the worker.)
        final org.apache.seatunnel.api.common.metrics.MetricsContext metricsContext =
                runningTask.getMetricsContext();
        Map<String, Object> jobEnvOptions = runningTask.getJobEnvOptions();
        this.dryRunSampleEnabled = DryRunSampleConfig.isEnabled(jobEnvOptions);
        this.dryRunSampleLimit = DryRunSampleConfig.getLimit(jobEnvOptions);
        this.dryRunSamplePrintData = DryRunSampleConfig.isPrintData(jobEnvOptions);
        this.processNs = metricsContext.counter(TRANSFORM_PROCESS_NANOS + "#" + action.getId());
        this.recordsIn = metricsContext.counter(TRANSFORM_RECORDS_IN + "#" + action.getId());
        this.recordsOut = metricsContext.counter(TRANSFORM_RECORDS_OUT + "#" + action.getId());
        initErrorHandlingTransforms();
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.open();
                if (dryRunSampleEnabled) {
                    log.info(
                            "Dry-run sample [transform:{}] schemas: {}",
                            t.getPluginName(),
                            describeProducedSchemas(t.getProducedCatalogTables()));
                }
            } catch (Exception e) {
                log.error(
                        "Open transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
    }

    static List<String> describeProducedSchemas(List<CatalogTable> catalogTables) {
        List<String> schemas = new ArrayList<>(catalogTables.size());
        for (CatalogTable catalogTable : catalogTables) {
            schemas.add(catalogTable.getTablePath() + ": " + catalogTable.getSeaTunnelRowType());
        }
        return schemas;
    }

    @Override
    public void received(Record<?> record) {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            if (barrier.prepareClose(this.runningTask.getTaskLocation())) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                flushErrorHandler(barrier.getId());
                snapshotErrorHandler(barrier.getId());
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
        } else if (record.getData() instanceof TableOperationEvent) {
            if (prepareClose) {
                return;
            }
            // Table operations do not change column shape, so transforms pass them through.
            collector.collect(record);
        } else if (record.getData() instanceof Signal) {
            if (prepareClose) {
                return;
            }
            if (record.getData() instanceof FlushSignal) {
                flushErrorHandler();
            }
            collector.collect(record);
        } else {
            if (prepareClose) {
                return;
            }
            T inputData = (T) record.getData();
            boolean metricsEnabled = runningTask != null && runningTask.isObservabilityEnabled();
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
            List<T> outputDataList;
            if (metricsEnabled) {
                recordsIn.inc();
                long startNs = System.nanoTime();
                outputDataList = transform(inputData);
                processNs.inc(System.nanoTime() - startNs);
            } else {
                outputDataList = transform(inputData);
            }
            if (!outputDataList.isEmpty()) {
                if (metricsEnabled) {
                    recordsOut.inc(outputDataList.size());
                }
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

    public List<T> transform(T inputData) {
        if (transform.isEmpty()) {
            return Collections.singletonList(inputData);
        }

        List<T> dataList = new ArrayList<>();
        dataList.add(inputData);

        for (int transformIndex = 0; transformIndex < transform.size(); transformIndex++) {
            SeaTunnelTransform<T> transformer = transform.get(transformIndex);
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
            if (dryRunSampleEnabled && dryRunSamplePrintData) {
                for (T output : dataList) {
                    if (dryRunSampleCounts[transformIndex] >= dryRunSampleLimit) {
                        break;
                    }
                    dryRunSampleCounts[transformIndex]++;
                    log.info(
                            "Dry-run sample [transform:{}] row {}: {}",
                            transformer.getPluginName(),
                            dryRunSampleCounts[transformIndex],
                            output);
                }
            }
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
        if (errorHandler != null) {
            try {
                errorHandler.close();
            } catch (Exception e) {
                log.error("Close ErrorHandler for transform stage failed", e);
                throw new IOException("Close ErrorHandler for transform stage failed", e);
            }
        }
        super.close();
    }

    private void flushErrorHandler() {
        flushErrorHandler(null);
    }

    private void flushErrorHandler(Long checkpointId) {
        if (errorHandler == null) {
            return;
        }
        try {
            if (checkpointId == null) {
                errorHandler.flush();
            } else {
                errorHandler.flush(checkpointId);
            }
        } catch (Exception e) {
            throw new RuntimeException("Flush ErrorHandler for transform stage failed", e);
        }
    }

    private void snapshotErrorHandler(long checkpointId) {
        if (errorHandler != null) {
            errorHandler.snapshotState(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (errorHandler != null) {
            errorHandler.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        if (errorHandler != null) {
            errorHandler.notifyCheckpointAborted(checkpointId);
        }
    }

    private void initErrorHandlingTransforms() {
        if (!(runningTask instanceof SeaTunnelTask)) {
            return;
        }
        SeaTunnelTask seaTunnelTask = (SeaTunnelTask) runningTask;
        Map<String, Object> envOptions = seaTunnelTask.getEnvOptions();

        StageErrorConfig stageConfig =
                ErrorHandlerConfigUtil.buildStageConfig(
                        envOptions, StageType.TRANSFORM, getJobIdOrDefault(seaTunnelTask));

        if (stageConfig.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }
        ErrorSinkRowWriter<T> errorSinkWriter = createErrorSinkWriter(seaTunnelTask, stageConfig);
        ErrorHandler<T> handler =
                createErrorHandler(
                        seaTunnelTask, stageConfig, errorSinkWriter, action.getId(), "TRANSFORM");
        this.errorHandler = handler;
        RowErrorClassifier<T> classifier = new DefaultRowErrorClassifier<>();

        for (int i = 0; i < transform.size(); i++) {
            SeaTunnelTransform<T> t = transform.get(i);
            if (t instanceof SeaTunnelFlatMapTransform) {
                transform.set(
                        i,
                        new ErrorHandlingFlatMapTransform<>(
                                (SeaTunnelFlatMapTransform<T>) t, handler, classifier));
            } else if (t instanceof SeaTunnelMapTransform) {
                transform.set(
                        i,
                        new ErrorHandlingMapTransform<>(
                                (SeaTunnelMapTransform<T>) t, handler, classifier));
            }
        }
    }

    private ErrorHandler<T> createErrorHandler(
            SeaTunnelTask seaTunnelTask,
            StageErrorConfig stageConfig,
            ErrorSinkRowWriter<T> errorSinkWriter,
            long actionId,
            String stageName) {
        TaskLocation location = seaTunnelTask.getTaskLocation();
        if (location == null || seaTunnelTask.getExecutionContext() == null) {
            return new ErrorHandler<>(stageConfig, errorSinkWriter, new LocalErrorHandlerCounter());
        }
        return new ErrorHandler<>(
                stageConfig,
                errorSinkWriter,
                new StateStoreErrorHandlerCounter(
                        seaTunnelTask
                                .getExecutionContext()
                                .getStateStores()
                                .errorHandlerCounterStore(),
                        location.getJobId(),
                        location.getPipelineId(),
                        actionId,
                        stageName));
    }

    private static long getJobIdOrDefault(SeaTunnelTask seaTunnelTask) {
        return seaTunnelTask.getTaskLocation() == null
                ? -1L
                : seaTunnelTask.getTaskLocation().getJobId();
    }

    @SuppressWarnings("unchecked")
    private ErrorSinkRowWriter<T> createErrorSinkWriter(
            SeaTunnelTask seaTunnelTask, StageErrorConfig stageConfig) {
        if (stageConfig.getMode() != ErrorHandlerMode.ROUTE) {
            return null;
        }
        ErrorSinkConfig sinkConfig = stageConfig.getSink();
        if (sinkConfig == null || !sinkConfig.isConfigured()) {
            return null;
        }
        DefaultErrorSinkWriter<T> writer =
                new DefaultErrorSinkWriter<>(
                        stageConfig,
                        sinkConfig,
                        seaTunnelTask.getTaskLocation().getJobId(),
                        seaTunnelTask.getTaskLocation().getTaskIndex(),
                        seaTunnelTask.getExecutionContext().getClassLoaderService(),
                        runningTask.getMetricsContext(),
                        event -> {});
        writer.open();
        return (ErrorSinkRowWriter<T>) new SynchronizedErrorSinkRowWriter<>(writer);
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
