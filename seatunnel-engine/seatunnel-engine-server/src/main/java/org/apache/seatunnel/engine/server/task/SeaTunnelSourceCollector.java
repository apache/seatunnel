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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.DataTypeChangeEventHandler;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlGate;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.metrics.ConnectorMetricsCalcContext;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceSampler;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;
import org.apache.seatunnel.engine.server.trace.StainTraceUtils;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.apache.seatunnel.api.common.metrics.MetricNames.FLUSH_SIGNAL_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.FLUSH_SIGNAL_TOTAL;

/** Collects source output records, forwards schema changes, and seeds stain trace payloads. */
@Slf4j
public class SeaTunnelSourceCollector<T> implements Collector<T> {

    private final Object checkpointLock;

    private final List<OneInputFlowLifeCycle<Record<?>>> outputs;

    private final ConnectorMetricsCalcContext connectorMetricsCalcContext;

    private final AtomicBoolean schemaChangeBeforeCheckpointSignal = new AtomicBoolean(false);

    private final AtomicBoolean schemaChangeAfterCheckpointSignal = new AtomicBoolean(false);

    private volatile boolean emptyThisPollNext;
    private final DataTypeChangeEventHandler dataTypeChangeEventHandler =
            new DataTypeChangeEventDispatcher();
    private Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();
    private SeaTunnelDataType rowType;
    private FlowControlGate flowControlGate;

    private final long sourceTaskId;
    private final int stainTraceMaxEntriesPerTrace;
    private final Counter stainTraceBudgetThrottledTotal;
    private final Counter stainTraceSamplesGeneratedTotal;
    private final Counter stainTraceEntriesTruncatedTotal;
    private final StainTraceSampler stainTraceSampler;
    private final Counter flushSignalTotal;
    private final Meter flushSignalQPS;
    private final LongSupplier currentTimeMillisSupplier;
    private final boolean dryRunSampleEnabled;
    private final int dryRunSampleLimit;
    private final boolean dryRunSamplePrintData;
    private final Runnable dryRunSampleComplete;
    private int dryRunSampleCount;

    public SeaTunnelSourceCollector(
            Object checkpointLock,
            List<OneInputFlowLifeCycle<Record<?>>> outputs,
            MetricsContext metricsContext,
            FlowControlStrategy flowControlStrategy,
            SeaTunnelDataType rowType,
            List<TablePath> tablePaths,
            SeaTunnelTask runningTask,
            EngineConfig engineConfig) {
        this(
                checkpointLock,
                outputs,
                metricsContext,
                flowControlStrategy,
                rowType,
                tablePaths,
                runningTask,
                engineConfig,
                null,
                null,
                System::currentTimeMillis);
    }

    public SeaTunnelSourceCollector(
            Object checkpointLock,
            List<OneInputFlowLifeCycle<Record<?>>> outputs,
            MetricsContext metricsContext,
            FlowControlStrategy flowControlStrategy,
            SeaTunnelDataType rowType,
            List<TablePath> tablePaths,
            SeaTunnelTask runningTask,
            EngineConfig engineConfig,
            LongSupplier currentTimeMillisSupplier) {
        this(
                checkpointLock,
                outputs,
                metricsContext,
                flowControlStrategy,
                rowType,
                tablePaths,
                runningTask,
                engineConfig,
                null,
                null,
                currentTimeMillisSupplier);
    }

    /** Constructor with task-level stain trace overrides from job env block. */
    public SeaTunnelSourceCollector(
            Object checkpointLock,
            List<OneInputFlowLifeCycle<Record<?>>> outputs,
            MetricsContext metricsContext,
            FlowControlStrategy flowControlStrategy,
            SeaTunnelDataType rowType,
            List<TablePath> tablePaths,
            SeaTunnelTask runningTask,
            EngineConfig engineConfig,
            Map<String, Object> taskEnvOption) {
        this(
                checkpointLock,
                outputs,
                metricsContext,
                flowControlStrategy,
                rowType,
                tablePaths,
                runningTask,
                engineConfig,
                taskEnvOption,
                null,
                System::currentTimeMillis);
    }

    public SeaTunnelSourceCollector(
            Object checkpointLock,
            List<OneInputFlowLifeCycle<Record<?>>> outputs,
            MetricsContext metricsContext,
            FlowControlStrategy flowControlStrategy,
            SeaTunnelDataType rowType,
            List<TablePath> tablePaths,
            SeaTunnelTask runningTask,
            EngineConfig engineConfig,
            Map<String, Object> taskEnvOption,
            Runnable dryRunSampleComplete) {
        this(
                checkpointLock,
                outputs,
                metricsContext,
                flowControlStrategy,
                rowType,
                tablePaths,
                runningTask,
                engineConfig,
                taskEnvOption,
                dryRunSampleComplete,
                System::currentTimeMillis);
    }

    SeaTunnelSourceCollector(
            Object checkpointLock,
            List<OneInputFlowLifeCycle<Record<?>>> outputs,
            MetricsContext metricsContext,
            FlowControlStrategy flowControlStrategy,
            SeaTunnelDataType rowType,
            List<TablePath> tablePaths,
            SeaTunnelTask runningTask,
            EngineConfig engineConfig,
            Map<String, Object> taskEnvOption,
            Runnable dryRunSampleComplete,
            LongSupplier currentTimeMillisSupplier) {
        this.checkpointLock = checkpointLock;
        this.outputs = outputs;
        this.rowType = rowType;
        this.currentTimeMillisSupplier =
                currentTimeMillisSupplier != null
                        ? currentTimeMillisSupplier
                        : System::currentTimeMillis;
        this.dryRunSampleEnabled =
                taskEnvOption != null && DryRunSampleConfig.isEnabled(taskEnvOption);
        this.dryRunSampleLimit =
                this.dryRunSampleEnabled ? DryRunSampleConfig.getLimit(taskEnvOption) : 0;
        this.dryRunSamplePrintData =
                this.dryRunSampleEnabled && DryRunSampleConfig.isPrintData(taskEnvOption);
        this.dryRunSampleComplete = dryRunSampleComplete;
        if (rowType instanceof MultipleRowType) {
            ((MultipleRowType) rowType)
                    .iterator()
                    .forEachRemaining(type -> this.rowTypeMap.put(type.getKey(), type.getValue()));
        }
        this.connectorMetricsCalcContext =
                new ConnectorMetricsCalcContext(
                        metricsContext,
                        PluginType.SOURCE,
                        CollectionUtils.isNotEmpty(tablePaths),
                        tablePaths);
        flowControlGate = FlowControlGate.create(flowControlStrategy);

        this.sourceTaskId = runningTask.getTaskLocation().getTaskID();
        this.stainTraceBudgetThrottledTotal =
                metricsContext.counter(StainTraceConstants.METRIC_BUDGET_THROTTLED_TOTAL);
        this.stainTraceSamplesGeneratedTotal =
                metricsContext.counter(StainTraceConstants.METRIC_SAMPLES_GENERATED_TOTAL);
        this.stainTraceEntriesTruncatedTotal =
                metricsContext.counter(StainTraceConstants.METRIC_ENTRIES_TRUNCATED_TOTAL);
        this.stainTraceMaxEntriesPerTrace = engineConfig.getStainTraceMaxEntriesPerTrace();
        this.flushSignalTotal = metricsContext.counter(FLUSH_SIGNAL_TOTAL);
        this.flushSignalQPS = metricsContext.meter(FLUSH_SIGNAL_QPS);

        // Compute effective stain trace settings.
        // When taskEnvOption is null (test / legacy path): engine config alone controls tracing.
        // When taskEnvOption is non-null (production job path): BOTH engine switch AND task-level
        // stain_trace.enabled=true must be set (double-switch requirement per docs).
        boolean effectiveEnabled;
        int effectiveSampleRate = engineConfig.getStainTraceSampleRate();
        if (taskEnvOption == null) {
            effectiveEnabled = engineConfig.isStainTraceEnabled();
        } else {
            boolean taskStainTraceEnabled = false;
            Object stainTraceObj = taskEnvOption.get("stain_trace");
            if (stainTraceObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stainTraceMap = (Map<String, Object>) stainTraceObj;
                Object enabledObj = stainTraceMap.get("enabled");
                taskStainTraceEnabled =
                        enabledObj != null && Boolean.parseBoolean(String.valueOf(enabledObj));
                Object intervalObj = stainTraceMap.get("sample_interval");
                if (intervalObj instanceof Number) {
                    effectiveSampleRate = ((Number) intervalObj).intValue();
                }
            }
            effectiveEnabled = engineConfig.isStainTraceEnabled() && taskStainTraceEnabled;
        }

        if (effectiveEnabled) {
            this.stainTraceSampler =
                    new StainTraceSampler(
                            true,
                            effectiveSampleRate,
                            engineConfig.getStainTraceMaxTracesPerSecondPerWorker(),
                            engineConfig.getStainTraceMaxEntriesPerTrace(),
                            stainTraceSamplesGeneratedTotal,
                            stainTraceBudgetThrottledTotal);
        } else {
            this.stainTraceSampler = null;
        }
        if (dryRunSampleEnabled) {
            log.info("Dry-run sample [source] schema: {}", rowType);
        }
    }

    /** Updates source-side metrics, samples new traces when enabled, and forwards the record. */
    @Override
    public void collect(T row) {
        if (dryRunSampleEnabled && dryRunSampleCount >= dryRunSampleLimit) {
            return;
        }
        try {
            if (row instanceof SeaTunnelRow) {
                String tableId = ((SeaTunnelRow) row).getTableId();
                // init the size of row early with rowType, this way is faster than init the size
                // without rowType
                int size;
                if (rowType instanceof SeaTunnelRowType) {
                    size = ((SeaTunnelRow) row).getBytesSize((SeaTunnelRowType) rowType);
                } else if (rowType instanceof MultipleRowType) {
                    size = ((SeaTunnelRow) row).getBytesSize(rowTypeMap.get(tableId));
                } else {
                    throw new SeaTunnelEngineException(
                            "Unsupported row type: " + rowType.getClass().getName());
                }
                flowControlGate.audit((SeaTunnelRow) row);
                connectorMetricsCalcContext.updateMetrics(row, tableId);
                tryStainTrace((SeaTunnelRow) row);
            }
            if (dryRunSamplePrintData) {
                dryRunSampleCount++;
                log.info("Dry-run sample [source] row {}: {}", dryRunSampleCount, row);
            } else if (dryRunSampleEnabled) {
                dryRunSampleCount++;
            }
            sendRecordToNext(new Record<>(row));
            emptyThisPollNext = false;
            if (dryRunSampleEnabled) {
                if (dryRunSampleCount == dryRunSampleLimit && dryRunSampleComplete != null) {
                    dryRunSampleComplete.run();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void collect(SchemaChangeEvent event) {
        try {
            if (rowType instanceof SeaTunnelRowType) {
                rowType = dataTypeChangeEventHandler.reset((SeaTunnelRowType) rowType).apply(event);
            } else if (rowType instanceof MultipleRowType) {
                String tableId = event.tablePath().toString();
                SeaTunnelRowType currentRowType = rowTypeMap.get(tableId);
                if (currentRowType == null) {
                    log.warn(
                            "Ignore schema change event for unknown table {}, current table ids: {}",
                            tableId,
                            rowTypeMap.keySet());
                    return;
                }
                rowTypeMap.put(
                        tableId, dataTypeChangeEventHandler.reset(currentRowType).apply(event));
            } else {
                throw new SeaTunnelEngineException(
                        "Unsupported row type: " + rowType.getClass().getName());
            }
            sendRecordToNext(new Record<>(event));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void collect(TableOperationEvent event) {
        try {
            sendRecordToNext(new Record<>(event));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void markSchemaChangeBeforeCheckpoint() {
        if (schemaChangeAfterCheckpointSignal.get()) {
            throw new IllegalStateException("schema-change-after checkpoint already marked.");
        }
        if (!schemaChangeBeforeCheckpointSignal.compareAndSet(false, true)) {
            throw new IllegalStateException("schema-change-before checkpoint already marked.");
        }
        log.info("mark schema-change-before checkpoint signal.");
    }

    @Override
    public void markSchemaChangeAfterCheckpoint() {
        if (schemaChangeBeforeCheckpointSignal.get()) {
            throw new IllegalStateException("schema-change-before checkpoint already marked.");
        }
        if (!schemaChangeAfterCheckpointSignal.compareAndSet(false, true)) {
            throw new IllegalStateException("schema-change-after checkpoint already marked.");
        }
        log.info("mark schema-change-after checkpoint signal.");
    }

    public boolean captureSchemaChangeBeforeCheckpointSignal() {
        if (schemaChangeBeforeCheckpointSignal.get()) {
            log.info("capture schema-change-before checkpoint signal.");
            return schemaChangeBeforeCheckpointSignal.getAndSet(false);
        }
        return false;
    }

    public boolean captureSchemaChangeAfterCheckpointSignal() {
        if (schemaChangeAfterCheckpointSignal.get()) {
            log.info("capture schema-change-after checkpoint signal.");
            return schemaChangeAfterCheckpointSignal.getAndSet(false);
        }
        return false;
    }

    @Override
    public Object getCheckpointLock() {
        return checkpointLock;
    }

    @Override
    public boolean isEmptyThisPollNext() {
        return emptyThisPollNext;
    }

    @Override
    public void resetEmptyThisPollNext() {
        this.emptyThisPollNext = true;
    }

    public void sendRecordToNext(Record<?> record) throws IOException {
        synchronized (checkpointLock) {
            for (OneInputFlowLifeCycle<Record<?>> output : outputs) {
                output.received(record);
            }
        }
    }

    /**
     * Broadcast a {@link FlushSignal} to all downstream outputs on behalf of a periodic timer tick.
     *
     * <p>This is the single entry point through which the engine's timer-flush mechanism injects
     * flush signals into the data flow. The signal is broadcast using the same checkpoint lock and
     * output channel as normal records, so it is strictly serialized with barriers and never
     * reorders relative to data. Downstream intermediate queues apply their own non-blocking offer
     * strategy to avoid stalling the timer thread when the queue is backlogged.
     *
     * @param jobId the id of the job that produced this signal
     * @param taskId the id of the source subtask that produced this signal
     */
    public void sendFlushSignal(long jobId, long taskId) throws IOException {
        sendRecordToNext(new Record<>(FlushSignal.of(jobId, taskId)));
        flushSignalTotal.inc();
        flushSignalQPS.markEvent();
    }

    /** Creates the first stain trace payload for a sampled row before it leaves the source task. */
    private void tryStainTrace(SeaTunnelRow row) {
        if (stainTraceSampler == null) {
            return;
        }
        if (StainTraceUtils.hasPayload(row)) {
            return;
        }
        long nowMs = currentTimeMillisSupplier.getAsLong();
        long traceId = stainTraceSampler.tryGenerateTraceId(sourceTaskId, nowMs);
        if (traceId == StainTraceConstants.NO_TRACE_ID) {
            return;
        }
        byte[] payload = StainTracePayload.init(traceId, nowMs);
        StainTracePayload.AppendResult result =
                StainTracePayload.append(
                        payload,
                        StainTraceStage.SOURCE_EMIT,
                        sourceTaskId,
                        nowMs,
                        stainTraceMaxEntriesPerTrace);
        if (result.getStatus() == StainTracePayload.AppendStatus.TRUNCATED) {
            stainTraceEntriesTruncatedTotal.inc();
        }
        if (result.getStatus() == StainTracePayload.AppendStatus.APPENDED) {
            payload = result.getPayload();
        }
        StainTraceUtils.setPayload(row, payload);
    }
}
