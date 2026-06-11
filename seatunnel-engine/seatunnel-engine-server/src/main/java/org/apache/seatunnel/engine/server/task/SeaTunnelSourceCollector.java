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
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
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
    private final LongSupplier currentTimeMillisSupplier;

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
            LongSupplier currentTimeMillisSupplier) {
        this.checkpointLock = checkpointLock;
        this.outputs = outputs;
        this.rowType = rowType;
        this.currentTimeMillisSupplier =
                currentTimeMillisSupplier != null
                        ? currentTimeMillisSupplier
                        : System::currentTimeMillis;
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
    }

    /** Updates source-side metrics, samples new traces when enabled, and forwards the record. */
    @Override
    public void collect(T row) {
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
            sendRecordToNext(new Record<>(row));
            emptyThisPollNext = false;
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
                rowTypeMap.put(
                        tableId,
                        dataTypeChangeEventHandler.reset(rowTypeMap.get(tableId)).apply(event));
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
