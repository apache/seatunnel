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
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventHandler;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlGate;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;

@Slf4j
public class SeaTunnelSourceCollector<T> implements Collector<T> {

    private final Object checkpointLock;

    private final List<OneInputFlowLifeCycle<Record<?>>> outputs;

    private final AtomicBoolean schemaChangeBeforeCheckpointSignal = new AtomicBoolean(false);

    private final AtomicBoolean schemaChangeAfterCheckpointSignal = new AtomicBoolean(false);

    private final Counter sourceReceivedCount;

    private final Meter sourceReceivedQPS;
    private final Counter sourceReceivedBytes;

    private final Meter sourceReceivedBytesPerSeconds;

    private volatile boolean emptyThisPollNext;
    private final DataTypeChangeEventHandler dataTypeChangeEventHandler =
            new DataTypeChangeEventDispatcher();
    private Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();
    private SeaTunnelDataType rowType;
    private FlowControlGate flowControlGate;

    public SeaTunnelSourceCollector(
            Object checkpointLock,
            List<OneInputFlowLifeCycle<Record<?>>> outputs,
            MetricsContext metricsContext,
            FlowControlStrategy flowControlStrategy,
            SeaTunnelDataType rowType) {
        this.checkpointLock = checkpointLock;
        this.outputs = outputs;
        this.rowType = rowType;
        if (rowType instanceof MultipleRowType) {
            ((MultipleRowType) rowType)
                    .iterator()
                    .forEachRemaining(
                            type -> {
                                this.rowTypeMap.put(type.getKey(), type.getValue());
                            });
        }
        sourceReceivedCount = metricsContext.counter(SOURCE_RECEIVED_COUNT);
        sourceReceivedQPS = metricsContext.meter(SOURCE_RECEIVED_QPS);
        sourceReceivedBytes = metricsContext.counter(SOURCE_RECEIVED_BYTES);
        sourceReceivedBytesPerSeconds = metricsContext.meter(SOURCE_RECEIVED_BYTES_PER_SECONDS);
        if (flowControlStrategy != null) {
            flowControlGate = FlowControlGate.create(flowControlStrategy);
        }
    }

    @Override
    public void collect(T row) {
        try {
            if (row instanceof SeaTunnelRow) {
                int size;
                if (rowType instanceof SeaTunnelRowType) {
                    size = ((SeaTunnelRow) row).getBytesSize((SeaTunnelRowType) rowType);
                } else if (rowType instanceof MultipleRowType) {
                    size =
                            ((SeaTunnelRow) row)
                                    .getBytesSize(
                                            rowTypeMap.get(((SeaTunnelRow) row).getTableId()));
                } else {
                    throw new SeaTunnelEngineException(
                            "Unsupported row type: " + rowType.getClass().getName());
                }
                sourceReceivedBytes.inc(size);
                sourceReceivedBytesPerSeconds.markEvent(size);
                if (flowControlGate != null) {
                    flowControlGate.audit((SeaTunnelRow) row);
                }
            }
            sendRecordToNext(new Record<>(row));
            emptyThisPollNext = false;
            sourceReceivedCount.inc();
            sourceReceivedQPS.markEvent();
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

    public boolean isEmptyThisPollNext() {
        return emptyThisPollNext;
    }

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
}
