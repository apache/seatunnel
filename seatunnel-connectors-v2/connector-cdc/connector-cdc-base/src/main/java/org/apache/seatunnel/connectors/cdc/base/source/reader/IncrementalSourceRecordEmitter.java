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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import org.apache.kafka.connect.source.SourceRecord;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeAfterWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeBeforeWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.getFetchTimestamp;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.getMessageTimestamp;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 */
@Slf4j
public class IncrementalSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecords, T, SourceSplitStateBase> {

    private static final String CDC_RECORD_FETCH_DELAY = "CDCRecordFetchDelay";
    private static final String CDC_RECORD_EMIT_DELAY = "CDCRecordEmitDelay";

    protected final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    protected final OutputCollector<T> outputCollector;

    protected final OffsetFactory offsetFactory;

    protected final Counter recordFetchDelay;
    protected final Counter recordEmitDelay;

    public IncrementalSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            OffsetFactory offsetFactory,
            MetricsContext metricsContext) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.outputCollector = new OutputCollector<>();
        this.offsetFactory = offsetFactory;
        this.recordFetchDelay = metricsContext.counter(CDC_RECORD_FETCH_DELAY);
        this.recordEmitDelay = metricsContext.counter(CDC_RECORD_EMIT_DELAY);
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, Collector<T> collector, SourceSplitStateBase splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            SourceRecord next = elementIterator.next();
            reportMetrics(next);
            processElement(next, collector, splitState);
        }
    }

    protected void reportMetrics(SourceRecord element) {
        long now = System.currentTimeMillis();
        // record the latest process time
        Long messageTimestamp = getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null) {
                recordFetchDelay.set(fetchTimestamp - messageTimestamp);
            }
            // report emit delay
            recordEmitDelay.set(now - messageTimestamp);
        }
    }

    protected void processElement(
            SourceRecord element, Collector<T> output, SourceSplitStateBase splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = getWatermark(element);
            if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setLowWatermark(watermark);
            } else if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            } else if ((isSchemaChangeBeforeWatermarkEvent(element)
                            || isSchemaChangeAfterWatermarkEvent(element))
                    && splitState.isIncrementalSplitState()) {
                emitElement(element, output);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isIncrementalSplitState()) {
            emitElement(element, output);
        } else if (isDataChangeRecord(element)) {
            if (splitState.isIncrementalSplitState()) {
                Offset position = getOffsetPosition(element);
                splitState.asIncrementalSplitState().setStartupOffset(position);
            }
            emitElement(element, output);
        } else {
            emitElement(element, output);
        }
    }

    private Offset getWatermark(SourceRecord watermarkEvent) {
        return getOffsetPosition(watermarkEvent.sourceOffset());
    }

    public Offset getOffsetPosition(SourceRecord dataRecord) {
        return getOffsetPosition(dataRecord.sourceOffset());
    }

    public Offset getOffsetPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return offsetFactory.specific(offsetStrMap);
    }

    protected void emitElement(SourceRecord element, Collector<T> output) throws Exception {
        outputCollector.output = output;
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }

    private static class OutputCollector<T> implements Collector<T> {
        private Collector<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void collect(SchemaChangeEvent event) {
            output.collect(event);
        }

        @Override
        public void markSchemaChangeBeforeCheckpoint() {
            output.markSchemaChangeBeforeCheckpoint();
        }

        @Override
        public void markSchemaChangeAfterCheckpoint() {
            output.markSchemaChangeAfterCheckpoint();
        }

        @Override
        public Object getCheckpointLock() {
            return output.getCheckpointLock();
        }
    }
}
