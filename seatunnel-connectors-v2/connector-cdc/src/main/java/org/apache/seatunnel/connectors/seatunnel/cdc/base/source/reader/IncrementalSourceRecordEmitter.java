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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.reader;

import static org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.seatunnel.connectors.seatunnel.cdc.base.utils.SourceRecordUtils.getHistoryRecord;
import static org.apache.seatunnel.connectors.seatunnel.cdc.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.seatunnel.connectors.seatunnel.cdc.base.utils.SourceRecordUtils.isSchemaChangeEvent;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.debezium.serializer.SeatunnelJsonTableChangeSerializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceRecords;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitState;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 */
@Slf4j
public class IncrementalSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecords, T, SourceSplitState> {
    private static final SeatunnelJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new SeatunnelJsonTableChangeSerializer();

    protected final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    protected final boolean includeSchemaChanges;
    protected final OutputCollector<T> outputCollector;
    protected final OffsetFactory offsetFactory;

    public IncrementalSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.includeSchemaChanges = includeSchemaChanges;
        this.outputCollector = new OutputCollector<>();
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, Collector<T> collector, SourceSplitState splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), collector, splitState);
        }
    }

    protected void processElement(
            SourceRecord element, Collector<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = getWatermark(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element)) {
            if (splitState.isStreamSplitState()) {
                Offset position = getOffsetPosition(element);
                splitState.asStreamSplitState().setStartingOffset(position);
            }
            emitElement(element, output);
        } else {
            // unknown element
            log.info("Meet unknown element {}, just skip.", element);
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
        return offsetFactory.newOffset(offsetStrMap);
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
        public Object getCheckpointLock() {
            return null;
        }
    }
}
