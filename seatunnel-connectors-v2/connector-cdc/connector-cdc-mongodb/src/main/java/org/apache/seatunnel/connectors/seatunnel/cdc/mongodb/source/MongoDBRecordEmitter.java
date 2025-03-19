/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;

import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeAfterWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeBeforeWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.getResumeToken;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.isDataChangeRecord;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.isHeartbeatEvent;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 */
public final class MongoDBRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    public MongoDBRecordEmitter(
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            SourceReader.Context context) {
        super(deserializationSchema, offsetFactory, context);
    }

    @Override
    protected void processElement(
            SourceRecord element, Collector<T> output, SourceSplitStateBase splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = getOffsetPosition(element);
            if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setLowWatermark(watermark);
            } else if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            } else if ((isSchemaChangeBeforeWatermarkEvent(element)
                            || isSchemaChangeAfterWatermarkEvent(element))
                    && splitState.isIncrementalSplitState()) {
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element) || isHeartbeatEvent(element)) {
            if (splitState.isIncrementalSplitState()) {
                updatePositionForStreamSplit(element, splitState);
            }
            emitElement(element, output);
        } else {
            emitElement(element, output);
        }
    }

    private void updatePositionForStreamSplit(
            SourceRecord element, SourceSplitStateBase splitState) {
        BsonDocument resumeToken = getResumeToken(element);
        IncrementalSplitState streamSplitState = splitState.asIncrementalSplitState();
        ChangeStreamOffset offset = (ChangeStreamOffset) streamSplitState.getStartupOffset();
        if (offset != null) {
            offset.updatePosition(resumeToken);
        }
        splitState.asIncrementalSplitState().setStartupOffset(offset);
    }
}
