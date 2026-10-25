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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;

/**
 * A {@link DeserializationSchema} wrapper that attaches Kafka record timestamp as {@code
 * CommonOptions.EVENT_TIME} metadata to emitted {@link SeaTunnelRow}s.
 *
 * <p>The timestamp for the current record is provided via {@link #setCurrentRecordTimestamp(Long)}
 * before deserialization is invoked.
 */
public class KafkaEventTimeDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final DeserializationSchema<SeaTunnelRow> delegate;

    private Long currentRecordTimestamp;

    public KafkaEventTimeDeserializationSchema(DeserializationSchema<SeaTunnelRow> delegate) {
        this.delegate = delegate;
    }

    public DeserializationSchema<SeaTunnelRow> getDelegate() {
        return delegate;
    }

    public void setCurrentRecordTimestamp(Long timestamp) {
        this.currentRecordTimestamp = timestamp;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        SeaTunnelRow row = delegate.deserialize(message);
        if (row == null) {
            return null;
        }
        attachEventTime(row);
        return row;
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        delegate.deserialize(
                message,
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        attachEventTime(record);
                        out.collect(record);
                    }

                    @Override
                    public void markSchemaChangeBeforeCheckpoint() {
                        out.markSchemaChangeBeforeCheckpoint();
                    }

                    @Override
                    public void collect(
                            org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent event) {
                        out.collect(event);
                    }

                    @Override
                    public void markSchemaChangeAfterCheckpoint() {
                        out.markSchemaChangeAfterCheckpoint();
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return out.getCheckpointLock();
                    }

                    @Override
                    public boolean isEmptyThisPollNext() {
                        return out.isEmptyThisPollNext();
                    }

                    @Override
                    public void resetEmptyThisPollNext() {
                        out.resetEmptyThisPollNext();
                    }
                });
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return delegate.getProducedType();
    }

    private void attachEventTime(SeaTunnelRow row) {
        if (row == null || currentRecordTimestamp == null || currentRecordTimestamp < 0) {
            return;
        }
        Object existing = row.getOptions().get(CommonOptions.EVENT_TIME.getName());
        if (existing == null) {
            MetadataUtil.setEventTime(row, currentRecordTimestamp);
        }
    }
}
