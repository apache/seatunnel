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
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A {@link DeserializationSchema} wrapper that appends Kafka message header values as additional
 * fields to the deserialized {@link SeaTunnelRow}.
 *
 * <p>The headers for the current record are provided via {@link #setCurrentRecordHeaders(Headers)}
 * before deserialization is invoked.
 */
public class KafkaHeadersDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final DeserializationSchema<SeaTunnelRow> delegate;
    private final List<String> headerFieldNames;
    private final SeaTunnelRowType extendedRowType;
    private final int baseFieldCount;

    private Headers currentRecordHeaders;

    public KafkaHeadersDeserializationSchema(
            DeserializationSchema<SeaTunnelRow> delegate,
            List<String> headerFieldNames,
            SeaTunnelRowType extendedRowType) {
        this.delegate = delegate;
        this.headerFieldNames = headerFieldNames;
        this.extendedRowType = extendedRowType;
        this.baseFieldCount = ((SeaTunnelRowType) delegate.getProducedType()).getTotalFields();
    }

    public void setCurrentRecordHeaders(Headers headers) {
        this.currentRecordHeaders = headers;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        SeaTunnelRow baseRow = delegate.deserialize(message);
        return appendHeaderFields(baseRow);
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        delegate.deserialize(
                message,
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        out.collect(appendHeaderFields(record));
                    }

                    @Override
                    public void collect(SchemaChangeEvent event) {
                        out.collect(event);
                    }

                    @Override
                    public void markSchemaChangeBeforeCheckpoint() {
                        out.markSchemaChangeBeforeCheckpoint();
                    }

                    @Override
                    public void markSchemaChangeAfterCheckpoint() {
                        out.markSchemaChangeAfterCheckpoint();
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return out.getCheckpointLock();
                    }
                });
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return extendedRowType;
    }

    private SeaTunnelRow appendHeaderFields(SeaTunnelRow baseRow) {
        if (baseRow == null) {
            return null;
        }
        Object[] fields = new Object[baseFieldCount + headerFieldNames.size()];
        System.arraycopy(baseRow.getFields(), 0, fields, 0, baseFieldCount);

        for (int i = 0; i < headerFieldNames.size(); i++) {
            String headerKey = headerFieldNames.get(i);
            if (currentRecordHeaders != null) {
                Header header = currentRecordHeaders.lastHeader(headerKey);
                fields[baseFieldCount + i] =
                        header != null && header.value() != null
                                ? new String(header.value(), StandardCharsets.UTF_8)
                                : null;
            }
        }

        SeaTunnelRow newRow = new SeaTunnelRow(fields);
        newRow.setRowKind(baseRow.getRowKind());
        newRow.setTableId(baseRow.getTableId());
        newRow.setOptions(baseRow.getOptions());
        return newRow;
    }
}
