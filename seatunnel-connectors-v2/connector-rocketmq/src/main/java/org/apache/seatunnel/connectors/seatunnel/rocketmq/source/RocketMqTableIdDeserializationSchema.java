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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;

public class RocketMqTableIdDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<SeaTunnelRow> delegate;
    private final String tableId;

    public RocketMqTableIdDeserializationSchema(
            DeserializationSchema<SeaTunnelRow> delegate, String tableId) {
        this.delegate = delegate;
        this.tableId = tableId;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        SeaTunnelRow row = delegate.deserialize(message);
        if (row != null) {
            row.setTableId(tableId);
        }
        return row;
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        delegate.deserialize(
                message,
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        record.setTableId(tableId);
                        out.collect(record);
                    }

                    @Override
                    public void markSchemaChangeBeforeCheckpoint() {
                        out.markSchemaChangeBeforeCheckpoint();
                    }

                    @Override
                    public void collect(SchemaChangeEvent event) {
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
}
