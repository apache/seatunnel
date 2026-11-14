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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.TableId;

import java.util.List;
import java.util.Map;

/**
 * Applies Oracle LOB placeholder handling after the standard Debezium row conversion.
 *
 * <p>Schema-change events and checkpoint signals are forwarded unchanged. Compatible Debezium JSON
 * output does not use this schema, because that format is the raw Debezium envelope.
 */
public class OracleLobAwareDebeziumDeserializeSchema
        implements DebeziumDeserializationSchema<SeaTunnelRow>, AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final DebeziumDeserializationSchema<SeaTunnelRow> delegate;
    private final OracleLobUnavailableValueHandler handler;

    public OracleLobAwareDebeziumDeserializeSchema(
            DebeziumDeserializationSchema<SeaTunnelRow> delegate,
            OracleLobUnavailableValueHandler handler) {
        this.delegate = delegate;
        this.handler = handler;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<SeaTunnelRow> out) throws Exception {
        delegate.deserialize(record, new LobHandlingCollector(record, out));
    }

    @Override
    public List<CatalogTable> getProducedType() {
        return delegate.getProducedType();
    }

    @Override
    public void restoreCheckpointProducedType(List<CatalogTable> checkpointDataType) {
        delegate.restoreCheckpointProducedType(checkpointDataType);
    }

    @Override
    public void restoreCheckpointHistoryTableChanges(
            Map<TableId, byte[]> checkpointHistoryTableChanges) {
        delegate.restoreCheckpointHistoryTableChanges(checkpointHistoryTableChanges);
    }

    @Override
    public SchemaChangeResolver getSchemaChangeResolver() {
        return delegate.getSchemaChangeResolver();
    }

    @Override
    public Map<TableId, byte[]> getHistoryTableChanges() {
        return delegate.getHistoryTableChanges();
    }

    @Override
    public void close() {
        handler.close();
    }

    private final class LobHandlingCollector implements Collector<SeaTunnelRow> {
        private final SourceRecord record;
        private final Collector<SeaTunnelRow> out;

        private LobHandlingCollector(SourceRecord record, Collector<SeaTunnelRow> out) {
            this.record = record;
            this.out = out;
        }

        @Override
        public void collect(SeaTunnelRow row) {
            out.collect(handler.handle(record, row, delegate.getProducedType()));
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

        @Override
        public boolean isEmptyThisPollNext() {
            return out.isEmptyThisPollNext();
        }

        @Override
        public void resetEmptyThisPollNext() {
            out.resetEmptyThisPollNext();
        }
    }
}
