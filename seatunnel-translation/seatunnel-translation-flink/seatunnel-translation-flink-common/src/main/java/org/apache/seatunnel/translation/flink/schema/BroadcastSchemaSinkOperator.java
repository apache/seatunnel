/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BroadcastSchemaSinkOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private static final int MAX_PENDING_ROWS = 100_000;
    private static final long MAX_PENDING_BYTES = 64L * 1024 * 1024;
    private static final int MAX_PENDING_CONTROLS = 10_000;
    private static final long MAX_PENDING_CONTROL_BYTES = 16L * 1024 * 1024;

    private final Map<String, CatalogTable> initialTables = new HashMap<>();
    private transient ValueState<TableState> tableState;
    private transient ListState<PendingRow> pendingRows;
    private transient Set<String> initializedWriters;

    public BroadcastSchemaSinkOperator(List<CatalogTable> tables) {
        for (CatalogTable table : tables) {
            initialTables.put(table.getTablePath().toString(), table.copy());
        }
    }

    @Getter
    @Setter
    public static class TableState implements Serializable {
        private Map<String, Long> sequences = new HashMap<>();
        private Map<String, SeaTunnelRow> controls = new HashMap<>();
        private SchemaChangeEvent latestSchema;
        private String latestSchemaId;
        private int pendingCount;
        private long pendingBytes;
        private long pendingControlBytes;
    }

    @Getter
    @Setter
    public static class PendingRow implements Serializable {
        private SeaTunnelRow row;
        private long timestamp;
        private boolean hasTimestamp;

        public PendingRow() {}

        public PendingRow(StreamRecord<SeaTunnelRow> record) {
            row = record.getValue();
            timestamp = record.getTimestamp();
            hasTimestamp = record.hasTimestamp();
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        tableState =
                getPartitionedState(
                        new ValueStateDescriptor<>("table-schema-progress", TableState.class));
        pendingRows =
                getPartitionedState(
                        new ListStateDescriptor<>("table-schema-pending-rows", PendingRow.class));
        initializedWriters = new HashSet<>();
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> record) throws Exception {
        String table = (String) getCurrentKey();
        TableState state = tableState.value();
        if (state == null) {
            state = new TableState();
        }
        if (initializedWriters.add(table) && state.latestSchema != null) {
            RestoreTableSchemaEvent restore =
                    new RestoreTableSchemaEvent(state.latestSchema.getChangeAfter().copy());
            restore.setJobId(state.latestSchema.getJobId());
            emitSchema(restore, state.latestSchemaId);
        }

        SeaTunnelRow row = record.getValue();
        if (SchemaEvolutionControlMessage.isSchemaBroadcast(row)) {
            applyControl(table, row, state);
            drainRows(state);
        } else if (state.pendingCount == 0 && isReady(row, state)) {
            SchemaEvolutionControlMessage.clearRequiredSchemaChange(row);
            output.collect(record);
            return;
        } else {
            long bytes = estimateBytes(row);
            if (state.pendingCount >= MAX_PENDING_ROWS
                    || state.pendingBytes + bytes > MAX_PENDING_BYTES) {
                throw new IllegalStateException("Schema buffer limit exceeded for table " + table);
            }
            pendingRows.add(new PendingRow(record));
            state.pendingCount++;
            state.pendingBytes += bytes;
        }
        tableState.update(state);
    }

    private void applyControl(String table, SeaTunnelRow row, TableState state) throws Exception {
        String id = SchemaEvolutionControlMessage.schemaChangeId(row);
        String producer = producer(id);
        long sequence = SchemaEvolutionControlMessage.schemaChangeSequence(id);
        if (sequence <= state.sequences.getOrDefault(producer, 0L)) {
            return;
        }
        if (!state.controls.containsKey(id)) {
            if (state.controls.size() >= MAX_PENDING_CONTROLS
                    || state.pendingControlBytes + estimateControlBytes(row)
                            > MAX_PENDING_CONTROL_BYTES) {
                throw new IllegalStateException("Schema control limit exceeded for table " + table);
            }
            state.controls.put(id, row);
            state.pendingControlBytes += estimateControlBytes(row);
        }
        long next = state.sequences.getOrDefault(producer, 0L) + 1;
        String nextId = SchemaEvolutionControlMessage.schemaChangeId(producer, next);
        while (state.controls.containsKey(nextId)) {
            SeaTunnelRow control = state.controls.get(nextId);
            SchemaChangeEvent event = SchemaEvolutionControlMessage.schemaChangeEvent(control);
            if (table.equals(event.tablePath().toString())
                    && !SchemaEvolutionControlMessage.isFilteredSchemaChange(control)) {
                SchemaRestorePlanGenerator.generate(initialTables.get(table), event);
                emitSchema(event, nextId);
                state.latestSchema = event;
                state.latestSchemaId = nextId;
            }
            state.controls.remove(nextId);
            state.pendingControlBytes -= estimateControlBytes(control);
            state.sequences.put(producer, next);
            drainRows(state);
            nextId = SchemaEvolutionControlMessage.schemaChangeId(producer, ++next);
        }
    }

    private boolean isReady(SeaTunnelRow row, TableState state) {
        String id = SchemaEvolutionControlMessage.requiredSchemaChangeId(row);
        return id == null
                || state.sequences.getOrDefault(producer(id), 0L)
                        >= SchemaEvolutionControlMessage.schemaChangeSequence(id);
    }

    private String producer(String id) {
        String producer = SchemaEvolutionControlMessage.schemaChangeProducerId(id);
        if (producer == null || SchemaEvolutionControlMessage.schemaChangeSequence(id) <= 0) {
            throw new IllegalArgumentException("Invalid schema change id: " + id);
        }
        return producer;
    }

    private void drainRows(TableState state) throws Exception {
        if (state.pendingCount == 0) {
            return;
        }
        List<PendingRow> remaining = new ArrayList<>();
        boolean blocked = false;
        for (PendingRow entry : pendingRows.get()) {
            blocked = blocked || !isReady(entry.row, state);
            if (blocked) {
                remaining.add(entry);
            } else {
                state.pendingCount--;
                state.pendingBytes -= estimateBytes(entry.row);
                SchemaEvolutionControlMessage.clearRequiredSchemaChange(entry.row);
                output.collect(
                        entry.hasTimestamp
                                ? new StreamRecord<>(entry.row, entry.timestamp)
                                : new StreamRecord<>(entry.row));
            }
        }
        pendingRows.update(remaining);
    }

    private void emitSchema(SchemaChangeEvent event, String id) {
        output.collect(
                new StreamRecord<>(SchemaEvolutionControlMessage.sinkSchemaChangeRow(event, id)));
    }

    private long estimateBytes(SeaTunnelRow row) {
        return Math.max(64L, row.getBytesSize());
    }

    private long estimateControlBytes(SeaTunnelRow row) {
        return Math.max(
                64L,
                2L
                        * (SchemaEvolutionControlMessage.schemaChangeId(row).length()
                                + SchemaEvolutionControlMessage.schemaChangeEvent(row)
                                        .toString()
                                        .length()));
    }
}
