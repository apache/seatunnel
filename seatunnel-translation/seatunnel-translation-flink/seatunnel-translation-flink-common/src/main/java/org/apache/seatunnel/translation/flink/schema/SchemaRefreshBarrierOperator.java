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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Keeps the latest schema beside each parallel sink writer and emits a local refresh before a
 * writer can receive data for a newer schema.
 *
 * <p>Input one contains data rows. Input two contains schema changes after the external DDL has
 * succeeded and is broadcast to every parallel instance. The latest schema changes are stored in
 * union operator state so that every instance receives the complete schema history after rescaling.
 * The local refreshed epochs are deliberately not checkpointed: a recovered sink writer is a new
 * in-memory writer and must refresh again before processing its first post-recovery row.
 */
@Slf4j
public class SchemaRefreshBarrierOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements TwoInputStreamOperator<SeaTunnelRow, SeaTunnelRow, SeaTunnelRow> {

    static final String SCHEMA_CHANGE_BROADCAST = "schema_change_broadcast";
    static final String SCHEMA_CHANGE_REFRESH = "schema_change_refresh";
    private transient Map<TableIdentifier, SchemaSnapshot> latestSchemas;
    private transient Map<TableIdentifier, Long> locallyRefreshedEpochs;
    private transient ListState<SchemaSnapshot> latestSchemaState;
    private transient boolean restored;

    /** Schema state shared by every restored parallel barrier instance. */
    @Getter
    @Setter
    public static class SchemaSnapshot implements Serializable {
        private static final long serialVersionUID = 1L;

        private TableIdentifier tableIdentifier;
        private SchemaChangeEvent schemaChangeEvent;
        private long epoch;

        public SchemaSnapshot() {}

        public SchemaSnapshot(SchemaChangeEvent schemaChangeEvent) {
            this.tableIdentifier = schemaChangeEvent.tableIdentifier();
            this.schemaChangeEvent = schemaChangeEvent;
            this.epoch = schemaChangeEvent.getCreatedTime();
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        ListStateDescriptor<SchemaSnapshot> descriptor =
                new ListStateDescriptor<>("latest-schema-refresh-snapshots", SchemaSnapshot.class);
        latestSchemaState = context.getOperatorStateStore().getUnionListState(descriptor);
        latestSchemas = new HashMap<>();
        locallyRefreshedEpochs = new HashMap<>();

        restored = context.isRestored();
        if (restored) {
            for (SchemaSnapshot snapshot : latestSchemaState.get()) {
                latestSchemas.merge(
                        snapshot.tableIdentifier,
                        snapshot,
                        (left, right) -> left.epoch >= right.epoch ? left : right);
            }
            log.info(
                    "Restored {} authoritative sink schema snapshot(s); local writers will "
                            + "refresh before their first matching data row",
                    latestSchemas.size());
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        latestSchemaState.clear();
        for (SchemaSnapshot snapshot : latestSchemas.values()) {
            latestSchemaState.add(snapshot);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        // A restored sink can receive a checkpoint callback before the first new data row. Refresh
        // eagerly so restored connector state is never committed with the writer's initial schema.
        if (restored) {
            for (SchemaSnapshot snapshot : latestSchemas.values()) {
                emitSchemaRefresh(snapshot);
            }
        }
    }

    /** Process a normal data row. */
    @Override
    public void processElement1(StreamRecord<SeaTunnelRow> element) {
        SchemaSnapshot snapshot = findSchemaSnapshot(element.getValue());
        if (snapshot != null && isLocalWriterStale(snapshot)) {
            // The refresh marker and data row use the same output edge. Flink therefore delivers
            // the marker to this subtask's sink writer before it can deliver the data row.
            emitSchemaRefresh(snapshot);
        }
        output.collect(element);
    }

    /** Process a schema control row broadcast after the external DDL succeeds. */
    @Override
    public void processElement2(StreamRecord<SeaTunnelRow> element) {
        SeaTunnelRow row = element.getValue();
        Map<String, Object> options = row.getOptions();
        if (options == null || !options.containsKey(SCHEMA_CHANGE_BROADCAST)) {
            return;
        }

        SchemaChangeEvent event = (SchemaChangeEvent) options.get(SCHEMA_CHANGE_BROADCAST);
        if (event.getChangeAfter() == null) {
            throw new IllegalStateException(
                    "Coordinated schema refresh requires the complete evolved schema");
        }

        SchemaSnapshot snapshot = new SchemaSnapshot(event);
        SchemaSnapshot current = latestSchemas.get(snapshot.tableIdentifier);
        if (current != null && snapshot.epoch < current.epoch) {
            throw new IllegalStateException(
                    String.format(
                            "Received outdated schema change %d for table %s after %d",
                            snapshot.epoch, snapshot.tableIdentifier, current.epoch));
        }
        latestSchemas.put(snapshot.tableIdentifier, snapshot);
        emitSchemaRefresh(snapshot);
    }

    private boolean isLocalWriterStale(SchemaSnapshot snapshot) {
        Long refreshedEpoch = locallyRefreshedEpochs.get(snapshot.tableIdentifier);
        return refreshedEpoch == null || refreshedEpoch < snapshot.epoch;
    }

    private SchemaSnapshot findSchemaSnapshot(SeaTunnelRow row) {
        if (latestSchemas.isEmpty()) {
            return null;
        }

        String rowTableId = row.getTableId();
        if (rowTableId == null || rowTableId.trim().isEmpty()) {
            return latestSchemas.size() == 1 ? latestSchemas.values().iterator().next() : null;
        }

        for (Map.Entry<TableIdentifier, SchemaSnapshot> entry : latestSchemas.entrySet()) {
            TableIdentifier tableIdentifier = entry.getKey();
            if (rowTableId.equals(tableIdentifier.toString())
                    || rowTableId.equals(tableIdentifier.toTablePath().getFullName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void emitSchemaRefresh(SchemaSnapshot snapshot) {
        SeaTunnelRow schemaRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put(SCHEMA_CHANGE_REFRESH, snapshot.schemaChangeEvent);
        options.put("schema_epoch", snapshot.epoch);
        schemaRow.setOptions(options);

        output.collect(new StreamRecord<>(schemaRow));
        locallyRefreshedEpochs.put(snapshot.tableIdentifier, snapshot.epoch);
    }
}
