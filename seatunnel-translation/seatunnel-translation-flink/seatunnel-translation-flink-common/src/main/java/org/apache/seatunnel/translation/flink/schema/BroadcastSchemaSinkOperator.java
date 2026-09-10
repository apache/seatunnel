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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;

/**
 * Sink-side data-plane gate for schema evolution.
 *
 * <p>Data rows are key-partitioned by table before this operator, while schema controls use a
 * broadcast-partitioned branch. Only the subtask that owns the table key sends a schema command to
 * its writer; the other gates consume the control only to advance the global source sequence. If a
 * row overtakes its control across the two branches, it remains buffered until the control arrives.
 * The owner then emits the schema command followed by dependent rows on the same forward channel,
 * preserving command-before-row ordering into the sink writer, including with unaligned
 * checkpoints.
 *
 * <p>Only the last applied sequence per source operator, the latest complete schema snapshot event
 * per table, and genuinely pending rows are checkpointed. Flink's barriers and channel state replay
 * controls that were not part of a completed checkpoint. After recovery, the operator derives one
 * compact initial-to-target replay plan per table and sends it to the newly-created sink writer
 * before any row. A physical schema operation must converge when that restoration encounters an
 * already-durable DDL.
 */
@Slf4j
public class BroadcastSchemaSinkOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private static final int MAX_PENDING_RECORDS = 100_000;
    private static final long MAX_PENDING_BYTES = 64L * 1024 * 1024;
    private static final int MAX_PENDING_SCHEMA_CHANGES = 10_000;
    private static final long MAX_PENDING_SCHEMA_CHANGE_BYTES = 16L * 1024 * 1024;
    private static final int MIN_ESTIMATED_RECORD_BYTES = 64;

    private final Map<TablePath, CatalogTable> initialSinkTables;

    /** Last contiguously applied sequence for each source-side producer. */
    private transient Map<String, Long> appliedSequences;

    private transient Map<TableIdentifier, LatestSchemaEventEntry> latestSchemaEvents;
    private transient Map<String, NavigableMap<Long, PendingSchemaEventEntry>> pendingSchemaChanges;

    private transient Queue<PendingRowEntry> pendingRows;
    private transient long pendingBytes;
    private transient int pendingSchemaChangeCount;
    private transient long pendingSchemaChangeBytes;
    private transient boolean replayRestoredSchemaEvents;

    private transient ListState<SchemaSequenceEntry> appliedSequenceState;
    private transient ListState<LatestSchemaEventEntry> latestSchemaEventState;
    private transient ListState<PendingSchemaEventEntry> pendingSchemaEventState;
    private transient ListState<PendingRowEntry> pendingRowState;

    public BroadcastSchemaSinkOperator(List<CatalogTable> initialSinkTables) {
        this.initialSinkTables = new HashMap<>();
        if (initialSinkTables != null) {
            for (CatalogTable initialSinkTable : initialSinkTables) {
                CatalogTable tableCopy = initialSinkTable.copy();
                this.initialSinkTables.put(tableCopy.getTablePath(), tableCopy);
            }
        }
    }

    @Getter
    @Setter
    public static class SchemaSequenceEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private String producerId;
        private long sequence;

        public SchemaSequenceEntry() {}

        public SchemaSequenceEntry(String producerId, long sequence) {
            this.producerId = producerId;
            this.sequence = sequence;
        }
    }

    @Getter
    @Setter
    public static class LatestSchemaEventEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private String eventId;
        private SchemaChangeEvent event;

        public LatestSchemaEventEntry() {}

        public LatestSchemaEventEntry(String eventId, SchemaChangeEvent event) {
            this.eventId = eventId;
            this.event = event;
        }
    }

    @Getter
    @Setter
    public static class PendingRowEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private SeaTunnelRow row;
        private long timestamp;
        private boolean hasTimestamp;

        public PendingRowEntry() {}

        public PendingRowEntry(SeaTunnelRow row, long timestamp, boolean hasTimestamp) {
            this.row = row;
            this.timestamp = timestamp;
            this.hasTimestamp = hasTimestamp;
        }
    }

    @Getter
    @Setter
    public static class PendingSchemaEventEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private String eventId;
        private SchemaChangeEvent event;
        private boolean applyToSink;

        public PendingSchemaEventEntry() {}

        public PendingSchemaEventEntry(
                String eventId, SchemaChangeEvent event, boolean applyToSink) {
            this.eventId = eventId;
            this.event = event;
            this.applyToSink = applyToSink;
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        appliedSequenceState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "applied-schema-sequences", SchemaSequenceEntry.class));
        latestSchemaEventState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "latest-applied-schema-events",
                                        LatestSchemaEventEntry.class));
        pendingSchemaEventState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "out-of-order-schema-events",
                                        PendingSchemaEventEntry.class));
        pendingRowState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "schema-gate-pending-rows", PendingRowEntry.class));

        appliedSequences = new HashMap<>();
        latestSchemaEvents = new HashMap<>();
        pendingSchemaChanges = new HashMap<>();
        pendingRows = new ArrayDeque<>();

        if (context.isRestored()) {
            for (SchemaSequenceEntry entry : appliedSequenceState.get()) {
                appliedSequences.merge(entry.producerId, entry.sequence, Math::max);
            }
            for (LatestSchemaEventEntry entry : latestSchemaEventState.get()) {
                rememberLatestSchemaEvent(entry.eventId, entry.event);
            }
            for (PendingSchemaEventEntry entry : pendingSchemaEventState.get()) {
                restorePendingSchemaChange(entry);
            }
            for (PendingRowEntry entry : pendingRowState.get()) {
                if (entry.row != null && isTableOwner(entry.row.getTableId())) {
                    pendingRows.add(entry);
                    pendingBytes += estimateRowBytes(entry.row);
                }
            }
            replayRestoredSchemaEvents = true;
            log.info(
                    "Restored schema gate with {} source sequences, {} table snapshots, "
                            + "{} out-of-order schema changes, {} pending rows, and {} pending bytes",
                    appliedSequences.size(),
                    latestSchemaEvents.size(),
                    pendingSchemaChangeCount,
                    pendingRows.size(),
                    pendingBytes);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        appliedSequenceState.clear();
        for (Map.Entry<String, Long> entry : appliedSequences.entrySet()) {
            if (ownsUnionStateKey(entry.getKey())) {
                appliedSequenceState.add(new SchemaSequenceEntry(entry.getKey(), entry.getValue()));
            }
        }

        latestSchemaEventState.clear();
        for (Map.Entry<TableIdentifier, LatestSchemaEventEntry> entry :
                latestSchemaEvents.entrySet()) {
            if (isTableOwner(entry.getKey().toTablePath())) {
                latestSchemaEventState.add(entry.getValue());
            }
        }

        pendingSchemaEventState.clear();
        for (Map.Entry<String, NavigableMap<Long, PendingSchemaEventEntry>> producerEntry :
                pendingSchemaChanges.entrySet()) {
            if (ownsUnionStateKey(producerEntry.getKey())) {
                for (PendingSchemaEventEntry entry : producerEntry.getValue().values()) {
                    pendingSchemaEventState.add(entry);
                }
            }
        }

        pendingRowState.clear();
        for (PendingRowEntry entry : pendingRows) {
            pendingRowState.add(entry);
        }
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> element) {
        replayRestoredSchemaEventsIfNeeded();

        SeaTunnelRow row = element.getValue();
        SchemaChangeEvent schemaChangeEvent = SchemaEvolutionControlMessage.schemaChangeEvent(row);
        if (schemaChangeEvent != null) {
            handleBroadcastedSchemaChange(
                    SchemaEvolutionControlMessage.schemaChangeId(row),
                    schemaChangeEvent,
                    !SchemaEvolutionControlMessage.isFilteredSchemaChange(row));
            return;
        }

        String requiredChangeId = SchemaEvolutionControlMessage.requiredSchemaChangeId(row);
        if (!replayRestoredSchemaEvents
                && (requiredChangeId == null || isSchemaChangeApplied(requiredChangeId))) {
            SchemaEvolutionControlMessage.clearRequiredSchemaChange(row);
            output.collect(element);
            return;
        }

        bufferPendingRow(element, requiredChangeId);
    }

    private void bufferPendingRow(StreamRecord<SeaTunnelRow> element, String requiredChangeId) {
        SeaTunnelRow row = element.getValue();
        long rowBytes = estimateRowBytes(row);
        if (pendingRows.size() >= MAX_PENDING_RECORDS
                || pendingBytes + rowBytes > MAX_PENDING_BYTES) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    String.format(
                            "Sink schema gate pending-row buffer overflow "
                                    + "(records=%d/%d, bytes=%d/%d) while waiting for %s",
                            pendingRows.size(),
                            MAX_PENDING_RECORDS,
                            pendingBytes + rowBytes,
                            MAX_PENDING_BYTES,
                            requiredChangeId),
                    null,
                    null);
        }
        pendingRows.add(new PendingRowEntry(row, element.getTimestamp(), element.hasTimestamp()));
        pendingBytes += rowBytes;
    }

    private void handleBroadcastedSchemaChange(
            String eventId, SchemaChangeEvent event, boolean applyToSink) {
        String producerId = SchemaEvolutionControlMessage.schemaChangeProducerId(eventId);
        long sequence = SchemaEvolutionControlMessage.schemaChangeSequence(eventId);
        if (producerId == null || sequence <= 0) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    "Invalid internal schema change sequence: " + eventId,
                    event.tableIdentifier(),
                    null);
        }
        if (applyToSink) {
            // A checkpoint must never retain a control that cannot reconstruct a fresh writer.
            createRestorePlan(event);
        }
        long appliedSequence = appliedSequences.getOrDefault(producerId, 0L);
        if (appliedSequence >= sequence) {
            drainPendingRows();
            return;
        }

        NavigableMap<Long, PendingSchemaEventEntry> producerChanges =
                pendingSchemaChanges.computeIfAbsent(producerId, ignored -> new TreeMap<>());
        if (!producerChanges.containsKey(sequence)) {
            bufferOutOfOrderSchemaChange(eventId, event, applyToSink, producerId, sequence);
        }

        stageContiguousSchemaChanges(producerId);
        drainPendingRows();
    }

    private void bufferOutOfOrderSchemaChange(
            String eventId,
            SchemaChangeEvent event,
            boolean applyToSink,
            String producerId,
            long sequence) {
        NavigableMap<Long, PendingSchemaEventEntry> producerChanges =
                pendingSchemaChanges.computeIfAbsent(producerId, ignored -> new TreeMap<>());
        if (producerChanges.containsKey(sequence)) {
            return;
        }

        long eventBytes = estimateSchemaChangeBytes(eventId, event);
        if (pendingSchemaChangeCount >= MAX_PENDING_SCHEMA_CHANGES
                || pendingSchemaChangeBytes + eventBytes > MAX_PENDING_SCHEMA_CHANGE_BYTES) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    String.format(
                            "Sink schema gate out-of-order control buffer overflow "
                                    + "(records=%d/%d, bytes=%d/%d) while waiting for sequence %d "
                                    + "from producer %s before sequence %d",
                            pendingSchemaChangeCount,
                            MAX_PENDING_SCHEMA_CHANGES,
                            pendingSchemaChangeBytes + eventBytes,
                            MAX_PENDING_SCHEMA_CHANGE_BYTES,
                            appliedSequences.getOrDefault(producerId, 0L) + 1,
                            producerId,
                            sequence),
                    event.tableIdentifier(),
                    null);
        }

        producerChanges.put(sequence, new PendingSchemaEventEntry(eventId, event, applyToSink));
        pendingSchemaChangeCount++;
        pendingSchemaChangeBytes += eventBytes;
        log.debug(
                "Buffered schema change {} while waiting to apply sequence {}",
                eventId,
                appliedSequences.getOrDefault(producerId, 0L) + 1);
    }

    private void stageContiguousSchemaChanges(String producerId) {
        if (replayRestoredSchemaEvents) {
            return;
        }

        NavigableMap<Long, PendingSchemaEventEntry> producerChanges =
                pendingSchemaChanges.get(producerId);
        if (producerChanges == null) {
            return;
        }

        long nextExpectedSequence = appliedSequences.getOrDefault(producerId, 0L) + 1;
        PendingSchemaEventEntry next = producerChanges.get(nextExpectedSequence);
        while (next != null) {
            if (next.applyToSink && isTableOwner(next.event.tablePath())) {
                rememberLatestSchemaEvent(next.eventId, next.event);
                emitApplySchemaEventToSink(next.event, next.eventId);
            }
            completePendingSchemaChange(producerId, nextExpectedSequence, next);
            nextExpectedSequence++;
            next = producerChanges.get(nextExpectedSequence);
        }
        if (producerChanges.isEmpty()) {
            pendingSchemaChanges.remove(producerId);
        }
    }

    private void completePendingSchemaChange(
            String producerId, long sequence, PendingSchemaEventEntry pendingSchemaEventEntry) {
        NavigableMap<Long, PendingSchemaEventEntry> producerChanges =
                pendingSchemaChanges.get(producerId);
        if (producerChanges != null) {
            producerChanges.remove(sequence);
        }
        pendingSchemaChangeCount--;
        pendingSchemaChangeBytes =
                Math.max(
                        0L,
                        pendingSchemaChangeBytes
                                - estimateSchemaChangeBytes(
                                        pendingSchemaEventEntry.eventId,
                                        pendingSchemaEventEntry.event));
        appliedSequences.put(producerId, sequence);
    }

    private void restorePendingSchemaChange(PendingSchemaEventEntry entry) {
        String producerId = SchemaEvolutionControlMessage.schemaChangeProducerId(entry.eventId);
        long sequence = SchemaEvolutionControlMessage.schemaChangeSequence(entry.eventId);
        if (producerId == null || sequence <= 0) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    "Invalid restored internal schema change sequence: " + entry.eventId,
                    entry.event == null ? null : entry.event.tableIdentifier(),
                    null);
        }
        if (appliedSequences.getOrDefault(producerId, 0L) >= sequence) {
            return;
        }
        bufferOutOfOrderSchemaChange(
                entry.eventId, entry.event, entry.applyToSink, producerId, sequence);
    }

    private void replayRestoredSchemaEventsIfNeeded() {
        if (!replayRestoredSchemaEvents) {
            return;
        }
        replayRestoredSchemaEvents = false;

        List<LatestSchemaEventEntry> restoredSnapshots =
                new ArrayList<>(latestSchemaEvents.values());
        restoredSnapshots.sort(
                Comparator.comparing(entry -> entry.event.tableIdentifier().toString()));
        int restoredTableCount = 0;
        for (LatestSchemaEventEntry entry : restoredSnapshots) {
            if (isTableOwner(entry.event.tablePath())) {
                AlterTableColumnsEvent restorePlan = createRestorePlan(entry.event);
                emitApplySchemaEventToSink(restorePlan, entry.eventId);
                restoredTableCount++;
            }
        }

        for (String producerId : new ArrayList<>(pendingSchemaChanges.keySet())) {
            stageContiguousSchemaChanges(producerId);
        }
        drainPendingRows();
        log.info(
                "Subtask {} restored {} owned table schemas into its sink writer",
                getRuntimeContext().getIndexOfThisSubtask(),
                restoredTableCount);
    }

    private void rememberLatestSchemaEvent(String eventId, SchemaChangeEvent event) {
        if (eventId == null || event == null) {
            return;
        }
        // Validate before checkpointing the event. A completed checkpoint must never contain a
        // snapshot that cannot reconstruct a newly-created writer after recovery.
        createRestorePlan(event);
        latestSchemaEvents.put(event.tableIdentifier(), new LatestSchemaEventEntry(eventId, event));
    }

    private AlterTableColumnsEvent createRestorePlan(SchemaChangeEvent event) {
        CatalogTable initialSinkTable = initialSinkTables.get(event.tablePath());
        return SchemaRestorePlanGenerator.generate(initialSinkTable, event);
    }

    private boolean isSchemaChangeApplied(String requiredChangeId) {
        String producerId = SchemaEvolutionControlMessage.schemaChangeProducerId(requiredChangeId);
        long sequence = SchemaEvolutionControlMessage.schemaChangeSequence(requiredChangeId);
        if (producerId == null || sequence <= 0) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    "Invalid internal required schema change sequence: " + requiredChangeId,
                    null,
                    null);
        }
        return appliedSequences.getOrDefault(producerId, 0L) >= sequence;
    }

    private void drainPendingRows() {
        if (replayRestoredSchemaEvents) {
            return;
        }
        while (!pendingRows.isEmpty()) {
            PendingRowEntry entry = pendingRows.peek();
            String requiredChangeId =
                    SchemaEvolutionControlMessage.requiredSchemaChangeId(entry.row);
            if (requiredChangeId != null && !isSchemaChangeApplied(requiredChangeId)) {
                return;
            }

            pendingRows.poll();
            pendingBytes = Math.max(0L, pendingBytes - estimateRowBytes(entry.row));
            SchemaEvolutionControlMessage.clearRequiredSchemaChange(entry.row);
            if (entry.hasTimestamp) {
                output.collect(new StreamRecord<>(entry.row, entry.timestamp));
            } else {
                output.collect(new StreamRecord<>(entry.row));
            }
        }
    }

    protected void emitApplySchemaEventToSink(SchemaChangeEvent event, String schemaChangeId) {
        output.collect(
                new StreamRecord<>(
                        SchemaEvolutionControlMessage.sinkSchemaChangeRow(event, schemaChangeId)));
        log.info(
                "Subtask {} emitted schema change {} for table {} to its sink writer",
                getRuntimeContext().getIndexOfThisSubtask(),
                schemaChangeId,
                event.tableIdentifier());
    }

    protected Map<String, Long> getAppliedSequences() {
        return appliedSequences;
    }

    protected Map<TableIdentifier, LatestSchemaEventEntry> getLatestSchemaEvents() {
        return latestSchemaEvents;
    }

    protected int getPendingSchemaChangeCount() {
        return pendingSchemaChangeCount;
    }

    /**
     * Each gate holds the same compact control state in memory. Snapshotting one deterministic
     * shard per subtask avoids writing a full copy from every sink subtask; union restore still
     * reconstructs the complete state in every gate after rescaling.
     */
    private boolean ownsUnionStateKey(String key) {
        int parallelism = Math.max(1, getRuntimeContext().getNumberOfParallelSubtasks());
        int subtask = Math.floorMod(getRuntimeContext().getIndexOfThisSubtask(), parallelism);
        return Math.floorMod(key.hashCode(), parallelism) == subtask;
    }

    /**
     * Returns whether this subtask receives data rows for the table from Flink's key partitioner.
     */
    private boolean isTableOwner(TablePath tablePath) {
        return isTableOwner(tablePath == null ? null : tablePath.toString());
    }

    private boolean isTableOwner(String tableId) {
        if (tableId == null || tableId.isEmpty()) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    "Cannot restore a pending schema-evolution row without a table identifier",
                    null,
                    null);
        }
        int parallelism = Math.max(1, getRuntimeContext().getNumberOfParallelSubtasks());
        int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        if (maxParallelism <= 0) {
            maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        }
        int owner =
                KeyGroupRangeAssignment.assignKeyToParallelOperator(
                        tableId, maxParallelism, parallelism);
        return owner == getRuntimeContext().getIndexOfThisSubtask();
    }

    private static long estimateRowBytes(SeaTunnelRow row) {
        return Math.max(MIN_ESTIMATED_RECORD_BYTES, row.getBytesSize());
    }

    private static long estimateSchemaChangeBytes(String eventId, SchemaChangeEvent event) {
        long idBytes = eventId == null ? 0L : eventId.length() * 2L;
        long eventBytes = event == null ? 0L : event.toString().length() * 2L;
        return Math.max(MIN_ESTIMATED_RECORD_BYTES, idBytes + eventBytes);
    }
}
