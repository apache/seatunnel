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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.event.TableEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.coordinator.LocalSchemaCoordinator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** operators added to the source and transformer pipelines to handle schema evolution */
@Slf4j
public class SchemaOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private static final int MAX_BUFFERED_ROWS_PER_KEY = 100000;
    private final Map<TableIdentifier, CatalogTable> localSchemaState;
    private String jobId;
    private final SupportSchemaEvolution source;
    private final Config pluginConfig;
    private volatile Long lastProcessedEventTime;
    private transient LocalSchemaCoordinator coordinator;
    private transient Map<String, List<BufferedDataRow>> bufferedDataRows;
    private volatile boolean schemaChangePending = false;
    private volatile CompletableFuture<Boolean> pendingSchemaFuture = null;
    private volatile boolean stateDirty = false;

    private transient ListState<SchemaStateEntry> localSchemaStateStore;
    private transient ListState<Long> lastProcessedEventTimeState;
    private transient ListState<Boolean> schemaChangePendingState;
    private transient ListState<BufferedDataEntry> bufferedDataRowsState;

    public SchemaOperator(String jobId, SupportSchemaEvolution source, Config pluginConfig) {
        this.jobId = jobId;
        this.source = source;
        this.pluginConfig = pluginConfig;
        this.localSchemaState = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        String flinkJobId = getRuntimeContext().getJobId().toString();
        if (!flinkJobId.equals(this.jobId)) {
            this.jobId = flinkJobId;
        }
        this.bufferedDataRows = new ConcurrentHashMap<>();
        this.coordinator = LocalSchemaCoordinator.getInstance(this.jobId);

        // if schema change was pending and we have buffered data, handle recovery scenario
        if (schemaChangePending && pendingSchemaFuture == null) {
            handleSchemaChangeRecovery();
        }

        log.info(
                "SchemaOperator opened for job: {}, recovered state - lastProcessedEventTime: {}, schemaChangePending: {}, bufferedDataRows size: {}",
                this.jobId,
                this.lastProcessedEventTime,
                this.schemaChangePending,
                bufferedDataRows.size());
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> streamRecord) {
        SeaTunnelRow element = streamRecord.getValue();

        if (!isSchemaEvolutionEnabled(pluginConfig)) {
            output.collect(streamRecord);
            return;
        }

        if ("__SCHEMA_CHANGE_EVENT__".equals(element.getTableId())
                && element.getOptions() != null) {
            Object object = element.getOptions().get("schema_change_event");
            if (object instanceof SchemaChangeEvent) {
                handleSchemaChangeEvent((SchemaChangeEvent) object);
                return;
            }
        }

        if (schemaChangePending) {
            String tableId = element.getTableId();
            if (tableId != null && lastProcessedEventTime != null) {
                String key = createKey(tableId, lastProcessedEventTime);
                bufferedDataRows(key, element, streamRecord.getTimestamp());
                return;
            }
        }

        output.collect(streamRecord);
    }

    private boolean isSchemaEvolutionEnabled(Config pluginConfig) {
        if (pluginConfig.hasPath("schema-changes.enabled")) {
            return pluginConfig.getBoolean("schema-changes.enabled");
        }

        return false;
    }

    private String createKey(String tableId, Long eventTime) {
        return tableId + "#" + eventTime;
    }

    private void bufferedDataRows(String key, SeaTunnelRow element, long timestamp) {
        try {
            BufferedDataRow bufferedRow = new BufferedDataRow(element, timestamp);

            synchronized (this) {
                List<BufferedDataRow> bufferedList =
                        bufferedDataRows.computeIfAbsent(key, k -> new ArrayList<>());

                if (bufferedList.size() >= MAX_BUFFERED_ROWS_PER_KEY) {
                    log.warn(
                            "Buffer for key {} exceeded max size {}, dropping oldest row",
                            key,
                            MAX_BUFFERED_ROWS_PER_KEY);
                    bufferedList.remove(0);
                }

                bufferedList.add(bufferedRow);
                stateDirty = true;

                log.debug(
                        "buffered data row for key: {}, total buffered: {}",
                        key,
                        bufferedList.size());
            }
        } catch (Exception e) {
            log.error("Failed to buffer data for key: {}, dropping this data row", key, e);
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        List<SchemaChangeType> supportedTypes = source.supports();
        if (supportedTypes == null || supportedTypes.isEmpty()) {
            log.info(
                    "Source: {} does not support any schema change types, skipping schema change event",
                    source);
            return;
        }

        if (!isSchemaChangeSupported(schemaChangeEvent, supportedTypes)) {
            log.warn(
                    "Schema change type {} not supported by source {}, skipping",
                    schemaChangeEvent.getEventType(),
                    source);
            return;
        }

        processSchemaChangeEvent(schemaChangeEvent);
    }

    private boolean isSchemaChangeSupported(
            SchemaChangeEvent event, List<SchemaChangeType> supportedTypes) {
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN);
            case SCHEMA_CHANGE_DROP_COLUMN:
                return supportedTypes.contains(SchemaChangeType.DROP_COLUMN);
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                return supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN);
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                return supportedTypes.contains(SchemaChangeType.RENAME_COLUMN);
            case SCHEMA_CHANGE_UPDATE_COLUMNS:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.DROP_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.RENAME_COLUMN);
            default:
                log.error("Unknown schema change event type: {}", event.getEventType());
                throw SchemaValidationException.unsupportedChangeType(
                        event.tableIdentifier(), jobId);
        }
    }

    private void processSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        TableIdentifier tableId = schemaChangeEvent.tableIdentifier();
        long eventTime = schemaChangeEvent.getCreatedTime();

        try {
            if (lastProcessedEventTime != null && eventTime <= lastProcessedEventTime) {
                throw SchemaValidationException.outdatedEvent(
                        tableId, jobId, eventTime, lastProcessedEventTime);
            }

            if (schemaChangeEvent instanceof TableEvent) {
                schemaChangeEvent.setJobId(jobId);
            }

            log.info(
                    "Starting schema change processing for table: {}, job: {}, event time: {}",
                    tableId,
                    jobId,
                    eventTime);

            String key = createKey(tableId.toString(), eventTime);

            // initialize buffer for this schema change
            synchronized (this) {
                List<BufferedDataRow> newBufferList = new ArrayList<>();
                bufferedDataRows.put(key, newBufferList);
                stateDirty = true;
            }

            schemaChangePending = true;

            sendSchemaChangeEventToDownstream(schemaChangeEvent);
            CatalogTable newSchema = schemaChangeEvent.getChangeAfter();
            if (newSchema != null) {
                localSchemaState.put(tableId, newSchema);
                log.debug("Updated local schema state for table: {}", tableId);
            }
            lastProcessedEventTime = eventTime;

            try {
                log.info(
                        "Synchronously processing schema change for table {} (epoch {}). Business data buffered.",
                        tableId,
                        eventTime);
                long timeoutMs = 300_000L;
                boolean success = coordinator.requestSchemaChange(tableId, eventTime, timeoutMs);

                if (success) {
                    if (schemaChangeEvent.getChangeAfter() != null) {
                        localSchemaState.put(tableId, schemaChangeEvent.getChangeAfter());
                    }
                    lastProcessedEventTime = eventTime;
                    log.info(
                            "Schema change for table {} (epoch {}) confirmed successfully by all sink subtasks.",
                            tableId,
                            eventTime);
                } else {
                    log.error(
                            "Schema change for table {} (epoch {}) failed or timed out.",
                            tableId,
                            eventTime);
                }

            } catch (Exception e) {
                log.error(
                        "Error during synchronous schema change processing for table {} (epoch {})",
                        tableId,
                        eventTime,
                        e);
            } finally {
                schemaChangePending = false;
                pendingSchemaFuture = null;
                releaseBufferedData(key, tableId);

                log.info(
                        "Synchronous schema change processing completed for table {}, data flow resumed",
                        tableId);
            }

            log.info(
                    "Synchronous schema change processing completed for table {}. Checkpoint barriers can propagate normally.",
                    tableId);
        } catch (Exception e) {
            log.error("Error starting schema change processing", e);
            schemaChangePending = false;
            try {
                schemaChangePendingState.clear();
                schemaChangePendingState.add(false);
            } catch (Exception stateException) {
                log.error(
                        "Error updating schemaChangePending state during error handling",
                        stateException);
            }
            pendingSchemaFuture = null;
        }
    }

    private void releaseBufferedData(String key, TableIdentifier tableId) {
        try {
            List<BufferedDataRow> bufferedRows;
            synchronized (this) {
                bufferedRows = bufferedDataRows.remove(key);
                stateDirty = true;
            }

            if (bufferedRows != null && !bufferedRows.isEmpty()) {
                log.info(
                        "Releasing {} buffered data rows after schema change processing for table {}",
                        bufferedRows.size(),
                        tableId);

                for (BufferedDataRow buffered : bufferedRows) {
                    output.collect(new StreamRecord<>(buffered.row, buffered.timestamp));
                }

                log.info(
                        "Successfully released {} buffered rows for table {}",
                        bufferedRows.size(),
                        tableId);
            }

        } catch (Exception e) {
            log.error(
                    "CRITICAL: Failed to release buffered data for key: {}. "
                            + "Data may be lost if this continues to fail!",
                    key,
                    e);

            try {
                Iterable<BufferedDataEntry> stateEntries = bufferedDataRowsState.get();
                for (BufferedDataEntry entry : stateEntries) {
                    if (entry.key.equals(key)) {
                        List<BufferedDataRow> stateData = entry.bufferedRows;
                        if (stateData != null && !stateData.isEmpty()) {
                            synchronized (this) {
                                bufferedDataRows.put(key, new ArrayList<>(stateData));
                                stateDirty = true;
                            }
                            log.info(
                                    "Restored {} rows to memory buffer for retry",
                                    stateData.size());
                        }
                        break;
                    }
                }
            } catch (Exception restoreException) {
                log.error("Failed to restore buffered data to memory", restoreException);
            }

            throw e;
        }
    }

    private void handleSchemaChangeRecovery() {
        log.info(
                "Detected schema change pending after recovery with {} buffered entries. "
                        + "Querying sink state to determine correct recovery action.",
                bufferedDataRows.size());

        try {
            // wait for sink operators to register their state providers with retry mechanism
            waitForSinkStateProviders(10, 500);

            boolean allDataReleased = true;
            int totalReleased = 0;

            for (Map.Entry<String, List<BufferedDataRow>> entry : bufferedDataRows.entrySet()) {
                String key = entry.getKey();
                List<BufferedDataRow> bufferedRows = entry.getValue();

                if (bufferedRows == null || bufferedRows.isEmpty()) {
                    continue;
                }

                String[] keyParts = key.split("#");
                if (keyParts.length != 2) {
                    log.warn("Invalid buffer key format: {}, releasing data", key);
                    releaseBufferedDataForKey(key, bufferedRows);
                    totalReleased += bufferedRows.size();
                    continue;
                }

                String tableIdStr = keyParts[0];
                long epoch;
                try {
                    epoch = Long.parseLong(keyParts[1]);
                } catch (NumberFormatException e) {
                    log.warn("Invalid epoch in buffer key: {}, releasing data", key);
                    releaseBufferedDataForKey(key, bufferedRows);
                    totalReleased += bufferedRows.size();
                    continue;
                }
                TableIdentifier tableId;
                String[] parts = tableIdStr.split("\\.");
                if (parts.length < 3) {
                    throw new IllegalArgumentException("Invalid table id format: " + tableIdStr);
                }
                tableId = TableIdentifier.of(parts[0], parts[1], parts[2]);

                // query sink processing status using string representation directly
                LocalSchemaCoordinator.SchemaProcessingStatus status =
                        coordinator.querySchemaProcessingStatus(tableId, epoch);

                switch (status) {
                    case FULLY_PROCESSED:
                        log.info(
                                "Schema change for table {} epoch {} fully processed, releasing {} buffered rows",
                                tableIdStr,
                                epoch,
                                bufferedRows.size());
                        releaseBufferedDataForKey(key, bufferedRows);
                        totalReleased += bufferedRows.size();
                        break;

                    case NOT_PROCESSED:
                        log.info(
                                "Schema change for table {} epoch {} not processed, need to restart coordination for {} buffered rows",
                                tableIdStr,
                                epoch,
                                bufferedRows.size());
                        restartSchemaChangeCoordination(tableId, epoch, key);
                        allDataReleased = false;
                        break;

                    case PARTIALLY_PROCESSED:
                        log.warn(
                                "Schema change for table {} epoch {} partially processed, need to restart coordination for {} buffered rows",
                                tableIdStr,
                                epoch,
                                bufferedRows.size());
                        restartSchemaChangeCoordination(tableId, epoch, key);
                        allDataReleased = false;
                        break;

                    default:
                        log.error(
                                "Unknown schema processing status: {}, releasing data to avoid deadlock",
                                status);
                        releaseBufferedDataForKey(key, bufferedRows);
                        totalReleased += bufferedRows.size();
                }
            }

            // only reset schemaChangePending if all data was released
            if (allDataReleased) {
                schemaChangePending = false;
                schemaChangePendingState.clear();
                schemaChangePendingState.add(false);
                log.info(
                        "Recovery completed: Released {} buffered data rows and resumed normal data flow.",
                        totalReleased);
            } else {
                log.info(
                        "Recovery in progress: Released {} buffered data rows, {} entries still need coordination.",
                        totalReleased,
                        bufferedDataRows.size());
            }

        } catch (Exception e) {
            log.error(
                    "Error during schema change recovery, releasing all buffered data to avoid deadlock",
                    e);
            releaseAllBufferedData();
        }
    }

    private void waitForSinkStateProviders(int maxRetries, long retryIntervalMs)
            throws InterruptedException {
        for (int i = 0; i < maxRetries; i++) {
            if (coordinator.querySchemaProcessingStatus(
                            TableIdentifier.of("test", "test", "test"), 0L)
                    != null) {
                log.info("Sink state providers registered after {} retries", i);
                return;
            }
            Thread.sleep(retryIntervalMs);
        }
        log.warn(
                "Sink state providers not fully registered after {} retries, proceeding anyway",
                maxRetries);
    }

    private void releaseBufferedDataForKey(String key, List<BufferedDataRow> bufferedRows) {
        try {
            for (BufferedDataRow buffered : bufferedRows) {
                output.collect(new StreamRecord<>(buffered.row, buffered.timestamp));
            }

            synchronized (this) {
                bufferedDataRows.remove(key);
                stateDirty = true;
            }
        } catch (Exception e) {
            log.error("Failed to release buffered data for key: {}", key, e);
        }
    }

    private void restartSchemaChangeCoordination(TableIdentifier tableId, long epoch, String key) {
        try {
            log.info("Restarting schema change coordination for table {} epoch {}", tableId, epoch);

            // create a new future for this coordination
            CompletableFuture<Boolean> newFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    long timeoutMs = 300_000L;
                                    boolean success =
                                            coordinator.requestSchemaChange(
                                                    tableId, epoch, timeoutMs);

                                    if (success) {
                                        log.info(
                                                "Restarted schema change coordination successful for table {} epoch {}",
                                                tableId,
                                                epoch);
                                    } else {
                                        log.error(
                                                "Restarted schema change coordination failed for table {} epoch {}",
                                                tableId,
                                                epoch);
                                    }

                                    return success;
                                } catch (Exception e) {
                                    log.error(
                                            "Error in restarted schema change coordination for table {} epoch {}",
                                            tableId,
                                            epoch,
                                            e);
                                    return false;
                                }
                            });

            newFuture.whenComplete(
                    (success, throwable) -> {
                        try {
                            if (throwable != null) {
                                log.error(
                                        "Restarted schema change future completed with exception",
                                        throwable);
                            }

                            // release the buffered data
                            List<BufferedDataRow> bufferedRows = bufferedDataRows.get(key);
                            if (bufferedRows != null) {
                                releaseBufferedDataForKey(key, bufferedRows);
                                log.info(
                                        "Released {} buffered rows after restarted coordination for key {}",
                                        bufferedRows.size(),
                                        key);
                            }

                            // check if this was the last pending coordination
                            if (bufferedDataRows.isEmpty()) {
                                schemaChangePending = false;
                                schemaChangePendingState.clear();
                                schemaChangePendingState.add(false);
                                log.info(
                                        "All schema change coordination completed, resumed normal data flow");
                            }

                        } catch (Exception e) {
                            log.error("Error in restarted coordination completion handling", e);
                        }
                    });

            if (pendingSchemaFuture == null) {
                pendingSchemaFuture = newFuture;
            }

        } catch (Exception e) {
            log.error(
                    "Failed to restart schema change coordination for table {} epoch {}, releasing data",
                    tableId,
                    epoch,
                    e);
            List<BufferedDataRow> bufferedRows = bufferedDataRows.get(key);
            if (bufferedRows != null) {
                releaseBufferedDataForKey(key, bufferedRows);
            }
        }
    }

    private void releaseAllBufferedData() {
        try {
            int totalReleased = 0;
            synchronized (this) {
                for (Map.Entry<String, List<BufferedDataRow>> entry : bufferedDataRows.entrySet()) {
                    List<BufferedDataRow> bufferedRows = entry.getValue();
                    if (bufferedRows != null && !bufferedRows.isEmpty()) {
                        for (BufferedDataRow buffered : bufferedRows) {
                            output.collect(new StreamRecord<>(buffered.row, buffered.timestamp));
                        }
                        totalReleased += bufferedRows.size();
                    }
                }

                bufferedDataRows.clear();
                stateDirty = true;
            }

            schemaChangePending = false;
            schemaChangePendingState.clear();
            schemaChangePendingState.add(false);

            log.info("Emergency recovery: Released {} buffered data rows", totalReleased);
        } catch (Exception e) {
            log.error("Failed to release all buffered data during emergency recovery", e);
        }
    }

    private void sendSchemaChangeEventToDownstream(SchemaChangeEvent schemaChangeEvent) {
        log.info(
                "Broadcasting SchemaChangeEvent to all downstream sink subtasks for table: {}",
                schemaChangeEvent.tableIdentifier());
        SeaTunnelRow broadcastRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_broadcast", schemaChangeEvent);
        broadcastRow.setOptions(options);

        output.collect(new StreamRecord<>(broadcastRow));
        log.info(
                "SchemaChangeEvent broadcast sent for table: {}",
                schemaChangeEvent.tableIdentifier());
    }

    @Override
    public void close() throws Exception {
        try {
            if (pendingSchemaFuture != null && !pendingSchemaFuture.isDone()) {
                log.info("Cancelling ongoing schema change request during close");
                pendingSchemaFuture.cancel(true);
            }
        } catch (Exception e) {
            log.warn("Error during SchemaOperator cleanup", e);
        } finally {
            super.close();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        try {
            // clear and update lastProcessedEventTime
            lastProcessedEventTimeState.clear();
            if (lastProcessedEventTime != null) {
                lastProcessedEventTimeState.add(lastProcessedEventTime);
            }

            // clear and update schemaChangePending
            schemaChangePendingState.clear();
            schemaChangePendingState.add(schemaChangePending);

            // clear and update local schema state
            localSchemaStateStore.clear();
            for (Map.Entry<TableIdentifier, CatalogTable> entry : localSchemaState.entrySet()) {
                localSchemaStateStore.add(new SchemaStateEntry(entry.getKey(), entry.getValue()));
            }

            // batch sync buffered data to state only when dirty
            if (stateDirty) {
                bufferedDataRowsState.clear();
                synchronized (this) {
                    for (Map.Entry<String, List<BufferedDataRow>> entry :
                            bufferedDataRows.entrySet()) {
                        bufferedDataRowsState.add(
                                new BufferedDataEntry(entry.getKey(), entry.getValue()));
                    }
                    stateDirty = false;
                }
            }

            log.debug(
                    "SchemaOperator state snapshot completed using operator state for checkpoint: {}, lastProcessedEventTime: {}, schemaChangePending: {}, localSchemaState size: {}, bufferedDataRows size: {}",
                    context.getCheckpointId(),
                    lastProcessedEventTime,
                    schemaChangePending,
                    localSchemaState.size(),
                    bufferedDataRows.size());
        } catch (Exception e) {
            log.error("Error during state snapshot", e);
            throw e;
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        if (this.bufferedDataRows == null) {
            this.bufferedDataRows = new ConcurrentHashMap<>();
        }

        ListStateDescriptor<SchemaStateEntry> localSchemaStateDescriptor =
                new ListStateDescriptor<>("localSchemaState", SchemaStateEntry.class);

        ListStateDescriptor<Long> lastProcessedEventTimeDescriptor =
                new ListStateDescriptor<>("lastProcessedEventTime", Long.class);

        ListStateDescriptor<Boolean> schemaChangePendingDescriptor =
                new ListStateDescriptor<>("schemaChangePending", Boolean.class);

        ListStateDescriptor<BufferedDataEntry> bufferedDataRowsDescriptor =
                new ListStateDescriptor<>("bufferedDataRows", BufferedDataEntry.class);

        this.localSchemaStateStore =
                context.getOperatorStateStore().getListState(localSchemaStateDescriptor);
        this.lastProcessedEventTimeState =
                context.getOperatorStateStore().getListState(lastProcessedEventTimeDescriptor);
        this.schemaChangePendingState =
                context.getOperatorStateStore().getListState(schemaChangePendingDescriptor);
        this.bufferedDataRowsState =
                context.getOperatorStateStore().getListState(bufferedDataRowsDescriptor);

        if (context.isRestored()) {
            // restore from operator state
            Iterable<Long> eventTimes = lastProcessedEventTimeState.get();
            for (Long eventTime : eventTimes) {
                this.lastProcessedEventTime = eventTime;
                break;
            }

            Iterable<Boolean> pendingFlags = schemaChangePendingState.get();
            for (Boolean pending : pendingFlags) {
                this.schemaChangePending = pending;
                break;
            }

            // restore schema state
            Iterable<SchemaStateEntry> schemaEntries = localSchemaStateStore.get();
            for (SchemaStateEntry entry : schemaEntries) {
                localSchemaState.put(entry.tableId, entry.catalogTable);
                log.info("Restored schema state for table: {}", entry.tableId);
            }

            // restore buffered data rows
            Iterable<BufferedDataEntry> bufferedEntries = bufferedDataRowsState.get();
            if (bufferedEntries != null) {
                synchronized (this) {
                    for (BufferedDataEntry entry : bufferedEntries) {
                        if (entry != null && entry.key != null && entry.bufferedRows != null) {
                            bufferedDataRows.put(entry.key, new ArrayList<>(entry.bufferedRows));
                            log.info(
                                    "Restored {} buffered data rows for key: {}",
                                    entry.bufferedRows.size(),
                                    entry.key);
                        }
                    }
                }
            }
        }

        log.info(
                "SchemaOperator state initialized using operator state - lastProcessedEventTime: {}, schemaChangePending: {}, localSchemaState size: {}, bufferedDataRows size: {}",
                this.lastProcessedEventTime,
                this.schemaChangePending,
                localSchemaState.size(),
                bufferedDataRows.size());
    }

    @Setter
    @Getter
    public static class BufferedDataRow implements Serializable {
        private static final long serialVersionUID = 1L;

        private SeaTunnelRow row;
        private long timestamp;

        public BufferedDataRow() {}

        public BufferedDataRow(SeaTunnelRow row, long timestamp) {
            this.row = row;
            this.timestamp = timestamp;
        }
    }

    @Setter
    @Getter
    public static class SchemaStateEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        private TableIdentifier tableId;
        private CatalogTable catalogTable;

        public SchemaStateEntry() {}

        public SchemaStateEntry(TableIdentifier tableId, CatalogTable catalogTable) {
            this.tableId = tableId;
            this.catalogTable = catalogTable;
        }
    }

    @Setter
    @Getter
    public static class BufferedDataEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        private String key;
        private List<BufferedDataRow> bufferedRows;

        public BufferedDataEntry() {}

        public BufferedDataEntry(String key, List<BufferedDataRow> bufferedRows) {
            this.key = key;
            this.bufferedRows = bufferedRows;
        }
    }
}
