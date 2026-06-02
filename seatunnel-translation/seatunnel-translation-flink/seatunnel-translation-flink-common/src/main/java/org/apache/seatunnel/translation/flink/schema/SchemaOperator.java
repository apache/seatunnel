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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.event.TableEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Operator placed after the source to handle schema evolution.
 *
 * <p>Schema change events are NOT processed synchronously in {@link #processElement}. Instead, they
 * are buffered and deferred until an additional checkpoint cycle has completed after the first
 * checkpoint that observed the pending DDL. This wait ensures that when the sink executes ALTER
 * TABLE, all XA transactions from prior checkpoint cycles have been fully committed by the {@code
 * FlinkGlobalCommitter} (which runs asynchronously after {@code notifyCheckpointComplete}), so
 * their metadata locks are released and the ALTER TABLE can acquire an exclusive MDL lock without
 * deadlock.
 *
 * <p>Per checkpoint cycle, at most ONE schema change is applied. If multiple DDLs arrive between
 * two checkpoints, they are processed across successive checkpoint cycles.
 *
 * <p>Flink 1.13 cannot continue checkpointing after some source subtasks have finished. When
 * high-parallelism CDC jobs hit that condition, pending schema changes would otherwise stay blocked
 * forever. Subclasses may override {@link #scheduleFallbackTimer()} to register a version-specific
 * timer that detects the stall and re-enters the task thread via {@link
 * #handleFallbackTimerOnTaskThread()} so the deferred DDL can still be applied safely. The base
 * implementation is a no-op, keeping the common module free of version-specific overhead.
 */
@Slf4j
public class SchemaOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private static final int MAX_BUFFERED_RECORDS = 100000;
    private static final long SCHEMA_CHANGE_TIMEOUT_MS = 300_000L;
    private static final int CHECKPOINT_WAIT_ROUNDS = 1;

    /** Exposed to subclasses so version-specific fallback timers can use the same threshold. */
    protected static final long CHECKPOINT_STALL_TIMEOUT_MS = 15_000L;

    private final Map<TableIdentifier, CatalogTable> localSchemaState;

    /** Exposed to subclasses for logging only. */
    protected String jobId;

    private final SupportSchemaEvolution source;
    private final Config pluginConfig;
    private volatile Long lastProcessedEventTime;
    private transient LocalSchemaCoordinator coordinator;
    private transient Queue<BufferedRecord> pendingQueue;
    private volatile boolean schemaChangePending = false;
    private long firstSeenCheckpointId = -1L;

    /**
     * Timestamp of the most recently completed checkpoint. Updated in {@link
     * #notifyCheckpointComplete} and read by {@link #handleFallbackTimerOnTaskThread} to detect
     * whether checkpoints have stalled.
     */
    protected volatile long lastCheckpointCompletedMs = -1L;

    private transient ListState<SchemaStateEntry> localSchemaStateStore;
    private transient ListState<Long> lastProcessedEventTimeState;
    private transient ListState<Boolean> schemaChangePendingState;
    private transient ListState<BufferedRecordEntry> bufferedRecordsState;
    private transient ListState<Long> firstSeenCheckpointIdState;

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
        if (this.pendingQueue == null) {
            this.pendingQueue = new LinkedList<>();
        }
        this.coordinator = LocalSchemaCoordinator.getInstance(this.jobId);

        log.info(
                "SchemaOperator opened for job: {}, schemaChangePending: {}, pendingQueue size: {}",
                this.jobId,
                this.schemaChangePending,
                this.pendingQueue.size());
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> streamRecord)
            throws InterruptedException {
        SeaTunnelRow element = streamRecord.getValue();

        if (!isSchemaEvolutionEnabled(pluginConfig)) {
            output.collect(streamRecord);
            return;
        }

        // detect schema change events
        if ("__SCHEMA_CHANGE_EVENT__".equals(element.getTableId())
                && element.getOptions() != null) {
            Object object = element.getOptions().get("schema_change_event");
            if (object instanceof SchemaChangeEvent) {
                handleSchemaChangeDetected((SchemaChangeEvent) object);
                return;
            }
        }

        // while a schema change is pending, buffer ALL subsequent records
        if (schemaChangePending) {
            enqueueDataRecord(element, streamRecord.getTimestamp());
            return;
        }

        output.collect(streamRecord);
    }

    private void handleSchemaChangeDetected(SchemaChangeEvent event) {
        List<SchemaChangeType> supportedTypes = source.supports();
        if (supportedTypes == null || supportedTypes.isEmpty()) {
            log.info("Source does not support any schema change types, skipping");
            return;
        }
        if (!isSchemaChangeSupported(event, supportedTypes)) {
            log.warn("Schema change type {} not supported, skipping", event.getEventType());
            return;
        }

        if (event instanceof TableEvent) {
            event.setJobId(jobId);
        }

        log.info(
                "Schema change detected for table {} (epoch {}). "
                        + "Deferring until next checkpoint completes to avoid XA/MDL deadlock.",
                event.tableIdentifier(),
                event.getCreatedTime());

        pendingQueue.add(BufferedRecord.schemaChange(event));
        schemaChangePending = true;
        scheduleFallbackTimer();
    }

    private void enqueueDataRecord(SeaTunnelRow row, long timestamp) {
        if (pendingQueue.size() >= MAX_BUFFERED_RECORDS) {
            TableIdentifier tableIdentifier = getPendingSchemaTableIdentifier();
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    String.format(
                            "Pending schema buffer overflow (max=%d). "
                                    + "Failing fast to avoid dropping schema change control events.",
                            MAX_BUFFERED_RECORDS),
                    tableIdentifier,
                    jobId);
        }
        pendingQueue.add(BufferedRecord.data(row, timestamp));
    }

    private TableIdentifier getPendingSchemaTableIdentifier() {
        for (BufferedRecord record : pendingQueue) {
            if (record.isSchemaChange && record.schemaEvent != null) {
                return record.schemaEvent.tableIdentifier();
            }
        }
        return null;
    }

    /**
     * Called by Flink after a checkpoint succeeds. Uses an extra completed checkpoint round to
     * ensure safety:
     *
     * <ul>
     *   <li>First time seeing the DDL: record {@link #firstSeenCheckpointId} but do NOT broadcast
     *       the DDL yet. At this point the {@code FlinkGlobalCommitter} may still be running {@code
     *       XA COMMIT} for this checkpoint's prepared transactions, holding MDL locks on the sink
     *       table.
     *   <li>{@code checkpointId >= firstSeenCheckpointId + CHECKPOINT_WAIT_ROUNDS}: the XA COMMIT
     *       from the earlier checkpoint cycle is guaranteed to have finished (at least one
     *       additional checkpoint cycle has completed, which implies the committer ran). The sink's
     *       ALTER TABLE will not encounter MDL lock, it is now safe to broadcast the DDL.
     * </ul>
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        lastCheckpointCompletedMs = System.currentTimeMillis();

        if (!schemaChangePending || pendingQueue.isEmpty()) {
            return;
        }

        BufferedRecord head = advancePastDataRecords();
        if (head == null) {
            return;
        }

        // first time seeing this DDL at head of queue — just record the checkpoint id
        if (firstSeenCheckpointId < 0) {
            firstSeenCheckpointId = checkpointId;
            log.info(
                    "Checkpoint {} completed. DDL for table {} (epoch {}) first seen. "
                            + "Waiting {} more checkpoint round(s) for XA COMMIT to finish.",
                    checkpointId,
                    head.schemaEvent.tableIdentifier(),
                    head.schemaEvent.getCreatedTime(),
                    CHECKPOINT_WAIT_ROUNDS);
            return;
        }

        if (checkpointId < firstSeenCheckpointId + CHECKPOINT_WAIT_ROUNDS) {
            log.info(
                    "Checkpoint {} completed. Still waiting for DDL on table {} (epoch {}). "
                            + "Need checkpoint >= {} (first seen at {}, wait rounds = {}).",
                    checkpointId,
                    head.schemaEvent.tableIdentifier(),
                    head.schemaEvent.getCreatedTime(),
                    firstSeenCheckpointId + CHECKPOINT_WAIT_ROUNDS,
                    firstSeenCheckpointId,
                    CHECKPOINT_WAIT_ROUNDS);
            return;
        }

        long waitedSince = firstSeenCheckpointId;
        SchemaChangeEvent event = head.schemaEvent;
        TableIdentifier tableId = event.tableIdentifier();
        long eventTime = event.getCreatedTime();

        log.info(
                "Checkpoint {} completed (waited since checkpoint {}). "
                        + "Applying deferred schema change for table {} (epoch {}).",
                checkpointId,
                waitedSince,
                tableId,
                eventTime);

        applyNextPendingSchemaChange();
    }

    /**
     * Handles a checkpoint-stall fallback on the task thread. Must be called from the Flink task
     * thread (e.g. via {@code ProcessingTimeService.registerTimer} callback) to keep {@code
     * output.collect} and operator state accesses thread-safe.
     *
     * <p>Safety fence: the DDL is applied only when at least one checkpoint has already completed
     * after the schema event ({@code firstSeenCheckpointId >= 0}). This preserves the guarantee
     * that XA transactions from the earlier checkpoint cycle have finished before ALTER TABLE runs.
     * If that fence has not been crossed yet, the fallback reschedules itself by calling {@link
     * #scheduleFallbackTimer()} and returns without applying anything.
     */
    protected void handleFallbackTimerOnTaskThread() throws InterruptedException {
        if (!schemaChangePending || pendingQueue.isEmpty()) {
            return;
        }

        if (lastCheckpointCompletedMs > 0
                && System.currentTimeMillis() - lastCheckpointCompletedMs
                        < CHECKPOINT_STALL_TIMEOUT_MS) {
            scheduleFallbackTimer();
            return;
        }

        BufferedRecord head = advancePastDataRecords();
        if (head == null) {
            return;
        }

        if (firstSeenCheckpointId < 0) {
            log.info(
                    "Fallback timer fired but no checkpoint has completed after schema event "
                            + "for table {} (epoch {}). Rescheduling fallback to preserve "
                            + "checkpoint-completion safety fence.",
                    head.schemaEvent.tableIdentifier(),
                    head.schemaEvent.getCreatedTime());
            scheduleFallbackTimer();
            return;
        }

        log.warn(
                "Checkpoint stall detected after first post-DDL checkpoint {}. "
                        + "Applying deferred DDL for table {} (epoch {}) via fallback timer. "
                        + "Note: data committed via normal Flink checkpoint lifecycle may be "
                        + "delayed until checkpoints resume.",
                firstSeenCheckpointId,
                head.schemaEvent.tableIdentifier(),
                head.schemaEvent.getCreatedTime());

        applyNextPendingSchemaChange();
    }

    /**
     * Schedules a fallback timer that will call {@link #handleFallbackTimerOnTaskThread()} if
     * checkpoints stall before the pending schema change can be applied.
     *
     * <p>The base implementation is a no-op: version-specific subclasses (e.g. {@code
     * SchemaOperator13}) override this to register a timer via {@code ProcessingTimeService},
     * keeping the common module free of version-specific timer infrastructure and reflection.
     */
    protected void scheduleFallbackTimer() {
        // no-op by default; overridden in version-specific subclasses
    }

    private BufferedRecord advancePastDataRecords() {
        BufferedRecord head = pendingQueue.peek();
        while (head != null && !head.isSchemaChange) {
            output.collect(new StreamRecord<>(head.row, head.timestamp));
            pendingQueue.poll();
            head = pendingQueue.peek();
        }
        if (head == null) {
            schemaChangePending = false;
            firstSeenCheckpointId = -1L;
        }
        return head;
    }

    private void applyNextPendingSchemaChange() throws InterruptedException {
        BufferedRecord head = pendingQueue.peek();
        if (head == null || !head.isSchemaChange) {
            return;
        }

        SchemaChangeEvent event = head.schemaEvent;
        TableIdentifier tableId = event.tableIdentifier();
        long eventTime = event.getCreatedTime();

        if (lastProcessedEventTime != null && eventTime <= lastProcessedEventTime) {
            log.warn(
                    "Skipping outdated schema change event (epoch {} <= last processed {})",
                    eventTime,
                    lastProcessedEventTime);
            pendingQueue.poll();
            firstSeenCheckpointId = -1L;
            drainDataUntilNextSchemaChange();
            return;
        }

        sendSchemaChangeEventToDownstream(event);

        boolean success =
                coordinator.requestSchemaChange(tableId, eventTime, SCHEMA_CHANGE_TIMEOUT_MS);
        if (!success) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    String.format(
                            "Schema change for table %s (epoch %d) failed during sink coordination.",
                            tableId, eventTime),
                    tableId,
                    jobId);
        }
        log.info(
                "Schema change for table {} (epoch {}) confirmed by all sink subtasks.",
                tableId,
                eventTime);
        pendingQueue.poll();
        firstSeenCheckpointId = -1L;

        CatalogTable newSchema = event.getChangeAfter();
        if (newSchema != null) {
            localSchemaState.put(tableId, newSchema);
        }
        lastProcessedEventTime = eventTime;

        drainDataUntilNextSchemaChange();

        log.info(
                "Schema change for table {} (epoch {}) processing complete. pendingQueue remaining: {}",
                tableId,
                eventTime,
                pendingQueue.size());
    }

    private void drainDataUntilNextSchemaChange() {
        int released = 0;
        while (!pendingQueue.isEmpty()) {
            BufferedRecord record = pendingQueue.peek();
            if (record.isSchemaChange) {
                // another DDL will stop here, wait for next checkpoint cycle
                log.info(
                        "Released {} buffered data records. Another schema change pending, "
                                + "waiting for next checkpoint.",
                        released);
                scheduleFallbackTimer();
                return;
            }
            pendingQueue.poll();
            output.collect(new StreamRecord<>(record.row, record.timestamp));
            released++;
        }

        // queue is empty
        schemaChangePending = false;
        log.info("Released {} buffered data records. Normal data flow resumed.", released);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        lastProcessedEventTimeState.clear();
        if (lastProcessedEventTime != null) {
            lastProcessedEventTimeState.add(lastProcessedEventTime);
        }

        schemaChangePendingState.clear();
        schemaChangePendingState.add(schemaChangePending);

        firstSeenCheckpointIdState.clear();
        firstSeenCheckpointIdState.add(firstSeenCheckpointId);

        localSchemaStateStore.clear();
        for (Map.Entry<TableIdentifier, CatalogTable> entry : localSchemaState.entrySet()) {
            localSchemaStateStore.add(new SchemaStateEntry(entry.getKey(), entry.getValue()));
        }

        bufferedRecordsState.clear();
        for (BufferedRecord record : pendingQueue) {
            bufferedRecordsState.add(
                    new BufferedRecordEntry(
                            record.isSchemaChange,
                            record.row,
                            record.timestamp,
                            record.schemaEvent));
        }

        log.debug(
                "State snapshot for checkpoint {}: lastEventTime={}, pending={}, "
                        + "firstSeenCkpt={}, queueSize={}",
                context.getCheckpointId(),
                lastProcessedEventTime,
                schemaChangePending,
                firstSeenCheckpointId,
                pendingQueue.size());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        if (this.pendingQueue == null) {
            this.pendingQueue = new LinkedList<>();
        }

        ListStateDescriptor<SchemaStateEntry> schemaDescriptor =
                new ListStateDescriptor<>("localSchemaState", SchemaStateEntry.class);
        ListStateDescriptor<Long> eventTimeDescriptor =
                new ListStateDescriptor<>("lastProcessedEventTime", Long.class);
        ListStateDescriptor<Boolean> pendingDescriptor =
                new ListStateDescriptor<>("schemaChangePending", Boolean.class);
        ListStateDescriptor<BufferedRecordEntry> bufferedDescriptor =
                new ListStateDescriptor<>("bufferedRecords", BufferedRecordEntry.class);
        ListStateDescriptor<Long> firstSeenCkptDescriptor =
                new ListStateDescriptor<>("firstSeenCheckpointId", Long.class);

        this.localSchemaStateStore = context.getOperatorStateStore().getListState(schemaDescriptor);
        this.lastProcessedEventTimeState =
                context.getOperatorStateStore().getListState(eventTimeDescriptor);
        this.schemaChangePendingState =
                context.getOperatorStateStore().getListState(pendingDescriptor);
        this.bufferedRecordsState =
                context.getOperatorStateStore().getListState(bufferedDescriptor);
        this.firstSeenCheckpointIdState =
                context.getOperatorStateStore().getListState(firstSeenCkptDescriptor);

        if (context.isRestored()) {
            for (Long t : lastProcessedEventTimeState.get()) {
                this.lastProcessedEventTime = t;
                break;
            }
            for (Boolean p : schemaChangePendingState.get()) {
                this.schemaChangePending = p;
                break;
            }
            for (Long ckpt : firstSeenCheckpointIdState.get()) {
                this.firstSeenCheckpointId = ckpt;
                break;
            }
            for (SchemaStateEntry entry : localSchemaStateStore.get()) {
                localSchemaState.put(entry.tableId, entry.catalogTable);
            }
            for (BufferedRecordEntry entry : bufferedRecordsState.get()) {
                if (entry.isSchemaChange) {
                    pendingQueue.add(BufferedRecord.schemaChange(entry.schemaEvent));
                } else {
                    pendingQueue.add(BufferedRecord.data(entry.row, entry.timestamp));
                }
            }
            log.info(
                    "State restored: lastEventTime={}, pending={}, firstSeenCkpt={}, queueSize={}",
                    lastProcessedEventTime,
                    schemaChangePending,
                    firstSeenCheckpointId,
                    pendingQueue.size());
        }
    }

    private boolean isSchemaEvolutionEnabled(Config config) {
        return config.hasPath("schema-changes.enabled")
                && config.getBoolean("schema-changes.enabled");
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

    private void sendSchemaChangeEventToDownstream(SchemaChangeEvent schemaChangeEvent) {
        log.info(
                "Broadcasting SchemaChangeEvent to downstream for table: {}",
                schemaChangeEvent.tableIdentifier());
        SeaTunnelRow broadcastRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_broadcast", schemaChangeEvent);
        broadcastRow.setOptions(options);
        output.collect(new StreamRecord<>(broadcastRow));
    }

    static class BufferedRecord {
        final boolean isSchemaChange;
        final SeaTunnelRow row;
        final long timestamp;
        final SchemaChangeEvent schemaEvent;

        private BufferedRecord(
                boolean isSchemaChange,
                SeaTunnelRow row,
                long timestamp,
                SchemaChangeEvent schemaEvent) {
            this.isSchemaChange = isSchemaChange;
            this.row = row;
            this.timestamp = timestamp;
            this.schemaEvent = schemaEvent;
        }

        static BufferedRecord data(SeaTunnelRow row, long timestamp) {
            return new BufferedRecord(false, row, timestamp, null);
        }

        static BufferedRecord schemaChange(SchemaChangeEvent event) {
            return new BufferedRecord(true, null, 0L, event);
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
    public static class BufferedRecordEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean isSchemaChange;
        private SeaTunnelRow row;
        private long timestamp;
        private SchemaChangeEvent schemaEvent;

        public BufferedRecordEntry() {}

        public BufferedRecordEntry(
                boolean isSchemaChange,
                SeaTunnelRow row,
                long timestamp,
                SchemaChangeEvent schemaEvent) {
            this.isSchemaChange = isSchemaChange;
            this.row = row;
            this.timestamp = timestamp;
            this.schemaEvent = schemaEvent;
        }
    }
}
