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
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.event.TableEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

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
 * #handleFallbackTimerOnTaskThread()}. Exactly-once jobs fail rather than bypassing the second
 * checkpoint fence; at-least-once jobs may release the DDL through the fallback. The base
 * implementation is a no-op, keeping the common module free of version-specific overhead.
 */
@Slf4j
public class SchemaOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private static final int MAX_BUFFERED_RECORDS = 100000;
    private static final long MAX_BUFFERED_BYTES = 64L * 1024 * 1024;
    private static final int MIN_ESTIMATED_RECORD_BYTES = 64;
    private static final int CHECKPOINT_WAIT_ROUNDS = 1;
    protected static final long DEFAULT_CHECKPOINT_STALL_TIMEOUT_MS = 15_000L;

    protected String jobId;
    private final SupportSchemaEvolution source;
    private final Config pluginConfig;
    private final boolean exactlyOnceMode;
    protected final long checkpointStallTimeoutMs;
    private String schemaChangeProducerId;
    private long schemaChangeSequence;
    private String lastEmittedSchemaChangeId;
    private transient Queue<BufferedRecord> pendingQueue;
    private transient long pendingBytes;
    private volatile boolean schemaChangePending = false;
    private long firstSeenCheckpointId = -1L;

    protected volatile long lastCheckpointCompletedMs = -1L;
    private transient ListState<SchemaEvolutionProtocolState> schemaEvolutionProtocolState;

    // Legacy state descriptors are retained so savepoints written before the atomic protocol
    // state was introduced remain restorable. New snapshots also keep them populated for rollback
    // compatibility, but restore always prefers schemaEvolutionProtocolState when it is present.
    private transient ListState<TableEventTimeEntry> lastProcessedEventTimesState;
    private transient ListState<Boolean> schemaChangePendingState;
    private transient ListState<BufferedRecordEntry> bufferedRecordsState;
    private transient ListState<Long> firstSeenCheckpointIdState;
    private transient ListState<String> schemaChangeProducerIdState;
    private transient ListState<Long> schemaChangeSequenceState;
    private transient ListState<String> lastEmittedSchemaChangeIdState;

    public SchemaOperator(String jobId, SupportSchemaEvolution source, Config pluginConfig) {
        this(jobId, source, pluginConfig, true);
    }

    public SchemaOperator(
            String jobId,
            SupportSchemaEvolution source,
            Config pluginConfig,
            boolean exactlyOnceMode) {
        this(jobId, source, pluginConfig, exactlyOnceMode, DEFAULT_CHECKPOINT_STALL_TIMEOUT_MS);
    }

    protected SchemaOperator(
            String jobId,
            SupportSchemaEvolution source,
            Config pluginConfig,
            boolean exactlyOnceMode,
            long checkpointStallTimeoutMs) {
        this.jobId = jobId;
        this.source = source;
        this.pluginConfig = pluginConfig;
        this.exactlyOnceMode = exactlyOnceMode;
        this.checkpointStallTimeoutMs = checkpointStallTimeoutMs;
    }

    @Override
    public void open() throws Exception {
        super.open();
        String flinkJobId = getRuntimeContext().getJobId().toString();
        if (!flinkJobId.equals(this.jobId)) {
            this.jobId = flinkJobId;
        }
        if (this.pendingQueue == null) {
            this.pendingQueue = new ArrayDeque<>();
        }

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

        // while a schema change is pending, buffer all subsequent records
        if (schemaChangePending) {
            enqueueDataRecord(element, streamRecord.getTimestamp());
            return;
        }

        emitDataRecord(element, streamRecord.getTimestamp());
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

        enqueueSchemaChange(event);
        schemaChangePending = true;
        scheduleFallbackTimer();
    }

    private void enqueueSchemaChange(SchemaChangeEvent event) {
        long eventBytes = estimateSchemaChangeBytes(event);
        ensurePendingCapacity(eventBytes, event.tableIdentifier());
        pendingQueue.add(BufferedRecord.schemaChange(event));
        pendingBytes += eventBytes;
    }

    private void enqueueDataRecord(SeaTunnelRow row, long timestamp) {
        long rowBytes = estimateRowBytes(row);
        ensurePendingCapacity(rowBytes, getPendingSchemaTableIdentifier());
        pendingQueue.add(BufferedRecord.data(row, timestamp));
        pendingBytes += rowBytes;
    }

    private void ensurePendingCapacity(long recordBytes, TableIdentifier tableIdentifier) {
        if (pendingQueue.size() >= MAX_BUFFERED_RECORDS
                || pendingBytes + recordBytes > MAX_BUFFERED_BYTES) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    String.format(
                            "Pending schema buffer overflow (records=%d/%d, bytes=%d/%d). "
                                    + "Failing fast to avoid dropping schema change control events.",
                            pendingQueue.size(),
                            MAX_BUFFERED_RECORDS,
                            pendingBytes + recordBytes,
                            MAX_BUFFERED_BYTES),
                    tableIdentifier,
                    jobId);
        }
    }

    private static long estimateRowBytes(SeaTunnelRow row) {
        return Math.max(MIN_ESTIMATED_RECORD_BYTES, row.getBytesSize());
    }

    private static long estimateSchemaChangeBytes(SchemaChangeEvent event) {
        return Math.max(MIN_ESTIMATED_RECORD_BYTES, event.toString().length() * 2L);
    }

    private static long estimateBufferedRecordBytes(BufferedRecord record) {
        return record.isSchemaChange
                ? estimateSchemaChangeBytes(record.schemaEvent)
                : estimateRowBytes(record.row);
    }

    private BufferedRecord pollPendingRecord() {
        BufferedRecord record = pendingQueue.poll();
        if (record != null) {
            pendingBytes = Math.max(0L, pendingBytes - estimateBufferedRecordBytes(record));
        }
        return record;
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
     * <p>Exactly-once mode never uses elapsed wall-clock time as proof that an asynchronous sink
     * commit finished. If the second checkpoint cannot complete, the task fails so recovery can
     * retry from Flink-managed state. At-least-once mode may use the timer after the first
     * completed checkpoint because it does not promise the XA commit fence.
     */
    protected void handleFallbackTimerOnTaskThread() throws InterruptedException {
        if (!schemaChangePending || pendingQueue.isEmpty()) {
            return;
        }

        if (lastCheckpointCompletedMs > 0
                && System.currentTimeMillis() - lastCheckpointCompletedMs
                        < checkpointStallTimeoutMs) {
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

        if (exactlyOnceMode) {
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    String.format(
                            "Checkpoint progress stalled after checkpoint %d while waiting for "
                                    + "the second completed checkpoint before applying schema "
                                    + "change epoch %d. Failing the task because exactly-once "
                                    + "schema evolution cannot infer XA commit completion from "
                                    + "elapsed time.",
                            firstSeenCheckpointId, head.schemaEvent.getCreatedTime()),
                    head.schemaEvent.tableIdentifier(),
                    jobId);
        }

        log.warn(
                "Checkpoint stall detected after first post-DDL checkpoint {}. "
                        + "Applying deferred DDL for table {} (epoch {}) via the at-least-once "
                        + "fallback timer.",
                firstSeenCheckpointId,
                head.schemaEvent.tableIdentifier(),
                head.schemaEvent.getCreatedTime());

        applyNextPendingSchemaChange();
    }

    /**
     * Schedules a fallback timer that will call {@link #handleFallbackTimerOnTaskThread()} if
     * checkpoints stall before the pending schema change can be applied or failed safely.
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
            emitDataRecord(head.row, head.timestamp);
            pollPendingRecord();
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

        String schemaChangeId =
                SchemaEvolutionControlMessage.schemaChangeId(
                        schemaChangeProducerId, ++schemaChangeSequence);
        sendSchemaChangeEventToDownstream(event, schemaChangeId);
        lastEmittedSchemaChangeId = schemaChangeId;
        pollPendingRecord();
        firstSeenCheckpointId = -1L;

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
            pollPendingRecord();
            emitDataRecord(record.row, record.timestamp);
            released++;
        }

        // queue is empty
        schemaChangePending = false;
        log.info("Released {} buffered data records. Normal data flow resumed.", released);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        List<BufferedRecordEntry> bufferedRecords = new ArrayList<>(pendingQueue.size());
        for (BufferedRecord record : pendingQueue) {
            bufferedRecords.add(
                    new BufferedRecordEntry(
                            record.isSchemaChange,
                            record.row,
                            record.timestamp,
                            record.schemaEvent));
        }

        // Flink redistributes each operator ListState independently during rescale. Persist every
        // field that defines one producer's schema protocol in a single entry so pending DDLs,
        // buffered rows and their producer/sequence identity cannot be assigned to different
        // subtasks when parallelism changes.
        schemaEvolutionProtocolState.clear();
        schemaEvolutionProtocolState.add(
                new SchemaEvolutionProtocolState(
                        schemaChangePending,
                        bufferedRecords,
                        firstSeenCheckpointId,
                        schemaChangeProducerId,
                        schemaChangeSequence,
                        lastEmittedSchemaChangeId));

        lastProcessedEventTimesState.clear();

        schemaChangePendingState.clear();
        schemaChangePendingState.add(schemaChangePending);

        firstSeenCheckpointIdState.clear();
        firstSeenCheckpointIdState.add(firstSeenCheckpointId);

        schemaChangeProducerIdState.clear();
        // Keep the producer and sequence paired in one redistributable state entry. The separate
        // sequence state remains populated for savepoint compatibility with the earlier layout.
        schemaChangeProducerIdState.add(
                SchemaEvolutionControlMessage.schemaChangeId(
                        schemaChangeProducerId, schemaChangeSequence));

        schemaChangeSequenceState.clear();
        schemaChangeSequenceState.add(schemaChangeSequence);

        lastEmittedSchemaChangeIdState.clear();
        if (lastEmittedSchemaChangeId != null) {
            lastEmittedSchemaChangeIdState.add(lastEmittedSchemaChangeId);
        }

        bufferedRecordsState.clear();
        for (BufferedRecordEntry record : bufferedRecords) {
            bufferedRecordsState.add(record);
        }

        log.debug(
                "State snapshot for checkpoint {}: pending={}, firstSeenCkpt={}, "
                        + "queueSize={}, queueBytes={}",
                context.getCheckpointId(),
                schemaChangePending,
                firstSeenCheckpointId,
                pendingQueue.size(),
                pendingBytes);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        if (this.pendingQueue == null) {
            this.pendingQueue = new ArrayDeque<>();
        }

        ListStateDescriptor<TableEventTimeEntry> eventTimeDescriptor =
                new ListStateDescriptor<>(
                        "lastProcessedEventTimeByTable", TableEventTimeEntry.class);
        ListStateDescriptor<SchemaEvolutionProtocolState> protocolStateDescriptor =
                new ListStateDescriptor<>(
                        "schemaEvolutionProtocolState", SchemaEvolutionProtocolState.class);
        ListStateDescriptor<Boolean> pendingDescriptor =
                new ListStateDescriptor<>("schemaChangePending", Boolean.class);
        ListStateDescriptor<BufferedRecordEntry> bufferedDescriptor =
                new ListStateDescriptor<>("bufferedRecords", BufferedRecordEntry.class);
        ListStateDescriptor<Long> firstSeenCkptDescriptor =
                new ListStateDescriptor<>("firstSeenCheckpointId", Long.class);
        ListStateDescriptor<String> producerIdDescriptor =
                new ListStateDescriptor<>("schemaChangeProducerId", String.class);
        ListStateDescriptor<Long> sequenceDescriptor =
                new ListStateDescriptor<>("schemaChangeSequence", Long.class);
        ListStateDescriptor<String> lastEmittedSchemaChangeIdDescriptor =
                new ListStateDescriptor<>("lastEmittedSchemaChangeId", String.class);

        this.schemaEvolutionProtocolState =
                context.getOperatorStateStore().getListState(protocolStateDescriptor);
        this.lastProcessedEventTimesState =
                context.getOperatorStateStore().getListState(eventTimeDescriptor);
        this.schemaChangePendingState =
                context.getOperatorStateStore().getListState(pendingDescriptor);
        this.bufferedRecordsState =
                context.getOperatorStateStore().getListState(bufferedDescriptor);
        this.firstSeenCheckpointIdState =
                context.getOperatorStateStore().getListState(firstSeenCkptDescriptor);
        this.schemaChangeProducerIdState =
                context.getOperatorStateStore().getListState(producerIdDescriptor);
        this.schemaChangeSequenceState =
                context.getOperatorStateStore().getListState(sequenceDescriptor);
        this.lastEmittedSchemaChangeIdState =
                context.getOperatorStateStore().getListState(lastEmittedSchemaChangeIdDescriptor);

        if (context.isRestored()) {
            List<SchemaEvolutionProtocolState> restoredProtocolStates = new ArrayList<>();
            for (SchemaEvolutionProtocolState protocolState : schemaEvolutionProtocolState.get()) {
                restoredProtocolStates.add(protocolState);
            }
            if (!restoredProtocolStates.isEmpty()) {
                restoreAtomicProtocolState(selectProtocolState(restoredProtocolStates));
            } else {
                restoreLegacyProtocolState();
            }
        }
        if (schemaChangeProducerId == null) {
            schemaChangeProducerId = UUID.randomUUID().toString();
        }
    }

    private SchemaEvolutionProtocolState selectProtocolState(
            List<SchemaEvolutionProtocolState> restoredStates) {
        SchemaEvolutionProtocolState activeState = null;
        for (SchemaEvolutionProtocolState state : restoredStates) {
            if (!state.hasProtocolHistory()) {
                continue;
            }
            if (activeState != null) {
                throw new IllegalStateException(
                        "Multiple active schema-evolution protocol states were assigned to one "
                                + "operator while incremental.parallelism=1. Refusing to merge "
                                + "different producers because that would break DDL/DML ordering.");
            }
            activeState = state;
        }
        return activeState != null ? activeState : restoredStates.get(0);
    }

    private void restoreAtomicProtocolState(SchemaEvolutionProtocolState state) {
        long restoredFirstSeenCheckpointId = state.firstSeenCheckpointId;
        this.schemaChangeProducerId = state.schemaChangeProducerId;
        this.schemaChangeSequence = state.schemaChangeSequence;
        this.lastEmittedSchemaChangeId = state.lastEmittedSchemaChangeId;
        restoreBufferedRecords(state.bufferedRecords);
        this.schemaChangePending = state.schemaChangePending || !pendingQueue.isEmpty();

        // A checkpoint completed before the failure cannot fence a sink transaction restored from
        // that checkpoint. Wait for two fresh checkpoint completions after every restore.
        this.firstSeenCheckpointId = -1L;
        log.info(
                "Atomic protocol state restored: pending={}, restoredFirstSeenCkpt={}, "
                        + "activeFirstSeenCkpt={}, producerId={}, sequence={}, queueSize={}, "
                        + "queueBytes={}",
                schemaChangePending,
                restoredFirstSeenCheckpointId,
                firstSeenCheckpointId,
                schemaChangeProducerId,
                schemaChangeSequence,
                pendingQueue.size(),
                pendingBytes);
    }

    private void restoreLegacyProtocolState() throws Exception {
        long restoredFirstSeenCheckpointId = -1L;
        for (Boolean p : schemaChangePendingState.get()) {
            this.schemaChangePending |= Boolean.TRUE.equals(p);
        }
        for (Long ckpt : firstSeenCheckpointIdState.get()) {
            restoredFirstSeenCheckpointId = Math.max(restoredFirstSeenCheckpointId, ckpt);
        }
        String legacyProducerId = null;
        long pairedSequence = -1L;
        for (String producerState : schemaChangeProducerIdState.get()) {
            String producerId = SchemaEvolutionControlMessage.schemaChangeProducerId(producerState);
            long sequence = SchemaEvolutionControlMessage.schemaChangeSequence(producerState);
            if (producerId != null && sequence >= 0 && sequence > pairedSequence) {
                this.schemaChangeProducerId = producerId;
                this.schemaChangeSequence = sequence;
                pairedSequence = sequence;
            } else if (producerId == null && legacyProducerId == null) {
                legacyProducerId = producerState;
            }
        }
        long legacySequence = 0L;
        for (Long sequence : schemaChangeSequenceState.get()) {
            legacySequence = Math.max(legacySequence, sequence);
        }
        if (pairedSequence < 0 && legacyProducerId != null) {
            this.schemaChangeProducerId = legacyProducerId;
            this.schemaChangeSequence = legacySequence;
        }
        long lastEmittedSequence = -1L;
        for (String schemaChangeId : lastEmittedSchemaChangeIdState.get()) {
            long sequence = SchemaEvolutionControlMessage.schemaChangeSequence(schemaChangeId);
            if (sequence > lastEmittedSequence) {
                this.lastEmittedSchemaChangeId = schemaChangeId;
                lastEmittedSequence = sequence;
            }
        }
        List<BufferedRecordEntry> legacyBufferedRecords = new ArrayList<>();
        for (BufferedRecordEntry entry : bufferedRecordsState.get()) {
            legacyBufferedRecords.add(entry);
        }
        restoreBufferedRecords(legacyBufferedRecords);
        this.schemaChangePending |= !pendingQueue.isEmpty();
        // A checkpoint completed before the failure cannot fence a sink transaction that was
        // restored from that checkpoint. Re-establish the fence so the restored pending DDL
        // waits for two fresh checkpoint-completion notifications before it is broadcast.
        // Keep reading and writing the old state for savepoint compatibility, but never reuse
        // its value as the active recovery fence.
        this.firstSeenCheckpointId = -1L;
        log.info(
                "State restored: pending={}, restoredFirstSeenCkpt={}, "
                        + "activeFirstSeenCkpt={}, queueSize={}, queueBytes={}",
                schemaChangePending,
                restoredFirstSeenCheckpointId,
                firstSeenCheckpointId,
                pendingQueue.size(),
                pendingBytes);
    }

    private void restoreBufferedRecords(List<BufferedRecordEntry> bufferedRecords) {
        if (bufferedRecords == null) {
            return;
        }
        for (BufferedRecordEntry entry : bufferedRecords) {
            BufferedRecord record =
                    entry.isSchemaChange
                            ? BufferedRecord.schemaChange(entry.schemaEvent)
                            : BufferedRecord.data(entry.row, entry.timestamp);
            pendingQueue.add(record);
            pendingBytes += estimateBufferedRecordBytes(record);
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
            case SCHEMA_CHANGE_ALTER_TABLE_COMMENT:
                return supportedTypes.contains(SchemaChangeType.ALTER_TABLE_COMMENT);
            case SCHEMA_CHANGE_ALTER_COLUMN_COMMENT:
                return supportedTypes.contains(SchemaChangeType.ALTER_COLUMN_COMMENT);
            case SCHEMA_CHANGE_UPDATE_COLUMNS:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.DROP_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.RENAME_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.ALTER_COLUMN_COMMENT);
            default:
                log.error("Unknown schema change event type: {}", event.getEventType());
                throw SchemaValidationException.unsupportedChangeType(
                        event.tableIdentifier(), jobId);
        }
    }

    private void sendSchemaChangeEventToDownstream(
            SchemaChangeEvent schemaChangeEvent, String schemaChangeId) {
        log.info(
                "Broadcasting SchemaChangeEvent to downstream for table: {}",
                schemaChangeEvent.tableIdentifier());
        SeaTunnelRow broadcastRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_BROADCAST, schemaChangeEvent);
        options.put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_ID, schemaChangeId);
        broadcastRow.setOptions(options);
        output.collect(new StreamRecord<>(broadcastRow));
    }

    private void emitDataRecord(SeaTunnelRow row, long timestamp) {
        SchemaEvolutionControlMessage.requireSchemaChange(row, lastEmittedSchemaChangeId);
        output.collect(new StreamRecord<>(row, timestamp));
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
    public static class TableEventTimeEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private TableIdentifier tableId;
        private long eventTime;

        public TableEventTimeEntry() {}

        public TableEventTimeEntry(TableIdentifier tableId, long eventTime) {
            this.tableId = tableId;
            this.eventTime = eventTime;
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

    /** Atomic snapshot of all state belonging to one source-side schema protocol producer. */
    @Setter
    @Getter
    public static class SchemaEvolutionProtocolState implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean schemaChangePending;
        private List<BufferedRecordEntry> bufferedRecords;
        private long firstSeenCheckpointId;
        private String schemaChangeProducerId;
        private long schemaChangeSequence;
        private String lastEmittedSchemaChangeId;

        public SchemaEvolutionProtocolState() {}

        public SchemaEvolutionProtocolState(
                boolean schemaChangePending,
                List<BufferedRecordEntry> bufferedRecords,
                long firstSeenCheckpointId,
                String schemaChangeProducerId,
                long schemaChangeSequence,
                String lastEmittedSchemaChangeId) {
            this.schemaChangePending = schemaChangePending;
            this.bufferedRecords = bufferedRecords;
            this.firstSeenCheckpointId = firstSeenCheckpointId;
            this.schemaChangeProducerId = schemaChangeProducerId;
            this.schemaChangeSequence = schemaChangeSequence;
            this.lastEmittedSchemaChangeId = lastEmittedSchemaChangeId;
        }

        private boolean hasProtocolHistory() {
            return schemaChangePending
                    || (bufferedRecords != null && !bufferedRecords.isEmpty())
                    || schemaChangeSequence > 0
                    || lastEmittedSchemaChangeId != null;
        }
    }
}
