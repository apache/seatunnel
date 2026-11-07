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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.source.DynamicLookupBootstrapReader;
import org.apache.seatunnel.api.source.SourceGateCommand;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.DynamicLookupStateEnvelope;
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;
import org.apache.seatunnel.engine.server.task.group.AbstractTaskGroupWithIntermediateQueue;
import org.apache.seatunnel.engine.server.task.group.queue.AbstractIntermediateQueue;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * First in-engine dynamic lookup runtime for same-parallelism forward inputs.
 *
 * <p>This lifecycle consumes fact and dimension records from engine-managed intermediate queues.
 * Dimension rows maintain an in-memory keyed view. Fact rows probe that view and emit the
 * planner-declared projection. The implementation deliberately keeps routing local to the existing
 * task-group queue path; cross-worker channel transport and external state handles remain later
 * protocol work.
 *
 * <p>Threading contract: {@link #collect(Collector)} and record mutation run on the task thread,
 * checkpoint completion callbacks may run on the checkpoint callback thread, and restore is invoked
 * before the task thread opens the lifecycle. {@link #checkpointStateLock} protects only barrier
 * and fact-gate callback state; dimension rows stay single-threaded by this lifecycle ordering.
 */
public final class DynamicLookupFlowLifeCycle extends ActionFlowLifeCycle
        implements OneOutputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicLookupFlowLifeCycle.class);

    /** Per-call fairness quota so a hot dimension queue cannot starve fact processing. */
    private static final int MAX_RECORDS_PER_PORT_DRAIN = 1024;

    /** Per-entry allowance; this estimate is not a measured JVM heap bound. */
    private static final long DIMENSION_STATE_ENTRY_RESIDENT_OVERHEAD_BYTES = 64L;

    private final DynamicLookupAction action;
    private final DynamicLookupConfig config;
    private final Map<LookupKey, SeaTunnelRow> dimensionState = new HashMap<>();
    /** Standalone serialized bytes per dimension entry, used to avoid full-map walks per row. */
    private final Map<LookupKey, Long> dimensionStateEntryBytes = new HashMap<>();

    private final Map<Long, BarrierAlignment> barrierAlignments = new HashMap<>();
    private final Map<Integer, Long> blockedPorts = new HashMap<>();
    /** Coordinates task-thread barrier alignment with checkpoint callback release/open events. */
    private final Object checkpointStateLock = new Object();

    private LookupKey pendingDimensionUpdateBeforeKey;

    private transient BlockingQueue<Record<?>> factQueue;
    private transient BlockingQueue<Record<?>> dimensionQueue;
    private transient BlockingQueue<SourceGateCommand> factGateCommandQueue;
    private transient long pendingFactGateOpenCheckpointId = -1L;
    private transient boolean factGateOpened;
    /** Prevents duplicate OPEN commands while open and abort callbacks race on the same gate. */
    private transient boolean factGateOpening;

    private boolean restoredFromDurableLookupState;
    private boolean dimensionBootstrapComplete;
    private long estimatedDimensionLogicalBytes;
    private long estimatedDimensionResidentBytes;
    private long innerJoinMissCount;

    public DynamicLookupFlowLifeCycle(
            DynamicLookupAction action,
            SeaTunnelTask runningTask,
            DynamicLookupConfig config,
            CompletableFuture<Void> completableFuture) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.config = config;
    }

    @Override
    public void open() throws Exception {
        super.open();
        factQueue = openQueue(DynamicLookupAction.FACT_INPUT);
        dimensionQueue = openQueue(DynamicLookupAction.DIMENSION_INPUT);
        factGateCommandQueue = openGateCommandQueue();
        if (restoredFromDurableLookupState && !isFactGateOpened()) {
            openFactGate();
        }
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        // Each empty port already performs a bounded blocking poll. An additional idle sleep
        // delays barriers and newly arrived facts without providing further backpressure.
        drainPort(DynamicLookupAction.DIMENSION_INPUT, dimensionQueue, collector);
        drainPort(DynamicLookupAction.FACT_INPUT, factQueue, collector);
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        clearDimensionState();
        resetCheckpointAlignmentState();
        pendingDimensionUpdateBeforeKey = null;
        dimensionBootstrapComplete = false;
        restoredFromDurableLookupState = false;
        for (ActionSubtaskState actionSubtaskState : actionStateList) {
            for (byte[] stateBytes : actionSubtaskState.getState()) {
                restoreDimensionState(stateBytes);
            }
        }
        restoredFromDurableLookupState = dimensionBootstrapComplete;
        enforceExactDimensionStateBudget();
        LOG.info(
                "Restored dynamic lookup state for task {} with {} dimension entries",
                runningTask.getTaskLocation(),
                dimensionState.size());
    }

    @Override
    public void close() throws IOException {
        clearDimensionState();
        resetCheckpointAlignmentState();
        pendingDimensionUpdateBeforeKey = null;
        super.close();
    }

    private BlockingQueue<Record<?>> openQueue(int inputPort) {
        if (!(runningTask.getTaskGroup() instanceof AbstractTaskGroupWithIntermediateQueue)) {
            throw new TaskRuntimeException(
                    "Dynamic lookup requires an intermediate-queue task group");
        }
        IntermediateQueueConfig queueConfig = config.getInputQueue(inputPort);
        AbstractIntermediateQueue<?> queue =
                ((AbstractTaskGroupWithIntermediateQueue) runningTask.getTaskGroup())
                        .getQueueCache(
                                queueConfig.getQueueID(),
                                queueConfig.getCapacity(),
                                runningTask.getMetricsContext());
        Object rawQueue = queue.getIntermediateQueue();
        if (!(rawQueue instanceof BlockingQueue)) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M0 requires blocking intermediate queues");
        }
        @SuppressWarnings("unchecked")
        BlockingQueue<Record<?>> typedQueue = (BlockingQueue<Record<?>>) rawQueue;
        return typedQueue;
    }

    private BlockingQueue<SourceGateCommand> openGateCommandQueue() {
        if (!(runningTask.getTaskGroup() instanceof AbstractTaskGroupWithIntermediateQueue)) {
            throw new TaskRuntimeException(
                    "Dynamic lookup fact gate requires an intermediate-queue task group");
        }
        IntermediateQueueConfig queueConfig = config.getFactGateCommandQueue();
        AbstractIntermediateQueue<?> queue =
                ((AbstractTaskGroupWithIntermediateQueue) runningTask.getTaskGroup())
                        .getQueueCache(
                                queueConfig.getQueueID(),
                                queueConfig.getCapacity(),
                                runningTask.getMetricsContext());
        Object rawQueue = queue.getIntermediateQueue();
        if (!(rawQueue instanceof BlockingQueue)) {
            throw new TaskRuntimeException(
                    "Dynamic lookup fact gate requires blocking command queue");
        }
        @SuppressWarnings("unchecked")
        BlockingQueue<SourceGateCommand> typedQueue = (BlockingQueue<SourceGateCommand>) rawQueue;
        return typedQueue;
    }

    private boolean drainPort(
            int inputPort, BlockingQueue<Record<?>> queue, Collector<Record<?>> collector)
            throws Exception {
        if (isInputPortBlocked(inputPort)) {
            return false;
        }
        boolean processed = false;
        int processedCount = 0;
        Record<?> record;
        while (processedCount < MAX_RECORDS_PER_PORT_DRAIN
                && !isInputPortBlocked(inputPort)
                && (record = queue.poll(10, TimeUnit.MILLISECONDS)) != null) {
            processed = true;
            processedCount++;
            processRecord(inputPort, record, collector);
        }
        return processed;
    }

    private void processRecord(int inputPort, Record<?> record, Collector<Record<?>> collector)
            throws Exception {
        Object data = record.getData();
        if (data == DynamicLookupBootstrapReader.Status.COMPLETE) {
            if (inputPort != DynamicLookupAction.DIMENSION_INPUT) {
                throw new TaskRuntimeException(
                        "Dynamic lookup bootstrap status arrived on fact input");
            }
            dimensionBootstrapComplete = true;
            return;
        }
        if (data instanceof Barrier) {
            alignBarrier(inputPort, (Barrier) data, collector);
            return;
        }
        if (prepareClose) {
            return;
        }
        if (data instanceof SchemaChangeEvent) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M0 rejects schema change events: " + data);
        }
        if (!(data instanceof SeaTunnelRow)) {
            collector.collect(record);
            return;
        }
        SeaTunnelRow row = (SeaTunnelRow) data;
        if (inputPort == DynamicLookupAction.DIMENSION_INPUT) {
            applyDimension(row);
            return;
        }
        SeaTunnelRow outputRow = projectFact(row);
        if (outputRow != null) {
            collector.collect(new Record<>(outputRow));
        }
    }

    private void applyDimension(SeaTunnelRow row) throws IOException {
        LookupKey key =
                LookupKey.from(row, action.getDescriptor().getDimension().getKeyFieldIndexes());
        if (row.getRowKind() == RowKind.DELETE) {
            rejectDanglingUpdateBefore(row);
            dimensionState.remove(key);
            removeDimensionStateBudget(key);
        } else if (row.getRowKind() == RowKind.INSERT) {
            rejectDanglingUpdateBefore(row);
            SeaTunnelRow copiedRow = row.copy();
            dimensionState.put(key, copiedRow);
            updateDimensionStateBudget(key, copiedRow);
        } else if (row.getRowKind() == RowKind.UPDATE_BEFORE) {
            if (pendingDimensionUpdateBeforeKey != null) {
                throw new TaskRuntimeException(
                        "Dynamic lookup M0 requires atomic dimension update pairs");
            }
            pendingDimensionUpdateBeforeKey = key;
        } else if (row.getRowKind() == RowKind.UPDATE_AFTER) {
            if (pendingDimensionUpdateBeforeKey != null
                    && !pendingDimensionUpdateBeforeKey.equals(key)) {
                throw new TaskRuntimeException(
                        "Dynamic lookup M0 rejects dimension primary-key updates");
            }
            pendingDimensionUpdateBeforeKey = null;
            SeaTunnelRow copiedRow = row.copy();
            dimensionState.put(key, copiedRow);
            updateDimensionStateBudget(key, copiedRow);
        }
        enforceEstimatedDimensionStateBudget();
    }

    private void rejectDanglingUpdateBefore(SeaTunnelRow row) {
        if (pendingDimensionUpdateBeforeKey != null) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M0 requires UPDATE_AFTER immediately after UPDATE_BEFORE, but"
                            + " got "
                            + row.getRowKind());
        }
    }

    private SeaTunnelRow projectFact(SeaTunnelRow factRow) {
        if (factRow.getRowKind() != RowKind.INSERT) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M0 supports append-only fact rows, but got "
                            + factRow.getRowKind());
        }
        LookupKey key =
                LookupKey.from(factRow, action.getDescriptor().getFact().getKeyFieldIndexes());
        SeaTunnelRow dimensionRow = dimensionState.get(key);
        if (dimensionRow == null
                && action.getDescriptor().getJoinType() == DynamicLookupDescriptor.JoinType.INNER) {
            innerJoinMissCount++;
            if (innerJoinMissCount == 1 || Long.bitCount(innerJoinMissCount) == 1) {
                LOG.warn(
                        "Dynamic lookup task {} dropped {} INNER-join fact rows because the"
                                + " dimension key was missing. output={}",
                        runningTask.getTaskLocation(),
                        innerJoinMissCount,
                        action.getDescriptor().getOutputId());
            }
            return null;
        }
        List<DynamicLookupProjectionField> fields = action.getDescriptor().getProjectionFields();
        Object[] output = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DynamicLookupProjectionField field = fields.get(i);
            if (field.getInputSide() == DynamicLookupProjectionField.InputSide.FACT) {
                output[i] = factRow.getField(field.getSourceFieldIndex());
            } else {
                output[i] =
                        dimensionRow == null
                                ? null
                                : dimensionRow.getField(field.getSourceFieldIndex());
            }
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(output);
        outputRow.setRowKind(factRow.getRowKind());
        outputRow.setTableId(action.getDescriptor().getOutputId());
        return outputRow;
    }

    private void alignBarrier(int inputPort, Barrier barrier, Collector<Record<?>> collector)
            throws IOException {
        boolean aligned;
        synchronized (checkpointStateLock) {
            BarrierAlignment alignment =
                    barrierAlignments.computeIfAbsent(
                            barrier.getId(), ignored -> new BarrierAlignment());
            Long blockedCheckpointId = blockedPorts.get(inputPort);
            if (blockedCheckpointId != null && blockedCheckpointId != barrier.getId()) {
                throw new TaskRuntimeException(
                        "Dynamic lookup input port "
                                + inputPort
                                + " is already aligned to checkpoint "
                                + blockedCheckpointId);
            }
            alignment.seenPorts.add(inputPort);
            blockedPorts.put(inputPort, barrier.getId());
            aligned =
                    alignment.seenPorts.contains(DynamicLookupAction.FACT_INPUT)
                            && alignment.seenPorts.contains(DynamicLookupAction.DIMENSION_INPUT);
            if (aligned) {
                barrierAlignments.remove(barrier.getId());
                alignment.seenPorts.forEach(blockedPorts::remove);
            }
        }
        if (!aligned) {
            return;
        }
        if (pendingDimensionUpdateBeforeKey != null) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M0 requires dimension update pairs before checkpoint barrier");
        }
        if (barrier.prepareClose(runningTask.getTaskLocation())) {
            prepareClose = true;
        }
        if (barrier.snapshot()) {
            runningTask.addState(
                    barrier,
                    ActionStateKey.of(action),
                    Collections.singletonList(snapshotDimensionState()));
            synchronized (checkpointStateLock) {
                if (dimensionBootstrapComplete
                        && !factGateOpened
                        && !factGateOpening
                        && pendingFactGateOpenCheckpointId < 0) {
                    pendingFactGateOpenCheckpointId = barrier.getId();
                    LOG.info(
                            "Dynamic lookup task {} will open the fact gate after checkpoint {} completes",
                            runningTask.getTaskLocation(),
                            barrier.getId());
                }
            }
        }
        runningTask.ack(barrier);
        collector.collect(new Record<>(barrier));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        boolean shouldOpen;
        synchronized (checkpointStateLock) {
            shouldOpen =
                    !factGateOpened
                            && !factGateOpening
                            && pendingFactGateOpenCheckpointId == checkpointId;
        }
        if (!shouldOpen) {
            return;
        }
        LOG.info(
                "Dynamic lookup task {} opens the fact gate after checkpoint {} completed",
                runningTask.getTaskLocation(),
                checkpointId);
        openFactGate();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        synchronized (checkpointStateLock) {
            if (pendingFactGateOpenCheckpointId == checkpointId) {
                pendingFactGateOpenCheckpointId = -1L;
            }
            releaseAbortedBarrierLocked(checkpointId);
        }
        LOG.info(
                "Dynamic lookup task {} released checkpoint {} after abort",
                runningTask.getTaskLocation(),
                checkpointId);
    }

    /**
     * Releases barrier alignment state for an aborted checkpoint.
     *
     * <p>Checkpoint aborts are a normal recovery path. Leaving one input port blocked after an
     * abort would permanently stop the lookup task when the peer port never receives that
     * checkpoint barrier.
     */
    private void releaseAbortedBarrierLocked(long checkpointId) {
        barrierAlignments.remove(checkpointId);
        blockedPorts.entrySet().removeIf(entry -> entry.getValue() == checkpointId);
    }

    /** Sends at most one OPEN command to the fact gate for the current runtime instance. */
    private void openFactGate() throws InterruptedException {
        BlockingQueue<SourceGateCommand> gateQueue = factGateCommandQueue;
        if (gateQueue == null) {
            throw new TaskRuntimeException(
                    "Dynamic lookup fact gate command queue is not initialized");
        }
        synchronized (checkpointStateLock) {
            if (factGateOpened || factGateOpening) {
                return;
            }
            factGateOpening = true;
        }
        boolean offered = false;
        try {
            offered = gateQueue.offer(SourceGateCommand.OPEN, 10, TimeUnit.SECONDS);
        } finally {
            synchronized (checkpointStateLock) {
                if (offered) {
                    factGateOpened = true;
                }
                pendingFactGateOpenCheckpointId = -1L;
                factGateOpening = false;
            }
        }
        if (!offered) {
            throw new TaskRuntimeException("Dynamic lookup fact gate command queue is full");
        }
    }

    /** Checks whether the given input port is currently blocked by an incomplete barrier. */
    private boolean isInputPortBlocked(int inputPort) {
        synchronized (checkpointStateLock) {
            return blockedPorts.containsKey(inputPort);
        }
    }

    /** Returns whether the fact source gate has already accepted its OPEN command. */
    private boolean isFactGateOpened() {
        synchronized (checkpointStateLock) {
            return factGateOpened;
        }
    }

    /** Clears checkpoint-alignment bookkeeping when the runtime restores or shuts down. */
    private void resetCheckpointAlignmentState() {
        synchronized (checkpointStateLock) {
            barrierAlignments.clear();
            blockedPorts.clear();
            pendingFactGateOpenCheckpointId = -1L;
            factGateOpened = false;
            factGateOpening = false;
        }
    }

    private byte[] snapshotDimensionState() throws IOException {
        int payloadLength = dimensionStatePayloadLength();
        enforceDimensionStateBudget(
                payloadLength, Math.max(payloadLength, estimatedDimensionResidentBytes));
        ByteBuffer envelope =
                ByteBuffer.allocate(
                        Math.addExact(
                                payloadLength,
                                Integer.BYTES * 3 + DynamicLookupStateEnvelope.DIGEST_LENGTH));
        envelope.putInt(DynamicLookupStateEnvelope.MAGIC)
                .putInt(DynamicLookupStateEnvelope.VERSION)
                .putInt(payloadLength);
        // Size first without retaining bytes, then serialize directly into the final envelope.
        // Dimension state is task-thread confined, so both passes see exactly the same cut.
        MessageDigest digest = newSha256();
        writeDimensionStatePayload(
                new DigestOutputStream(new ByteBufferOutputStream(envelope), digest));
        // Reject a shrinking second pass before publishing a misplaced digest as a durable cut.
        if (envelope.position() != Integer.BYTES * 3 + payloadLength) {
            throw new IOException("Dynamic lookup snapshot payload length changed between passes");
        }
        envelope.put(digest.digest());
        return envelope.array();
    }

    /**
     * Serializes the in-memory dimension map into the checkpoint payload form.
     *
     * <p>M0 uses serialized bytes for logical admission. Mutation uses a standalone-entry estimate;
     * restore and snapshot additionally count the full payload. No full-map buffer is retained
     * here.
     */
    private void writeDimensionStatePayload(OutputStream payloadStream) throws IOException {
        // The readiness byte is part of the SHA-256 protected payload, including empty snapshots.
        payloadStream.write(dimensionBootstrapComplete ? 1 : 0);
        try (ObjectOutputStream outputStream = new ObjectOutputStream(payloadStream)) {
            outputStream.writeInt(dimensionState.size());
            for (Map.Entry<LookupKey, SeaTunnelRow> entry : dimensionState.entrySet()) {
                outputStream.writeObject(entry.getKey());
                outputStream.writeObject(entry.getValue());
            }
        }
    }

    private int dimensionStatePayloadLength() throws IOException {
        CountingOutputStream counter = new CountingOutputStream();
        writeDimensionStatePayload(counter);
        return Math.toIntExact(counter.bytes);
    }

    private void updateDimensionStateBudget(LookupKey key, SeaTunnelRow row) throws IOException {
        CountingOutputStream counter = new CountingOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(counter)) {
            output.writeObject(key);
            output.writeObject(row);
        }
        long serializedEntryBytes = counter.bytes;
        removeDimensionStateBudget(key);
        dimensionStateEntryBytes.put(key, serializedEntryBytes);
        estimatedDimensionLogicalBytes += serializedEntryBytes;
        estimatedDimensionResidentBytes += estimateResidentEntryBytes(serializedEntryBytes);
    }

    private void removeDimensionStateBudget(LookupKey key) {
        Long previousBytes = dimensionStateEntryBytes.remove(key);
        if (previousBytes == null) {
            return;
        }
        estimatedDimensionLogicalBytes -= previousBytes;
        estimatedDimensionResidentBytes -= estimateResidentEntryBytes(previousBytes);
    }

    private void enforceEstimatedDimensionStateBudget() {
        enforceDimensionStateBudget(
                estimatedDimensionLogicalBytes, estimatedDimensionResidentBytes);
    }

    private void enforceExactDimensionStateBudget() throws IOException {
        int payloadLength = dimensionStatePayloadLength();
        enforceDimensionStateBudget(
                payloadLength, Math.max(payloadLength, estimatedDimensionResidentBytes));
    }

    private void enforceDimensionStateBudget(long logicalStateBytes, long residentStateBytes) {
        if (logicalStateBytes > config.getMaxLogicalStateBytesPerSubtask()) {
            throw new TaskRuntimeException(
                    "Dynamic lookup dimension state exceeds logical budget. bytes="
                            + logicalStateBytes
                            + ", max="
                            + config.getMaxLogicalStateBytesPerSubtask());
        }
        if (residentStateBytes > config.getMaxResidentStateBytesPerSubtask()) {
            throw new TaskRuntimeException(
                    "Dynamic lookup dimension state exceeds resident budget. bytes="
                            + residentStateBytes
                            + ", max="
                            + config.getMaxResidentStateBytesPerSubtask());
        }
    }

    private static long estimateResidentEntryBytes(long serializedEntryBytes) {
        return serializedEntryBytes + DIMENSION_STATE_ENTRY_RESIDENT_OVERHEAD_BYTES;
    }

    private void clearDimensionState() {
        dimensionState.clear();
        dimensionStateEntryBytes.clear();
        estimatedDimensionLogicalBytes = 0L;
        estimatedDimensionResidentBytes = 0L;
        innerJoinMissCount = 0L;
        dimensionBootstrapComplete = false;
    }

    private void restoreDimensionState(byte[] stateBytes)
            throws IOException, ClassNotFoundException {
        if (stateBytes == null || stateBytes.length == 0) {
            return;
        }
        if (stateBytes.length < Integer.BYTES * 3 + DynamicLookupStateEnvelope.DIGEST_LENGTH) {
            throw new IOException("Dynamic lookup dimension state envelope is truncated");
        }
        ByteBuffer envelope = ByteBuffer.wrap(stateBytes);
        int magic = envelope.getInt();
        if (magic != DynamicLookupStateEnvelope.MAGIC) {
            throw new IOException("Invalid dynamic lookup dimension state magic: " + magic);
        }
        int version = envelope.getInt();
        if (version != DynamicLookupStateEnvelope.VERSION) {
            throw new IOException("Unsupported dynamic lookup dimension state version: " + version);
        }
        int payloadLength = envelope.getInt();
        if (payloadLength < 0
                || envelope.remaining()
                        != payloadLength + DynamicLookupStateEnvelope.DIGEST_LENGTH) {
            throw new IOException("Invalid dynamic lookup dimension state payload length");
        }
        enforceDimensionStateBudget(payloadLength, payloadLength);
        byte[] payload = new byte[payloadLength];
        envelope.get(payload);
        byte[] expectedDigest = new byte[DynamicLookupStateEnvelope.DIGEST_LENGTH];
        envelope.get(expectedDigest);
        if (!Arrays.equals(expectedDigest, sha256(payload))) {
            throw new IOException("Dynamic lookup dimension state SHA-256 digest mismatch");
        }
        if (payload.length == 0 || payload[0] != 0 && payload[0] != 1) {
            throw new IOException("Invalid dynamic lookup bootstrap readiness flag");
        }
        dimensionBootstrapComplete = payload[0] == 1;
        try (ObjectInputStream inputStream =
                new DynamicLookupStateObjectInputStream(
                        new ByteArrayInputStream(payload, 1, payload.length - 1))) {
            int entryCount = inputStream.readInt();
            if (entryCount < 0) {
                throw new IOException(
                        "Invalid dynamic lookup dimension state count: " + entryCount);
            }
            for (int i = 0; i < entryCount; i++) {
                LookupKey key = (LookupKey) inputStream.readObject();
                SeaTunnelRow row = (SeaTunnelRow) inputStream.readObject();
                dimensionState.put(key, row);
                updateDimensionStateBudget(key, row);
                enforceEstimatedDimensionStateBudget();
            }
        }
    }

    /** Restricts dimension checkpoint payloads to SeaTunnel row values and lookup-key classes. */
    private static final class DynamicLookupStateObjectInputStream extends ObjectInputStream {

        private DynamicLookupStateObjectInputStream(ByteArrayInputStream input) throws IOException {
            super(input);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor)
                throws IOException, ClassNotFoundException {
            String className = descriptor.getName();
            if (isAllowedClass(className)) {
                return super.resolveClass(descriptor);
            }
            throw new IOException("Rejected dynamic lookup dimension state class: " + className);
        }

        private static boolean isAllowedClass(String className) {
            if (className.startsWith("[")) {
                String component = className.substring(className.lastIndexOf('[') + 1);
                return component.length() == 1 && "ZBCSIJFD".contains(component)
                        || component.startsWith("L")
                                && component.endsWith(";")
                                && isAllowedClass(component.substring(1, component.length() - 1));
            }
            return ALLOWED_STATE_CLASSES.contains(className)
                    || className.equals(LookupKey.class.getName())
                    || className.equals(ByteArrayKeyPart.class.getName());
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException {
            throw new IOException("Dynamic lookup dimension state must not contain proxy classes");
        }
    }

    // Include serialization superclasses and java.time's serialization proxy by exact name.
    private static final Set<String> ALLOWED_STATE_CLASSES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "java.lang.Object",
                                    "java.lang.String",
                                    "java.lang.Number",
                                    "java.lang.Enum",
                                    "java.lang.Boolean",
                                    "java.lang.Byte",
                                    "java.lang.Short",
                                    "java.lang.Integer",
                                    "java.lang.Long",
                                    "java.lang.Float",
                                    "java.lang.Double",
                                    "java.lang.Character",
                                    "java.math.BigDecimal",
                                    "java.math.BigInteger",
                                    "java.sql.Date",
                                    "java.sql.Time",
                                    "java.sql.Timestamp",
                                    "java.util.Date",
                                    "java.time.Ser",
                                    "java.time.LocalDate",
                                    "java.time.LocalTime",
                                    "java.time.LocalDateTime",
                                    "java.time.OffsetDateTime",
                                    "java.time.ZoneOffset",
                                    "java.time.Instant",
                                    "java.util.ArrayList",
                                    "java.util.Map",
                                    "java.util.List",
                                    "java.util.HashMap",
                                    "java.util.LinkedHashMap",
                                    "java.util.Arrays$ArrayList",
                                    "java.util.Collections$EmptyList",
                                    "java.util.Collections$SingletonList",
                                    "java.util.Collections$EmptyMap",
                                    "java.util.Collections$SingletonMap",
                                    SeaTunnelRow.class.getName(),
                                    RowKind.class.getName())));

    /** Counts serialized bytes without allocating a payload-sized staging buffer. */
    private static final class CountingOutputStream extends OutputStream {
        private long bytes;

        @Override
        public void write(int value) {
            bytes++;
        }

        @Override
        public void write(byte[] values, int offset, int length) {
            bytes += length;
        }
    }

    /** Writes the serialized payload directly into the final checkpoint envelope. */
    private static final class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buffer;

        private ByteBufferOutputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int value) {
            buffer.put((byte) value);
        }

        @Override
        public void write(byte[] values, int offset, int length) {
            buffer.put(values, offset, length);
        }
    }

    private static byte[] sha256(byte[] payload) throws IOException {
        return newSha256().digest(payload);
    }

    private static MessageDigest newSha256() throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is required by the Java runtime", e);
        }
    }

    public static boolean isDimensionStateEnvelope(byte[] stateBytes) {
        return DynamicLookupStateEnvelope.hasEnvelopeMagic(stateBytes);
    }

    private static final class BarrierAlignment {
        private final Set<Integer> seenPorts = new HashSet<>();
    }

    private static final class LookupKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<Object> values;

        private LookupKey(List<Object> values) {
            this.values = values;
        }

        private static LookupKey from(SeaTunnelRow row, List<Integer> indexes) {
            List<Object> values = new ArrayList<>(indexes.size());
            for (Integer index : indexes) {
                Object value = row.getField(index);
                if (value instanceof byte[]) {
                    byte[] bytes = (byte[]) value;
                    values.add(new ByteArrayKeyPart(Arrays.copyOf(bytes, bytes.length)));
                } else {
                    values.add(value);
                }
            }
            return new LookupKey(values);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof LookupKey)) {
                return false;
            }
            LookupKey that = (LookupKey) other;
            return values.equals(that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(values);
        }
    }

    private static final class ByteArrayKeyPart implements Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] bytes;

        private ByteArrayKeyPart(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ByteArrayKeyPart)) {
                return false;
            }
            ByteArrayKeyPart that = (ByteArrayKeyPart) other;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
