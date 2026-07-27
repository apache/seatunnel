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
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;
import org.apache.seatunnel.engine.server.task.group.AbstractTaskGroupWithIntermediateQueue;
import org.apache.seatunnel.engine.server.task.group.queue.AbstractIntermediateQueue;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
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
 * task-group queue path; cross-worker channel transport, external state handles, and source gates
 * remain separate protocol work.
 */
public final class DynamicLookupFlowLifeCycle extends ActionFlowLifeCycle
        implements OneOutputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private static final int DIMENSION_STATE_MAGIC = 0x44594C4B;
    private static final int DIMENSION_STATE_VERSION = 1;
    private static final int DIMENSION_STATE_DIGEST_LENGTH = 32;

    private final DynamicLookupAction action;
    private final DynamicLookupConfig config;
    private final Map<LookupKey, SeaTunnelRow> dimensionState = new HashMap<>();
    private final Map<Long, BarrierAlignment> barrierAlignments = new HashMap<>();
    private LookupKey pendingDimensionUpdateBeforeKey;

    private transient BlockingQueue<Record<?>> factQueue;
    private transient BlockingQueue<Record<?>> dimensionQueue;
    private transient BlockingQueue<SourceGateCommand> factGateCommandQueue;
    private transient volatile long pendingFactGateOpenCheckpointId = -1L;
    private transient volatile boolean factGateOpened;
    private boolean restoredFromDurableLookupState;

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
        if (restoredFromDurableLookupState && !factGateOpened) {
            openFactGate();
        }
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        boolean processed = false;
        processed |= drainPort(DynamicLookupAction.DIMENSION_INPUT, dimensionQueue, collector);
        processed |= drainPort(DynamicLookupAction.FACT_INPUT, factQueue, collector);
        if (!processed) {
            Thread.sleep(100);
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        dimensionState.clear();
        pendingDimensionUpdateBeforeKey = null;
        restoredFromDurableLookupState = !actionStateList.isEmpty();
        for (ActionSubtaskState actionSubtaskState : actionStateList) {
            for (byte[] stateBytes : actionSubtaskState.getState()) {
                restoreDimensionState(stateBytes);
            }
        }
    }

    @Override
    public void close() throws IOException {
        dimensionState.clear();
        barrierAlignments.clear();
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
                    "Dynamic lookup M1 requires blocking intermediate queues");
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
        boolean processed = false;
        Record<?> record;
        while ((record = queue.poll(10, TimeUnit.MILLISECONDS)) != null) {
            processed = true;
            processRecord(inputPort, record, collector);
        }
        return processed;
    }

    private void processRecord(int inputPort, Record<?> record, Collector<Record<?>> collector)
            throws Exception {
        Object data = record.getData();
        if (data instanceof Barrier) {
            alignBarrier(inputPort, (Barrier) data, collector);
            return;
        }
        if (prepareClose) {
            return;
        }
        if (data instanceof SchemaChangeEvent) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M1 rejects schema change events: " + data);
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

    private void applyDimension(SeaTunnelRow row) {
        LookupKey key =
                LookupKey.from(row, action.getDescriptor().getDimension().getKeyFieldIndexes());
        if (row.getRowKind() == RowKind.DELETE) {
            rejectDanglingUpdateBefore(row);
            dimensionState.remove(key);
        } else if (row.getRowKind() == RowKind.INSERT) {
            rejectDanglingUpdateBefore(row);
            dimensionState.put(key, row.copy());
        } else if (row.getRowKind() == RowKind.UPDATE_BEFORE) {
            if (pendingDimensionUpdateBeforeKey != null) {
                throw new TaskRuntimeException(
                        "Dynamic lookup M1 requires atomic dimension update pairs");
            }
            pendingDimensionUpdateBeforeKey = key;
        } else if (row.getRowKind() == RowKind.UPDATE_AFTER) {
            if (pendingDimensionUpdateBeforeKey != null
                    && !pendingDimensionUpdateBeforeKey.equals(key)) {
                throw new TaskRuntimeException(
                        "Dynamic lookup M1 rejects dimension primary-key updates");
            }
            pendingDimensionUpdateBeforeKey = null;
            dimensionState.put(key, row.copy());
        }
    }

    private void rejectDanglingUpdateBefore(SeaTunnelRow row) {
        if (pendingDimensionUpdateBeforeKey != null) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M1 requires UPDATE_AFTER immediately after UPDATE_BEFORE, but"
                            + " got "
                            + row.getRowKind());
        }
    }

    private SeaTunnelRow projectFact(SeaTunnelRow factRow) {
        if (factRow.getRowKind() != RowKind.INSERT) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M1 supports append-only fact rows, but got "
                            + factRow.getRowKind());
        }
        LookupKey key =
                LookupKey.from(factRow, action.getDescriptor().getFact().getKeyFieldIndexes());
        SeaTunnelRow dimensionRow = dimensionState.get(key);
        if (dimensionRow == null
                && action.getDescriptor().getJoinType() == DynamicLookupDescriptor.JoinType.INNER) {
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
        BarrierAlignment alignment =
                barrierAlignments.computeIfAbsent(
                        barrier.getId(), ignored -> new BarrierAlignment());
        alignment.seenPorts.add(inputPort);
        if (!alignment.seenPorts.contains(DynamicLookupAction.FACT_INPUT)
                || !alignment.seenPorts.contains(DynamicLookupAction.DIMENSION_INPUT)) {
            return;
        }
        barrierAlignments.remove(barrier.getId());
        if (pendingDimensionUpdateBeforeKey != null) {
            throw new TaskRuntimeException(
                    "Dynamic lookup M1 requires dimension update pairs before checkpoint barrier");
        }
        if (barrier.prepareClose(runningTask.getTaskLocation())) {
            prepareClose = true;
        }
        if (barrier.snapshot()) {
            runningTask.addState(
                    barrier,
                    ActionStateKey.of(action),
                    Collections.singletonList(snapshotDimensionState()));
            if (!factGateOpened && pendingFactGateOpenCheckpointId < 0) {
                pendingFactGateOpenCheckpointId = barrier.getId();
            }
        }
        runningTask.ack(barrier);
        collector.collect(new Record<>(barrier));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!factGateOpened && pendingFactGateOpenCheckpointId == checkpointId) {
            openFactGate();
            pendingFactGateOpenCheckpointId = -1L;
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        if (pendingFactGateOpenCheckpointId == checkpointId) {
            pendingFactGateOpenCheckpointId = -1L;
        }
    }

    private void openFactGate() throws InterruptedException {
        if (factGateOpened) {
            return;
        }
        boolean offered = factGateCommandQueue.offer(SourceGateCommand.OPEN, 10, TimeUnit.SECONDS);
        if (!offered) {
            throw new TaskRuntimeException("Dynamic lookup fact gate command queue is full");
        }
        factGateOpened = true;
    }

    private byte[] snapshotDimensionState() throws IOException {
        ByteArrayOutputStream payloadStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(payloadStream)) {
            outputStream.writeInt(dimensionState.size());
            for (Map.Entry<LookupKey, SeaTunnelRow> entry : dimensionState.entrySet()) {
                outputStream.writeObject(entry.getKey());
                outputStream.writeObject(entry.getValue());
            }
        }
        byte[] payload = payloadStream.toByteArray();
        byte[] digest = sha256(payload);
        return ByteBuffer.allocate(Integer.BYTES * 3 + payload.length + digest.length)
                .putInt(DIMENSION_STATE_MAGIC)
                .putInt(DIMENSION_STATE_VERSION)
                .putInt(payload.length)
                .put(payload)
                .put(digest)
                .array();
    }

    private void restoreDimensionState(byte[] stateBytes)
            throws IOException, ClassNotFoundException {
        if (stateBytes == null || stateBytes.length == 0) {
            return;
        }
        if (stateBytes.length < Integer.BYTES * 3 + DIMENSION_STATE_DIGEST_LENGTH) {
            throw new IOException("Dynamic lookup dimension state envelope is truncated");
        }
        ByteBuffer envelope = ByteBuffer.wrap(stateBytes);
        int magic = envelope.getInt();
        if (magic != DIMENSION_STATE_MAGIC) {
            throw new IOException("Invalid dynamic lookup dimension state magic: " + magic);
        }
        int version = envelope.getInt();
        if (version != DIMENSION_STATE_VERSION) {
            throw new IOException("Unsupported dynamic lookup dimension state version: " + version);
        }
        int payloadLength = envelope.getInt();
        if (payloadLength < 0
                || envelope.remaining() != payloadLength + DIMENSION_STATE_DIGEST_LENGTH) {
            throw new IOException("Invalid dynamic lookup dimension state payload length");
        }
        byte[] payload = new byte[payloadLength];
        envelope.get(payload);
        byte[] expectedDigest = new byte[DIMENSION_STATE_DIGEST_LENGTH];
        envelope.get(expectedDigest);
        if (!Arrays.equals(expectedDigest, sha256(payload))) {
            throw new IOException("Dynamic lookup dimension state SHA-256 digest mismatch");
        }
        try (ObjectInputStream inputStream =
                new ObjectInputStream(new ByteArrayInputStream(payload))) {
            int entryCount = inputStream.readInt();
            if (entryCount < 0) {
                throw new IOException(
                        "Invalid dynamic lookup dimension state count: " + entryCount);
            }
            for (int i = 0; i < entryCount; i++) {
                LookupKey key = (LookupKey) inputStream.readObject();
                SeaTunnelRow row = (SeaTunnelRow) inputStream.readObject();
                dimensionState.put(key, row);
            }
        }
    }

    private static byte[] sha256(byte[] payload) throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is required by the Java runtime", e);
        }
    }

    public static boolean isDimensionStateEnvelope(byte[] stateBytes) {
        if (stateBytes == null || stateBytes.length < Integer.BYTES) {
            return false;
        }
        return ByteBuffer.wrap(stateBytes).getInt() == DIMENSION_STATE_MAGIC;
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
