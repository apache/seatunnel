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
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupSideSpec;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.DynamicLookupStateEnvelope;
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;
import org.apache.seatunnel.engine.server.task.group.AbstractTaskGroupWithIntermediateQueue;
import org.apache.seatunnel.engine.server.task.group.queue.AbstractIntermediateQueue;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Covers dynamic lookup dimension mutation, projection, and state-budget admission. */
public class DynamicLookupFlowLifeCycleDataPlaneTest {

    @Test
    void projectFactShouldUseLatestDimensionRow() throws Exception {
        Fixture flow =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        flow.dimension.add(new Record<>(dimensionRow(1, "Alice")));

        flow.fact.add(new Record<>(factRow(1, "order-1")));
        flow.collect();
        SeaTunnelRow output = (SeaTunnelRow) flow.output.get(0).getData();

        Assertions.assertEquals("order-1", output.getField(0));
        Assertions.assertEquals("Alice", output.getField(1));
    }

    @Test
    void innerJoinMissShouldDropFactRow() throws Exception {
        Fixture flow =
                newFlow(DynamicLookupDescriptor.JoinType.INNER, Long.MAX_VALUE, Long.MAX_VALUE);

        flow.fact.add(new Record<>(factRow(404, "missing")));
        flow.collect();
        Assertions.assertEquals(Collections.emptyList(), flow.output);
    }

    @Test
    void dimensionMutationShouldFailFastWhenResidentBudgetIsExceeded() throws Exception {
        Fixture flow = newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, 1L);

        flow.dimension.add(new Record<>(dimensionRow(1, "oversized")));
        TaskRuntimeException exception =
                Assertions.assertThrows(TaskRuntimeException.class, flow::collect);

        Assertions.assertTrue(exception.getMessage().contains("resident budget"));
    }

    @Test
    void snapshotShouldRestoreSupportedValuesThroughTheProductionAllowlist() throws Exception {
        Fixture original =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        Object[] values = {
            new java.math.BigDecimal("123.45"),
            java.time.LocalDate.of(2026, 1, 2),
            java.time.LocalTime.of(12, 30),
            java.time.LocalDateTime.of(2026, 1, 2, 12, 30),
            new byte[] {1, 2, 3},
            new Integer[][] {{1, 2}, {3}},
            new SeaTunnelRow(new Object[] {"nested", 9L}),
            Collections.singletonMap("map", "value")
        };
        for (int i = 0; i < values.length; i++) {
            original.dimension.add(new Record<>(new SeaTunnelRow(new Object[] {i, values[i]})));
        }
        original.collect();
        Barrier barrier = Mockito.mock(Barrier.class);
        Mockito.when(barrier.getId()).thenReturn(1L);
        Mockito.when(barrier.snapshot()).thenReturn(true);
        original.dimension.add(new Record<>(barrier));
        original.fact.add(new Record<>(barrier));
        original.collect();
        org.mockito.ArgumentCaptor<List<byte[]>> state =
                org.mockito.ArgumentCaptor.forClass(List.class);
        Mockito.verify(original.task)
                .addState(Mockito.eq(barrier), Mockito.any(ActionStateKey.class), state.capture());

        Fixture restored =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        restored.flow.restoreState(
                Collections.singletonList(
                        new ActionSubtaskState(
                                ActionStateKey.of(restored.action), 0, state.getValue())));
        for (int i = 0; i < values.length; i++) {
            restored.fact.add(new Record<>(factRow(i, "order-" + i)));
        }
        restored.collect();
        Assertions.assertEquals(values.length, restored.output.size());
        for (int i = 0; i < values.length; i++) {
            Assertions.assertArrayEquals(
                    new Object[] {"order-" + i, values[i]},
                    ((SeaTunnelRow) restored.output.get(i).getData()).getFields());
        }
    }

    @Test
    void restoreShouldRejectUnlistedJdkClassEvenWithValidDigest() throws Exception {
        Fixture fixture =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(0);
        try (ObjectOutputStream output = new ObjectOutputStream(payload)) {
            output.writeInt(1);
            output.writeObject(new java.util.PriorityQueue<>());
        }
        byte[] bytes = payload.toByteArray();
        byte[] envelope =
                ByteBuffer.allocate(12 + bytes.length + 32)
                        .putInt(DynamicLookupStateEnvelope.MAGIC)
                        .putInt(DynamicLookupStateEnvelope.VERSION)
                        .putInt(bytes.length)
                        .put(bytes)
                        .put(MessageDigest.getInstance("SHA-256").digest(bytes))
                        .array();
        IOException error =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                fixture.flow.restoreState(
                                        Collections.singletonList(
                                                new ActionSubtaskState(
                                                        ActionStateKey.of(fixture.action),
                                                        0,
                                                        Collections.singletonList(envelope)))));
        Assertions.assertEquals(
                "Rejected dynamic lookup dimension state class: java.util.PriorityQueue",
                error.getMessage());
    }

    @Test
    void snapshotShouldRejectShrinkingSerializationBeforeAcknowledgingAnchor() throws Exception {
        Fixture fixture =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        ShrinkingValue value = new ShrinkingValue();
        fixture.dimension.add(new Record<>(new SeaTunnelRow(new Object[] {1, value})));
        fixture.collect();
        // Admission serialized this value once. Reset so the snapshot's counting pass is larger.
        value.writes = 0;
        fixture.dimension.add(new Record<>(DynamicLookupBootstrapReader.Status.COMPLETE));
        Barrier barrier = Mockito.mock(Barrier.class);
        Mockito.when(barrier.getId()).thenReturn(31L);
        Mockito.when(barrier.snapshot()).thenReturn(true);
        fixture.dimension.add(new Record<>(barrier));
        fixture.fact.add(new Record<>(barrier));

        IOException error = Assertions.assertThrows(IOException.class, fixture::collect);

        Assertions.assertEquals(
                "Dynamic lookup snapshot payload length changed between passes",
                error.getMessage());
        Mockito.verify(fixture.task, Mockito.never())
                .addState(Mockito.any(), Mockito.any(ActionStateKey.class), Mockito.anyList());
        Mockito.verify(fixture.task, Mockito.never()).ack(barrier);
        fixture.flow.notifyCheckpointComplete(31L);
        Assertions.assertTrue(fixture.gate.isEmpty());
        Assertions.assertTrue(fixture.output.isEmpty());
    }

    @Test
    void partialBootstrapCheckpointMustKeepFactGateClosedAcrossRestore() throws Exception {
        Fixture original =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        List<byte[]> partial = snapshot(original, 21L);
        original.flow.notifyCheckpointComplete(21L);
        Assertions.assertEquals(Collections.emptyList(), new ArrayList<>(original.gate));
        Assertions.assertFalse(DynamicLookupStateEnvelope.hasCompletedBootstrap(partial.get(0)));

        Fixture restored =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        restored.flow.restoreState(
                Collections.singletonList(
                        new ActionSubtaskState(ActionStateKey.of(restored.action), 0, partial)));
        restored.flow.open();
        Assertions.assertEquals(Collections.emptyList(), new ArrayList<>(restored.gate));

        restored.dimension.add(new Record<>(DynamicLookupBootstrapReader.Status.COMPLETE));
        snapshot(restored, 22L);
        restored.flow.notifyCheckpointAborted(22L);
        Assertions.assertTrue(restored.gate.isEmpty());
        List<byte[]> ready = snapshot(restored, 23L);
        restored.flow.notifyCheckpointComplete(23L);
        Assertions.assertEquals(
                Collections.singletonList(SourceGateCommand.OPEN), new ArrayList<>(restored.gate));
        Assertions.assertTrue(DynamicLookupStateEnvelope.hasCompletedBootstrap(ready.get(0)));

        Fixture durable =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        durable.flow.restoreState(
                Collections.singletonList(
                        new ActionSubtaskState(ActionStateKey.of(durable.action), 0, ready)));
        durable.flow.open();
        Assertions.assertEquals(
                Collections.singletonList(SourceGateCommand.OPEN), new ArrayList<>(durable.gate));
    }

    private static List<byte[]> snapshot(Fixture fixture, long checkpointId) throws Exception {
        Barrier barrier = Mockito.mock(Barrier.class);
        Mockito.when(barrier.getId()).thenReturn(checkpointId);
        Mockito.when(barrier.snapshot()).thenReturn(true);
        fixture.dimension.add(new Record<>(barrier));
        fixture.fact.add(new Record<>(barrier));
        fixture.collect();
        org.mockito.ArgumentCaptor<List<byte[]>> state =
                org.mockito.ArgumentCaptor.forClass(List.class);
        Mockito.verify(fixture.task)
                .addState(Mockito.eq(barrier), Mockito.any(ActionStateKey.class), state.capture());
        return state.getValue();
    }

    private static Fixture newFlow(
            DynamicLookupDescriptor.JoinType joinType,
            long maxLogicalStateBytes,
            long maxResidentStateBytes)
            throws Exception {
        DynamicLookupAction action = Mockito.mock(DynamicLookupAction.class);
        Mockito.when(action.getDescriptor()).thenReturn(descriptor(joinType));
        Mockito.when(action.getName()).thenReturn("lookup-test");
        Mockito.when(action.getOperatorUid()).thenReturn("lookup-test");
        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(runningTask.getTaskLocation()).thenReturn(Mockito.mock(TaskLocation.class));
        DynamicLookupConfig config = Mockito.mock(DynamicLookupConfig.class);
        Mockito.when(config.getMaxLogicalStateBytesPerSubtask()).thenReturn(maxLogicalStateBytes);
        Mockito.when(config.getMaxResidentStateBytesPerSubtask()).thenReturn(maxResidentStateBytes);
        AbstractTaskGroupWithIntermediateQueue group =
                Mockito.mock(AbstractTaskGroupWithIntermediateQueue.class);
        Mockito.when(runningTask.getTaskGroup()).thenReturn(group);
        BlockingQueue<Record<?>> fact = new LinkedBlockingQueue<>();
        BlockingQueue<Record<?>> dimension = new LinkedBlockingQueue<>();
        Map<Long, BlockingQueue<?>> queues = new HashMap<>();
        queues.put(0L, fact);
        queues.put(1L, dimension);
        BlockingQueue<SourceGateCommand> gate = new LinkedBlockingQueue<>();
        queues.put(2L, gate);
        Mockito.when(config.getInputQueue(0)).thenReturn(new IntermediateQueueConfig(0L, 10));
        Mockito.when(config.getInputQueue(1)).thenReturn(new IntermediateQueueConfig(1L, 10));
        Mockito.when(config.getFactGateCommandQueue())
                .thenReturn(new IntermediateQueueConfig(2L, 10));
        Mockito.when(group.getQueueCache(Mockito.anyLong(), Mockito.anyInt(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            AbstractIntermediateQueue<?> queue =
                                    Mockito.mock(AbstractIntermediateQueue.class);
                            Mockito.doReturn(queues.get(invocation.getArgument(0)))
                                    .when(queue)
                                    .getIntermediateQueue();
                            return queue;
                        });
        DynamicLookupFlowLifeCycle flow =
                new DynamicLookupFlowLifeCycle(
                        action, runningTask, config, new CompletableFuture<>());
        flow.open();
        return new Fixture(flow, action, runningTask, fact, dimension, gate);
    }

    private static DynamicLookupDescriptor descriptor(DynamicLookupDescriptor.JoinType joinType) {
        return new DynamicLookupDescriptor(
                "lookup_out",
                new DynamicLookupSideSpec(
                        "fact", "catalog.db.fact", Arrays.asList("id"), Arrays.asList(0)),
                new DynamicLookupSideSpec(
                        "dimension", "catalog.db.dimension", Arrays.asList("id"), Arrays.asList(0)),
                joinType,
                Arrays.asList(
                        new DynamicLookupProjectionField(
                                DynamicLookupProjectionField.InputSide.FACT, "order", 1, "order"),
                        new DynamicLookupProjectionField(
                                DynamicLookupProjectionField.InputSide.DIMENSION,
                                "name",
                                1,
                                "name")));
    }

    private static SeaTunnelRow factRow(int key, String order) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {key, order});
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static SeaTunnelRow dimensionRow(int key, String name) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {key, name});
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    /**
     * Emulates a serializer whose payload shrinks between the counting and writing passes.
     *
     * <p>The counter is reset after admission to isolate the checkpoint's two serialization passes.
     */
    private static final class ShrinkingValue implements Serializable {
        private static final long serialVersionUID = 1L;
        private transient int writes;

        private void writeObject(ObjectOutputStream output) throws IOException {
            output.write(new byte[writes++ == 0 ? 64 : 0]);
        }
    }

    /** Drives the public lifecycle with real input queues and captured output records. */
    private static final class Fixture {
        private final DynamicLookupFlowLifeCycle flow;
        private final DynamicLookupAction action;
        private final SeaTunnelTask task;
        private final BlockingQueue<Record<?>> fact;
        private final BlockingQueue<Record<?>> dimension;
        private final BlockingQueue<SourceGateCommand> gate;
        private final List<Record<?>> output = new ArrayList<>();

        private Fixture(
                DynamicLookupFlowLifeCycle flow,
                DynamicLookupAction action,
                SeaTunnelTask task,
                BlockingQueue<Record<?>> fact,
                BlockingQueue<Record<?>> dimension,
                BlockingQueue<SourceGateCommand> gate) {
            this.flow = flow;
            this.action = action;
            this.task = task;
            this.fact = fact;
            this.dimension = dimension;
            this.gate = gate;
        }

        private void collect() throws Exception {
            flow.collect(
                    new Collector<Record<?>>() {
                        @Override
                        public void collect(Record<?> record) {
                            output.add(record);
                        }

                        @Override
                        public void close() {}
                    });
        }
    }
}
