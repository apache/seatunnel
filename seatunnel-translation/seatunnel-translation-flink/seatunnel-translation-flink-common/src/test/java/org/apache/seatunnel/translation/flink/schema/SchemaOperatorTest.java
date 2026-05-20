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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaCoordinationException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.coordinator.LocalSchemaCoordinator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaOperatorTest {

    @Test
    void testWaitRoundBeforeReleasingBufferedRecords() throws Exception {
        LocalSchemaCoordinator coordinator = Mockito.mock(LocalSchemaCoordinator.class);
        Mockito.when(
                        coordinator.requestSchemaChange(
                                Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
                .thenReturn(true);

        OperatorTestContext context = createOperator(false);
        setField(context.operator, "coordinator", coordinator);

        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        SeaTunnelRow row = createDataRow("row-after-schema");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 100L));
        context.operator.processElement(new StreamRecord<>(row, 101L));

        context.operator.notifyCheckpointComplete(10L);

        assertTrue(context.output.records.isEmpty());
        assertEquals(10L, getLongField(context.operator, "firstSeenCheckpointId"));
        assertEquals(2, getPendingQueue(context.operator).size());
        Mockito.verifyNoInteractions(coordinator);

        context.operator.notifyCheckpointComplete(11L);

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), event);
        assertEquals(row, context.output.records.get(1).getValue());
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertEquals(-1L, getLongField(context.operator, "firstSeenCheckpointId"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
        Mockito.verify(coordinator)
                .requestSchemaChange(event.tableIdentifier(), event.getCreatedTime(), 300_000L);
    }

    @Test
    void testSnapshotRestoreWithPendingSchemaChange() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        OperatorTestContext originalContext = createOperator(stateStore, false);

        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        SeaTunnelRow row = createDataRow("buffered-across-restore");

        originalContext.operator.processElement(new StreamRecord<>(createSchemaRow(event), 200L));
        originalContext.operator.processElement(new StreamRecord<>(row, 201L));
        originalContext.operator.notifyCheckpointComplete(20L);
        originalContext.operator.snapshotState(snapshotContext(20L));

        LocalSchemaCoordinator restoredCoordinator = Mockito.mock(LocalSchemaCoordinator.class);
        Mockito.when(
                        restoredCoordinator.requestSchemaChange(
                                Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
                .thenReturn(true);

        OperatorTestContext restoredContext = createOperator(stateStore, true);
        setField(restoredContext.operator, "coordinator", restoredCoordinator);

        assertEquals(20L, getLongField(restoredContext.operator, "firstSeenCheckpointId"));
        assertEquals(2, getPendingQueue(restoredContext.operator).size());
        assertTrue(getBooleanField(restoredContext.operator, "schemaChangePending"));

        restoredContext.operator.notifyCheckpointComplete(21L);

        assertEquals(2, restoredContext.output.records.size());
        assertSchemaBroadcast(restoredContext.output.records.get(0), event);
        assertEquals(row, restoredContext.output.records.get(1).getValue());
        assertTrue(getPendingQueue(restoredContext.operator).isEmpty());
        assertFalse(getBooleanField(restoredContext.operator, "schemaChangePending"));
        Mockito.verify(restoredCoordinator)
                .requestSchemaChange(event.tableIdentifier(), event.getCreatedTime(), 300_000L);
    }

    @Test
    void testCoordinationFailureKeepsBufferedRecordsBlocked() throws Exception {
        LocalSchemaCoordinator coordinator = Mockito.mock(LocalSchemaCoordinator.class);
        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        Mockito.when(
                        coordinator.requestSchemaChange(
                                Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
                .thenThrow(
                        SchemaCoordinationException.timeout(
                                event.tableIdentifier(),
                                "job-under-test",
                                1L,
                                new RuntimeException("timeout")));

        OperatorTestContext context = createOperator(false);
        setField(context.operator, "coordinator", coordinator);

        SeaTunnelRow row = createDataRow("must-stay-buffered");
        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 300L));
        context.operator.processElement(new StreamRecord<>(row, 301L));
        context.operator.notifyCheckpointComplete(30L);

        assertThrows(
                SchemaEvolutionException.class,
                () -> context.operator.notifyCheckpointComplete(31L));

        assertEquals(1, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), event);
        assertTrue(getBooleanField(context.operator, "schemaChangePending"));
        assertEquals(30L, getLongField(context.operator, "firstSeenCheckpointId"));
        Queue<SchemaOperator.BufferedRecord> pendingQueue = getPendingQueue(context.operator);
        assertEquals(2, pendingQueue.size());
        assertTrue(pendingQueue.peek().isSchemaChange);
    }

    private static OperatorTestContext createOperator(boolean restored) throws Exception {
        return createOperator(new OperatorStateStoreStub(), restored);
    }

    private static OperatorTestContext createOperator(
            OperatorStateStoreStub stateStore, boolean restored) throws Exception {
        SupportSchemaEvolution source = Mockito.mock(SupportSchemaEvolution.class);
        Mockito.when(source.supports())
                .thenReturn(Collections.singletonList(SchemaChangeType.ADD_COLUMN));

        SchemaOperator operator =
                new SchemaOperator(
                        "bootstrap-job-id",
                        source,
                        ConfigFactory.parseString("schema-changes.enabled = true"));

        CollectingOutput output = new CollectingOutput();
        setField(operator, AbstractStreamOperator.class, "output", output);
        setField(operator, AbstractStreamOperator.class, "runtimeContext", runtimeContext());
        setField(
                operator,
                AbstractStreamOperator.class,
                "stateHandler",
                Mockito.mock(StreamOperatorStateHandler.class));

        StateInitializationContext initializationContext =
                Mockito.mock(StateInitializationContext.class);
        Mockito.when(initializationContext.getOperatorStateStore()).thenReturn(stateStore);
        Mockito.when(initializationContext.isRestored()).thenReturn(restored);

        operator.initializeState(initializationContext);
        operator.open();
        return new OperatorTestContext(operator, output);
    }

    private static StreamingRuntimeContext runtimeContext() {
        StreamingRuntimeContext runtimeContext = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(runtimeContext.getJobId()).thenReturn(new JobID());
        return runtimeContext;
    }

    private static StateSnapshotContext snapshotContext(long checkpointId) throws Exception {
        StateSnapshotContext snapshotContext = Mockito.mock(StateSnapshotContext.class);
        Mockito.when(snapshotContext.getCheckpointId()).thenReturn(checkpointId);
        return snapshotContext;
    }

    private static AlterTableAddColumnEvent createSchemaChangeEvent() {
        return AlterTableAddColumnEvent.add(
                TableIdentifier.of("catalog", "database", "table"),
                PhysicalColumn.of("added_col", BasicType.STRING_TYPE, 64L, true, null, null));
    }

    private static SeaTunnelRow createSchemaRow(AlterTableAddColumnEvent event) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        row.setTableId("__SCHEMA_CHANGE_EVENT__");
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("schema_change_event", event);
        row.setOptions(options);
        return row;
    }

    private static SeaTunnelRow createDataRow(String value) {
        SeaTunnelRow row = new SeaTunnelRow(1);
        row.setTableId("catalog.database.table");
        row.setField(0, value);
        return row;
    }

    private static void assertSchemaBroadcast(
            StreamRecord<SeaTunnelRow> record, AlterTableAddColumnEvent event) {
        Object broadcastEvent = record.getValue().getOptions().get("schema_change_broadcast");
        assertInstanceOf(AlterTableAddColumnEvent.class, broadcastEvent);
        assertEquals(event, broadcastEvent);
    }

    @SuppressWarnings("unchecked")
    private static Queue<SchemaOperator.BufferedRecord> getPendingQueue(SchemaOperator operator)
            throws Exception {
        return (Queue<SchemaOperator.BufferedRecord>) getField(operator, "pendingQueue");
    }

    private static boolean getBooleanField(Object target, String fieldName) throws Exception {
        return (boolean) getField(target, fieldName);
    }

    private static long getLongField(Object target, String fieldName) throws Exception {
        return (long) getField(target, fieldName);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void setField(Object target, Class<?> owner, String fieldName, Object value)
            throws Exception {
        Field field = owner.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static final class OperatorTestContext {
        private final SchemaOperator operator;
        private final CollectingOutput output;

        private OperatorTestContext(SchemaOperator operator, CollectingOutput output) {
            this.operator = operator;
            this.output = output;
        }
    }

    private static final class CollectingOutput implements Output<StreamRecord<SeaTunnelRow>> {
        private final List<StreamRecord<SeaTunnelRow>> records = new ArrayList<>();

        @Override
        public void collect(StreamRecord<SeaTunnelRow> record) {
            records.add(record);
        }

        @Override
        public void close() {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}
    }

    private static final class OperatorStateStoreStub implements OperatorStateStore {
        private final Map<String, TestingListState<?>> listStates = new LinkedHashMap<>();

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) {
            throw new UnsupportedOperationException("Broadcast state is not needed in this test");
        }

        @SuppressWarnings("unchecked")
        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
            return (ListState<S>)
                    listStates.computeIfAbsent(
                            stateDescriptor.getName(), ignored -> new TestingListState<>());
        }

        @Override
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) {
            return getListState(stateDescriptor);
        }

        @Override
        public Set<String> getRegisteredStateNames() {
            return listStates.keySet();
        }

        @Override
        public Set<String> getRegisteredBroadcastStateNames() {
            return Collections.emptySet();
        }
    }

    private static final class TestingListState<T> implements ListState<T> {
        private final List<T> values = new ArrayList<>();

        @Override
        public Iterable<T> get() {
            return new ArrayList<>(values);
        }

        @Override
        public void add(T value) {
            values.add(value);
        }

        @Override
        public void update(List<T> values) {
            this.values.clear();
            if (values != null) {
                this.values.addAll(values);
            }
        }

        @Override
        public void addAll(List<T> values) {
            if (values != null) {
                this.values.addAll(values);
            }
        }

        @Override
        public void clear() {
            values.clear();
        }
    }
}
