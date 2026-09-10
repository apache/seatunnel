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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class SchemaRefreshBarrierOperatorTest {

    @Test
    void shouldRefreshRecoveredWriterBeforeFirstMatchingDataRow() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        OperatorTestContext initial = createOperator(stateStore, false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent();

        initial.operator.processElement2(new StreamRecord<>(createSchemaControlRow(event)));
        initial.operator.processElement1(new StreamRecord<>(createDataRow()));

        assertEquals(2, initial.output.records.size());
        assertRefresh(initial.output.records.get(0), event);
        assertSame(
                event,
                initial.output.records.get(0).getValue().getOptions().get("schema_change_refresh"));
        assertEquals(3, initial.output.records.get(1).getValue().getField(0));

        initial.operator.snapshotState(snapshotContext(10L));

        OperatorTestContext restored = createOperator(stateStore, true);
        restored.operator.open();

        assertEquals(1, restored.output.records.size());
        assertRefresh(restored.output.records.get(0), event);

        restored.output.records.clear();
        restored.operator.processElement1(new StreamRecord<>(createDataRow()));

        assertEquals(1, restored.output.records.size());
        assertEquals(3, restored.output.records.get(0).getValue().getField(0));
        restored.operator.close();
    }

    @Test
    void shouldNotRefreshAgainWithoutRecoveryOrNewerSchema() throws Exception {
        OperatorTestContext context = createOperator(new OperatorStateStoreStub(), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent();

        context.operator.processElement2(new StreamRecord<>(createSchemaControlRow(event)));
        context.output.records.clear();

        context.operator.processElement1(new StreamRecord<>(createDataRow()));
        context.operator.processElement1(new StreamRecord<>(createDataRow()));

        assertEquals(2, context.output.records.size());
        assertFalse(
                context.output.records.stream()
                        .anyMatch(
                                record ->
                                        record.getValue().getOptions() != null
                                                && record.getValue()
                                                        .getOptions()
                                                        .containsKey("schema_change_refresh")));
    }

    private static void assertRefresh(
            StreamRecord<SeaTunnelRow> record, AlterTableAddColumnEvent event) {
        Map<String, Object> options = record.getValue().getOptions();
        assertSame(event, options.get("schema_change_refresh"));
        assertEquals(event.getCreatedTime(), options.get("schema_epoch"));
        assertEquals(2, options.size());
    }

    private static OperatorTestContext createOperator(
            OperatorStateStoreStub stateStore, boolean restored) throws Exception {
        SchemaRefreshBarrierOperator operator = new SchemaRefreshBarrierOperator();
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
        return new OperatorTestContext(operator, output);
    }

    private static StreamingRuntimeContext runtimeContext() {
        StreamingRuntimeContext runtimeContext = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(runtimeContext.getJobId()).thenReturn(new JobID());
        Mockito.when(runtimeContext.getIndexOfThisSubtask()).thenReturn(0);
        Mockito.when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(2);
        return runtimeContext;
    }

    private static StateSnapshotContext snapshotContext(long checkpointId) {
        StateSnapshotContext context = Mockito.mock(StateSnapshotContext.class);
        Mockito.when(context.getCheckpointId()).thenReturn(checkpointId);
        return context;
    }

    private static AlterTableAddColumnEvent createSchemaChangeEvent() {
        TableIdentifier tableIdentifier = TableIdentifier.of("catalog", "database", "table");
        PhysicalColumn id = PhysicalColumn.of("id", BasicType.INT_TYPE, 4L, false, null, null);
        PhysicalColumn email =
                PhysicalColumn.of("email", BasicType.STRING_TYPE, 128L, true, null, null);
        AlterTableAddColumnEvent event = AlterTableAddColumnEvent.add(tableIdentifier, email);
        event.setChangeAfter(
                CatalogTable.of(
                        tableIdentifier,
                        TableSchema.builder().column(id).column(email).build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
        event.setJobId("schema-refresh-recovery-test");
        return event;
    }

    private static SeaTunnelRow createSchemaControlRow(AlterTableAddColumnEvent event) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("schema_change_broadcast", event);
        row.setOptions(options);
        return row;
    }

    private static SeaTunnelRow createDataRow() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {3, "after-recovery@example.com"});
        row.setTableId("catalog.database.table");
        return row;
    }

    private static void setField(Object target, Class<?> owner, String name, Object value)
            throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class OperatorTestContext {
        private final SchemaRefreshBarrierOperator operator;
        private final CollectingOutput output;

        private OperatorTestContext(
                SchemaRefreshBarrierOperator operator, CollectingOutput output) {
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
            throw new UnsupportedOperationException();
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
