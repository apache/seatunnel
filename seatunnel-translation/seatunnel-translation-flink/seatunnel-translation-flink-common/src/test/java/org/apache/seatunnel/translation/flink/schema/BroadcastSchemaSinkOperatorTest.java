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
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BroadcastSchemaSinkOperatorTest {
    private static final TableIdentifier TABLE_A = TableIdentifier.of("catalog", "db", "a");
    private static final TableIdentifier TABLE_B = TableIdentifier.of("catalog", "db", "b");

    @Test
    void testSchemaPrecedesRowsAndDuplicateDoesNotRepeatDdl() throws Exception {
        Context context = new Context();
        SeaTunnelRow control = control(TABLE_A, 1);
        context.process(TABLE_A, control);
        context.process(TABLE_A, control);
        SeaTunnelRow row = row(TABLE_A, 1);
        context.process(TABLE_A, row);
        assertEquals(2, context.output.size());
        assertSame(
                control.getOptions().get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_BROADCAST),
                schema(context.output.get(0)));
        assertSame(row, context.output.get(1).getValue());
        assertNull(SchemaEvolutionControlMessage.requiredSchemaChangeId(row));
    }

    @Test
    void testRestoreDoesNotBorrowAnotherTablesProgress() throws Exception {
        Context first = new Context();
        first.process(TABLE_A, control(TABLE_A, 1));
        first.process(TABLE_B, row(TABLE_B, 1));
        Context restored = first.restore();
        restored.process(TABLE_B, row(TABLE_B, 1));
        assertTrue(restored.output.isEmpty());
        restored.process(TABLE_B, control(TABLE_A, 1));
        assertEquals(2, restored.output.size());
        assertNull(schema(restored.output.get(0)));
        restored.process(TABLE_A, row(TABLE_A, 1));
        assertTrue(schema(restored.output.get(2)) instanceof RestoreTableSchemaEvent);
        assertEquals(4, restored.output.size());
    }

    @Test
    void testPendingControlsAndRowsRecoverTogether() throws Exception {
        Context first = new Context();
        first.process(TABLE_A, control(TABLE_A, 2));
        first.process(TABLE_A, row(TABLE_A, 2));
        Context restored = first.restore();
        assertTrue(restored.output.isEmpty());
        restored.process(TABLE_A, control(TABLE_A, 1));
        assertEquals(3, restored.output.size());
        assertTrue(schema(restored.output.get(0)) instanceof AlterTableAddColumnEvent);
        assertTrue(schema(restored.output.get(1)) instanceof AlterTableAddColumnEvent);
        assertNull(schema(restored.output.get(2)));
        assertEquals(0, restored.states.get(path(TABLE_A)).getPendingCount());
    }

    @Test
    void testReorderedControlsReleaseRowsBeforeTheNextSchema() throws Exception {
        Context context = new Context();
        context.process(TABLE_A, control(TABLE_A, 2));
        SeaTunnelRow data = row(TABLE_A, 1);
        context.process(TABLE_A, data);
        context.process(TABLE_A, control(TABLE_A, 1));
        assertEquals(3, context.output.size());
        assertTrue(schema(context.output.get(0)) instanceof AlterTableAddColumnEvent);
        assertSame(data, context.output.get(1).getValue());
        assertTrue(schema(context.output.get(2)) instanceof AlterTableAddColumnEvent);
    }

    @Test
    void testPendingRowsKeepOrderAndTimestamp() throws Exception {
        Context context = new Context();
        SeaTunnelRow first = row(TABLE_A, 2);
        SeaTunnelRow second = row(TABLE_A, 1);
        context.process(TABLE_A, first);
        context.process(TABLE_A, second);
        context.process(TABLE_A, control(TABLE_A, 1));
        assertEquals(1, context.output.size());
        context.process(TABLE_A, control(TABLE_A, 2));
        assertSame(first, context.output.get(2).getValue());
        assertSame(second, context.output.get(3).getValue());
        assertEquals(123L, context.output.get(2).getTimestamp());
    }

    @Test
    void testFilteredControlAdvancesProgressWithoutDdl() throws Exception {
        Context context = new Context();
        context.process(TABLE_A, row(TABLE_A, 1));
        SeaTunnelRow control = control(TABLE_A, 1);
        control.getOptions().put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_FILTERED, true);
        context.process(TABLE_A, control);
        assertEquals(1, context.output.size());
        assertNull(schema(context.output.get(0)));
    }

    @Test
    void testInvalidControlFailsWithoutAdvancingProgress() throws Exception {
        Context context = new Context();
        SeaTunnelRow control = control(TABLE_A, 1);
        control.getOptions().put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_ID, "invalid");
        assertThrows(IllegalArgumentException.class, () -> context.process(TABLE_A, control));
        assertTrue(context.output.isEmpty());
    }

    @Test
    void testTableRouterCopiesControlsAndPreservesData() throws Exception {
        SchemaEvolutionStreamUtils.SchemaTableRouter router =
                new SchemaEvolutionStreamUtils.SchemaTableRouter(tables());
        List<SeaTunnelRow> routed = new ArrayList<>();
        org.apache.flink.util.Collector<SeaTunnelRow> collector =
                mock(org.apache.flink.util.Collector.class);
        doAnswer(
                        call -> {
                            routed.add(call.getArgument(0));
                            return null;
                        })
                .when(collector)
                .collect(any());
        router.flatMap(control(TABLE_A, 1), collector);
        assertEquals(
                Arrays.asList(path(TABLE_A), path(TABLE_B)),
                Arrays.asList(routed.get(0).getTableId(), routed.get(1).getTableId()));
        routed.get(0).getOptions().clear();
        assertTrue(SchemaEvolutionControlMessage.isSchemaBroadcast(routed.get(1)));
        SeaTunnelRow data = row(TABLE_A, 1);
        router.flatMap(data, collector);
        assertSame(data, routed.get(2));
        data.setTableId("unknown");
        assertThrows(IllegalArgumentException.class, () -> router.flatMap(data, collector));
    }

    private static Object schema(StreamRecord<SeaTunnelRow> record) {
        return record.getValue().getOptions() == null
                ? null
                : record.getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT);
    }

    private static String path(TableIdentifier id) {
        return id.toTablePath().toString();
    }

    private static List<CatalogTable> tables() {
        return Arrays.asList(table(TABLE_A, false), table(TABLE_B, false));
    }

    private static CatalogTable table(TableIdentifier id, boolean added) {
        TableSchema.Builder builder =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, 11L, false, null, null));
        if (added) {
            builder.column(
                    PhysicalColumn.of("added", BasicType.STRING_TYPE, 64L, true, null, null));
        }
        return CatalogTable.of(
                id, builder.build(), Collections.emptyMap(), Collections.emptyList(), null);
    }

    private static SeaTunnelRow control(TableIdentifier id, long sequence) {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        id,
                        PhysicalColumn.of("added", BasicType.STRING_TYPE, 64L, true, null, null));
        event.setChangeAfter(table(id, true));
        SeaTunnelRow row = new SeaTunnelRow(0);
        row.setOptions(new HashMap<>());
        row.getOptions().put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_BROADCAST, event);
        row.getOptions()
                .put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_ID, "producer#" + sequence);
        return row;
    }

    private static SeaTunnelRow row(TableIdentifier id, long sequence) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(path(id));
        SchemaEvolutionControlMessage.requireSchemaChange(row, "producer#" + sequence);
        return row;
    }

    private static <T> T checkpointCopy(T value, Class<T> type) throws Exception {
        TypeSerializer<T> serializer =
                TypeInformation.of(type).createSerializer(new ExecutionConfig());
        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(value, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    @SuppressWarnings("unchecked")
    private static class Context {
        private String currentKey;
        private final Map<String, BroadcastSchemaSinkOperator.TableState> states = new HashMap<>();
        private final Map<String, List<BroadcastSchemaSinkOperator.PendingRow>> rows =
                new HashMap<>();
        private final List<StreamRecord<SeaTunnelRow>> output = new ArrayList<>();
        private final ValueState<BroadcastSchemaSinkOperator.TableState> tableState =
                mock(ValueState.class);
        private final ListState<BroadcastSchemaSinkOperator.PendingRow> pendingRows =
                mock(ListState.class);
        private final BroadcastSchemaSinkOperator operator =
                new BroadcastSchemaSinkOperator(tables()) {
                    @Override
                    public Object getCurrentKey() {
                        return currentKey;
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    protected <S extends State> S getPartitionedState(
                            StateDescriptor<S, ?> descriptor) {
                        if ("table-schema-progress".equals(descriptor.getName())) {
                            return (S) tableState;
                        }
                        if ("table-schema-pending-rows".equals(descriptor.getName())) {
                            return (S) pendingRows;
                        }
                        throw new IllegalArgumentException(descriptor.getName());
                    }
                };

        Context() throws Exception {
            when(tableState.value()).thenAnswer(call -> states.get(currentKey));
            doAnswer(
                            call -> {
                                states.put(currentKey, call.getArgument(0));
                                return null;
                            })
                    .when(tableState)
                    .update(any());
            when(pendingRows.get())
                    .thenAnswer(call -> rows.getOrDefault(currentKey, Collections.emptyList()));
            doAnswer(
                            call -> {
                                rows.computeIfAbsent(currentKey, key -> new ArrayList<>())
                                        .add(call.getArgument(0));
                                return null;
                            })
                    .when(pendingRows)
                    .add(any());
            doAnswer(
                            call -> {
                                rows.put(currentKey, new ArrayList<>(call.getArgument(0)));
                                return null;
                            })
                    .when(pendingRows)
                    .update(any());
            Output<StreamRecord<SeaTunnelRow>> collector = mock(Output.class);
            doAnswer(
                            call -> {
                                output.add(call.getArgument(0));
                                return null;
                            })
                    .when(collector)
                    .collect(any(StreamRecord.class));
            setField("output", collector);
            setField("stateHandler", mock(StreamOperatorStateHandler.class));
            operator.initializeState(mock(StateInitializationContext.class));
        }

        private void setField(String name, Object value) throws Exception {
            Field field = AbstractStreamOperator.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(operator, value);
        }

        void process(TableIdentifier table, SeaTunnelRow row) throws Exception {
            currentKey = path(table);
            operator.processElement(new StreamRecord<>(row, 123L));
        }

        Context restore() throws Exception {
            Context result = new Context();
            for (Map.Entry<String, BroadcastSchemaSinkOperator.TableState> entry :
                    states.entrySet()) {
                result.states.put(
                        entry.getKey(),
                        checkpointCopy(
                                entry.getValue(), BroadcastSchemaSinkOperator.TableState.class));
            }
            for (Map.Entry<String, List<BroadcastSchemaSinkOperator.PendingRow>> entry :
                    rows.entrySet()) {
                List<BroadcastSchemaSinkOperator.PendingRow> restored = new ArrayList<>();
                for (BroadcastSchemaSinkOperator.PendingRow row : entry.getValue()) {
                    restored.add(checkpointCopy(row, BroadcastSchemaSinkOperator.PendingRow.class));
                }
                result.rows.put(entry.getKey(), restored);
            }
            return result;
        }
    }
}
