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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BroadcastSchemaSinkOperatorTest {

    private static final int MAX_PARALLELISM = 128;
    private static final TableIdentifier DEFAULT_TABLE_ID =
            TableIdentifier.of("catalog", "database", "table");

    @Test
    void testForwardsSchemaChangeToSinkWriter() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(100L);

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

        assertEquals(1, context.output.records.size());
        Map<String, Object> options = context.output.records.get(0).getValue().getOptions();
        assertSame(event, options.get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertEquals(
                schemaChangeId(event), options.get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_ID));
        assertFalse(options.containsKey("schema_subtask_id"));
    }

    @Test
    void testParallelTableOwnerAppliesSchemaBeforeReleasingRows() throws Exception {
        OperatorTestContext context =
                createParallelOperator(ownerSubtask(DEFAULT_TABLE_ID, 4), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(110L);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, schemaChangeId(event));

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));
        context.operator.processElement(new StreamRecord<>(dataRow));

        assertEquals(2, context.output.records.size());
        assertSame(
                event,
                context.output
                        .records
                        .get(0)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(dataRow, context.output.records.get(1).getValue());
        assertEquals(1L, context.operator.getAppliedSequences().get("producer-1"));
        assertEquals(1, context.operator.getLatestSchemaEvents().size());
    }

    @Test
    void testParallelNonOwnerConsumesControlWithoutApplyingSchema() throws Exception {
        int owner = ownerSubtask(DEFAULT_TABLE_ID, 4);
        OperatorTestContext context = createParallelOperator((owner + 1) % 4, false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(120L);

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

        assertTrue(context.output.records.isEmpty());
        assertEquals(1L, context.operator.getAppliedSequences().get("producer-1"));
        assertTrue(context.operator.getLatestSchemaEvents().isEmpty());
    }

    @Test
    void testDuplicateSequenceIsForwardedOnlyOncePerExecutionAttempt() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(200L);

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));
        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

        assertEquals(1, context.output.records.size());
    }

    @Test
    void testOvertakingRowWaitsForSchemaCommandOnParallelTableOwner() throws Exception {
        OperatorTestContext context =
                createParallelOperator(ownerSubtask(DEFAULT_TABLE_ID, 4), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(300L);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, schemaChangeId(event));

        context.operator.processElement(new StreamRecord<>(dataRow, 123L));
        assertTrue(context.output.records.isEmpty());

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

        assertEquals(2, context.output.records.size());
        assertSame(
                event,
                context.output
                        .records
                        .get(0)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(dataRow, context.output.records.get(1).getValue());
        assertEquals(123L, context.output.records.get(1).getTimestamp());
        assertFalse(
                dataRow.getOptions() != null
                        && dataRow.getOptions()
                                .containsKey(
                                        SchemaEvolutionControlMessage.REQUIRED_SCHEMA_CHANGE_ID));
    }

    @Test
    void testRestoreRebuildsWriterSchemaBeforeReleasingRow() throws Exception {
        AlterTableAddColumnEvent event = createSchemaChangeEvent(400L);
        BroadcastSchemaSinkOperator.SchemaSequenceEntry restoredSequence =
                new BroadcastSchemaSinkOperator.SchemaSequenceEntry("producer-1", 400L);
        BroadcastSchemaSinkOperator.LatestSchemaEventEntry restoredEvent =
                new BroadcastSchemaSinkOperator.LatestSchemaEventEntry(
                        schemaChangeId(event, 400L), event);
        OperatorTestContext context =
                createOperator(
                        Collections.singletonList(restoredSequence),
                        Collections.singletonList(restoredEvent),
                        true);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, schemaChangeId(event, 400L));

        context.operator.processElement(new StreamRecord<>(dataRow));

        assertEquals(2, context.output.records.size());
        assertRestoresTargetSchema(context.output.records.get(0), event);
        assertSame(dataRow, context.output.records.get(1).getValue());
    }

    @Test
    void testParallelRestoreRunsOnlyOnCurrentTableOwnerBeforeRows() throws Exception {
        AlterTableAddColumnEvent event = createSchemaChangeEvent(410L);
        BroadcastSchemaSinkOperator.SchemaSequenceEntry restoredSequence =
                new BroadcastSchemaSinkOperator.SchemaSequenceEntry("producer-1", 1L);
        BroadcastSchemaSinkOperator.LatestSchemaEventEntry restoredEvent =
                new BroadcastSchemaSinkOperator.LatestSchemaEventEntry(
                        schemaChangeId(event), event);
        OperatorTestContext context =
                createParallelOperator(
                        ownerSubtask(DEFAULT_TABLE_ID, 4),
                        Collections.singletonList(restoredSequence),
                        Collections.singletonList(restoredEvent),
                        Collections.emptyList(),
                        true);
        SeaTunnelRow dataRow = createDataRow();

        context.operator.processElement(new StreamRecord<>(dataRow));

        assertEquals(2, context.output.records.size());
        assertRestoresTargetSchema(context.output.records.get(0), event);
        assertSame(dataRow, context.output.records.get(1).getValue());
    }

    @Test
    void testRescaledPendingRowIsRestoredOnlyByNewTableOwner() throws Exception {
        AlterTableAddColumnEvent event = createSchemaChangeEvent(415L);
        SeaTunnelRow pendingRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(pendingRow, schemaChangeId(event));
        BroadcastSchemaSinkOperator.PendingRowEntry restoredPendingRow =
                new BroadcastSchemaSinkOperator.PendingRowEntry(pendingRow, 123L, true);
        int parallelism = 3;
        int owner = ownerSubtask(DEFAULT_TABLE_ID, parallelism);

        for (int subtask = 0; subtask < parallelism; subtask++) {
            OperatorTestContext context =
                    createParallelOperator(
                            subtask,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.singletonList(restoredPendingRow),
                            true,
                            parallelism);

            context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

            if (subtask == owner) {
                assertEquals(2, context.output.records.size());
                assertSame(pendingRow, context.output.records.get(1).getValue());
                assertEquals(123L, context.output.records.get(1).getTimestamp());
            } else {
                assertTrue(context.output.records.isEmpty());
            }
        }
    }

    @Test
    void testCheckpointedPendingRowIsReassignedAfterRescale() throws Exception {
        AlterTableAddColumnEvent event = createSchemaChangeEvent(416L);
        SeaTunnelRow pendingRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(pendingRow, schemaChangeId(event));
        TestingOperatorStateStore checkpointState = new TestingOperatorStateStore();
        int oldParallelism = 5;
        OperatorTestContext oldOwner =
                createStateStoreOperator(
                        checkpointState,
                        ownerSubtask(DEFAULT_TABLE_ID, oldParallelism),
                        oldParallelism,
                        Collections.singletonList(createInitialTable(DEFAULT_TABLE_ID)),
                        false);

        oldOwner.operator.processElement(new StreamRecord<>(pendingRow, 123L));
        oldOwner.operator.snapshotState(checkpoint(1L));

        assertTrue(oldOwner.output.records.isEmpty());
        assertEquals(1, checkpointState.size("schema-gate-pending-rows"));

        int newParallelism = 3;
        int newOwner = ownerSubtask(DEFAULT_TABLE_ID, newParallelism);
        for (int subtask = 0; subtask < newParallelism; subtask++) {
            OperatorTestContext restored =
                    createStateStoreOperator(
                            checkpointState,
                            subtask,
                            newParallelism,
                            Collections.singletonList(createInitialTable(DEFAULT_TABLE_ID)),
                            true);

            restored.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

            if (subtask == newOwner) {
                assertEquals(2, restored.output.records.size());
                assertSame(pendingRow, restored.output.records.get(1).getValue());
                assertEquals(123L, restored.output.records.get(1).getTimestamp());
            } else {
                assertTrue(restored.output.records.isEmpty());
            }
        }
    }

    @Test
    void testAppliedSequenceCheckpointRestoresSchemaBeforeDependentRow() throws Exception {
        AlterTableAddColumnEvent event = createSchemaChangeEvent(417L);
        TestingOperatorStateStore checkpointState = new TestingOperatorStateStore();
        OperatorTestContext original =
                createStateStoreOperator(
                        checkpointState,
                        0,
                        1,
                        Collections.singletonList(createInitialTable(DEFAULT_TABLE_ID)),
                        false);

        // Treat this output as an in-flight command that has not reached the writer yet. The gate
        // has already advanced its sequence when the checkpoint snapshots its managed state.
        original.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));
        original.operator.snapshotState(checkpoint(1L));

        assertEquals(1, original.output.records.size());
        assertEquals(1, checkpointState.size("applied-schema-sequences"));
        assertEquals(1, checkpointState.size("latest-applied-schema-events"));

        OperatorTestContext restored =
                createStateStoreOperator(
                        checkpointState,
                        0,
                        1,
                        Collections.singletonList(createInitialTable(DEFAULT_TABLE_ID)),
                        true);
        SeaTunnelRow dependentRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dependentRow, schemaChangeId(event));

        restored.operator.processElement(new StreamRecord<>(dependentRow));

        assertEquals(2, restored.output.records.size());
        assertRestoresTargetSchema(restored.output.records.get(0), event);
        assertSame(dependentRow, restored.output.records.get(1).getValue());
    }

    @Test
    void testRestoredPendingControlRunsAfterConfirmedSchemaRestore() throws Exception {
        AlterTableAddColumnEvent confirmedEvent = createSchemaChangeEvent(420L);
        AlterTableAddColumnEvent pendingEvent =
                createSchemaChangeEvent(DEFAULT_TABLE_ID, "second_col", 421L);
        BroadcastSchemaSinkOperator.SchemaSequenceEntry restoredSequence =
                new BroadcastSchemaSinkOperator.SchemaSequenceEntry("producer-1", 1L);
        BroadcastSchemaSinkOperator.LatestSchemaEventEntry restoredEvent =
                new BroadcastSchemaSinkOperator.LatestSchemaEventEntry(
                        schemaChangeId(confirmedEvent), confirmedEvent);
        BroadcastSchemaSinkOperator.PendingSchemaEventEntry restoredPendingEvent =
                new BroadcastSchemaSinkOperator.PendingSchemaEventEntry(
                        schemaChangeId(pendingEvent, 2L), pendingEvent, true);
        OperatorTestContext context =
                createParallelOperator(
                        ownerSubtask(DEFAULT_TABLE_ID, 4),
                        Collections.singletonList(restoredSequence),
                        Collections.singletonList(restoredEvent),
                        Collections.singletonList(restoredPendingEvent),
                        true);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(
                dataRow, schemaChangeId(pendingEvent, 2L));

        context.operator.processElement(new StreamRecord<>(dataRow));
        assertEquals(3, context.output.records.size());
        assertRestoresTargetSchema(context.output.records.get(0), confirmedEvent);
        assertSame(
                pendingEvent,
                context.output
                        .records
                        .get(1)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(dataRow, context.output.records.get(2).getValue());
        assertEquals(2L, context.operator.getAppliedSequences().get("producer-1"));
    }

    @Test
    void testFilteredSchemaChangeReleasesDependentRowWithoutCallingSinkWriter() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(500L);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, schemaChangeId(event));

        context.operator.processElement(new StreamRecord<>(dataRow));
        context.operator.processElement(
                new StreamRecord<>(
                        SchemaEvolutionControlMessage.transformedSchemaChangeRow(
                                createBroadcastRow(event), null)));

        assertEquals(1, context.output.records.size());
        assertSame(dataRow, context.output.records.get(0).getValue());
        assertFalse(
                dataRow.getOptions() != null
                        && dataRow.getOptions()
                                .containsKey(
                                        SchemaEvolutionControlMessage.REQUIRED_SCHEMA_CHANGE_ID));
    }

    @Test
    void testRepeatedChangesKeepOnlyLatestSnapshotForTable() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        AlterTableAddColumnEvent firstEvent = createSchemaChangeEvent(500L);
        AlterTableAddColumnEvent secondEvent = createSchemaChangeEvent(501L);

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(firstEvent)));
        context.operator.processElement(new StreamRecord<>(createBroadcastRow(secondEvent, 2L)));

        assertEquals(2, context.output.records.size());
        assertEquals(1, context.operator.getLatestSchemaEvents().size());
        assertSame(
                secondEvent,
                context.operator.getLatestSchemaEvents().values().iterator().next().getEvent());
    }

    @Test
    void testOutOfOrderSchemaChangesWaitForContiguousSequence() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        AlterTableAddColumnEvent firstEvent = createSchemaChangeEvent(600L);
        AlterTableAddColumnEvent secondEvent = createSchemaChangeEvent(601L);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, schemaChangeId(secondEvent, 2L));

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(secondEvent, 2L)));
        context.operator.processElement(new StreamRecord<>(dataRow));

        assertTrue(context.output.records.isEmpty());
        assertEquals(1, context.operator.getPendingSchemaChangeCount());

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(firstEvent, 1L)));

        assertEquals(3, context.output.records.size());
        assertSame(
                firstEvent,
                context.output
                        .records
                        .get(0)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(
                secondEvent,
                context.output
                        .records
                        .get(1)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(dataRow, context.output.records.get(2).getValue());
        assertEquals(2L, context.operator.getAppliedSequences().get("producer-1"));
        assertEquals(0, context.operator.getPendingSchemaChangeCount());
    }

    @Test
    void testRestoreRetainsOutOfOrderSchemaChanges() throws Exception {
        AlterTableAddColumnEvent firstEvent = createSchemaChangeEvent(700L);
        AlterTableAddColumnEvent secondEvent = createSchemaChangeEvent(701L);
        AlterTableAddColumnEvent thirdEvent = createSchemaChangeEvent(702L);
        BroadcastSchemaSinkOperator.SchemaSequenceEntry restoredSequence =
                new BroadcastSchemaSinkOperator.SchemaSequenceEntry("producer-1", 1L);
        BroadcastSchemaSinkOperator.LatestSchemaEventEntry restoredEvent =
                new BroadcastSchemaSinkOperator.LatestSchemaEventEntry(
                        schemaChangeId(firstEvent, 1L), firstEvent);
        BroadcastSchemaSinkOperator.PendingSchemaEventEntry pendingEvent =
                new BroadcastSchemaSinkOperator.PendingSchemaEventEntry(
                        schemaChangeId(thirdEvent, 3L), thirdEvent, true);
        OperatorTestContext context =
                createOperator(
                        Collections.singletonList(restoredSequence),
                        Collections.singletonList(restoredEvent),
                        Collections.singletonList(pendingEvent),
                        true);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, schemaChangeId(thirdEvent, 3L));

        context.operator.processElement(new StreamRecord<>(dataRow));
        assertEquals(1, context.output.records.size());
        assertRestoresTargetSchema(context.output.records.get(0), firstEvent);

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(secondEvent, 2L)));

        assertEquals(4, context.output.records.size());
        assertSame(
                secondEvent,
                context.output
                        .records
                        .get(1)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(
                thirdEvent,
                context.output
                        .records
                        .get(2)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
        assertSame(dataRow, context.output.records.get(3).getValue());
        assertEquals(3L, context.operator.getAppliedSequences().get("producer-1"));
        assertEquals(0, context.operator.getPendingSchemaChangeCount());
    }

    @Test
    void testOutOfOrderSchemaChangeBufferIsBounded() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        setField(
                context.operator,
                BroadcastSchemaSinkOperator.class,
                "pendingSchemaChangeCount",
                10_000);

        SchemaEvolutionException exception =
                assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                context.operator.processElement(
                                        new StreamRecord<>(
                                                createBroadcastRow(
                                                        createSchemaChangeEvent(800L), 2L))));

        assertTrue(exception.getMessage().contains("out-of-order control buffer overflow"));
        assertTrue(context.output.records.isEmpty());
    }

    @Test
    void testReplicatedUnionStateIsSnapshottedAsSingleAggregateCopy() throws Exception {
        AlterTableAddColumnEvent firstEvent =
                createSchemaChangeEvent(
                        TableIdentifier.of("catalog", "database", "first_table"),
                        "first_col",
                        900L);
        AlterTableAddColumnEvent secondEvent =
                createSchemaChangeEvent(
                        TableIdentifier.of("catalog", "database", "second_table"),
                        "second_col",
                        901L);
        List<TestingOperatorStateStore> stateStores = new ArrayList<>();

        for (int subtask = 0; subtask < 4; subtask++) {
            TestingOperatorStateStore stateStore = new TestingOperatorStateStore();
            BroadcastSchemaSinkOperator operator =
                    createSnapshotOperator(
                            stateStore,
                            subtask,
                            4,
                            Arrays.asList(
                                    createInitialTable(firstEvent.tableIdentifier()),
                                    createInitialTable(secondEvent.tableIdentifier())));
            operator.processElement(
                    new StreamRecord<>(createBroadcastRow(firstEvent, "producer-1", 1L)));
            operator.processElement(
                    new StreamRecord<>(createBroadcastRow(secondEvent, "producer-2", 1L)));
            operator.processElement(
                    new StreamRecord<>(createBroadcastRow(firstEvent, "producer-1", 3L)));
            operator.snapshotState(checkpoint(1L));
            operator.notifyCheckpointComplete(1L);
            operator.snapshotState(checkpoint(2L));
            stateStores.add(stateStore);
        }

        assertEquals(
                2,
                stateStores.stream()
                        .mapToInt(store -> store.size("applied-schema-sequences"))
                        .sum());
        assertEquals(
                2,
                stateStores.stream()
                        .mapToInt(store -> store.size("latest-applied-schema-events"))
                        .sum());
        assertEquals(
                1,
                stateStores.stream()
                        .mapToInt(store -> store.size("out-of-order-schema-events"))
                        .sum());
    }

    @Test
    void testMalformedDependencyFailsInsteadOfWaitingForBufferOverflow() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        SeaTunnelRow dataRow = createDataRow();
        SchemaEvolutionControlMessage.requireSchemaChange(dataRow, "malformed-sequence");

        assertThrows(
                SchemaEvolutionException.class,
                () -> context.operator.processElement(new StreamRecord<>(dataRow)));
        assertTrue(context.output.records.isEmpty());
    }

    @Test
    void testSchemaEventWithoutCompleteTargetFailsBeforeItCanBeCheckpointed() throws Exception {
        OperatorTestContext context = createOperator(Collections.emptyList(), false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent(1_000L);
        event.setChangeAfter(null);

        SchemaEvolutionException exception =
                assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                context.operator.processElement(
                                        new StreamRecord<>(createBroadcastRow(event))));

        assertTrue(exception.getMessage().contains("no complete changeAfter snapshot"));
        assertTrue(context.output.records.isEmpty());
        assertTrue(context.operator.getLatestSchemaEvents().isEmpty());
    }

    @Test
    void testCatalogQualifiedInitialSchemaMatchesNormalizedSchemaEvent() throws Exception {
        TableIdentifier initialTableId = TableIdentifier.of("MySQL", "database", "table");
        TableIdentifier eventTableId = TableIdentifier.of("", "database", "table");
        AlterTableAddColumnEvent event = createSchemaChangeEvent(eventTableId, "added_col", 1_001L);
        event.setChangeAfter(
                createCatalogTable(
                        initialTableId,
                        initialColumn(),
                        PhysicalColumn.of(
                                "added_col", BasicType.STRING_TYPE, 64L, true, null, null)));
        OperatorTestContext context =
                createOperator(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        false,
                        Collections.singletonList(createInitialTable(initialTableId)));

        context.operator.processElement(new StreamRecord<>(createBroadcastRow(event)));

        assertEquals(1, context.output.records.size());
        assertSame(
                event,
                context.output
                        .records
                        .get(0)
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT));
    }

    private static OperatorTestContext createOperator(
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            boolean restored)
            throws Exception {
        return createOperator(
                restoredSequences, Collections.emptyList(), Collections.emptyList(), restored);
    }

    private static OperatorTestContext createParallelOperator(int subtask, boolean restored)
            throws Exception {
        return createParallelOperator(
                subtask,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                restored);
    }

    private static OperatorTestContext createParallelOperator(
            int subtask,
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            List<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> restoredEvents,
            List<BroadcastSchemaSinkOperator.PendingSchemaEventEntry> restoredPendingEvents,
            boolean restored)
            throws Exception {
        return createParallelOperator(
                subtask,
                restoredSequences,
                restoredEvents,
                restoredPendingEvents,
                Collections.emptyList(),
                restored,
                4);
    }

    private static OperatorTestContext createParallelOperator(
            int subtask,
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            List<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> restoredEvents,
            List<BroadcastSchemaSinkOperator.PendingSchemaEventEntry> restoredPendingEvents,
            List<BroadcastSchemaSinkOperator.PendingRowEntry> restoredPendingRows,
            boolean restored,
            int parallelism)
            throws Exception {
        return createOperator(
                restoredSequences,
                restoredEvents,
                restoredPendingEvents,
                restoredPendingRows,
                restored,
                Collections.singletonList(createInitialTable(DEFAULT_TABLE_ID)),
                subtask,
                parallelism);
    }

    private static OperatorTestContext createOperator(
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            List<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> restoredEvents,
            boolean restored)
            throws Exception {
        return createOperator(restoredSequences, restoredEvents, Collections.emptyList(), restored);
    }

    private static OperatorTestContext createOperator(
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            List<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> restoredEvents,
            List<BroadcastSchemaSinkOperator.PendingSchemaEventEntry> restoredPendingEvents,
            boolean restored)
            throws Exception {
        return createOperator(
                restoredSequences,
                restoredEvents,
                restoredPendingEvents,
                restored,
                Collections.singletonList(createInitialTable(DEFAULT_TABLE_ID)));
    }

    private static OperatorTestContext createOperator(
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            List<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> restoredEvents,
            List<BroadcastSchemaSinkOperator.PendingSchemaEventEntry> restoredPendingEvents,
            boolean restored,
            List<CatalogTable> initialTables)
            throws Exception {
        return createOperator(
                restoredSequences,
                restoredEvents,
                restoredPendingEvents,
                Collections.emptyList(),
                restored,
                initialTables,
                0,
                1);
    }

    private static OperatorTestContext createOperator(
            List<BroadcastSchemaSinkOperator.SchemaSequenceEntry> restoredSequences,
            List<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> restoredEvents,
            List<BroadcastSchemaSinkOperator.PendingSchemaEventEntry> restoredPendingEvents,
            List<BroadcastSchemaSinkOperator.PendingRowEntry> restoredPendingRows,
            boolean restored,
            List<CatalogTable> initialTables,
            int subtask,
            int parallelism)
            throws Exception {
        BroadcastSchemaSinkOperator operator = new BroadcastSchemaSinkOperator(initialTables);
        CollectingOutput output = new CollectingOutput();
        StreamingRuntimeContext runtimeContext = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(runtimeContext.getIndexOfThisSubtask()).thenReturn(subtask);
        Mockito.when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        Mockito.when(runtimeContext.getMaxNumberOfParallelSubtasks()).thenReturn(MAX_PARALLELISM);
        Mockito.when(runtimeContext.getJobId()).thenReturn(new JobID());

        setField(operator, AbstractStreamOperator.class, "output", output);
        setField(operator, AbstractStreamOperator.class, "runtimeContext", runtimeContext);
        setField(
                operator,
                AbstractStreamOperator.class,
                "stateHandler",
                Mockito.mock(StreamOperatorStateHandler.class));

        ListState<BroadcastSchemaSinkOperator.SchemaSequenceEntry> sequenceState =
                listState(restoredSequences);
        ListState<BroadcastSchemaSinkOperator.LatestSchemaEventEntry> schemaEventState =
                listState(restoredEvents);
        ListState<BroadcastSchemaSinkOperator.PendingSchemaEventEntry> pendingSchemaEventState =
                listState(restoredPendingEvents);
        ListState<BroadcastSchemaSinkOperator.PendingRowEntry> pendingState =
                listState(restoredPendingRows);

        OperatorStateStore stateStore = Mockito.mock(OperatorStateStore.class);
        Mockito.when(stateStore.getUnionListState(Mockito.any(ListStateDescriptor.class)))
                .thenAnswer(
                        invocation -> {
                            ListStateDescriptor<?> descriptor = invocation.getArgument(0);
                            if ("applied-schema-sequences".equals(descriptor.getName())) {
                                return sequenceState;
                            }
                            if ("latest-applied-schema-events".equals(descriptor.getName())) {
                                return schemaEventState;
                            }
                            if ("out-of-order-schema-events".equals(descriptor.getName())) {
                                return pendingSchemaEventState;
                            }
                            return pendingState;
                        });

        StateInitializationContext initializationContext =
                Mockito.mock(StateInitializationContext.class);
        Mockito.when(initializationContext.getOperatorStateStore()).thenReturn(stateStore);
        Mockito.when(initializationContext.isRestored()).thenReturn(restored);
        operator.initializeState(initializationContext);
        return new OperatorTestContext(operator, output);
    }

    private static StateSnapshotContext checkpoint(long checkpointId) {
        StateSnapshotContext context = Mockito.mock(StateSnapshotContext.class);
        Mockito.when(context.getCheckpointId()).thenReturn(checkpointId);
        return context;
    }

    private static BroadcastSchemaSinkOperator createSnapshotOperator(
            TestingOperatorStateStore stateStore,
            int subtask,
            int parallelism,
            List<CatalogTable> initialTables)
            throws Exception {
        return createStateStoreOperator(stateStore, subtask, parallelism, initialTables, false)
                .operator;
    }

    private static OperatorTestContext createStateStoreOperator(
            TestingOperatorStateStore stateStore,
            int subtask,
            int parallelism,
            List<CatalogTable> initialTables,
            boolean restored)
            throws Exception {
        BroadcastSchemaSinkOperator operator = new BroadcastSchemaSinkOperator(initialTables);
        CollectingOutput output = new CollectingOutput();
        StreamingRuntimeContext runtimeContext = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(runtimeContext.getIndexOfThisSubtask()).thenReturn(subtask);
        Mockito.when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        Mockito.when(runtimeContext.getMaxNumberOfParallelSubtasks()).thenReturn(MAX_PARALLELISM);
        Mockito.when(runtimeContext.getJobId()).thenReturn(new JobID());

        setField(operator, AbstractStreamOperator.class, "output", output);
        setField(operator, AbstractStreamOperator.class, "runtimeContext", runtimeContext);
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

    @SuppressWarnings("unchecked")
    private static <T> ListState<T> listState(List<T> restoredValues) throws Exception {
        ListState<T> state = Mockito.mock(ListState.class);
        Mockito.when(state.get()).thenReturn(restoredValues);
        return state;
    }

    private static AlterTableAddColumnEvent createSchemaChangeEvent(long createdTime)
            throws Exception {
        return createSchemaChangeEvent(DEFAULT_TABLE_ID, "added_col", createdTime);
    }

    private static AlterTableAddColumnEvent createSchemaChangeEvent(
            TableIdentifier tableIdentifier, String columnName, long createdTime) throws Exception {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        tableIdentifier,
                        PhysicalColumn.of(
                                columnName, BasicType.STRING_TYPE, 64L, true, null, null));
        event.setChangeAfter(
                createCatalogTable(
                        tableIdentifier,
                        initialColumn(),
                        PhysicalColumn.of(
                                columnName, BasicType.STRING_TYPE, 64L, true, null, null)));
        Field field = findField(event.getClass(), "createdTime");
        field.setAccessible(true);
        field.setLong(event, createdTime);
        return event;
    }

    private static CatalogTable createInitialTable(TableIdentifier tableIdentifier) {
        return createCatalogTable(tableIdentifier, initialColumn());
    }

    private static Column initialColumn() {
        return PhysicalColumn.of("id", BasicType.INT_TYPE, 11L, false, null, null);
    }

    private static CatalogTable createCatalogTable(
            TableIdentifier tableIdentifier, Column... columns) {
        return CatalogTable.of(
                tableIdentifier,
                TableSchema.builder().columns(Arrays.asList(columns)).build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static void assertRestoresTargetSchema(
            StreamRecord<SeaTunnelRow> outputRecord, SchemaChangeEvent latestEvent) {
        Object restoreEvent =
                outputRecord
                        .getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_EVENT);
        assertTrue(restoreEvent instanceof AlterTableColumnsEvent);
        TableSchema rebuilt =
                new TableSchemaChangeEventDispatcher()
                        .reset(createInitialTable(latestEvent.tableIdentifier()).getTableSchema())
                        .apply((SchemaChangeEvent) restoreEvent);
        assertEquals(latestEvent.getChangeAfter().getTableSchema(), rebuilt);
    }

    private static SeaTunnelRow createBroadcastRow(AlterTableAddColumnEvent event) {
        return createBroadcastRow(event, 1L);
    }

    private static SeaTunnelRow createBroadcastRow(AlterTableAddColumnEvent event, long sequence) {
        return createBroadcastRow(event, "producer-1", sequence);
    }

    private static SeaTunnelRow createBroadcastRow(
            AlterTableAddColumnEvent event, String producerId, long sequence) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_BROADCAST, event);
        options.put(
                SchemaEvolutionControlMessage.SCHEMA_CHANGE_ID,
                SchemaEvolutionControlMessage.schemaChangeId(producerId, sequence));
        row.setOptions(options);
        return row;
    }

    private static String schemaChangeId(AlterTableAddColumnEvent event) {
        return schemaChangeId(event, 1L);
    }

    private static String schemaChangeId(AlterTableAddColumnEvent event, long sequence) {
        return SchemaEvolutionControlMessage.schemaChangeId("producer-1", sequence);
    }

    private static SeaTunnelRow createDataRow() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"data"});
        row.setTableId(DEFAULT_TABLE_ID.toTablePath().toString());
        return row;
    }

    private static int ownerSubtask(TableIdentifier tableIdentifier, int parallelism) {
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                tableIdentifier.toTablePath().toString(), MAX_PARALLELISM, parallelism);
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
        private final BroadcastSchemaSinkOperator operator;
        private final CollectingOutput output;

        private OperatorTestContext(BroadcastSchemaSinkOperator operator, CollectingOutput output) {
            this.operator = operator;
            this.output = output;
        }
    }

    private static final class TestingOperatorStateStore implements OperatorStateStore {
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

        private int size(String stateName) {
            TestingListState<?> state = listStates.get(stateName);
            return state == null ? 0 : state.size();
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

        private int size() {
            return values.size();
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
}
