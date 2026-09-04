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

import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
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
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaOperatorTest {

    @Test
    void testWaitRoundBeforeReleasingBufferedRecords() throws Exception {
        OperatorTestContext context = createOperator(false);

        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        SeaTunnelRow row = createDataRow("row-after-schema");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 100L));
        context.operator.processElement(new StreamRecord<>(row, 101L));

        context.operator.notifyCheckpointComplete(10L);

        assertTrue(context.output.records.isEmpty());
        assertEquals(10L, getLongField(context.operator, "firstSeenCheckpointId"));
        assertEquals(2, getPendingQueue(context.operator).size());

        context.operator.notifyCheckpointComplete(11L);

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), event);
        assertEquals(row, context.output.records.get(1).getValue());
        assertSchemaDependency(row);
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertEquals(-1L, getLongField(context.operator, "firstSeenCheckpointId"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
    }

    @Test
    void testRestoreReestablishesCheckpointFenceForPendingSchemaChange() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        OperatorTestContext originalContext = createOperator(stateStore, false);

        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        SeaTunnelRow row = createDataRow("buffered-across-restore");

        originalContext.operator.processElement(new StreamRecord<>(createSchemaRow(event), 200L));
        originalContext.operator.processElement(new StreamRecord<>(row, 201L));
        originalContext.operator.notifyCheckpointComplete(20L);
        originalContext.operator.snapshotState(snapshotContext(20L));

        OperatorTestContext restoredContext = createOperator(stateStore, true);

        assertEquals(-1L, getLongField(restoredContext.operator, "firstSeenCheckpointId"));
        assertEquals(2, getPendingQueue(restoredContext.operator).size());
        assertTrue(getBooleanField(restoredContext.operator, "schemaChangePending"));

        restoredContext.operator.notifyCheckpointComplete(21L);

        assertTrue(restoredContext.output.records.isEmpty());
        assertEquals(21L, getLongField(restoredContext.operator, "firstSeenCheckpointId"));
        assertEquals(2, getPendingQueue(restoredContext.operator).size());

        restoredContext.operator.notifyCheckpointComplete(22L);

        assertEquals(2, restoredContext.output.records.size());
        assertSchemaBroadcast(restoredContext.output.records.get(0), event);
        assertEquals(row, restoredContext.output.records.get(1).getValue());
        assertSchemaDependency(row);
        assertTrue(getPendingQueue(restoredContext.operator).isEmpty());
        assertFalse(getBooleanField(restoredContext.operator, "schemaChangePending"));
    }

    @Test
    void testReleasedRowsCarrySchemaDependency() throws Exception {
        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        OperatorTestContext context = createOperator(false);

        SeaTunnelRow row = createDataRow("dependent-row");
        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 300L));
        context.operator.processElement(new StreamRecord<>(row, 301L));
        context.operator.notifyCheckpointComplete(30L);
        context.operator.notifyCheckpointComplete(31L);

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), event);
        assertSchemaDependency(context.output.records.get(1).getValue());
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
    }

    @Test
    void testMultipleQueuedSchemaChangesAreAppliedAcrossCheckpointRounds() throws Exception {
        OperatorTestContext context = createOperator(false);

        AlterTableAddColumnEvent firstEvent =
                createSchemaChangeEvent(
                        TableIdentifier.of("catalog", "database", "first_table"),
                        "first_added_col",
                        100L);
        AlterTableAddColumnEvent secondEvent =
                createSchemaChangeEvent(
                        TableIdentifier.of("catalog", "database", "second_table"),
                        "second_added_col",
                        200L);
        SeaTunnelRow rowBetween = createDataRow("row-between-ddls");
        SeaTunnelRow rowAfter = createDataRow("row-after-ddls");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(firstEvent), 500L));
        context.operator.processElement(new StreamRecord<>(rowBetween, 501L));
        context.operator.processElement(new StreamRecord<>(createSchemaRow(secondEvent), 502L));
        context.operator.processElement(new StreamRecord<>(rowAfter, 503L));

        context.operator.notifyCheckpointComplete(50L);
        context.operator.notifyCheckpointComplete(51L);

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), firstEvent);
        assertEquals(rowBetween, context.output.records.get(1).getValue());
        assertSchemaDependency(rowBetween);
        assertTrue(getBooleanField(context.operator, "schemaChangePending"));
        assertEquals(-1L, getLongField(context.operator, "firstSeenCheckpointId"));
        assertEquals(2, getPendingQueue(context.operator).size());

        context.operator.notifyCheckpointComplete(52L);
        assertEquals(2, context.output.records.size());
        assertEquals(52L, getLongField(context.operator, "firstSeenCheckpointId"));

        context.operator.notifyCheckpointComplete(53L);

        assertEquals(4, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(2), secondEvent);
        assertEquals(rowAfter, context.output.records.get(3).getValue());
        assertSchemaDependency(rowAfter);
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
    }

    @Test
    void testLowerEpochForDifferentTableIsProcessedIndependently() throws Exception {
        OperatorTestContext context = createOperator(false);

        AlterTableAddColumnEvent newerEvent =
                createSchemaChangeEvent(
                        TableIdentifier.of("catalog", "database", "first_table"),
                        "newer_col",
                        200L);
        AlterTableAddColumnEvent olderEventForOtherTable =
                createSchemaChangeEvent(
                        TableIdentifier.of("catalog", "database", "second_table"),
                        "older_col",
                        100L);
        SeaTunnelRow rowAfter = createDataRow("row-after-skipped-ddl");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(newerEvent), 600L));
        context.operator.processElement(
                new StreamRecord<>(createSchemaRow(olderEventForOtherTable), 601L));
        context.operator.processElement(new StreamRecord<>(rowAfter, 602L));

        context.operator.notifyCheckpointComplete(60L);
        context.operator.notifyCheckpointComplete(61L);
        context.operator.notifyCheckpointComplete(62L);
        context.operator.notifyCheckpointComplete(63L);

        assertEquals(3, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), newerEvent);
        assertSchemaBroadcast(context.output.records.get(1), olderEventForOtherTable);
        assertEquals(rowAfter, context.output.records.get(2).getValue());
        assertSchemaDependency(rowAfter);
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
    }

    @Test
    void testCommentEventIsSupportedSchemaChange() throws Exception {
        OperatorTestContext context =
                createOperator(
                        Collections.singletonList(SchemaChangeType.ALTER_TABLE_COMMENT), false);
        AlterTableCommentEvent event =
                AlterTableCommentEvent.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        "old comment",
                        "new comment");
        SeaTunnelRow row = createDataRow("row-after-comment-change");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 400L));
        context.operator.processElement(new StreamRecord<>(row, 401L));
        context.operator.notifyCheckpointComplete(40L);
        context.operator.notifyCheckpointComplete(41L);

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), event);
        assertEquals(row, context.output.records.get(1).getValue());
    }

    @Test
    void testCreatedTimeDoesNotDiscardAValidSchemaChange() throws Exception {
        OperatorTestContext context = createOperator(false);
        TableIdentifier table = TableIdentifier.of("catalog", "database", "table");
        AlterTableAddColumnEvent newerEvent = createSchemaChangeEvent(table, "newer_col", 200L);
        AlterTableAddColumnEvent olderEvent = createSchemaChangeEvent(table, "older_col", 100L);

        context.operator.processElement(new StreamRecord<>(createSchemaRow(newerEvent)));
        context.operator.processElement(new StreamRecord<>(createSchemaRow(olderEvent)));
        context.operator.notifyCheckpointComplete(64L);
        context.operator.notifyCheckpointComplete(65L);
        context.operator.notifyCheckpointComplete(66L);
        context.operator.notifyCheckpointComplete(67L);

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), newerEvent);
        assertSchemaBroadcast(context.output.records.get(1), olderEvent);
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
    }

    @Test
    void testRestoreDoesNotUseCreatedTimeAsSchemaEventIdentity() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        OperatorTestContext originalContext = createOperator(stateStore, false);
        TableIdentifier firstTable = TableIdentifier.of("catalog", "database", "first_table");
        TableIdentifier secondTable = TableIdentifier.of("catalog", "database", "second_table");
        AlterTableAddColumnEvent appliedEvent =
                createSchemaChangeEvent(firstTable, "newer_col", 200L);

        originalContext.operator.processElement(new StreamRecord<>(createSchemaRow(appliedEvent)));
        originalContext.operator.notifyCheckpointComplete(68L);
        originalContext.operator.notifyCheckpointComplete(69L);
        originalContext.operator.snapshotState(snapshotContext(69L));
        addStateValue(
                stateStore,
                "lastProcessedEventTimeByTable",
                SchemaOperator.TableEventTimeEntry.class,
                new SchemaOperator.TableEventTimeEntry(firstTable, 999L));

        OperatorTestContext restoredContext = createOperator(stateStore, true);
        AlterTableAddColumnEvent olderSameTable =
                createSchemaChangeEvent(firstTable, "older_col", 100L);
        AlterTableAddColumnEvent olderOtherTable =
                createSchemaChangeEvent(secondTable, "other_col", 100L);
        restoredContext.operator.processElement(
                new StreamRecord<>(createSchemaRow(olderSameTable)));
        restoredContext.operator.processElement(
                new StreamRecord<>(createSchemaRow(olderOtherTable)));
        restoredContext.operator.notifyCheckpointComplete(70L);
        restoredContext.operator.notifyCheckpointComplete(71L);
        restoredContext.operator.notifyCheckpointComplete(72L);
        restoredContext.operator.notifyCheckpointComplete(73L);

        assertEquals(2, restoredContext.output.records.size());
        assertSchemaBroadcast(restoredContext.output.records.get(0), olderSameTable);
        assertSchemaBroadcast(restoredContext.output.records.get(1), olderOtherTable);
        assertFalse(getBooleanField(restoredContext.operator, "schemaChangePending"));
    }

    @Test
    void testLegacyCreatedTimeAndAtomicProtocolStatesAreRegistered() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();

        createOperator(stateStore, false);

        assertFalse(stateStore.getRegisteredStateNames().contains("localSchemaState"));
        assertTrue(stateStore.getRegisteredStateNames().contains("lastProcessedEventTimeByTable"));
        assertTrue(stateStore.getRegisteredStateNames().contains("schemaEvolutionProtocolState"));
    }

    @Test
    void testScaleDownRestoreKeepsActiveProtocolStateAtomic() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        AlterTableAddColumnEvent pendingEvent = createSchemaChangeEvent();
        SeaTunnelRow pendingRow = createDataRow("pending-row");
        String lastEmittedSchemaChangeId =
                SchemaEvolutionControlMessage.schemaChangeId("active-producer", 7L);

        addStateValue(
                stateStore,
                "schemaEvolutionProtocolState",
                SchemaOperator.SchemaEvolutionProtocolState.class,
                new SchemaOperator.SchemaEvolutionProtocolState(
                        false, Collections.emptyList(), -1L, "inactive-producer", 0L, null));
        addStateValue(
                stateStore,
                "schemaEvolutionProtocolState",
                SchemaOperator.SchemaEvolutionProtocolState.class,
                new SchemaOperator.SchemaEvolutionProtocolState(
                        true,
                        Arrays.asList(
                                new SchemaOperator.BufferedRecordEntry(
                                        true, null, 0L, pendingEvent),
                                new SchemaOperator.BufferedRecordEntry(
                                        false, pendingRow, 123L, null)),
                        20L,
                        "active-producer",
                        7L,
                        lastEmittedSchemaChangeId));

        // Poison the independently redistributed legacy fields. A restore from the new state must
        // ignore these values rather than construct a producer/queue tuple that never existed.
        addStateValue(stateStore, "schemaChangePending", Boolean.class, false);
        addStateValue(
                stateStore,
                "schemaChangeProducerId",
                String.class,
                SchemaEvolutionControlMessage.schemaChangeId("wrong-producer", 99L));
        addStateValue(stateStore, "schemaChangeSequence", Long.class, 99L);

        OperatorTestContext restoredContext = createOperator(stateStore, true);

        assertTrue(getBooleanField(restoredContext.operator, "schemaChangePending"));
        assertEquals(-1L, getLongField(restoredContext.operator, "firstSeenCheckpointId"));
        assertEquals(7L, getLongField(restoredContext.operator, "schemaChangeSequence"));
        assertEquals(
                "active-producer", getField(restoredContext.operator, "schemaChangeProducerId"));
        assertEquals(
                lastEmittedSchemaChangeId,
                getField(restoredContext.operator, "lastEmittedSchemaChangeId"));
        assertEquals(2, getPendingQueue(restoredContext.operator).size());

        restoredContext.operator.notifyCheckpointComplete(21L);
        restoredContext.operator.notifyCheckpointComplete(22L);

        assertEquals(2, restoredContext.output.records.size());
        String restoredSchemaChangeId =
                SchemaEvolutionControlMessage.schemaChangeId(
                        restoredContext.output.records.get(0).getValue());
        assertEquals(
                "active-producer",
                SchemaEvolutionControlMessage.schemaChangeProducerId(restoredSchemaChangeId));
        assertEquals(
                8L, SchemaEvolutionControlMessage.schemaChangeSequence(restoredSchemaChangeId));
        assertEquals(
                restoredSchemaChangeId,
                SchemaEvolutionControlMessage.requiredSchemaChangeId(
                        restoredContext.output.records.get(1).getValue()));
    }

    @Test
    void testLegacyScaleDownRestoreSelectsPairedActiveProducerState() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        AlterTableAddColumnEvent pendingEvent = createSchemaChangeEvent();
        String activeSchemaChangeId =
                SchemaEvolutionControlMessage.schemaChangeId("active-producer", 7L);

        addStateValue(stateStore, "schemaChangePending", Boolean.class, false);
        addStateValue(stateStore, "schemaChangePending", Boolean.class, true);
        addStateValue(stateStore, "firstSeenCheckpointId", Long.class, -1L);
        addStateValue(stateStore, "firstSeenCheckpointId", Long.class, 20L);
        addStateValue(
                stateStore,
                "schemaChangeProducerId",
                String.class,
                SchemaEvolutionControlMessage.schemaChangeId("inactive-producer", 0L));
        addStateValue(stateStore, "schemaChangeProducerId", String.class, activeSchemaChangeId);
        addStateValue(stateStore, "schemaChangeSequence", Long.class, 0L);
        addStateValue(stateStore, "schemaChangeSequence", Long.class, 7L);
        addStateValue(stateStore, "lastEmittedSchemaChangeId", String.class, activeSchemaChangeId);
        addStateValue(
                stateStore,
                "bufferedRecords",
                SchemaOperator.BufferedRecordEntry.class,
                new SchemaOperator.BufferedRecordEntry(true, null, 0L, pendingEvent));

        OperatorTestContext restoredContext = createOperator(stateStore, true);

        assertTrue(getBooleanField(restoredContext.operator, "schemaChangePending"));
        assertEquals(-1L, getLongField(restoredContext.operator, "firstSeenCheckpointId"));
        assertEquals(7L, getLongField(restoredContext.operator, "schemaChangeSequence"));
        assertEquals(
                "active-producer", getField(restoredContext.operator, "schemaChangeProducerId"));
        assertEquals(
                activeSchemaChangeId,
                getField(restoredContext.operator, "lastEmittedSchemaChangeId"));
        assertEquals(1, getPendingQueue(restoredContext.operator).size());
    }

    /**
     * Verifies that {@link SchemaOperator#handleFallbackTimerOnTaskThread()} correctly respects the
     * checkpoint-completion safety fence even when called from a stall-detection timer.
     *
     * <p>The test invokes the handler directly (as if a processing-time timer fired) to keep the
     * unit test independent of Flink's timer infrastructure. In production, the handler is called
     * by {@link SchemaOperator13#scheduleFallbackTimer()} via {@code
     * ProcessingTimeService.registerTimer}.
     *
     * <p>The base {@link SchemaOperator#scheduleFallbackTimer()} is a no-op; this test verifies
     * only the handler logic, not the scheduling mechanism.
     */
    @Test
    void testFallbackTimerFailsExactlyOnceJobWhenSecondCheckpointStalls() throws Exception {
        OperatorTestContext context = createOperator(false);

        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        SeaTunnelRow row = createDataRow("row-released-after-fallback");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 400L));
        context.operator.processElement(new StreamRecord<>(row, 401L));

        // Simulate timer firing before any checkpoint has completed (firstSeenCheckpointId < 0).
        // The handler must NOT apply the DDL — it must call scheduleFallbackTimer() to wait for
        // the checkpoint-completion safety fence (guards XA/MDL conflicts).
        invokeNoArgMethod(context.operator, "handleFallbackTimerOnTaskThread");

        assertTrue(context.output.records.isEmpty());
        assertTrue(getBooleanField(context.operator, "schemaChangePending"));
        assertEquals(2, getPendingQueue(context.operator).size());
        assertEquals(-1L, getLongField(context.operator, "firstSeenCheckpointId"));

        // Complete the first post-DDL checkpoint — sets firstSeenCheckpointId, not yet safe to
        // apply (need one additional round, so notifyCheckpointComplete stops here).
        context.operator.notifyCheckpointComplete(40L);

        assertTrue(context.output.records.isEmpty());
        assertEquals(40L, getLongField(context.operator, "firstSeenCheckpointId"));
        assertTrue(getBooleanField(context.operator, "schemaChangePending"));

        // Simulate checkpoint stall: move lastCheckpointCompletedMs into the past beyond
        // CHECKPOINT_STALL_TIMEOUT_MS (15 s). This mirrors the Flink 1.13 behaviour where
        // high-parallelism CDC jobs stop checkpointing after some source subtasks finish.
        setField(
                context.operator,
                "lastCheckpointCompletedMs",
                System.currentTimeMillis() - 20_000L);

        SchemaEvolutionException exception =
                assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                invokeNoArgMethod(
                                        context.operator, "handleFallbackTimerOnTaskThread"));

        assertTrue(exception.getMessage().contains("second completed checkpoint"));
        assertTrue(context.output.records.isEmpty());
        assertTrue(getBooleanField(context.operator, "schemaChangePending"));
        assertEquals(2, getPendingQueue(context.operator).size());
    }

    @Test
    void testFallbackTimerCanReleaseAtLeastOnceJob() throws Exception {
        OperatorTestContext context = createOperator(new OperatorStateStoreStub(), false, false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent();
        SeaTunnelRow row = createDataRow("row-released-after-fallback");

        context.operator.processElement(new StreamRecord<>(createSchemaRow(event), 410L));
        context.operator.processElement(new StreamRecord<>(row, 411L));
        context.operator.notifyCheckpointComplete(41L);
        setField(
                context.operator,
                "lastCheckpointCompletedMs",
                System.currentTimeMillis() - 20_000L);

        invokeNoArgMethod(context.operator, "handleFallbackTimerOnTaskThread");

        assertEquals(2, context.output.records.size());
        assertSchemaBroadcast(context.output.records.get(0), event);
        assertEquals(row, context.output.records.get(1).getValue());
        assertSchemaDependency(row);
        assertFalse(getBooleanField(context.operator, "schemaChangePending"));
        assertTrue(getPendingQueue(context.operator).isEmpty());
    }

    @Test
    void testRestoredOperatorTagsRowsWithLastEmittedSchemaChange() throws Exception {
        OperatorStateStoreStub stateStore = new OperatorStateStoreStub();
        OperatorTestContext originalContext = createOperator(stateStore, false);
        AlterTableAddColumnEvent event = createSchemaChangeEvent();

        originalContext.operator.processElement(new StreamRecord<>(createSchemaRow(event), 700L));
        originalContext.operator.notifyCheckpointComplete(70L);
        originalContext.operator.notifyCheckpointComplete(71L);
        String emittedSchemaChangeId =
                SchemaEvolutionControlMessage.schemaChangeId(
                        originalContext.output.records.get(0).getValue());
        originalContext.operator.snapshotState(snapshotContext(72L));

        OperatorTestContext restoredContext = createOperator(stateStore, true);
        SeaTunnelRow restoredRow = createDataRow("row-after-restore");
        restoredContext.operator.processElement(new StreamRecord<>(restoredRow, 701L));

        assertEquals(1, restoredContext.output.records.size());
        assertSchemaDependency(restoredContext.output.records.get(0).getValue());
        assertEquals(
                emittedSchemaChangeId,
                SchemaEvolutionControlMessage.requiredSchemaChangeId(restoredRow));
    }

    @Test
    void testPendingBufferHasByteLimit() throws Exception {
        OperatorTestContext context = createOperator(false);
        context.operator.processElement(
                new StreamRecord<>(createSchemaRow(createSchemaChangeEvent()), 800L));
        setField(context.operator, "pendingBytes", 64L * 1024 * 1024);

        SchemaEvolutionException exception =
                assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                context.operator.processElement(
                                        new StreamRecord<>(createDataRow("overflow"), 801L)));

        assertTrue(exception.getMessage().contains("Pending schema buffer overflow"));
        assertEquals(1, getPendingQueue(context.operator).size());
    }

    @Test
    void testPendingBufferLimitAlsoCoversSchemaEvents() throws Exception {
        OperatorTestContext context = createOperator(false);
        context.operator.processElement(
                new StreamRecord<>(createSchemaRow(createSchemaChangeEvent()), 900L));
        setField(context.operator, "pendingBytes", 64L * 1024 * 1024);

        SchemaEvolutionException exception =
                assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                context.operator.processElement(
                                        new StreamRecord<>(
                                                createSchemaRow(
                                                        createSchemaChangeEvent(
                                                                TableIdentifier.of(
                                                                        "catalog",
                                                                        "database",
                                                                        "second_table"),
                                                                "second_col",
                                                                System.currentTimeMillis() + 1)),
                                                901L)));

        assertTrue(exception.getMessage().contains("Pending schema buffer overflow"));
        assertEquals(1, getPendingQueue(context.operator).size());
    }

    private static OperatorTestContext createOperator(boolean restored) throws Exception {
        return createOperator(new OperatorStateStoreStub(), restored);
    }

    private static OperatorTestContext createOperator(
            OperatorStateStoreStub stateStore, boolean restored) throws Exception {
        return createOperator(
                stateStore, Collections.singletonList(SchemaChangeType.ADD_COLUMN), restored, true);
    }

    private static OperatorTestContext createOperator(
            List<SchemaChangeType> supportedTypes, boolean restored) throws Exception {
        return createOperator(new OperatorStateStoreStub(), supportedTypes, restored, true);
    }

    private static OperatorTestContext createOperator(
            OperatorStateStoreStub stateStore, boolean restored, boolean exactlyOnceMode)
            throws Exception {
        return createOperator(
                stateStore,
                Collections.singletonList(SchemaChangeType.ADD_COLUMN),
                restored,
                exactlyOnceMode);
    }

    private static OperatorTestContext createOperator(
            OperatorStateStoreStub stateStore,
            List<SchemaChangeType> supportedTypes,
            boolean restored,
            boolean exactlyOnceMode)
            throws Exception {
        SupportSchemaEvolution source = Mockito.mock(SupportSchemaEvolution.class);
        Mockito.when(source.supports()).thenReturn(supportedTypes);

        SchemaOperator operator = createSchemaOperator(source, exactlyOnceMode);

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

    private static SchemaOperator createSchemaOperator(
            SupportSchemaEvolution source, boolean exactlyOnceMode) throws Exception {
        Class<?> configClass =
                Class.forName("org.apache.seatunnel.shade.com.typesafe.config.Config");
        Object config =
                Proxy.newProxyInstance(
                        configClass.getClassLoader(),
                        new Class<?>[] {configClass},
                        (proxy, method, args) -> {
                            if ("hasPath".equals(method.getName())
                                    || "getBoolean".equals(method.getName())) {
                                return true;
                            }
                            return null;
                        });
        return (SchemaOperator)
                SchemaOperator.class
                        .getConstructor(
                                String.class,
                                SupportSchemaEvolution.class,
                                configClass,
                                boolean.class)
                        .newInstance("bootstrap-job-id", source, config, exactlyOnceMode);
    }

    private static StateSnapshotContext snapshotContext(long checkpointId) throws Exception {
        StateSnapshotContext snapshotContext = Mockito.mock(StateSnapshotContext.class);
        Mockito.when(snapshotContext.getCheckpointId()).thenReturn(checkpointId);
        return snapshotContext;
    }

    private static <T> void addStateValue(
            OperatorStateStoreStub stateStore, String stateName, Class<T> type, T value)
            throws Exception {
        stateStore.getListState(new ListStateDescriptor<>(stateName, type)).add(value);
    }

    private static AlterTableAddColumnEvent createSchemaChangeEvent() {
        return createSchemaChangeEvent(
                TableIdentifier.of("catalog", "database", "table"),
                "added_col",
                System.currentTimeMillis());
    }

    private static AlterTableAddColumnEvent createSchemaChangeEvent(
            TableIdentifier tableIdentifier, String columnName, long createdTime) {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        tableIdentifier,
                        PhysicalColumn.of(
                                columnName, BasicType.STRING_TYPE, 64L, true, null, null));
        setCreatedTime(event, createdTime);
        return event;
    }

    private static void setCreatedTime(AlterTableAddColumnEvent event, long createdTime) {
        try {
            Field field = findField(event.getClass(), "createdTime");
            field.setAccessible(true);
            field.setLong(event, createdTime);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to set deterministic schema event epoch", e);
        }
    }

    private static SeaTunnelRow createSchemaRow(SchemaChangeEvent event) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        row.setTableId("__SCHEMA_CHANGE_EVENT__");
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("schema_change_event", event);
        row.setOptions(options);
        return row;
    }

    private static SeaTunnelRow createDataRow(String value) {
        SeaTunnelRow row = new SeaTunnelRow(1);
        row.setTableId("database.table");
        row.setField(0, value);
        return row;
    }

    private static void assertSchemaBroadcast(
            StreamRecord<SeaTunnelRow> record, SchemaChangeEvent event) {
        Object broadcastEvent =
                record.getValue()
                        .getOptions()
                        .get(SchemaEvolutionControlMessage.SCHEMA_CHANGE_BROADCAST);
        assertInstanceOf(event.getClass(), broadcastEvent);
        assertEquals(event, broadcastEvent);
        String schemaChangeId = SchemaEvolutionControlMessage.schemaChangeId(record.getValue());
        assertNotNull(SchemaEvolutionControlMessage.schemaChangeProducerId(schemaChangeId));
        assertTrue(SchemaEvolutionControlMessage.schemaChangeSequence(schemaChangeId) > 0);
    }

    private static void assertSchemaDependency(SeaTunnelRow row) {
        String requiredSchemaChangeId = SchemaEvolutionControlMessage.requiredSchemaChangeId(row);
        assertNotNull(SchemaEvolutionControlMessage.schemaChangeProducerId(requiredSchemaChangeId));
        assertTrue(SchemaEvolutionControlMessage.schemaChangeSequence(requiredSchemaChangeId) > 0);
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

    private static Object invokeNoArgMethod(Object target, String methodName) throws Exception {
        java.lang.reflect.Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        try {
            return method.invoke(target);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
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
