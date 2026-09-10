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

import org.apache.seatunnel.api.sink.SchemaChangeApplier;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportCoordinatedSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExternalSchemaChangeOperatorTest {

    private static final TableIdentifier SOURCE_TABLE =
            TableIdentifier.of("catalog", "source", "users");
    private static final TablePath PHYSICAL_SINK_TABLE = TablePath.of("sink", "users");

    @Test
    void shouldApplyEachControlEventOnceAndReuseTheTableApplier() throws Exception {
        SchemaChangeApplier applier = mock(SchemaChangeApplier.class);
        SeaTunnelSink<?, ?, ?, ?> sink = coordinatedSink(Optional.empty(), applier);
        OperatorTestContext context = createOperator(sink);
        AlterTableAddColumnEvent firstEvent = schemaChangeEvent("email");
        AlterTableAddColumnEvent secondEvent = schemaChangeEvent("phone");

        context.operator.processElement(new StreamRecord<>(schemaControlRow(firstEvent)));
        context.operator.processElement(new StreamRecord<>(schemaControlRow(secondEvent)));

        SupportCoordinatedSchemaEvolutionSink coordinatedSink =
                (SupportCoordinatedSchemaEvolutionSink) sink;
        verify(coordinatedSink, times(1)).createSchemaChangeApplier(SOURCE_TABLE.toTablePath());
        verify(applier, times(1)).apply(firstEvent);
        verify(applier, times(1)).apply(secondEvent);
        assertEquals(2, context.output.records.size());
        assertSame(firstEvent, schemaEvent(context.output.records.get(0)));
        assertSame(secondEvent, schemaEvent(context.output.records.get(1)));
    }

    @Test
    void shouldUseResolvedPhysicalSinkTable() throws Exception {
        SchemaChangeApplier applier = mock(SchemaChangeApplier.class);
        CatalogTable sinkTable = catalogTable(TableIdentifier.of("catalog", PHYSICAL_SINK_TABLE));
        SeaTunnelSink<?, ?, ?, ?> sink = coordinatedSink(Optional.of(sinkTable), applier);
        OperatorTestContext context = createOperator(sink);
        AlterTableAddColumnEvent event = schemaChangeEvent("email");

        context.operator.processElement(new StreamRecord<>(schemaControlRow(event)));

        verify((SupportCoordinatedSchemaEvolutionSink) sink)
                .createSchemaChangeApplier(PHYSICAL_SINK_TABLE);
        verify(applier).apply(event);
    }

    @Test
    void shouldRejectControlEventWithoutCompleteEvolvedSchema() throws Exception {
        SchemaChangeApplier applier = mock(SchemaChangeApplier.class);
        SeaTunnelSink<?, ?, ?, ?> sink = coordinatedSink(Optional.empty(), applier);
        OperatorTestContext context = createOperator(sink);
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(SOURCE_TABLE, stringColumn("email"));

        assertThrows(
                IllegalStateException.class,
                () -> context.operator.processElement(new StreamRecord<>(schemaControlRow(event))));

        verify((SupportCoordinatedSchemaEvolutionSink) sink, never())
                .createSchemaChangeApplier(Mockito.any());
        verify(applier, never()).apply(Mockito.any());
        assertEquals(0, context.output.records.size());
    }

    @Test
    void shouldNotForwardControlEventWhenExternalApplyFails() throws Exception {
        SchemaChangeApplier applier = mock(SchemaChangeApplier.class);
        SeaTunnelSink<?, ?, ?, ?> sink = coordinatedSink(Optional.empty(), applier);
        OperatorTestContext context = createOperator(sink);
        AlterTableAddColumnEvent event = schemaChangeEvent("email");
        doThrow(new IOException("DDL failed")).when(applier).apply(event);

        assertThrows(
                IOException.class,
                () -> context.operator.processElement(new StreamRecord<>(schemaControlRow(event))));

        assertEquals(0, context.output.records.size());
    }

    @Test
    void shouldCloseEveryCachedApplierWhenOneCloseFails() throws Exception {
        SchemaChangeApplier first = mock(SchemaChangeApplier.class);
        SchemaChangeApplier second = mock(SchemaChangeApplier.class);
        SeaTunnelSink<?, ?, ?, ?> sink = coordinatedSink(Optional.empty(), first);
        OperatorTestContext context = createOperator(sink);
        Map<TablePath, SchemaChangeApplier> appliers = new LinkedHashMap<>();
        appliers.put(TablePath.of("sink", "first"), first);
        appliers.put(TablePath.of("sink", "second"), second);
        setField(
                context.operator,
                ExternalSchemaChangeOperator.class,
                "schemaChangeAppliers",
                appliers);
        doThrow(new IOException("first close failed")).when(first).close();

        assertThrows(IOException.class, context.operator::close);

        verify(first).close();
        verify(second).close();
    }

    @SuppressWarnings("unchecked")
    private static SeaTunnelSink<?, ?, ?, ?> coordinatedSink(
            Optional<CatalogTable> writeCatalogTable, SchemaChangeApplier applier)
            throws IOException {
        SeaTunnelSink<?, ?, ?, ?> sink =
                mock(
                        SeaTunnelSink.class,
                        Mockito.withSettings()
                                .extraInterfaces(SupportCoordinatedSchemaEvolutionSink.class));
        when(sink.getWriteCatalogTable()).thenReturn(writeCatalogTable);
        when(((SupportCoordinatedSchemaEvolutionSink) sink)
                        .createSchemaChangeApplier(Mockito.any()))
                .thenReturn(applier);
        return sink;
    }

    private static OperatorTestContext createOperator(SeaTunnelSink<?, ?, ?, ?> sink)
            throws Exception {
        ExternalSchemaChangeOperator operator = new ExternalSchemaChangeOperator(sink);
        CollectingOutput output = new CollectingOutput();
        setField(operator, AbstractStreamOperator.class, "output", output);
        setField(
                operator,
                ExternalSchemaChangeOperator.class,
                "schemaChangeAppliers",
                new HashMap<TablePath, SchemaChangeApplier>());
        return new OperatorTestContext(operator, output);
    }

    private static AlterTableAddColumnEvent schemaChangeEvent(String columnName) {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(SOURCE_TABLE, stringColumn(columnName));
        event.setChangeAfter(catalogTable(SOURCE_TABLE));
        return event;
    }

    private static CatalogTable catalogTable(TableIdentifier tableIdentifier) {
        return CatalogTable.of(
                tableIdentifier,
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 4L, false, null, null))
                        .column(stringColumn("email"))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static PhysicalColumn stringColumn(String name) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, 128L, true, null, null);
    }

    private static SeaTunnelRow schemaControlRow(AlterTableAddColumnEvent event) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("schema_change_broadcast", event);
        row.setOptions(options);
        return row;
    }

    private static Object schemaEvent(StreamRecord<SeaTunnelRow> record) {
        return record.getValue().getOptions().get("schema_change_broadcast");
    }

    private static void setField(Object target, Class<?> owner, String name, Object value)
            throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class OperatorTestContext {
        private final ExternalSchemaChangeOperator operator;
        private final CollectingOutput output;

        private OperatorTestContext(
                ExternalSchemaChangeOperator operator, CollectingOutput output) {
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
}
