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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.Unit;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaChangePolicyException;
import org.apache.seatunnel.api.table.schema.exception.SinkWriterSchemaException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.execution.SuppressRestartsException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Protects Flink sink coordination acknowledgements, policy failure, and writer compatibility.
 *
 * <p>The real coordinator observes both successful metadata drops and deterministic negative
 * acknowledgements without sink mutation.
 */
class FlinkSinkWriterTest {

    @Test
    void testSchemaChangeEventDoesNotWriteDataRow() throws Exception {
        SchemaAwareRecordingSinkWriter delegate = new SchemaAwareRecordingSinkWriter();
        FlinkSinkWriter<String, String> flinkSinkWriter = createWriter(delegate);

        AlterTableAddColumnEvent event = createAddColumnEvent();
        flinkSinkWriter.write(createSchemaChangeRow(event), null);

        Assertions.assertTrue(delegate.writtenRows.isEmpty());
        Assertions.assertEquals(Collections.singletonList(event), delegate.appliedSchemaChanges);
    }

    @Test
    void testInheritedNoOpWriterDropsSchemaChangeDuringCompatibilityWindow() throws Exception {
        RecordingSinkWriter delegate = new RecordingSinkWriter();
        FlinkSinkWriter<String, String> flinkSinkWriter = createWriter(delegate);

        flinkSinkWriter.write(createSchemaChangeRow(createAddColumnEvent()), null);

        Assertions.assertTrue(delegate.writtenRows.isEmpty());
    }

    @Test
    void testExplicitDeprecatedSchemaChangeOverrideRemainsSupported() throws Exception {
        LegacySchemaChangeWriter delegate = new LegacySchemaChangeWriter();
        FlinkSinkWriter<String, String> flinkSinkWriter = createWriter(delegate);

        AlterTableAddColumnEvent event = createAddColumnEvent();
        flinkSinkWriter.write(createSchemaChangeRow(event), null);

        Assertions.assertEquals(Collections.singletonList(event), delegate.appliedSchemaChanges);
    }

    @Test
    void testSchemaChangeEventFailsWhenSinkApplyFails() {
        FailingSchemaAwareSinkWriter delegate = new FailingSchemaAwareSinkWriter();
        FlinkSinkWriter<String, String> flinkSinkWriter = createWriter(delegate);

        SinkWriterSchemaException error =
                Assertions.assertThrows(
                        SinkWriterSchemaException.class,
                        () ->
                                flinkSinkWriter.write(
                                        createSchemaChangeRow(createAddColumnEvent()), null));

        Assertions.assertTrue(error.getMessage().contains("Failed to apply schema change"));
        Assertions.assertTrue(delegate.writtenRows.isEmpty());
    }

    @Test
    void testUnsupportedCommentDroppedWithoutSinkMutation() throws Exception {
        SchemaAwareRecordingSinkWriter delegate = new SchemaAwareRecordingSinkWriter();
        FlinkSinkWriter<String, String> writer =
                new FlinkSinkWriter<>(
                        delegate,
                        createInitContext(),
                        new RecordingContext(),
                        7L,
                        Collections.singletonList(SchemaChangeType.DROP_COLUMN));
        TableIdentifier table = TableIdentifier.of("catalog", "database", "table");
        SchemaChangeEvent[] events = {
            AlterTableCommentEvent.of(table, "old", "new"),
            new AlterTableColumnsEvent(
                    table,
                    Collections.singletonList(
                            AlterColumnCommentEvent.of(table, "id", "old", "new")))
        };
        for (SchemaChangeEvent event : events) {
            event.setJobId(UUID.randomUUID().toString());
            writer.write(createSchemaChangeRow(event), null);
            Assertions.assertTrue(delegate.appliedSchemaChanges.isEmpty());
            Assertions.assertTrue(delegate.writtenRows.isEmpty());
        }
    }

    @Test
    void testSchemaChangeEventFailsWithoutRestart() {
        SchemaAwareRecordingSinkWriter delegate = new SchemaAwareRecordingSinkWriter();
        FlinkSinkWriter<String, String> writer =
                new FlinkSinkWriter<>(
                        delegate,
                        createInitContext(),
                        new RecordingContext(),
                        7L,
                        Collections.singletonList(SchemaChangeType.DROP_COLUMN));
        SuppressRestartsException error =
                Assertions.assertThrows(
                        SuppressRestartsException.class,
                        () -> writer.write(createSchemaChangeRow(createAddColumnEvent()), null));
        Assertions.assertInstanceOf(SchemaChangePolicyException.class, error.getCause());
        Assertions.assertTrue(error.getCause().getMessage().contains("not supported end to end"));
        Assertions.assertTrue(delegate.writtenRows.isEmpty());
        Assertions.assertTrue(delegate.appliedSchemaChanges.isEmpty());
    }

    private static FlinkSinkWriter<String, String> createWriter(
            SinkWriter<SeaTunnelRow, String, String> delegate) {
        return new FlinkSinkWriter<>(delegate, createInitContext(), new RecordingContext(), 7L);
    }

    private static WriterInitContext createInitContext() {
        WriterInitContext initContext = mock(WriterInitContext.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.empty());
        return initContext;
    }

    private static AlterTableAddColumnEvent createAddColumnEvent() {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("catalog", "database", "table"),
                        PhysicalColumn.of(
                                "added_col", BasicType.STRING_TYPE, 64L, true, null, null));
        event.setJobId("job-under-test");
        return event;
    }

    private static SeaTunnelRow createSchemaChangeRow(SchemaChangeEvent event) {
        SeaTunnelRow schemaEvent = new SeaTunnelRow(0);
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("schema_change_event", event);
        options.put("schema_subtask_id", 0L);
        schemaEvent.setOptions(options);
        return schemaEvent;
    }

    private static class RecordingSinkWriter implements SinkWriter<SeaTunnelRow, String, String> {

        protected final List<SeaTunnelRow> writtenRows = new ArrayList<>();

        @Override
        public void write(SeaTunnelRow element) {
            writtenRows.add(element);
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public List<String> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static class SchemaAwareRecordingSinkWriter extends RecordingSinkWriter
            implements SupportSchemaEvolutionSinkWriter {

        private final List<SchemaChangeEvent> appliedSchemaChanges = new ArrayList<>();

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            appliedSchemaChanges.add(event);
        }
    }

    private static class LegacySchemaChangeWriter extends RecordingSinkWriter {
        private final List<SchemaChangeEvent> appliedSchemaChanges = new ArrayList<>();

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            appliedSchemaChanges.add(event);
        }
    }

    private static class FailingSchemaAwareSinkWriter extends RecordingSinkWriter
            implements SupportSchemaEvolutionSinkWriter {

        @Override
        public void applySchemaChange(SchemaChangeEvent event) throws IOException {
            throw new IOException("apply failed");
        }
    }

    private static class RecordingContext implements SinkWriter.Context {

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return new NoopMetricsContext();
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }

    private static class NoopMetricsContext implements MetricsContext {

        @Override
        public Counter counter(String name) {
            return new Counter() {
                @Override
                public void inc() {}

                @Override
                public void inc(long n) {}

                @Override
                public void dec() {}

                @Override
                public void dec(long n) {}

                @Override
                public void set(long n) {}

                @Override
                public long getCount() {
                    return 0;
                }

                @Override
                public String name() {
                    return name;
                }

                @Override
                public Unit unit() {
                    return Unit.COUNT;
                }
            };
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public Meter meter(String name) {
            return new Meter() {
                @Override
                public void markEvent() {}

                @Override
                public void markEvent(long n) {}

                @Override
                public double getRate() {
                    return 0;
                }

                @Override
                public long getCount() {
                    return 0;
                }

                @Override
                public String name() {
                    return name;
                }

                @Override
                public Unit unit() {
                    return Unit.COUNT;
                }
            };
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }
    }
}
