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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SinkWriterSchemaException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.coordinator.LocalSchemaCoordinator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Verifies schema-change dispatch and coordinator acknowledgements in the Flink 1.20 adapter. Both
 * supported and unsupported writers must keep control records out of the data path.
 */
class FlinkSinkWriterSchemaChangeTest {

    @Test
    void unsupportedWriterFailsAndSendsFailureAck() {
        RecordingWriter delegate = new RecordingWriter();
        FlinkSinkWriter<String, String> writer = writer(delegate);
        AlterTableAddColumnEvent event = schemaChangeEvent();
        LocalSchemaCoordinator coordinator = Mockito.mock(LocalSchemaCoordinator.class);

        try (MockedStatic<LocalSchemaCoordinator> coordinators =
                Mockito.mockStatic(LocalSchemaCoordinator.class)) {
            coordinators
                    .when(() -> LocalSchemaCoordinator.getInstance(event.getJobId()))
                    .thenReturn(coordinator);

            SinkWriterSchemaException failure =
                    Assertions.assertThrows(
                            SinkWriterSchemaException.class,
                            () -> writer.write(controlRow(event), null));

            Assertions.assertTrue(
                    failure.getMessage().contains("Failed to apply schema change in Flink sink"));
            Assertions.assertEquals(Collections.emptyList(), delegate.rows);
            Mockito.verify(coordinator)
                    .notifySchemaChangeApplied(
                            event.tableIdentifier(), event.getCreatedTime(), 0, false);
        }
    }

    @Test
    void supportedWriterAppliesEventBeforeWritingRowsAndSendsSuccessAck() throws Exception {
        SchemaAwareWriter delegate = new SchemaAwareWriter();
        FlinkSinkWriter<String, String> writer = writer(delegate);
        AlterTableAddColumnEvent event = schemaChangeEvent();
        LocalSchemaCoordinator coordinator = Mockito.mock(LocalSchemaCoordinator.class);

        try (MockedStatic<LocalSchemaCoordinator> coordinators =
                Mockito.mockStatic(LocalSchemaCoordinator.class)) {
            coordinators
                    .when(() -> LocalSchemaCoordinator.getInstance(event.getJobId()))
                    .thenReturn(coordinator);

            writer.write(controlRow(event), null);
            Assertions.assertSame(event, delegate.appliedEvent);
            Assertions.assertEquals(Collections.emptyList(), delegate.rows);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"value"});
            writer.write(row, null);

            Assertions.assertEquals(Collections.singletonList(row), delegate.rows);
            Mockito.verify(coordinator)
                    .notifySchemaChangeApplied(
                            event.tableIdentifier(), event.getCreatedTime(), 0, true);
        }
    }

    private static FlinkSinkWriter<String, String> writer(RecordingWriter delegate) {
        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);
        Mockito.when(context.getMetricsContext())
                .thenReturn(Mockito.mock(MetricsContext.class, Mockito.RETURNS_DEEP_STUBS));
        return new FlinkSinkWriter<>(delegate, null, context);
    }

    private static AlterTableAddColumnEvent schemaChangeEvent() {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("catalog", "database", "table"),
                        PhysicalColumn.of(
                                "added_col", BasicType.STRING_TYPE, (Long) null, true, null, null));
        event.setJobId("flink-20-schema-change-test");
        return event;
    }

    private static SeaTunnelRow controlRow(SchemaChangeEvent event) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_event", event);
        options.put("schema_subtask_id", 0L);
        row.setOptions(options);
        return row;
    }

    /**
     * Records data rows while inheriting the unsupported schema-change hook. Any control record
     * reaching this writer is visible in the recorded data output.
     */
    private static class RecordingWriter implements SinkWriter<SeaTunnelRow, String, String> {
        protected final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void write(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    /**
     * Records schema-handler delivery independently of data writes. The captured event proves
     * dispatch is not silently skipped for supported writers.
     */
    private static class SchemaAwareWriter extends RecordingWriter
            implements SupportSchemaEvolutionSinkWriter {
        private SchemaChangeEvent appliedEvent;

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            appliedEvent = event;
        }
    }
}
