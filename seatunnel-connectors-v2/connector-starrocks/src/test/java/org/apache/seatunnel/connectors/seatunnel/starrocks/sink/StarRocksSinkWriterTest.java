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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StarRocksSinkWriterTest {

    @Test
    void shouldRegisterTimerFlushAction() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(StarRocksSinkManager.class)) {
            createWriter(context);

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            actionCaptor.getValue().run();
            verify(mockedManager.constructed().get(0), times(1)).flush();
        }
    }

    @Test
    void shouldPropagateTimerFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(StarRocksSinkManager.class)) {
            createWriter(context);
            StarRocksSinkManager manager = mockedManager.constructed().get(0);
            IOException expected = new IOException("timer flush failed");
            doThrow(expected).when(manager).flush();

            verify(context).registerFlushAction(actionCaptor.capture());
            IOException actual =
                    Assertions.assertThrows(IOException.class, actionCaptor.getValue()::run);
            Assertions.assertSame(expected, actual);
        }
    }

    @Test
    void schemaChangeFlushesTheOldManagerBeforeTheDdlAndWritesNewRowsWithTheNewSchema()
            throws Exception {
        List<String> events = new ArrayList<>();
        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(
                        StarRocksSinkManager.class,
                        (manager, ctx) -> {
                            int index = ctx.getCount() - 1;
                            events.add("new-manager-" + index);
                            doAnswer(
                                            invocation -> {
                                                events.add("close-manager-" + index);
                                                return null;
                                            })
                                    .when(manager)
                                    .close();
                        })) {
            RecordingWriter writer = createWriter(mock(SinkWriter.Context.class), events);
            StarRocksSinkManager before = mockedManager.constructed().get(0);
            writer.write(row(1));

            writer.applySchemaChange(addColumn("name"));
            writer.write(rowWithName(2, "n"));

            StarRocksSinkManager after = mockedManager.constructed().get(1);
            // old rows loaded, table altered, only then a manager for the new schema
            Assertions.assertEquals(
                    Arrays.asList("new-manager-0", "close-manager-0", "ddl", "new-manager-1"),
                    events);
            verify(before).write("{\"id\":1}");
            verify(after).write("{\"id\":2,\"name\":\"n\"}");
            verify(before, never()).write("{\"id\":2,\"name\":\"n\"}");
        }
    }

    @Test
    void restoreEventFlushesTheOldManagerWithoutRunningDdl() throws Exception {
        List<String> events = new ArrayList<>();
        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(
                        StarRocksSinkManager.class,
                        (manager, ctx) -> {
                            int index = ctx.getCount() - 1;
                            events.add("new-manager-" + index);
                            doAnswer(
                                            invocation -> {
                                                events.add("close-manager-" + index);
                                                return null;
                                            })
                                    .when(manager)
                                    .close();
                        })) {
            RecordingWriter writer = createWriter(mock(SinkWriter.Context.class), events);

            writer.applySchemaChange(
                    new RestoreTableSchemaEvent(
                            CatalogTable.of(
                                    TABLE_IDENTIFIER,
                                    schema("id", "name"),
                                    Collections.emptyMap(),
                                    Collections.emptyList(),
                                    "")));

            Assertions.assertEquals(
                    Arrays.asList("new-manager-0", "close-manager-0", "new-manager-1"), events);
            Assertions.assertEquals(2, mockedManager.constructed().size());
        }
    }

    @Test
    void failedFlushBeforeSchemaChangeKeepsTheOldManagerAndSkipsTheDdl() throws Exception {
        List<String> events = new ArrayList<>();
        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(
                        StarRocksSinkManager.class,
                        (manager, ctx) -> {
                            events.add("new-manager-" + (ctx.getCount() - 1));
                            // StarRocksSinkManager reports a failed Stream Load this way
                            doThrow(
                                            new StarRocksConnectorException(
                                                    StarRocksConnectorErrorCode
                                                            .WRITE_RECORDS_FAILED,
                                                    "stream load failed"))
                                    .when(manager)
                                    .close();
                        })) {
            RecordingWriter writer = createWriter(mock(SinkWriter.Context.class), events);
            StarRocksSinkManager before = mockedManager.constructed().get(0);

            StarRocksConnectorException thrown =
                    Assertions.assertThrows(
                            StarRocksConnectorException.class,
                            () -> writer.applySchemaChange(addColumn("name")));

            // The failure reaches the engine untouched; no DDL ran and no manager was replaced, so
            // the retained rows are still in the manager that keeps their label.
            Assertions.assertTrue(thrown.getMessage().contains("stream load failed"));
            Assertions.assertEquals(Collections.singletonList("new-manager-0"), events);
            Assertions.assertEquals(1, mockedManager.constructed().size());
            writer.write(row(3));
            verify(before).write("{\"id\":3}");
        }
    }

    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of("starrocks", TablePath.of("test", "timer_flush"));

    private static TableSchema schema(String... names) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String name : names) {
            builder.column(
                    PhysicalColumn.of(
                            name,
                            "id".equals(name) ? BasicType.INT_TYPE : BasicType.STRING_TYPE,
                            (Long) null,
                            false,
                            null,
                            null));
        }
        return builder.build();
    }

    private static AlterTableAddColumnEvent addColumn(String name) {
        return AlterTableAddColumnEvent.add(
                TABLE_IDENTIFIER,
                PhysicalColumn.of(name, BasicType.STRING_TYPE, (Long) null, true, null, null));
    }

    private static SeaTunnelRow row(int id) {
        return new SeaTunnelRow(new Object[] {id});
    }

    private static SeaTunnelRow rowWithName(int id, String name) {
        return new SeaTunnelRow(new Object[] {id, name});
    }

    /** Records the DDL call instead of opening a JDBC connection. */
    private static class RecordingWriter extends StarRocksSinkWriter {
        private final List<String> events;

        RecordingWriter(
                SinkWriter.Context context,
                SinkConfig sinkConfig,
                TableSchema tableSchema,
                List<String> events) {
            super(context, sinkConfig, tableSchema, TABLE_IDENTIFIER.toTablePath());
            this.events = events;
        }

        @Override
        protected void applySchemaChangeToStarRocks(SchemaChangeEvent event) {
            events.add("ddl");
        }
    }

    private RecordingWriter createWriter(SinkWriter.Context context, List<String> events) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setLoadFormat(SinkConfig.StreamLoadFormat.JSON);
        return new RecordingWriter(context, sinkConfig, schema("id"), events);
    }

    private StarRocksSinkWriter createWriter(SinkWriter.Context context) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setLoadFormat(SinkConfig.StreamLoadFormat.JSON);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .build();
        return new StarRocksSinkWriter(
                context, sinkConfig, tableSchema, TablePath.of("test", "timer_flush"));
    }
}
