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

package org.apache.seatunnel.connectors.doris.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisSchemaChangeException;
import org.apache.seatunnel.connectors.doris.rest.models.RespContent;
import org.apache.seatunnel.connectors.doris.schema.SchemaChangeManager;
import org.apache.seatunnel.connectors.doris.sink.LoadStatus;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DorisSinkWriterTest {

    @Test
    void testTimerFlushRegisteredAndReopensLoadForNonTwoPhaseCommit() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        when(frontendLoad.stopLoad()).thenReturn(successRespContent(1L));
        SinkWriter.Context context = mock(SinkWriter.Context.class);

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            context,
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, false),
                            "job_1",
                            factory);

            ArgumentCaptor<RunnableWithException> flushAction =
                    ArgumentCaptor.forClass(RunnableWithException.class);
            verify(context).registerFlushAction(flushAction.capture());

            flushAction.getValue().run();

            verify(frontendLoad, times(1)).stopLoad();
            verify(frontendLoad, times(2)).startLoad(anyString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testTimerFlushNotRegisteredForTwoPhaseCommit() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());
        SinkWriter.Context context = mock(SinkWriter.Context.class);

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            context,
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, true),
                            "job_1",
                            factory);

            verify(context, never()).registerFlushAction(any());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testInitializeLoadUsesFrontendWhenDirectToBeDisabled() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        when(frontendLoad.stopLoad()).thenReturn(successRespContent(11L));

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, false),
                            "job_1",
                            factory);

            Assertions.assertEquals(
                    Collections.singletonList("fe1:8030"), factory.getCreatedHosts());
            verify(frontendLoad, times(1)).startLoad(anyString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testDirectToBeWithoutTwoPhaseCommitUsesBackendOnly() throws Exception {
        DorisStreamLoad backendLoad = mock(DorisStreamLoad.class);
        when(backendLoad.stopLoad()).thenReturn(successRespContent(12L));

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("be1:8040", backendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(true, false),
                            "job_1",
                            factory);

            Assertions.assertEquals(
                    Collections.singletonList("be1:8040"), factory.getCreatedHosts());
            verify(backendLoad, times(1)).startLoad(anyString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testDirectToBeUsesBackendForWriteButFrontendForTwoPhaseCommit() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        DorisStreamLoad backendLoad = mock(DorisStreamLoad.class);
        when(backendLoad.stopLoad()).thenReturn(successRespContent(22L));
        when(backendLoad.getDb()).thenReturn("test_db");
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);
        factory.register("be1:8040", backendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(true, true),
                            "job_1",
                            factory);

            Assertions.assertEquals(
                    Arrays.asList("fe1:8030", "be1:8040"), factory.getCreatedHosts());
            verify(frontendLoad, times(1)).abortPreCommit(anyString(), anyLong());
            verify(backendLoad, times(1)).startLoad(anyString());

            Optional<DorisCommitInfo> commitInfo = writer.prepareCommit();
            Assertions.assertTrue(commitInfo.isPresent());
            Assertions.assertEquals("fe1:8030", commitInfo.get().getHostPort());
            Assertions.assertEquals("test_db", commitInfo.get().getDb());

            writer.abortPrepare();
            verify(frontendLoad, times(2)).abortPreCommit(anyString(), anyLong());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testDirectToBeClosesControlStreamLoadWhenBackendInitializationFails() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);
        factory.registerFailure("be1:8040", new IllegalStateException("be init failed"));

        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        new DorisSinkWriter(
                                new DefaultSinkWriterContext(0, 1),
                                new ArrayList<>(),
                                mockCatalogTable(),
                                createSinkConfig(true, true),
                                "job_1",
                                factory));

        verify(frontendLoad, times(1)).close();
    }

    @Test
    void testInitializeLoadClosesFailedStreamLoadWhenAbortPreCommitFails() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doThrow(new IllegalStateException("abort failed"))
                .when(frontendLoad)
                .abortPreCommit(anyString(), anyLong());

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        new DorisSinkWriter(
                                new DefaultSinkWriterContext(0, 1),
                                new ArrayList<>(),
                                mockCatalogTable(),
                                createSinkConfig(true, true),
                                "job_1",
                                factory));

        verify(frontendLoad, times(1)).close();
    }

    @Test
    void testApplySchemaChangeFlushesInFlightLoadBeforeAlterForNonTwoPhaseCommit()
            throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        when(frontendLoad.stopLoad()).thenReturn(successRespContent(31L));
        SchemaChangeManager schemaChangeManager = mock(SchemaChangeManager.class);

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, false),
                            "job_1",
                            factory);
            writer.setSchemaChangeManager(schemaChangeManager);

            writer.applySchemaChange(addColumnEvent());

            // The crux of the fix is the ordering: the in-flight load is closed (committing the
            // old-schema rows against the still-unaltered table) BEFORE the DDL is applied, and a
            // fresh load is reopened only AFTER the DDL. Assert the order across both mocks so a
            // regression such as "DDL -> stopLoad" (which would commit old-schema rows against the
            // new table structure) is caught, not just the call counts.
            InOrder inOrder = inOrder(frontendLoad, schemaChangeManager);
            inOrder.verify(frontendLoad).stopLoad();
            inOrder.verify(schemaChangeManager)
                    .applySchemaChange(any(TablePath.class), any(SchemaChangeEvent.class));
            inOrder.verify(frontendLoad).startLoad(anyString());
            // exactly one flush here (close() would add another for non-2PC, hence verify before
            // close), and a fresh load reopened (init load + the reopened one).
            verify(frontendLoad, times(1)).stopLoad();
            verify(frontendLoad, times(2)).startLoad(anyString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testApplySchemaChangeKeepsTransactionIntactForTwoPhaseCommit() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());
        SchemaChangeManager schemaChangeManager = mock(SchemaChangeManager.class);

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, true),
                            "job_1",
                            factory);
            writer.setSchemaChangeManager(schemaChangeManager);

            writer.applySchemaChange(addColumnEvent());

            // 2PC must not flush mid-checkpoint: no extra stopLoad/startLoad, only the init load.
            verify(frontendLoad, never()).stopLoad();
            verify(frontendLoad, times(1)).startLoad(anyString());
            verify(schemaChangeManager, times(1))
                    .applySchemaChange(any(TablePath.class), any(SchemaChangeEvent.class));
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testApplySchemaChangeRejectsTwoPhaseCommitWithCsvFormat() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());
        SchemaChangeManager schemaChangeManager = mock(SchemaChangeManager.class);

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, true, "csv"),
                            "job_1",
                            factory);
            writer.setSchemaChangeManager(schemaChangeManager);

            DorisSinkWriter schemaChangeWriter = writer;
            DorisSchemaChangeException exception =
                    Assertions.assertThrows(
                            DorisSchemaChangeException.class,
                            () -> schemaChangeWriter.applySchemaChange(addColumnEvent()));
            Assertions.assertTrue(exception.getMessage().contains("format=json"));

            verify(frontendLoad, never()).stopLoad();
            verify(frontendLoad, times(1)).startLoad(anyString());
            verify(schemaChangeManager, never())
                    .applySchemaChange(any(TablePath.class), any(SchemaChangeEvent.class));
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private DorisSinkConfig createSinkConfig(boolean directToBe, boolean enable2PC) {
        return createSinkConfig(directToBe, enable2PC, "json");
    }

    private DorisSinkConfig createSinkConfig(boolean directToBe, boolean enable2PC, String format) {
        return createSinkConfig(directToBe, enable2PC, format, Collections.emptyMap());
    }

    private DorisSinkConfig createSinkConfig(
            boolean directToBe,
            boolean enable2PC,
            String format,
            Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>(extraOptions);
        options.put("fenodes", "fe1:8030");
        options.put("benodes", "be1:8040");
        options.put("direct_to_be", directToBe);
        options.put("sink.enable-2pc", enable2PC);
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("sink.label-prefix", "test_job");
        options.put("doris.config", createStreamLoadProperties(format));
        return DorisSinkConfig.of(ReadonlyConfig.fromMap(options));
    }

    private Map<String, String> createStreamLoadProperties(String format) {
        Map<String, String> properties = new HashMap<>();
        properties.put("format", format);
        properties.put("read_json_by_line", "true");
        return properties;
    }

    private CatalogTable mockCatalogTable() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, (Long) null, true, null, null))
                        .build();
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTablePath()).thenReturn(TablePath.of("test_db", "test_table"));
        when(catalogTable.getTableSchema()).thenReturn(tableSchema);
        when(catalogTable.getSeaTunnelRowType()).thenReturn(tableSchema.toPhysicalRowDataType());
        return catalogTable;
    }

    private AlterTableAddColumnEvent addColumnEvent() {
        return AlterTableAddColumnEvent.add(
                TableIdentifier.of("doris", "test_db", null, "test_table"),
                PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, true, null, null));
    }

    private RespContent successRespContent(long txnId) {
        RespContent respContent = new RespContent();
        respContent.setTxnId(txnId);
        respContent.setStatus(LoadStatus.SUCCESS);
        return respContent;
    }

    private static final class RecordingStreamLoadFactory implements DorisStreamLoadFactory {
        private final Map<String, DorisStreamLoad> streamLoads = new HashMap<>();
        private final Map<String, RuntimeException> createFailures = new HashMap<>();
        private final List<String> createdHosts = new ArrayList<>();

        private void register(String host, DorisStreamLoad streamLoad) {
            streamLoads.put(host, streamLoad);
        }

        private void registerFailure(String host, RuntimeException exception) {
            createFailures.put(host, exception);
        }

        @Override
        public DorisStreamLoad create(
                String hostPort,
                TablePath tablePath,
                DorisSinkConfig dorisSinkConfig,
                LabelGenerator labelGenerator) {
            createdHosts.add(hostPort);
            RuntimeException createFailure = createFailures.get(hostPort);
            if (createFailure != null) {
                throw createFailure;
            }
            DorisStreamLoad streamLoad = streamLoads.get(hostPort);
            if (streamLoad == null) {
                throw new IllegalArgumentException("Unexpected host " + hostPort);
            }
            return streamLoad;
        }

        private List<String> getCreatedHosts() {
            return createdHosts;
        }
    }

    @Test
    void testTimestampTzZoneWarningNamesColumnsAndJvmZoneWhenZoneUnset() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "created_at", "updated_at", "local_ts"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE,
                            LocalTimeType.OFFSET_DATE_TIME_TYPE,
                            LocalTimeType.OFFSET_DATE_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        Optional<String> warning =
                DorisSinkWriter.timestampTzZoneWarning(
                        TablePath.of("db", "orders"), rowType, null, ZoneId.of("UTC"));

        Assertions.assertTrue(warning.isPresent());
        Assertions.assertTrue(warning.get().contains("db.orders"), warning.get());
        Assertions.assertTrue(warning.get().contains("[created_at, updated_at]"), warning.get());
        Assertions.assertTrue(warning.get().contains("JVM default zone UTC"), warning.get());
        Assertions.assertFalse(warning.get().contains("local_ts"), warning.get());
    }

    @Test
    void testNoTimestampTzZoneWarningWhenZoneSetOrNoTimestampTzColumn() {
        SeaTunnelRowType withTz =
                new SeaTunnelRowType(
                        new String[] {"created_at"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});
        SeaTunnelRowType withoutTz =
                new SeaTunnelRowType(
                        new String[] {"id", "local_ts"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        TablePath tablePath = TablePath.of("db", "orders");

        Assertions.assertFalse(
                DorisSinkWriter.timestampTzZoneWarning(
                                tablePath, withTz, ZoneId.of("Asia/Shanghai"), ZoneId.of("UTC"))
                        .isPresent());
        Assertions.assertFalse(
                DorisSinkWriter.timestampTzZoneWarning(tablePath, withoutTz, null, ZoneId.of("UTC"))
                        .isPresent());
    }

    @Test
    void testTimestampTzZoneWarningFindsNestedColumns() {
        SeaTunnelRowType payload =
                new SeaTunnelRowType(
                        new String[] {"name", "created_at"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, LocalTimeType.OFFSET_DATE_TIME_TYPE
                        });
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "payload", "events", "attrs"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE,
                            payload,
                            ArrayType.OFFSET_DATE_TIME_ARRAY_TYPE,
                            new MapType<>(
                                    BasicType.STRING_TYPE, LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        });

        Optional<String> warning =
                DorisSinkWriter.timestampTzZoneWarning(
                        TablePath.of("db", "orders"), rowType, null, ZoneId.of("UTC"));

        Assertions.assertTrue(warning.isPresent());
        Assertions.assertTrue(
                warning.get().contains("[payload.created_at, events[], attrs.<value>]"),
                warning.get());
    }

    @Test
    void testTimestampTzZoneWarningLoggedOnceWhenWriterCreatedWithTimestampTzColumn()
            throws Exception {
        List<String> warnings =
                captureTimestampTzZoneWarnings(
                        () -> {
                            DorisSinkWriter writer =
                                    new DorisSinkWriter(
                                            new DefaultSinkWriterContext(0, 1),
                                            new ArrayList<>(),
                                            mockCatalogTable(timestampTzColumn("created_at")),
                                            createSinkConfig(false, false),
                                            "job_1",
                                            frontendOnlyFactory());
                            try {
                                writer.setSchemaChangeManager(mock(SchemaChangeManager.class));
                                writer.applySchemaChange(addTimestampTzColumnEvent("updated_at"));
                            } finally {
                                writer.close();
                            }
                        });

        Assertions.assertEquals(1, warnings.size(), warnings.toString());
        Assertions.assertTrue(
                warnings.get(0)
                        .contains("test_db.test_table has TIMESTAMP_TZ columns [created_at]"),
                warnings.get(0));
    }

    @Test
    void testTimestampTzZoneWarningLoggedOnceWhenSchemaChangeAddsTimestampTzColumn()
            throws Exception {
        List<String> warnings =
                captureTimestampTzZoneWarnings(
                        () -> {
                            DorisSinkWriter writer =
                                    new DorisSinkWriter(
                                            new DefaultSinkWriterContext(0, 1),
                                            new ArrayList<>(),
                                            mockCatalogTable(),
                                            createSinkConfig(false, false),
                                            "job_1",
                                            frontendOnlyFactory());
                            try {
                                writer.setSchemaChangeManager(mock(SchemaChangeManager.class));
                                writer.applySchemaChange(addColumnEvent());
                                writer.applySchemaChange(addTimestampTzColumnEvent("created_at"));
                                writer.applySchemaChange(addTimestampTzColumnEvent("updated_at"));
                            } finally {
                                writer.close();
                            }
                        });

        Assertions.assertEquals(1, warnings.size(), warnings.toString());
        Assertions.assertTrue(
                warnings.get(0).contains("TIMESTAMP_TZ columns [created_at]"), warnings.get(0));
    }

    @Test
    void testNoTimestampTzZoneWarningLoggedWhenZoneConfigured() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("sink.datetime-timezone", "Asia/Shanghai");
        List<String> warnings =
                captureTimestampTzZoneWarnings(
                        () ->
                                new DorisSinkWriter(
                                                new DefaultSinkWriterContext(0, 1),
                                                new ArrayList<>(),
                                                mockCatalogTable(timestampTzColumn("created_at")),
                                                createSinkConfig(false, false, "json", options),
                                                "job_1",
                                                frontendOnlyFactory())
                                        .close());

        Assertions.assertTrue(warnings.isEmpty(), warnings.toString());
    }

    private List<String> captureTimestampTzZoneWarnings(RunnableWithException action)
            throws Exception {
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        AbstractAppender appender =
                new AbstractAppender(
                        "timestamp-tz-zone-warning", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        String message = event.getMessage().getFormattedMessage();
                        if (event.getLevel() == Level.WARN
                                && message.contains("'sink.datetime-timezone' is not set")) {
                            warnings.add(message);
                        }
                    }
                };
        appender.start();
        Logger logger = (Logger) LogManager.getLogger(DorisSinkWriter.class);
        Level originalLevel = logger.getLevel();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try {
            action.run();
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(originalLevel);
            appender.stop();
        }
        return new ArrayList<>(warnings);
    }

    private PhysicalColumn timestampTzColumn(String name) {
        return PhysicalColumn.of(
                name, LocalTimeType.OFFSET_DATE_TIME_TYPE, (Long) null, true, null, null);
    }

    private AlterTableAddColumnEvent addTimestampTzColumnEvent(String name) {
        return AlterTableAddColumnEvent.add(
                TableIdentifier.of("doris", "test_db", null, "test_table"),
                timestampTzColumn(name));
    }

    private RecordingStreamLoadFactory frontendOnlyFactory() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        when(frontendLoad.stopLoad()).thenReturn(successRespContent(41L));
        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);
        return factory;
    }

    private CatalogTable mockCatalogTable(PhysicalColumn extraColumn) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, (Long) null, true, null, null))
                        .column(extraColumn)
                        .build();
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTablePath()).thenReturn(TablePath.of("test_db", "test_table"));
        when(catalogTable.getTableSchema()).thenReturn(tableSchema);
        when(catalogTable.getSeaTunnelRowType()).thenReturn(tableSchema.toPhysicalRowDataType());
        return catalogTable;
    }
}
