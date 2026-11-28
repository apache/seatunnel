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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSource;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceState;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.DLFAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(60)
class PaimonRestCatalogIT {
    @TempDir private Path directory;

    @Test
    void testBearerCatalogSourceSinkAndSaveMode() throws Exception {
        RESTCatalogServer server =
                new RESTCatalogServer(
                        directory.toString(),
                        new BearTokenAuthProvider("test-bearer"),
                        serverConfig(),
                        "managed-warehouse");
        server.start();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Map<String, Object> options = options(server.getUrl());
            options.put("token.provider", "bear");
            options.put("token", "test-bearer");
            ReadonlyConfig config = ReadonlyConfig.fromMap(options);
            CatalogTable table = catalogTable();
            PaimonSink sink =
                    (PaimonSink)
                            new PaimonSinkFactory()
                                    .createSink(
                                            new TableSinkFactoryContext(
                                                    table, config, getClass().getClassLoader()))
                                    .createSink();
            try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
                handler.open();
                handler.handleSaveMode();
            }
            JobContext job = new JobContext(1L);
            job.setJobMode(JobMode.BATCH);
            job.setEnableCheckpoint(false);
            sink.setJobContext(job);
            PaimonSinkWriter writer = sink.createWriter(new DefaultSinkWriterContext(0, 1));
            SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo> committer =
                    sink.createAggregatedCommitter().get();
            try {
                writer.write(new SeaTunnelRow(new Object[] {1, "rest-row"}));
                PaimonCommitInfo commit = writer.prepareCommit(1L).get();
                committer.init();
                assertTrue(
                        committer
                                .commit(
                                        Collections.singletonList(
                                                committer.combine(
                                                        Collections.singletonList(commit))))
                                .isEmpty());
            } finally {
                writer.close();
                committer.close();
            }
            PaimonSource source =
                    (PaimonSource)
                            new PaimonSourceFactory()
                                    .<SeaTunnelRow, PaimonSourceSplit, PaimonSourceState>
                                            createSource(
                                                    new TableSourceFactoryContext(
                                                            config, getClass().getClassLoader()))
                                    .createSource();
            assertEquals(1, source.getProducedCatalogTables().size());
            source.setJobContext(job);
            readThroughSource(source);
            try (PaimonCatalog catalog =
                    (PaimonCatalog) new PaimonCatalogFactory().createCatalog("paimon", config)) {
                catalog.open();
                assertTrue(catalog.listDatabases().contains("test_db"));
                assertTrue(catalog.listTables("test_db").contains("test_table"));
                Table paimonTable = catalog.getPaimonTable(TablePath.of("test_db", "test_table"));
                ReadBuilder read = paimonTable.newReadBuilder();
                try (RecordReaderIterator<InternalRow> rows =
                        new RecordReaderIterator<>(
                                read.newRead().createReader(read.newScan().plan()))) {
                    assertTrue(rows.hasNext());
                    InternalRow row = rows.next();
                    assertEquals(1, row.getInt(0));
                    assertEquals("rest-row", row.getString(1).toString());
                    assertFalse(rows.hasNext());
                }
            }
            options.put("token", "wrong-bearer");
            assertThrows(
                    PaimonConnectorException.class,
                    () ->
                            new PaimonCatalogLoader(
                                            new PaimonConfig(ReadonlyConfig.fromMap(options)))
                                    .loadCatalog());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            server.shutdown();
        }
    }

    @Test
    void testServerStorageDefaultIsNotOverriddenByHadoopDefaults() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put(RESTCatalogInternalOptions.PREFIX.key(), "test");
        defaults.put("fs.s3a.path.style.access", "true");
        RESTCatalogServer server =
                new RESTCatalogServer(
                        directory.toString(),
                        new BearTokenAuthProvider("test-bearer"),
                        new ConfigResponse(defaults, Collections.emptyMap()),
                        "managed-warehouse");
        server.start();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Map<String, Object> options = options(server.getUrl());
            options.put("token.provider", "bear");
            options.put("token", "test-bearer");
            try (Catalog catalog =
                    new PaimonCatalogLoader(new PaimonConfig(ReadonlyConfig.fromMap(options)))
                            .loadCatalog()) {
                assertEquals("true", catalog.options().get("fs.s3a.path.style.access"));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            server.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    private void readThroughSource(PaimonSource source) throws Exception {
        SourceReader.Context readerContext = mock(SourceReader.Context.class);
        when(readerContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext =
                mock(SourceSplitEnumerator.Context.class);
        when(enumeratorContext.registeredReaders()).thenReturn(Collections.singleton(0));
        when(enumeratorContext.currentParallelism()).thenReturn(1);
        List<SeaTunnelRow> records = new ArrayList<>();
        try (SourceReader<SeaTunnelRow, PaimonSourceSplit> reader =
                        source.createReader(readerContext);
                SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> enumerator =
                        source.createEnumerator(enumeratorContext)) {
            doAnswer(
                            invocation -> {
                                reader.addSplits(invocation.getArgument(1));
                                return null;
                            })
                    .when(enumeratorContext)
                    .assignSplit(eq(0), anyList());
            doAnswer(
                            invocation -> {
                                reader.handleNoMoreSplits();
                                return null;
                            })
                    .when(enumeratorContext)
                    .signalNoMoreSplits(0);
            reader.open();
            enumerator.open();
            enumerator.registerReader(0);
            enumerator.run();
            reader.pollNext(
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow row) {
                            records.add(row);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return records;
                        }
                    });
            verify(readerContext).signalNoMoreElement();
        }
        assertEquals(1, records.size());
        assertEquals(1, records.get(0).getField(0));
        assertEquals("rest-row", records.get(0).getField(1));
        assertEquals("test_db.test_table", records.get(0).getTableId());
    }

    @Test
    void testDlfSignedRequestAgainstLocalServer() throws Exception {
        RESTCatalogServer server =
                new RESTCatalogServer(
                        directory.toString(),
                        DLFAuthProvider.buildAKToken(
                                "test-id", "test-secret", "test-session", "cn-hangzhou"),
                        serverConfig(),
                        "managed-warehouse");
        server.start();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Map<String, Object> options = options(server.getUrl());
            options.put("token.provider", "dlf");
            options.put("dlf.access-key-id", "test-id");
            options.put("dlf.access-key-secret", "test-secret");
            options.put("dlf.security-token", "test-session");
            options.put("dlf.region", "cn-hangzhou");
            try (Catalog catalog =
                    new PaimonCatalogLoader(new PaimonConfig(ReadonlyConfig.fromMap(options)))
                            .loadCatalog()) {
                catalog.createDatabase("dlf_test", false);
                assertTrue(catalog.listDatabases().contains("dlf_test"));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            server.shutdown();
        }
    }

    private ConfigResponse serverConfig() {
        return new ConfigResponse(
                Collections.singletonMap(RESTCatalogInternalOptions.PREFIX.key(), "test"),
                Collections.emptyMap());
    }

    private Map<String, Object> options(String uri) {
        Map<String, Object> options = new HashMap<>();
        options.put("catalog_type", "rest");
        options.put("catalog_uri", uri);
        options.put("warehouse", "managed-warehouse");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("paimon.table.write-props", Collections.singletonMap("bucket", "1"));
        return options;
    }

    private CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "value",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("paimon", "test_db", null, "test_table"),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "REST test");
    }
}
