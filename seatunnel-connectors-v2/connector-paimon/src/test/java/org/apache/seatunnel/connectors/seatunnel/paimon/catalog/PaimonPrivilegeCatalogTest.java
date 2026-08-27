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

package org.apache.seatunnel.connectors.seatunnel.paimon.catalog;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceFactory;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.privilege.FileBasedPrivilegeManagerLoader;
import org.apache.paimon.privilege.NoPrivilegeException;
import org.apache.paimon.privilege.PrivilegeType;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PaimonPrivilegeCatalogTest {

    private PaimonCatalog authorizedCatalog;
    private PaimonCatalog unAuthorizedCatalog;
    private PaimonCatalog rootUserPaimonCatalog;
    private String CATALOG_NAME = "paimon_catalog";
    private String DATABASE_NAME = "test_db";
    private String TABLE_NAME = "test_table";
    private CatalogTable catalogTable;
    @TempDir protected static java.nio.file.Path temporaryFolder;
    private String warehouse;
    private String rootUser = "root";
    private String rootPassword = "123456";
    private String bucketKey = "f0";
    private String authorizeUser = "paimon";
    private String authorizeUserPassword = "123456";
    private String unAuthorizeUser = "unauthorized_paimon";
    private String unAuthorizeUserPassword = "123456";

    private int writeRows = 0;

    @BeforeAll
    public void before() {
        warehouse = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        initPrivilege();
        rootUserPaimonCatalog = createPaimonCatalog(rootUser, rootPassword);
        authorizedCatalog = createPaimonCatalog(authorizeUser, authorizeUserPassword);
        unAuthorizedCatalog = createPaimonCatalog(unAuthorizeUser, unAuthorizeUserPassword);

        createUser(authorizeUser, authorizeUserPassword);
        grantPrivilege(
                authorizeUser,
                new PrivilegeType[] {
                    PrivilegeType.CREATE_TABLE,
                    PrivilegeType.ALTER_TABLE,
                    PrivilegeType.SELECT,
                    PrivilegeType.INSERT
                });
        createUser(unAuthorizeUser, unAuthorizeUserPassword);

        createDatabase();
        catalogTable = buildTable(TABLE_NAME);

        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        rootUserPaimonCatalog.createTable(tablePath, catalogTable, false);
    }

    private CatalogTable buildTable(String tableName) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < 5; i++) {
            schemaBuilder.column(
                    PhysicalColumn.of(
                            "f" + i,
                            BasicType.STRING_TYPE,
                            (Long) null,
                            false,
                            null,
                            String.format("f%s col", i)));
        }

        TableSchema tableSchema =
                schemaBuilder.primaryKey(PrimaryKey.of("pk", Arrays.asList("f0"))).build();

        CatalogTable cTable =
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test table");
        return cTable;
    }

    private void initPrivilege() {
        org.apache.paimon.options.Options catalogOptions = new org.apache.paimon.options.Options();
        catalogOptions.set(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        FileIO fileIO = new ResolvingFileIO();
        fileIO.configure(catalogContext);

        PrivilegedCatalog priCatalog =
                new PrivilegedCatalog(
                        CatalogFactory.createCatalog(catalogContext),
                        new FileBasedPrivilegeManagerLoader(
                                warehouse, fileIO, rootUser, rootPassword));
        if (!priCatalog.privilegeManager().privilegeEnabled()) {
            priCatalog.privilegeManager().initializePrivilege(rootPassword);
        }
    }

    private void createUser(String user, String password) {
        Optional<Object> catalog = ReflectionUtils.getField(rootUserPaimonCatalog, "catalog");
        assertTrue(catalog.isPresent() && catalog.get() instanceof PrivilegedCatalog);
        PrivilegedCatalog priCatalog = (PrivilegedCatalog) catalog.get();
        priCatalog.privilegeManager().createUser(user, password);
    }

    private void grantPrivilege(String user, PrivilegeType[] privilegeTypes) {
        Optional<Object> catalog = ReflectionUtils.getField(rootUserPaimonCatalog, "catalog");
        assertTrue(catalog.isPresent() && catalog.get() instanceof PrivilegedCatalog);
        PrivilegedCatalog priCatalog = (PrivilegedCatalog) catalog.get();
        String fullTableName = Identifier.create(DATABASE_NAME, TABLE_NAME).getFullName();
        for (PrivilegeType type : privilegeTypes) {
            if (type == PrivilegeType.CREATE_TABLE) {
                priCatalog
                        .privilegeManager()
                        .grant(user, DATABASE_NAME, PrivilegeType.CREATE_TABLE);
            } else {
                priCatalog.privilegeManager().grant(user, fullTableName, type);
            }
        }
    }

    private void createDatabase() {
        try {
            TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
            rootUserPaimonCatalog.createDatabase(tablePath, false);
        } catch (DatabaseAlreadyExistException e) {
            log.info("database already exist");
        }
    }

    private Map<String, Object> getPaimonProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", warehouse);
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("bucket", "2");
        writeProps.put("bucket-key", bucketKey);
        properties.put("paimon.table.write-props", writeProps);
        return properties;
    }

    private PaimonCatalog createPaimonCatalog(String user, String password) {
        Map<String, Object> properties = getPaimonProperties();
        if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password)) {
            properties.put("user", user);
            properties.put("password", password);
        }
        PaimonCatalog pCatalog =
                new PaimonCatalog(CATALOG_NAME, ReadonlyConfig.fromMap(properties));
        pCatalog.open();
        return pCatalog;
    }

    @Test
    public void createCatalogWithNotUserAndPassword() {
        assertThrows(
                PaimonConnectorException.class,
                () -> {
                    try {
                        createPaimonCatalog(null, null);
                    } catch (PaimonConnectorException e) {
                        assertTrue(
                                e.getMessage()
                                        .contains(
                                                "paimon privilege is enabled, user and password is required"));
                        throw e;
                    }
                });
    }

    @Test
    public void createCatalogWithErrorPassword() {
        PaimonCatalog catalog = createPaimonCatalog(authorizeUser, "errorpassword");
        assertThrows(
                CatalogException.class,
                () -> {
                    TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
                    try {
                        catalog.createTable(tablePath, catalogTable, false);
                    } catch (CatalogException e) {
                        assertTrue(
                                e.getCause()
                                        .getMessage()
                                        .contains(
                                                String.format(
                                                        "User %s not found, or password incorrect.",
                                                        authorizeUser)));
                        throw e;
                    }
                });
    }

    @Test
    public void testCreateTable() {
        TablePath tablePath = TablePath.of(DATABASE_NAME, "privilege_test_table");
        CatalogTable catalogTable = buildTable("privilege_test_table");
        // The permission to create tables
        authorizedCatalog.createTable(tablePath, catalogTable, false);

        // No permission to create tables
        assertThrows(
                CatalogException.class,
                () -> {
                    try {
                        unAuthorizedCatalog.createTable(tablePath, catalogTable, false);
                    } catch (CatalogException e) {
                        assertTrue(
                                e.getCause()
                                        .getMessage()
                                        .contains(
                                                String.format(
                                                        "User %s doesn't have privilege CREATE_TABLE on",
                                                        unAuthorizeUser)));
                        throw e;
                    }
                });
    }

    @Test
    public void testAlertTable() {
        Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
        SchemaChange change = SchemaChange.addColumn("f5", DataTypes.STRING());
        authorizedCatalog.alterTable(identifier, change, false);

        assertThrows(
                NoPrivilegeException.class,
                () -> {
                    try {
                        unAuthorizedCatalog.alterTable(identifier, change, false);
                    } catch (NoPrivilegeException e) {
                        assertTrue(
                                e.getMessage()
                                        .contains(
                                                "User "
                                                        + unAuthorizeUser
                                                        + " doesn't have privilege ALTER_TABLE on table"));
                        throw e;
                    }
                });
    }

    @Test
    @Order(2)
    public void testWriteTable() throws IOException {
        List<SeaTunnelRow> rows = getWriteRows();
        writeTable(authorizedCatalog, rows);
        writeRows = rows.size();

        assertThrows(
                NoPrivilegeException.class,
                () -> {
                    try {
                        writeTable(unAuthorizedCatalog, rows);
                    } catch (NoPrivilegeException e) {
                        assertTrue(
                                e.getMessage()
                                        .contains(
                                                String.format(
                                                        "User %s doesn't have privilege INSERT on table",
                                                        unAuthorizeUser)));
                        throw e;
                    }
                });
    }

    @Test
    @Order(3)
    public void testReadTable() throws Exception {
        List<SeaTunnelRow> rows = readTable(authorizedCatalog);
        assertTrue(rows.size() == writeRows);

        assertThrows(
                NoPrivilegeException.class,
                () -> {
                    try {
                        readTable(unAuthorizedCatalog);
                    } catch (NoPrivilegeException e) {
                        assertTrue(
                                e.getMessage()
                                        .contains(
                                                "User "
                                                        + unAuthorizeUser
                                                        + " doesn't have privilege SELECT on table"));
                        throw e;
                    }
                });
    }

    private List<SeaTunnelRow> readTable(PaimonCatalog paimonCatalog) throws Exception {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);

        Optional<Object> config = ReflectionUtils.getField(paimonCatalog, "readonlyConfig");
        assertTrue(config.isPresent() && config.get() instanceof ReadonlyConfig);
        ReadonlyConfig readonlyConfig = (ReadonlyConfig) config.get();

        PaimonSourceFactory factory = new PaimonSourceFactory();
        SeaTunnelSource<Object, SourceSplit, Serializable> source =
                factory.createSource(
                                new TableSourceFactoryContext(
                                        readonlyConfig,
                                        Thread.currentThread().getContextClassLoader()))
                        .createSource();
        source.setJobContext(context);
        Set<Integer> registeredReaders = new HashSet<>();
        List<SourceReader> readers = new ArrayList<>();
        Set<Integer> unfinishedReaders = new HashSet<>();
        int parallelism = 1;
        SourceSplitEnumerator enumerator =
                source.createEnumerator(
                        new SourceSplitEnumerator.Context<SourceSplit>() {
                            @Override
                            public int currentParallelism() {
                                return parallelism;
                            }

                            @Override
                            public Set<Integer> registeredReaders() {
                                return registeredReaders;
                            }

                            @Override
                            public void assignSplit(int subtaskId, List<SourceSplit> splits) {
                                if (registeredReaders().isEmpty()) {
                                    return;
                                }
                                SourceReader reader = readers.get(subtaskId);
                                if (splits.isEmpty()) {
                                    reader.handleNoMoreSplits();
                                } else {
                                    reader.addSplits(splits);
                                }
                            }

                            @Override
                            public void signalNoMoreSplits(int subtask) {
                                SourceReader reader = readers.get(subtask);
                                reader.handleNoMoreSplits();
                            }

                            @Override
                            public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
                                SourceReader reader = readers.get(subtaskId);
                                reader.handleSourceEvent(event);
                            }

                            @Override
                            public MetricsContext getMetricsContext() {
                                return new AbstractMetricsContext() {};
                            }

                            @Override
                            public EventListener getEventListener() {
                                return event -> {};
                            }
                        });
        enumerator.open();
        for (int i = 0; i < parallelism; i++) {
            int finalI = i;
            SourceReader<Object, SourceSplit> reader =
                    source.createReader(
                            new SourceReader.Context() {
                                @Override
                                public int getIndexOfSubtask() {
                                    return finalI;
                                }

                                @Override
                                public Boundedness getBoundedness() {
                                    return Boundedness.BOUNDED;
                                }

                                @Override
                                public void signalNoMoreElement() {
                                    unfinishedReaders.remove(finalI);
                                }

                                @Override
                                public void sendSplitRequest() {
                                    enumerator.handleSplitRequest(finalI);
                                }

                                @Override
                                public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
                                    enumerator.handleSourceEvent(finalI, sourceEvent);
                                }

                                @Override
                                public MetricsContext getMetricsContext() {
                                    return new AbstractMetricsContext() {};
                                }

                                @Override
                                public EventListener getEventListener() {
                                    return event -> {};
                                }
                            });
            unfinishedReaders.add(i);
            registeredReaders.add(i);
            readers.add(reader);
            enumerator.registerReader(i);
        }
        enumerator.run();

        List<SeaTunnelRow> rows = new ArrayList<>();
        while (!unfinishedReaders.isEmpty()) {
            for (int i = 0; i < parallelism; i++) {
                SourceReader reader = readers.get(i);
                if (unfinishedReaders.contains(i)) {
                    reader.pollNext(
                            new Collector() {
                                @Override
                                public void collect(Object record) {
                                    rows.add((SeaTunnelRow) record);
                                }

                                @Override
                                public Object getCheckpointLock() {
                                    return reader;
                                }
                            });
                }
            }
        }
        enumerator.close();
        for (SourceReader reader : readers) {
            reader.close();
        }

        return rows;
    }

    private List<SeaTunnelRow> getWriteRows() {
        List<SeaTunnelRow> rows =
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"f0", "f1", "f2", "f3", "f4"}),
                        new SeaTunnelRow(new Object[] {"f10", "f11", "f12", "f13", "f14"}));
        return rows;
    }

    private void writeTable(PaimonCatalog paimonCatalog, List<SeaTunnelRow> rows)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);

        Optional<Object> config = ReflectionUtils.getField(paimonCatalog, "readonlyConfig");
        assertTrue(config.isPresent() && config.get() instanceof ReadonlyConfig);
        ReadonlyConfig readonlyConfig = (ReadonlyConfig) config.get();
        TableSinkFactoryContext tableSinkFactoryContext =
                new TableSinkFactoryContext(
                        catalogTable,
                        readonlyConfig,
                        Thread.currentThread().getContextClassLoader());

        PaimonSinkFactory factory = new PaimonSinkFactory();
        SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink =
                factory.createSink(tableSinkFactoryContext).createSink();
        sink.setJobContext(context);
        int parallelism = 1;
        List<Object> commitInfos = new ArrayList<>();

        for (int i = 0; i < parallelism; i++) {
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                    sink.createWriter(new DefaultSinkWriterContext(i, parallelism));
            for (SeaTunnelRow row : rows) {
                sinkWriter.write(row);
            }
            Optional<?> commitInfo = sinkWriter.prepareCommit(1);
            sinkWriter.snapshotState(1);
            sinkWriter.close();
            if (commitInfo.isPresent()) {
                commitInfos.add(commitInfo.get());
            }
        }

        Optional<? extends SinkCommitter<?>> sinkCommitter = sink.createCommitter();
        Optional<? extends SinkAggregatedCommitter<?, ?>> aggregatedCommitterOptional =
                sink.createAggregatedCommitter();

        if (!commitInfos.isEmpty()) {
            if (aggregatedCommitterOptional.isPresent()) {
                SinkAggregatedCommitter<?, ?> aggregatedCommitter =
                        aggregatedCommitterOptional.get();
                MultiTableResourceManager resourceManager = null;
                if (aggregatedCommitter instanceof SupportMultiTableSinkAggregatedCommitter) {
                    resourceManager =
                            ((SupportMultiTableSinkAggregatedCommitter<?>) aggregatedCommitter)
                                    .initMultiTableResourceManager(1, 1);
                }
                aggregatedCommitter.init();
                if (resourceManager != null) {
                    ((SupportMultiTableSinkAggregatedCommitter<?>) aggregatedCommitter)
                            .setMultiTableResourceManager(resourceManager, 0);
                }

                Object aggregatedCommitInfoT =
                        ((SinkAggregatedCommitter) aggregatedCommitter).combine(commitInfos);
                ((SinkAggregatedCommitter) aggregatedCommitter)
                        .commit(Collections.singletonList(aggregatedCommitInfoT));
                aggregatedCommitter.close();
            } else if (sinkCommitter.isPresent()) {
                ((SinkCommitter) sinkCommitter.get()).commit(commitInfos);
            } else {
                throw new RuntimeException("No committer found");
            }
        }
    }

    @AfterAll
    public void after() {
        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        try {
            rootUserPaimonCatalog.dropTable(tablePath, false);
            rootUserPaimonCatalog.dropDatabase(tablePath, false);
        } catch (TableNotExistException e) {
            log.info("table not exist");
        } catch (DatabaseNotExistException e) {
            log.info("database not exist");
        }
        rootUserPaimonCatalog.close();
    }
}
