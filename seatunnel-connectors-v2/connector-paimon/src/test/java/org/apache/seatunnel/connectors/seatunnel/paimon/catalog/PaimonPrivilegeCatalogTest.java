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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssignerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.privilege.FileBasedPrivilegeManagerLoader;
import org.apache.paimon.privilege.NoPrivilegeException;
import org.apache.paimon.privilege.PrivilegeType;
import org.apache.paimon.privilege.PrivilegedCatalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Slf4j
public class PaimonPrivilegeCatalogTest {

    private static PaimonCatalog authorizedCatalog;
    private static PaimonCatalog unAuthorizedCatalog;
    private static PaimonCatalog rootUserPaimonCatalog;
    private static String CATALOG_NAME = "paimon_catalog";
    private static String DATABASE_NAME = "test_db";
    private static String TABLE_NAME = "test_table";

    private PaimonSinkWriter paimonSinkWriter;
    private static SinkWriter.Context context;
    private final String commitUser = UUID.randomUUID().toString();
    private static CatalogTable catalogTable;
    @TempDir protected static java.nio.file.Path temporaryFolder;
    private static String warehouse;
    private static String rootUser = "root";
    private static String rootPassword = "123456";
    private static String bucketKey = "f0";
    private static String authorizeUser = "paimon";
    private static String authorizeUserPassword = "123456";
    private static String unAuthorizeUser = "unauthorized_paimon";
    private static String unAuthorizeUserPassword = "123456";

    @BeforeAll
    public static void before() {
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

        context =
                new SinkWriter.Context() {
                    @Override
                    public int getIndexOfSubtask() {
                        return 0;
                    }

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };
    }

    private static CatalogTable buildTable(String tableName) {
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

    private static void initPrivilege() {
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

    private static void createUser(String user, String password) {
        Optional<Object> catalog = ReflectionUtils.getField(rootUserPaimonCatalog, "catalog");
        assertTrue(catalog.isPresent() && catalog.get() instanceof PrivilegedCatalog);
        PrivilegedCatalog priCatalog = (PrivilegedCatalog) catalog.get();
        priCatalog.privilegeManager().createUser(user, password);
    }

    private static void grantPrivilege(String user, PrivilegeType[] privilegeTypes) {
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

    private static void createDatabase() {
        try {
            TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
            rootUserPaimonCatalog.createDatabase(tablePath, false);
        } catch (DatabaseAlreadyExistException e) {
            log.info("database already exist");
        }
    }

    private static Map<String, Object> getPaimonProperties() {
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

    private static PaimonCatalog createPaimonCatalog(String user, String password) {
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
    public void testWriteTable() {
        writeTable(authorizedCatalog);

        assertThrows(
                NoPrivilegeException.class,
                () -> {
                    try {
                        writeTable(unAuthorizedCatalog);
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

    private void writeTable(PaimonCatalog paimonCatalog) {
        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);

        JobContext jobContext = new JobContext();
        jobContext.setJobMode(JobMode.BATCH);

        Optional<Object> config = ReflectionUtils.getField(paimonCatalog, "readonlyConfig");
        assertTrue(config.isPresent() && config.get() instanceof ReadonlyConfig);
        ReadonlyConfig readonlyConfig = (ReadonlyConfig) config.get();

        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        commitUser,
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration(),
                        new PaimonBucketAssignerFactory());

        String[] fields = catalogTable.getTableSchema().getFieldNames();
        SeaTunnelRow row = new SeaTunnelRow(fields.length);
        try {
            for (int j = 0; j < fields.length; j++) {
                row.setField(j, "val" + j);
            }
            paimonSinkWriter.write(row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void after() {
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
