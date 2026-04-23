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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
@DisabledOnOs(OS.WINDOWS)
public class IcebergEngineTimerFlushIT extends TestSuiteBase implements TestResource {

    private static final String CATALOG_DIR = "/tmp/seatunnel_mnt/iceberg/timer-flush-cdc/";
    private static final String CATALOG_NAME = "timer_flush_cdc_catalog";
    private static final String NAMESPACE = "timer_flush_cdc_ns";

    private static final String MYSQL_HOST = "mysql_cdc_iceberg_timer";
    private static final String MYSQL_USER = "st_user";
    private static final String MYSQL_PASSWORD = "seatunnel";
    private static final String MYSQL_DATABASE = "mysql_cdc";

    private static final String ENABLED_SRC_TABLE = "timer_flush_enabled_src";
    private static final String ENABLED_ICEBERG_TABLE = "iceberg_timer_flush_enabled";
    private static final String DISABLED_SRC_TABLE = "timer_flush_disabled_src";
    private static final String DISABLED_ICEBERG_TABLE = "iceberg_timer_flush_disabled";
    private static final String RESTORE_SRC_TABLE = "timer_flush_restore_src";
    private static final String RESTORE_ICEBERG_TABLE = "iceberg_timer_flush_restore";

    private static final MySqlContainer MYSQL_CONTAINER =
            new MySqlContainer(MySqlVersion.V8_0)
                    .withConfigurationOverride("mysql/server-gtids/my.cnf")
                    .withSetupSQL("mysql/setup.sql")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(MYSQL_HOST)
                    .withDatabaseName(MYSQL_DATABASE)
                    .withUsername(MYSQL_USER)
                    .withPassword(MYSQL_PASSWORD)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("mysql-iceberg-timer")));

    private static String mysqlDriverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    private static String zstdUrl() {
        return "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-5/zstd-jni-1.5.5-5.jar";
    }

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                for (String table :
                        new String[] {
                            ENABLED_ICEBERG_TABLE, DISABLED_ICEBERG_TABLE, RESTORE_ICEBERG_TABLE
                        }) {
                    container.execInContainer(
                            "sh",
                            "-c",
                            "mkdir -p " + CATALOG_DIR + NAMESPACE + "/" + table + "/data");
                    container.execInContainer(
                            "sh",
                            "-c",
                            "mkdir -p " + CATALOG_DIR + NAMESPACE + "/" + table + "/metadata");
                }
                container.execInContainer("sh", "-c", "chmod -R 777 " + CATALOG_DIR);

                Container.ExecResult r =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Iceberg/lib"
                                        + " && mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib"
                                        + " && cd /tmp/seatunnel/plugins/Iceberg/lib && wget -q "
                                        + zstdUrl()
                                        + " && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget -q "
                                        + mysqlDriverUrl());
                Assertions.assertEquals(0, r.getExitCode(), r.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("MySQL container started");
        inventoryDatabase.createAndInitialize();
        log.info("MySQL database initialized");
    }

    @TestTemplate
    public void testTimerFlushEnabled(TestContainer container) throws Exception {
        createSourceTable(ENABLED_SRC_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/iceberg/iceberg_engine_timer_flush_enabled.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        insertRows(ENABLED_SRC_TABLE, 1, 10);

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            int rowCount = loadIcebergRowCount(ENABLED_ICEBERG_TABLE);
                            log.info("Polling {}: {} rows", ENABLED_ICEBERG_TABLE, rowCount);
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "Engine timer (sink.flush.interval=3000) should have committed "
                                            + "Iceberg data files while the CDC job is running.");
                        });
    }

    @TestTemplate
    public void testTimerFlushDisabled(TestContainer container) throws Exception {
        createSourceTable(DISABLED_SRC_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/iceberg/iceberg_engine_timer_flush_disabled.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        insertRows(DISABLED_SRC_TABLE, 1, 10);

        given().ignoreExceptions()
                .await()
                .during(15, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            int rowCount = loadIcebergRowCount(DISABLED_ICEBERG_TABLE);
                            log.info("Polling {}: {} rows", DISABLED_ICEBERG_TABLE, rowCount);
                            Assertions.assertEquals(
                                    0,
                                    rowCount,
                                    "With enable_timer_flush=false the Iceberg table must have "
                                            + "zero committed rows while the CDC job runs.");
                        });
    }

    @TestTemplate
    public void testTimerFlushRestore(TestContainer container) throws Exception {
        createSourceTable(RESTORE_SRC_TABLE);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/iceberg/iceberg_engine_timer_flush_restore.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        insertRows(RESTORE_SRC_TABLE, 1, 10);

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            int rowCount = loadIcebergRowCount(RESTORE_ICEBERG_TABLE);
                            log.info(
                                    "Phase-1 polling {}: {} rows", RESTORE_ICEBERG_TABLE, rowCount);
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "Engine timer should commit Iceberg rows before savepoint.");
                        });

        // savepoint + restore
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/iceberg/iceberg_engine_timer_flush_restore.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        insertRows(RESTORE_SRC_TABLE, 11, 20);

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            int rowCount = loadIcebergRowCount(RESTORE_ICEBERG_TABLE);
                            log.info(
                                    "Phase-2 polling {}: {} rows", RESTORE_ICEBERG_TABLE, rowCount);
                            Assertions.assertTrue(
                                    rowCount > 10,
                                    "Restored engine timer must commit additional Iceberg rows, "
                                            + "confirming restoreWriter registers the flush action.");
                        });
    }

    private void createSourceTable(String tableName) {
        executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + MYSQL_DATABASE
                        + "."
                        + tableName
                        + " (id INT NOT NULL PRIMARY KEY, name VARCHAR(255), score INT)");
        executeSql("TRUNCATE TABLE " + MYSQL_DATABASE + "." + tableName);
    }

    private void insertRows(String tableName, int idFrom, int idTo) {
        for (int i = idFrom; i <= idTo; i++) {
            executeSql(
                    "INSERT INTO "
                            + MYSQL_DATABASE
                            + "."
                            + tableName
                            + " VALUES ("
                            + i
                            + ", 'name_"
                            + i
                            + "', "
                            + (i * 10)
                            + ")");
        }
        log.info("Inserted rows {}-{} into {}", idFrom, idTo, tableName);
    }

    private void executeSql(String sql) {
        try (Connection conn =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int loadIcebergRowCount(String tableName) {
        List<Record> results = new ArrayList<>();
        IcebergTableLoader tableLoader = getTableLoader(tableName);
        try {
            Table table = tableLoader.loadTable();
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record r : records) {
                    results.add(r);
                }
            } catch (Exception e) {
                log.debug("Iceberg table {} read error: {}", tableName, e.getMessage());
            }
        } catch (Exception ex) {
            log.debug("Iceberg table {} not yet readable: {}", tableName, ex.getMessage());
        }
        return results.size();
    }

    private IcebergTableLoader getTableLoader(String tableName) {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), CATALOG_NAME);
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), NAMESPACE);
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), tableName);
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        IcebergTableLoader tableLoader =
                IcebergTableLoader.create(new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)));
        tableLoader.open();
        return tableLoader;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }
}
