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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb;

import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils.MariaDbContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils.MariaDbVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support restore")
public class MariaDbCDCWithMissingBinlogDiagnosticsIT extends TestSuiteBase
        implements TestResource {
    private static final String MARIADB_HOST = "mysql_cdc_e2e";
    private static final String MARIADB_USER_NAME = "mysqluser";
    private static final String MARIADB_USER_PASSWORD = "mysqlpw";
    private static final String MARIADB_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table";

    private static final MariaDbContainer MARIADB_CONTAINER =
            createMariaDbContainer(MariaDbVersion.V10_11);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MARIADB_CONTAINER, MARIADB_DATABASE, "mysqluser", "mysqlpw", MARIADB_DATABASE);

    private static MariaDbContainer createMariaDbContainer(MariaDbVersion version) {
        return new MariaDbContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MARIADB_HOST, "mariadb_cdc_e2e")
                .withDatabaseName(MARIADB_DATABASE)
                .withUsername(MARIADB_USER_NAME)
                .withPassword(MARIADB_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(
                                DockerLoggerFactory.getLogger("mariadb-docker-image")));
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.ofClassName("org.mariadb.jdbc.Driver")
                        .copyTo(container, "/tmp/seatunnel/plugins/MariaDB-CDC/lib");
                DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                        .copyTo(container, "/tmp/seatunnel/plugins/MariaDB-CDC/lib");
            };

    @BeforeAll
    @Override
    public void startUp() {
        log.info("Starting MariaDB containers...");
        Startables.deepStart(Stream.of(MARIADB_CONTAINER)).join();
        log.info("MariaDB Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("MariaDB ddl execution is complete");
    }

    @Override
    @AfterAll
    public void tearDown() {
        if (MARIADB_CONTAINER != null) {
            MARIADB_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testRestoreFailsAndLogsDiagnosticsWhenBinlogMissing(TestContainer container)
            throws InterruptedException, IOException {
        clearTable(MARIADB_DATABASE, SINK_TABLE);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mariadbcdc_to_mariadb_with_binlog_delete.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> source =
                                    query(
                                            "select count(1) from "
                                                    + MARIADB_DATABASE
                                                    + "."
                                                    + SOURCE_TABLE);
                            List<List<Object>> sink =
                                    query(
                                            "select count(1) from "
                                                    + MARIADB_DATABASE
                                                    + "."
                                                    + SINK_TABLE);
                            Assertions.assertEquals(source, sink);
                        });

        // Ensure checkpoint offset is in a newer binlog file
        executeSql("flush binary logs");
        executeSql(
                "INSERT INTO "
                        + MARIADB_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (id, f_int, f_varchar) VALUES (200, 200, 'binlog_rotate')");

        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        query(
                                                "select count(1) from "
                                                        + MARIADB_DATABASE
                                                        + "."
                                                        + SOURCE_TABLE),
                                        query(
                                                "select count(1) from "
                                                        + MARIADB_DATABASE
                                                        + "."
                                                        + SINK_TABLE)));

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // Rotate again, then purge all previous binlogs (including the one referenced by savepoint)
        executeSql("flush binary logs");
        String purgeToBinlog = query("show master status").get(0).get(0).toString();
        executeSql("purge binary logs to '" + purgeToBinlog + "'");

        // Restore should fail due to missing binlog, but must output diagnostics for
        // troubleshooting
        Container.ExecResult restoreResult =
                container.restoreJob(
                        "/mariadbcdc_to_mariadb_with_binlog_delete.conf", String.valueOf(jobId));
        Assertions.assertNotEquals(
                0,
                restoreResult.getExitCode(),
                "Expected restore to fail due to missing binlog, but exit code is 0");

        String logs =
                restoreResult.getStdout()
                        + "\n"
                        + restoreResult.getStderr()
                        + "\n"
                        + container.getServerLogs();
        Assertions.assertTrue(
                logs.contains("MariaDB-CDC diagnostic: connected to MariaDB"),
                "Expected diagnostic logs to include MariaDB server identity, but not found in server logs");
        Assertions.assertTrue(
                logs.contains("Connector requires binlog file")
                        || logs.contains("MariaDB-CDC diagnostic: GTID set is not available")
                        || logs.contains("MariaDB-CDC diagnostic: variable")
                        || logs.contains(
                                "Some of the GTIDs needed to replicate have been already purged"),
                "Expected diagnostic logs to include missing binlog or GTID diagnostics, but not found in logs");
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MARIADB_CONTAINER.getJdbcUrl(),
                MARIADB_CONTAINER.getUsername(),
                MARIADB_CONTAINER.getPassword());
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
