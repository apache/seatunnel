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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class DorisCDCSinkIT extends AbstractDorisIT {

    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    private static final String DDL_SINK =
            "CREATE TABLE IF NOT EXISTS "
                    + DATABASE
                    + "."
                    + SINK_TABLE
                    + " (\n"
                    + "  uuid   BIGINT,\n"
                    + "  name    VARCHAR(128),\n"
                    + "  score   INT\n"
                    + ")ENGINE=OLAP\n"
                    + "UNIQUE KEY(`uuid`)\n"
                    + "DISTRIBUTED BY HASH(`uuid`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_allocation\" = \"tag.location.default: 1\""
                    + ")";

    // mysql
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String LARGEINT_SOURCE_TABLE = "doris_largeint_source";
    private static final String LARGEINT_SINK_TABLE = "doris_largeint_sink";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar mysqlDriver = DependencyJar.of(com.mysql.cj.jdbc.Driver.class);
                mysqlDriver.copyTo(container, "/tmp/seatunnel/plugins/Doris-CDC/lib");
                mysqlDriver.copyTo(container, "/tmp/seatunnel/plugins/jdbc/lib");
            };

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    @BeforeAll
    public void init() {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
        initializeJdbcTable();
    }

    @AfterAll
    public void close() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testDorisCDCSink(TestContainer container) throws Exception {

        clearTable(DATABASE, SINK_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/write-cdc-changelog-to-doris.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        String sinkSql = String.format("select * from %s.%s", DATABASE, SINK_TABLE);

        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice", 95), Arrays.asList(2L, "Bob", 88))
                        .collect(Collectors.toSet());

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Set<List<Object>> actual = new HashSet<>();
                            try (Statement sinkStatement = jdbcConnection.createStatement();
                                    ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql)) {
                                while (sinkResultSet.next()) {
                                    List<Object> row =
                                            Arrays.asList(
                                                    sinkResultSet.getLong("uuid"),
                                                    sinkResultSet.getString("name"),
                                                    sinkResultSet.getInt("score"));
                                    actual.add(row);
                                }
                            }
                            Assertions.assertIterableEquals(expected, actual);
                        });

        executeSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE + " WHERE uuid = 1");

        Set<List<Object>> expectedAfterDelete =
                Stream.<List<Object>>of(Arrays.asList(2L, "Bob", 88)).collect(Collectors.toSet());

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Set<List<Object>> actual = new HashSet<>();
                            try (Statement sinkStatement = jdbcConnection.createStatement();
                                    ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql)) {
                                while (sinkResultSet.next()) {
                                    List<Object> row =
                                            Arrays.asList(
                                                    sinkResultSet.getLong("uuid"),
                                                    sinkResultSet.getString("name"),
                                                    sinkResultSet.getInt("score"));
                                    actual.add(row);
                                }
                            }
                            Assertions.assertIterableEquals(expectedAfterDelete, actual);
                        });
        executeSql(
                "INSERT INTO " + MYSQL_DATABASE + "." + SOURCE_TABLE + " VALUES (1, 'Alice', 95)");
    }

    @TestTemplate
    public void testDorisLargeintToMysqlAutoCreate(TestContainer container) throws Exception {
        executeDorisSql(
                "CREATE TABLE IF NOT EXISTS "
                        + DATABASE
                        + "."
                        + LARGEINT_SOURCE_TABLE
                        + " (id INT, large_value LARGEINT) ENGINE=OLAP DUPLICATE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES "
                        + "(\"replication_allocation\" = \"tag.location.default: 1\")");
        executeDorisSql("TRUNCATE TABLE " + DATABASE + "." + LARGEINT_SOURCE_TABLE);
        executeDorisSql(
                "INSERT INTO "
                        + DATABASE
                        + "."
                        + LARGEINT_SOURCE_TABLE
                        + " VALUES (1, 170141183460469231731687303715884105727), "
                        + "(2, -170141183460469231731687303715884105728), "
                        + "(3, 123456789012345678901)");
        executeSql("DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + LARGEINT_SINK_TABLE);

        Container.ExecResult result = container.executeJob("/doris_largeint_to_mysql.conf");
        Assertions.assertEquals(0, result.getExitCode());

        List<BigDecimal> values = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT large_value FROM "
                                        + MYSQL_DATABASE
                                        + "."
                                        + LARGEINT_SINK_TABLE
                                        + " ORDER BY id")) {
            while (resultSet.next()) {
                values.add(resultSet.getBigDecimal(1));
            }
        }
        Assertions.assertEquals(
                Arrays.asList(
                        new BigDecimal("170141183460469231731687303715884105727"),
                        new BigDecimal("-170141183460469231731687303715884105728"),
                        new BigDecimal("123456789012345678901")),
                values);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT NUMERIC_PRECISION FROM information_schema.columns "
                                        + "WHERE table_schema = '"
                                        + MYSQL_DATABASE
                                        + "' AND table_name = '"
                                        + LARGEINT_SINK_TABLE
                                        + "' AND column_name = 'large_value'")) {
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(39, resultSet.getInt(1));
        }
    }

    private void initializeJdbcTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            // create databases
            statement.execute(CREATE_DATABASE);
            // create sink table
            statement.execute(DDL_SINK);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private void executeDorisSql(String sql) {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearTable(String database, String tableName) {
        executeDorisSql("truncate table " + database + "." + tableName);
    }
}
