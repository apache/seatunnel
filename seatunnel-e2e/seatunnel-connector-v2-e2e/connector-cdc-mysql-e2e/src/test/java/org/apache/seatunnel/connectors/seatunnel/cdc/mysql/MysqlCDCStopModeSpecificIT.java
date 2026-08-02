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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Integration test for MySQL CDC {@code stop.mode = "specific"}.
 *
 * <p>Starts a MySQL CDC job that reads the binlog between a configured start offset and a
 * configured stop offset, and asserts that the job terminates (FINISHED) at the stop offset instead
 * of running forever, and that only the rows written before the stop offset are synced.
 */
@Slf4j
public class MysqlCDCStopModeSpecificIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table";

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

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            MysqlCDCDriverResolver::copyMySQLDriverToContainer;

    @BeforeAll
    @Override
    public void startUp() {
        log.info("Starting Mysql container for stop.mode=specific e2e test...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testMysqlCdcStopModeSpecificTerminatesAtStopOffset(TestContainer container)
            throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mysqlcdc_stop_mode_specific.conf";

        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        // Current binlog position, used as the start offset.
        BinlogOffset startOffset = getCurrentBinlogOffset();

        // Rows written before the stop offset: must be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (21), (22)", MYSQL_DATABASE, SOURCE_TABLE));

        // Current binlog position after the rows above: the stop offset.
        BinlogOffset stopOffset = getCurrentBinlogOffset();

        // Rows written after the stop offset: must NOT be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (23), (24)", MYSQL_DATABASE, SOURCE_TABLE));

        String[] variables = {
            "start_offset_file=" + startOffset.getFilename(),
            "start_offset_pos=" + startOffset.getPosition(),
            "stop_offset_file=" + stopOffset.getFilename(),
            "stop_offset_pos=" + stopOffset.getPosition()
        };

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(jobConfigFile, jobId, variables);
                            } catch (Exception e) {
                                log.error("Commit task exception :" + e.getMessage());
                                throw new RuntimeException(e);
                            }
                        });

        // The bounded job must terminate on its own at the stop offset.
        Container.ExecResult result = jobFuture.get(120, TimeUnit.SECONDS);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        // Only the rows written before the stop offset must be present in the sink.
        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> sinkIds = queryIds();
                            Assertions.assertTrue(
                                    sinkIds.contains(Collections.singletonList(21))
                                            && sinkIds.contains(Collections.singletonList(22)),
                                    "rows before the stop offset must be synced, got: " + sinkIds);
                            Assertions.assertFalse(
                                    sinkIds.contains(Collections.singletonList(23))
                                            || sinkIds.contains(Collections.singletonList(24)),
                                    "rows after the stop offset must not be synced, got: "
                                            + sinkIds);
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> queryIds() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "select id from %s.%s", MYSQL_DATABASE, SINK_TABLE))) {
            List<List<Object>> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(Collections.singletonList(resultSet.getObject(1)));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearTable(String database, String tableName) {
        try (Connection connection = getJdbcConnection()) {
            connection
                    .createStatement()
                    .execute(String.format("TRUNCATE TABLE %s.%s", database, tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private BinlogOffset getCurrentBinlogOffset() {
        JdbcSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(MYSQL_CONTAINER.getUsername())
                        .password(MYSQL_CONTAINER.getPassword())
                        .databaseList(MYSQL_CONTAINER.getDatabaseName());
        MySqlDialect mySqlDialect =
                new MySqlDialect((MySqlSourceConfigFactory) configFactory, Collections.emptyList());
        JdbcConnection jdbcConnection = mySqlDialect.openJdbcConnection(configFactory.create(0));
        return MySqlConnectionUtils.currentBinlogOffset(jdbcConnection);
    }
}
