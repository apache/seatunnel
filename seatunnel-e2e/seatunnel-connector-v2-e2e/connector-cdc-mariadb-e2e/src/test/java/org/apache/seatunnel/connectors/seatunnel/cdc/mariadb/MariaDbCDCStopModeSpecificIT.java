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

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset.MariaDbBinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.MariaDbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils.MariaDbContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils.MariaDbVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbConnectionUtils;
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

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.FormatDescriptionEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import io.debezium.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason =
                "Currently FLINK and SPARK do not support the bounded incremental split "
                        + "termination for MySQL CDC stop.mode=specific")
public class MariaDbCDCStopModeSpecificIT extends TestSuiteBase implements TestResource {

    private static final String MARIADB_HOST = "mysql_cdc_e2e";
    private static final String MARIADB_USER_NAME = "mysqluser";
    private static final String MARIADB_USER_PASSWORD = "mysqlpw";
    private static final String MARIADB_DATABASE = "mysql_cdc";
    private static final MariaDbContainer MARIADB_CONTAINER =
            createMariaDbContainer(MariaDbVersion.V10_11);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MARIADB_CONTAINER, MARIADB_DATABASE, "mysqluser", "mysqlpw", MARIADB_DATABASE);

    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table";

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
        log.info("Starting Mysql container for stop.mode=specific e2e test...");
        Startables.deepStart(Stream.of(MARIADB_CONTAINER)).join();
        inventoryDatabase.createAndInitialize();
        log.info("MariaDB ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MARIADB_CONTAINER != null) {
            MARIADB_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testMysqlCdcStopModeSpecificTerminatesAtStopOffset(TestContainer container)
            throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mariadbcdc_stop_mode_specific.conf";

        clearTable(MARIADB_DATABASE, SOURCE_TABLE);
        clearTable(MARIADB_DATABASE, SINK_TABLE);

        // Current binlog position, used as the start offset.
        MariaDbBinlogOffset startOffset = getCurrentMariaDbBinlogOffset();

        // Rows written before the stop offset: must be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (21), (22)",
                        MARIADB_DATABASE, SOURCE_TABLE));

        // Current binlog position after the rows above: the stop offset.
        MariaDbBinlogOffset stopOffset = getCurrentMariaDbBinlogOffset();

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

        // Wait until the rows before the stop offset are synced, so the snapshot phase
        // (if any) has completed and only the binlog phase is still running.
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> sinkIds = queryIds();
                            Assertions.assertTrue(
                                    sinkIds.contains(Collections.singletonList(21))
                                            && sinkIds.contains(Collections.singletonList(22)),
                                    "rows before the stop offset must be synced, got: " + sinkIds);
                        });

        // Rows written after the stop offset: must NOT be synced. They are inserted only
        // after the snapshot phase has completed, so they can only be picked up by the
        // binlog phase, which must stop at the configured stop offset.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (23), (24)",
                        MARIADB_DATABASE, SOURCE_TABLE));

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

    @TestTemplate
    public void testMysqlCdcStopModeSpecificWithTimestampStartup(TestContainer container)
            throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mariadbcdc_stop_mode_specific_timestamp.conf";

        clearTable(MARIADB_DATABASE, SOURCE_TABLE);
        clearTable(MARIADB_DATABASE, SINK_TABLE);

        // Rows written before the startup timestamp: must NOT be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (31), (32)",
                        MARIADB_DATABASE, SOURCE_TABLE));

        // MySQL binlog timestamps have second granularity, wait so the startup timestamp
        // is clearly after the rows above.
        Thread.sleep(3000L);

        // Take the startup timestamp before inserting the rows that must be synced,
        // so their binlog event timestamps are greater than the startup timestamp.
        long startTimestamp = getCurrentBinlogTimestamp() + 2000L;

        // Rows written after the startup timestamp: must be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (33), (34)",
                        MARIADB_DATABASE, SOURCE_TABLE));

        // Current binlog position after the rows above: the stop offset.
        MariaDbBinlogOffset stopOffset = getCurrentMariaDbBinlogOffset();

        String[] variables = {
            "start_timestamp=" + startTimestamp,
            "stop_offset_file=" + stopOffset.getFilename(),
            "stop_offset_pos=" + stopOffset.getPosition()
        };
        log.info("Startup timestamp :{}", variables[0]);

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

        // Wait until the rows after the startup timestamp are synced, so the snapshot phase
        // (if any) has completed and only the binlog phase is still running.
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> sinkIds = queryIds();
                            Assertions.assertTrue(
                                    sinkIds.contains(Collections.singletonList(33))
                                            && sinkIds.contains(Collections.singletonList(34)),
                                    "rows after the startup timestamp must be synced, got: "
                                            + sinkIds);
                        });

        // Rows written after the stop offset: must NOT be synced. They are inserted only
        // after the snapshot phase has completed, so they can only be picked up by the
        // binlog phase, which must stop at the configured stop offset.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (35), (36)",
                        MARIADB_DATABASE, SOURCE_TABLE));

        // The bounded job must terminate on its own at the stop offset.
        Container.ExecResult result = jobFuture.get(120, TimeUnit.SECONDS);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        // Only the rows written after the startup timestamp and before the stop offset
        // must be present in the sink.
        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> sinkIds = queryIds();
                            Assertions.assertTrue(
                                    sinkIds.contains(Collections.singletonList(33))
                                            && sinkIds.contains(Collections.singletonList(34)),
                                    "rows after the startup timestamp must be synced, got: "
                                            + sinkIds);
                            Assertions.assertFalse(
                                    sinkIds.contains(Collections.singletonList(31))
                                            || sinkIds.contains(Collections.singletonList(32)),
                                    "rows before the startup timestamp must not be synced, got: "
                                            + sinkIds);
                            Assertions.assertFalse(
                                    sinkIds.contains(Collections.singletonList(35))
                                            || sinkIds.contains(Collections.singletonList(36)),
                                    "rows after the stop offset must not be synced, got: "
                                            + sinkIds);
                        });
    }

    private long getCurrentBinlogTimestamp() {
        MariaDbBinlogOffset binlogOffset = getCurrentMariaDbBinlogOffset();

        JdbcSourceConfigFactory configFactory =
                new MariaDbSourceConfigFactory()
                        .hostname(MARIADB_CONTAINER.getHost())
                        .port(MARIADB_CONTAINER.getDatabasePort())
                        .username(MARIADB_CONTAINER.getUsername())
                        .password(MARIADB_CONTAINER.getPassword())
                        .databaseList(MARIADB_CONTAINER.getDatabaseName());
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        MariaDbDialect mariaDbDialect =
                new MariaDbDialect(
                        (MariaDbSourceConfigFactory) configFactory, Collections.emptyList());
        BinaryLogClient client =
                MariaDbConnectionUtils.createBinaryClient(jdbcSourceConfig.getDbzConfiguration());

        final String showBinaryLogStmt =
                "SHOW BINLOG EVENTS IN '" + binlogOffset.getFilename() + "'";
        List<Long> logPosList = new ArrayList<>();
        JdbcConnection.ResultSetConsumer rsc =
                rs -> {
                    while (rs.next()) {
                        logPosList.add(rs.getLong(5));
                    }
                };
        try (JdbcConnection jdbc = mariaDbDialect.openJdbcConnection(jdbcSourceConfig)) {
            jdbc.query(showBinaryLogStmt, rsc);
            if (logPosList.isEmpty()) {
                return System.currentTimeMillis();
            }
            Long pos =
                    logPosList.stream()
                            .distinct()
                            .sorted(Collections.reverseOrder())
                            .collect(Collectors.toList())
                            .get(1);

            ArrayBlockingQueue<Long> binlogTimestamps = new ArrayBlockingQueue<>(1);
            BinaryLogClient.EventListener eventListener =
                    event -> {
                        EventData data = event.getData();
                        if (data instanceof RotateEventData
                                || data instanceof FormatDescriptionEventData) {
                            return;
                        }
                        EventHeaderV4 header = event.getHeader();
                        long timestamp = header.getTimestamp();
                        if (timestamp > 0) {
                            binlogTimestamps.offer(timestamp);
                            try {
                                client.disconnect();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
            try {
                client.registerEventListener(eventListener);
                client.setBinlogFilename(binlogOffset.getFilename());
                client.setBinlogPosition(pos);
                client.connect();
            } finally {
                client.unregisterEventListener(eventListener);
            }
            return binlogTimestamps.take();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MARIADB_CONTAINER.getJdbcUrl(),
                MARIADB_CONTAINER.getUsername(),
                MARIADB_CONTAINER.getPassword());
    }

    private List<List<Object>> queryIds() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "select id from %s.%s", MARIADB_DATABASE, SINK_TABLE))) {
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

    private MariaDbBinlogOffset getCurrentMariaDbBinlogOffset() {
        JdbcSourceConfigFactory configFactory =
                new MariaDbSourceConfigFactory()
                        .hostname(MARIADB_CONTAINER.getHost())
                        .port(MARIADB_CONTAINER.getDatabasePort())
                        .username(MARIADB_CONTAINER.getUsername())
                        .password(MARIADB_CONTAINER.getPassword())
                        .databaseList(MARIADB_CONTAINER.getDatabaseName());
        MariaDbDialect mariaDbDialect =
                new MariaDbDialect(
                        (MariaDbSourceConfigFactory) configFactory, Collections.emptyList());
        JdbcConnection jdbcConnection = mariaDbDialect.openJdbcConnection(configFactory.create(0));
        return MariaDbConnectionUtils.currentBinlogOffset(jdbcConnection);
    }
}
