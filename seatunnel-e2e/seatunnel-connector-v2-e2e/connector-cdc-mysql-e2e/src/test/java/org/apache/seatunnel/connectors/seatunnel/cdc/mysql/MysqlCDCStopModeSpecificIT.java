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

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
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
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

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
                        "INSERT INTO %s.%s (id) VALUES (23), (24)", MYSQL_DATABASE, SOURCE_TABLE));

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
        String jobConfigFile = "/mysqlcdc_stop_mode_specific_timestamp.conf";

        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        // Rows written before the startup timestamp: must NOT be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (31), (32)", MYSQL_DATABASE, SOURCE_TABLE));

        // MySQL binlog timestamps have second granularity, wait so the startup timestamp
        // is clearly after the rows above.
        Thread.sleep(3000L);

        // Take the startup timestamp before inserting the rows that must be synced,
        // so their binlog event timestamps are greater than the startup timestamp.
        long startTimestamp = getCurrentBinlogTimestamp() + 2000L;

        // Rows written after the startup timestamp: must be synced.
        executeSql(
                String.format(
                        "INSERT INTO %s.%s (id) VALUES (33), (34)", MYSQL_DATABASE, SOURCE_TABLE));

        // Current binlog position after the rows above: the stop offset.
        BinlogOffset stopOffset = getCurrentBinlogOffset();

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
                        "INSERT INTO %s.%s (id) VALUES (35), (36)", MYSQL_DATABASE, SOURCE_TABLE));

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

    @TestTemplate
    public void testMysqlCdcInitialStartupWithLatestStop(TestContainer container) throws Exception {
        runLatestStopStartupMode(container, "initial");
    }

    @TestTemplate
    public void testMysqlCdcEarliestStartupWithLatestStop(TestContainer container)
            throws Exception {
        runLatestStopStartupMode(container, "earliest");
    }

    @TestTemplate
    public void testMysqlCdcLatestStartupWithLatestStop(TestContainer container) throws Exception {
        runLatestStopStartupMode(container, "latest");
    }

    @TestTemplate
    public void testMysqlCdcSpecificStartupWithLatestStop(TestContainer container)
            throws Exception {
        runLatestStopStartupMode(container, "specific");
    }

    @TestTemplate
    public void testMysqlCdcTimestampStartupWithLatestStop(TestContainer container)
            throws Exception {
        runLatestStopStartupMode(container, "timestamp");
    }

    private void runLatestStopStartupMode(TestContainer container, String startupMode)
            throws Exception {
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        // Bulk-insert rows before starting the job.
        // - initial: 2000 rows so the snapshot phase spans many splits deterministically
        //   (snapshot.split.size=20 -> 100 splits, parallelism=1, consumed in ascending-key
        //   order): the readiness gate below (first row visible in the sink) then fires while
        //   ~99 splits are still unread, so the UPDATE is guaranteed to land inside the
        //   snapshot window. A smaller table could be snapshotted entirely within one polling
        //   interval and complete before the gate is observed (the flaky race SEZ9 flagged).
        // - earliest: keep 200 rows — there is no snapshot phase (binlog replay from the
        //   earliest position), and a larger bulk would only slow the replay.
        // - latest/specific/timestamp: 200 rows, no snapshot window involved.
        int bulkRowCount = "initial".equals(startupMode) ? 2000 : 200;
        StringBuilder bulkInsert =
                new StringBuilder(
                        String.format(
                                "INSERT INTO %s.%s (id, f_varchar) VALUES ",
                                MYSQL_DATABASE, SOURCE_TABLE));
        for (int i = 1; i <= bulkRowCount; i++) {
            if (i > 1) {
                bulkInsert.append(", ");
            }
            bulkInsert.append("(").append(i).append(", 'bulk')");
        }
        executeSql(bulkInsert.toString());

        List<String> variables = new ArrayList<>();
        variables.add("startup_mode=" + startupMode);
        String jobConfigFile = "/mysqlcdc_stop_mode_latest.conf";
        if ("specific".equals(startupMode)) {
            BinlogOffset startOffset = getCurrentBinlogOffset();
            jobConfigFile = "/mysqlcdc_stop_mode_latest_specific.conf";
            variables.add("specific_offset_file=" + startOffset.getFilename());
            variables.add("specific_offset_pos=" + startOffset.getPosition());
        }
        if ("timestamp".equals(startupMode)) {
            jobConfigFile = "/mysqlcdc_stop_mode_latest_timestamp.conf";
            variables.add("timestamp=" + (getCurrentBinlogTimestamp() - 1000L));
        }
        final String configFile = jobConfigFile;

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        configFile, jobId, variables.toArray(new String[0]));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals("RUNNING", container.getJobStatus(jobId)));

        if ("initial".equals(startupMode)) {
            // Structural readiness gate for snapshot-taking startups: wait until the first
            // bulk row (id=1) has reached the sink. With 2000 rows across snapshot.split.size=20
            // (100 splits, parallelism=1, ascending-key order), this signal fires while ~99
            // splits are still unread — the UPDATE below is therefore guaranteed to land
            // inside the snapshot window and be picked up by the binlog phase.
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "bulk",
                                            queryVarcharById(1),
                                            "snapshot phase must have started reading"));
            // Defensive check for the residual race SEZ9 flagged: if the LAST-chunk row is
            // already visible, the whole snapshot completed before the gate fired and the
            // UPDATE below would land after the stop offset. Fail loudly instead of letting
            // the post-UPDATE assertion mislead as data loss. (Row-lock stalls are not usable
            // here: the snapshot reader issues a plain MVCC SELECT, so an open FOR UPDATE
            // transaction on a trailing row does not block it.)
            Assertions.assertNull(
                    queryVarcharById(2000),
                    "snapshot finished too early: last-chunk row already in sink; "
                            + "increase the initial bulk row count");
        }

        // Issue a change while the job is running.
        // - initial: the sink-readiness gate above guarantees the snapshot phase is still in
        //   progress, so the update must be picked up by the binlog phase; with the stale
        //   split-creation stop offset the binlog phase would terminate immediately past it
        //   and this row would be silently dropped.
        // - earliest: there is no snapshot phase (binlog replay starts from the earliest
        //   position, completedSnapshotSplitInfos is empty), so whether the update is
        //   captured depends on the enumerator's stop-offset resolution timing versus the
        //   update — not a deterministic contract, therefore not asserted.
        // - latest/specific/timestamp: no snapshot window, the update lands after the stop
        //   offset is resolved and is not read — which is expected.
        executeSql(
                String.format(
                        "UPDATE %s.%s SET f_varchar = 'latest-stop' WHERE id = 21",
                        MYSQL_DATABASE, SOURCE_TABLE));

        if ("initial".equals(startupMode)) {
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "latest-stop",
                                            queryVarcharById(21),
                                            "post-snapshot update must be synced"));
        }

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals("FINISHED", container.getJobStatus(jobId)));
        Assertions.assertEquals(0, jobFuture.get(30, TimeUnit.SECONDS).getExitCode());
    }

    private long getCurrentBinlogTimestamp() {
        BinlogOffset binlogOffset = getCurrentBinlogOffset();

        JdbcSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(MYSQL_CONTAINER.getUsername())
                        .password(MYSQL_CONTAINER.getPassword())
                        .databaseList(MYSQL_CONTAINER.getDatabaseName());
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        MySqlDialect mySqlDialect =
                new MySqlDialect((MySqlSourceConfigFactory) configFactory, Collections.emptyList());
        BinaryLogClient client =
                MySqlConnectionUtils.createBinaryClient(jdbcSourceConfig.getDbzConfiguration());

        final String showBinaryLogStmt =
                "SHOW BINLOG EVENTS IN '" + binlogOffset.getFilename() + "'";
        List<Long> logPosList = new ArrayList<>();
        JdbcConnection.ResultSetConsumer rsc =
                rs -> {
                    while (rs.next()) {
                        logPosList.add(rs.getLong(5));
                    }
                };
        try (JdbcConnection jdbc = mySqlDialect.openJdbcConnection(jdbcSourceConfig)) {
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

    private String queryVarcharById(int id) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "select f_varchar from %s.%s where id = %d",
                                        MYSQL_DATABASE, SINK_TABLE, id))) {
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
            return null;
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
