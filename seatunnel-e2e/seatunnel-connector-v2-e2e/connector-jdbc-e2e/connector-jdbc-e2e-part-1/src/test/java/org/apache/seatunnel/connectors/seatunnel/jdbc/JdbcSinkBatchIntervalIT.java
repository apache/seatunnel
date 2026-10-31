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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.common.error.RowErrorEvent;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkWriter;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class JdbcSinkBatchIntervalIT extends TestSuiteBase implements TestResource {

    private static final String PG_IMAGE = "postgres:14-alpine";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("org.postgresql.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @Test
    void testHealthyManualCommitCloseDeliversPendingRows() throws Exception {
        String table = "sink_close_healthy";
        createCleanupTable(table);
        JdbcSinkWriter writer = cleanupWriter(table, 10, null);
        try {
            writer.write(new SeaTunnelRow(new Object[] {1L, "good", 2}));
            Assertions.assertEquals(0, getSinkRowCount(table));
        } finally {
            writer.close();
        }
        assertCommittedRow(table, 1L);
        Assertions.assertEquals(1L, getExecutionAttempts(table));
    }

    @Test
    void testFailedCloseDoesNotReplayBatch() throws Exception {
        String table = "sink_close_failed";
        createCleanupTable(table);
        JdbcSinkWriter writer = cleanupWriter(table, 1, null);
        try {
            Assertions.assertThrows(
                    JdbcConnectorException.class,
                    () -> writer.write(new SeaTunnelRow(new Object[] {1L, "bad", -1})));
        } finally {
            Assertions.assertThrows(JdbcConnectorException.class, writer::close);
        }
        Assertions.assertEquals(0, getSinkRowCount(table));
        Assertions.assertEquals(1L, getExecutionAttempts(table));
    }

    @Test
    void testRowErrorRollbackAllowsNextBatch() throws Exception {
        String table = "sink_close_recovery";
        createCleanupTable(table);
        List<RowErrorEvent> errors = new ArrayList<>();
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        when(context.getRowErrorCollector()).thenReturn(Optional.of(errors::add));
        JdbcSinkWriter writer = cleanupWriter(table, 1, context);
        try {
            writer.write(new SeaTunnelRow(new Object[] {1L, "bad", -1}));
            Assertions.assertEquals(1, errors.size());
            writer.write(new SeaTunnelRow(new Object[] {2L, "good", 2}));
            Assertions.assertEquals(0, getSinkRowCount(table));
        } finally {
            writer.close();
        }
        assertCommittedRow(table, 2L);
        Assertions.assertEquals(2L, getExecutionAttempts(table));
    }

    /** Uses the real writer and PostgreSQL transaction path without an engine container. */
    private JdbcSinkWriter cleanupWriter(String table, int batchSize, SinkWriter.Context context) {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder()
                        .url(postgreSQLContainer.getJdbcUrl())
                        .driverName("org.postgresql.Driver")
                        .username(postgreSQLContainer.getUsername())
                        .password(postgreSQLContainer.getPassword())
                        .autoCommit(false)
                        .batchSize(batchSize)
                        .maxRetries(0)
                        .build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(connectionConfig)
                        .database("public")
                        .table(table)
                        .simpleSql(
                                "INSERT INTO " + table + " (pk_id, name, score) VALUES (?, ?, ?)")
                        .build();
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "pk_id", BasicType.LONG_TYPE, 0L, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 255L, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "score", BasicType.INT_TYPE, 0L, true, null, null))
                        .build();
        return new JdbcSinkWriter(
                TablePath.of("public", table),
                context,
                new PostgresDialect(),
                sinkConfig,
                schema,
                schema,
                null);
    }

    /** Sequence increments survive rollback and expose every database execution attempt. */
    private void createCleanupTable(String table) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format(CREATE_TABLE_SQL, table));
            statement.execute("CREATE SEQUENCE " + table + "_attempts");
            statement.execute(
                    "CREATE FUNCTION "
                            + table
                            + "_check() RETURNS trigger AS $$ BEGIN "
                            + "PERFORM nextval('"
                            + table
                            + "_attempts'); "
                            + "IF NEW.score < 0 THEN RAISE EXCEPTION 'controlled row failure' USING ERRCODE = '22003'; END IF; "
                            + "RETURN NEW; END; $$ LANGUAGE plpgsql");
            statement.execute(
                    "CREATE TRIGGER check_row BEFORE INSERT ON "
                            + table
                            + " FOR EACH ROW EXECUTE FUNCTION "
                            + table
                            + "_check()");
        }
    }

    private long getExecutionAttempts(String table) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT last_value, is_called FROM " + table + "_attempts")) {
            Assertions.assertTrue(result.next());
            return result.getBoolean(2) ? result.getLong(1) : 0L;
        }
    }

    private void assertCommittedRow(String table, long id) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery("SELECT pk_id, name, score FROM " + table)) {
            Assertions.assertTrue(result.next());
            Assertions.assertEquals(id, result.getLong(1));
            Assertions.assertEquals("good", result.getString(2));
            Assertions.assertEquals(2, result.getInt(3));
            Assertions.assertFalse(result.next());
        }
    }

    @TestTemplate
    public void testBatchIntervalFlush(TestContainer container) throws SQLException {
        AtomicBoolean jobFinished = new AtomicBoolean(false);

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/jdbc_postgres_sink_batch_interval.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                jobFinished.set(true);
                            }
                        });

        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    jobFinished.get(),
                                    "Job should still be running when timer flush is detected - "
                                            + "if the job already finished, the flush may have come from close()");
                            int rowCount = getSinkRowCount("sink_batch_interval_timer");
                            log.info(
                                    "Polling sink_batch_interval_timer during job execution: {} rows, jobFinished={}",
                                    rowCount,
                                    jobFinished.get());
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "Timer flush should have written rows to the database "
                                            + "BEFORE job completion (batch_size=100000 is never reached)");
                        });

        Container.ExecResult result = jobFuture.join();
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        truncateTable("sink_batch_interval_timer");
    }

    @TestTemplate
    public void testBatchSizeFlush(TestContainer container) throws SQLException {
        AtomicBoolean jobFinished = new AtomicBoolean(false);

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob("/jdbc_postgres_sink_batch_size.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                jobFinished.set(true);
                            }
                        });

        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    jobFinished.get(),
                                    "Job should still be running when batch_size flush is detected");
                            int rowCount = getSinkRowCount("sink_batch_size_only");
                            log.info(
                                    "Polling sink_batch_size_only during job execution: {} rows, jobFinished={}",
                                    rowCount,
                                    jobFinished.get());
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "batch_size flush should have written rows to the database "
                                            + "BEFORE job completion");
                            Assertions.assertEquals(
                                    0,
                                    rowCount % 5,
                                    "Row count should be a multiple of batch_size(5), "
                                            + "actual: "
                                            + rowCount);
                        });

        Container.ExecResult result = jobFuture.join();
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        truncateTable("sink_batch_size_only");
    }

    @TestTemplate
    public void testBatchIntervalWithBatchSize1(TestContainer container)
            throws SQLException, InterruptedException {
        AtomicBoolean jobFinished = new AtomicBoolean(false);

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/jdbc_postgres_sink_batch_interval_with_batch_size_1.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                jobFinished.set(true);
                            }
                        });

        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    jobFinished.get(),
                                    "Job should still be running when batch_size=1 flush is detected");
                            int rowCount = getSinkRowCount("sink_batch_interval_bs1");
                            log.info(
                                    "Polling sink_batch_interval_bs1 during job execution: {} rows, jobFinished={}",
                                    rowCount,
                                    jobFinished.get());
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "batch_size=1 should flush each row immediately "
                                            + "BEFORE job completion");
                        });

        int firstCount = getSinkRowCount("sink_batch_interval_bs1");
        Thread.sleep(5000);
        Assertions.assertFalse(jobFinished.get(), "Job should still be running for second poll");
        int secondCount = getSinkRowCount("sink_batch_interval_bs1");
        log.info(
                "batch_size=1 incremental check: firstCount={}, secondCount={}",
                firstCount,
                secondCount);
        Assertions.assertTrue(
                secondCount > firstCount,
                "Row count should keep growing with batch_size=1 (per-row flush), "
                        + "firstCount="
                        + firstCount
                        + ", secondCount="
                        + secondCount);

        Container.ExecResult result = jobFuture.join();
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        truncateTable("sink_batch_interval_bs1");
    }

    private int getSinkRowCount(String tableName) throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery("SELECT count(*) FROM " + tableName)) {
                Assertions.assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }
    }

    private void truncateTable(String tableName) throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE TABLE " + tableName);
            }
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());
    }

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS %s(\n"
                    + "pk_id BIGINT NOT NULL PRIMARY KEY,\n"
                    + "name VARCHAR(255),\n"
                    + "score INT\n"
                    + ")";

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_interval_timer"));
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_size_only"));
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_interval_bs1"));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSQL table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }
}
