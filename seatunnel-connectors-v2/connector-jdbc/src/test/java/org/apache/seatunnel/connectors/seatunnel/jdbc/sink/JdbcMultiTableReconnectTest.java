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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.error.RowErrorEvent;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.SinkIdentifier;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcTransactionState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite.SqliteDialect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression coverage for reconnecting the active JDBC writer in a multi-table sink. */
class JdbcMultiTableReconnectTest {

    private static final String ACTIVE_TABLE_ID = "source.active_table";
    private static final String IDLE_TABLE_ID = "source.idle_table";

    @TempDir Path tempDir;

    /**
     * Verifies that generated upsert SQL keeps the active table's reduced buffer across a broken
     * connection, rebuilds its statements, and replays each buffered row once.
     */
    @Test
    void generatedSqlReplaysActiveTableBufferAfterReconnect() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("multi-table-reconnect.db");
        createTables(jdbcUrl);

        TrackingSqliteDialect activeDialect = new TrackingSqliteDialect();
        TrackingSqliteDialect idleDialect = new TrackingSqliteDialect();
        TestJdbcSinkWriter activeWriter = createWriter(jdbcUrl, "active_table", activeDialect);
        TestJdbcSinkWriter idleWriter = createWriter(jdbcUrl, "idle_table", idleDialect);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        writers.put(SinkIdentifier.of(ACTIVE_TABLE_ID, 0), activeWriter);
        writers.put(SinkIdentifier.of(IDLE_TABLE_ID, 0), idleWriter);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        try {
            coordinator.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            coordinator.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));
            coordinator.snapshotState(1L);

            activeDialect.getConnectionProvider().failNextBatch();
            coordinator.prepareCommit(2L);
        } finally {
            coordinator.close();
        }

        assertEquals(Arrays.asList("1:first", "2:second"), queryRows(jdbcUrl, "active_table"));
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
        assertEquals(1, activeDialect.getConnectionProvider().reestablishConnectionCalls);
        assertEquals(0, idleDialect.getConnectionProvider().reestablishConnectionCalls);
        assertTrue(activeDialect.generatedUpsertSqlCalls > 0);
    }

    /**
     * With checkpointing, a manual-commit writer keeps every batch-size flush in one open
     * transaction until prepareCommit. If a later batch loses the connection, that transaction and
     * the earlier batch are gone. Reconnecting and replaying only the current batch would let the
     * checkpoint commit a partial result, so the writer must fail instead and leave recovery to the
     * last checkpoint.
     */
    @Test
    void manualCommitWriterDoesNotDropEarlierBatchWhenLaterBatchReconnects() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("manual-commit-reconnect.db");
        createTables(jdbcUrl);

        Map<String, Object> manualCommit = new HashMap<>();
        manualCommit.put("auto_commit", false);
        manualCommit.put("batch_size", 2);
        TrackingSqliteDialect dialect = new TrackingSqliteDialect();
        TestJdbcSinkWriter writer = createWriter(jdbcUrl, "active_table", dialect, manualCommit);
        try {
            // batch_size = 2: rows 1 and 2 are flushed into the open, uncommitted transaction.
            writer.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            writer.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));

            // The next batch loses the connection; the database rolls rows 1 and 2 back.
            dialect.getConnectionProvider().failNextBatch();
            writer.write(insertRow(ACTIVE_TABLE_ID, 3, "third"));
            assertThrows(
                    JdbcConnectorException.class,
                    () -> writer.write(insertRow(ACTIVE_TABLE_ID, 4, "fourth")));
        } finally {
            try {
                writer.close();
            } catch (Exception expected) {
                // The writer already failed; close reports the same flush failure.
            }
        }

        // No partial result was committed: the job recovers rows 1-4 from the last checkpoint.
        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
        assertEquals(0, dialect.getConnectionProvider().reestablishConnectionCalls);
    }

    /**
     * The connection pool replaces a dead cached connection on the next getConnection call. If the
     * connection that held flushed but uncommitted batches dies before the checkpoint, the commit
     * must not succeed on the new, empty connection.
     */
    @Test
    void manualCommitWriterFailsCheckpointWhenConnectionIsReplacedBeforeCommit() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("manual-commit-replaced.db");
        createTables(jdbcUrl);

        Map<String, Object> manualCommit = new HashMap<>();
        manualCommit.put("auto_commit", false);
        manualCommit.put("batch_size", 2);
        TrackingSqliteDialect dialect = new TrackingSqliteDialect();
        TestJdbcSinkWriter writer = createWriter(jdbcUrl, "active_table", dialect, manualCommit);
        TrackingConnectionProvider provider = dialect.getConnectionProvider();
        try {
            writer.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            writer.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));
            provider.replaceDeadConnectionOnGet();

            // The connection dies; the database rolls rows 1 and 2 back.
            provider.dropConnection();

            JdbcConnectorException exception =
                    assertThrows(JdbcConnectorException.class, () -> writer.prepareCommit(1L));
            assertTrue(exception.getCause() instanceof JdbcConnectorException);
            assertTrue(exception.getCause().getMessage().contains("replaced before commit"));
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
                // The writer already failed the checkpoint.
            }
        }

        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
    }

    /**
     * Two tables on one queue index share one connection and one transaction. If table A's
     * checkpoint fails and rolls that transaction back, table B's flushed batch is discarded too,
     * so table B must not report the checkpoint as complete.
     */
    @Test
    void sharedQueueRollbackFailsOtherTablesPrepareCommit() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("shared-queue-rollback-commit.db");
        createTables(jdbcUrl);
        SharedQueueDialect dialect = new SharedQueueDialect();
        TestJdbcSinkWriter tableA = createWriter(jdbcUrl, "active_table", dialect, manualCommit());
        TestJdbcSinkWriter tableB = createWriter(jdbcUrl, "idle_table", dialect, manualCommit());
        try {
            flushBothTablesThenRollBackThroughTableA(dialect, tableA, tableB);

            assertThrows(JdbcConnectorException.class, () -> tableB.prepareCommit(1L));
        } finally {
            closeQuietly(tableB);
            closeQuietly(tableA);
        }

        assertTrue(dialect.transactionState.isPoisoned());
        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
    }

    /** Same as above for the close path, which also commits. */
    @Test
    void sharedQueueRollbackFailsOtherTablesClose() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("shared-queue-rollback-close.db");
        createTables(jdbcUrl);
        SharedQueueDialect dialect = new SharedQueueDialect();
        TestJdbcSinkWriter tableA = createWriter(jdbcUrl, "active_table", dialect, manualCommit());
        TestJdbcSinkWriter tableB = createWriter(jdbcUrl, "idle_table", dialect, manualCommit());
        try {
            flushBothTablesThenRollBackThroughTableA(dialect, tableA, tableB);

            assertThrows(JdbcConnectorException.class, tableB::close);
        } finally {
            closeQuietly(tableA);
        }

        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
    }

    /**
     * The pool replaces the dead shared connection after table A flushed. Table B opens on the
     * replacement and flushes its own batch there, without ever seeing an error. Committing the
     * replacement would complete the checkpoint without table A's rows.
     */
    @Test
    void sharedQueueReplacedConnectionFailsOtherTablesCommit() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("shared-queue-replaced.db");
        createTables(jdbcUrl);
        SharedQueueDialect dialect = new SharedQueueDialect();
        TestJdbcSinkWriter tableA = createWriter(jdbcUrl, "active_table", dialect, manualCommit());
        TestJdbcSinkWriter tableB = createWriter(jdbcUrl, "idle_table", dialect, manualCommit());
        try {
            // batch_size = 2: table A flushes rows 1 and 2 into the shared transaction.
            tableA.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));
            dialect.slot().replaceDeadConnectionOnGet();

            // The connection dies; the database rolls rows 1 and 2 back.
            dialect.slot().dropConnection();

            tableB.write(insertRow(IDLE_TABLE_ID, 10, "ten"));
            assertThrows(JdbcConnectorException.class, () -> tableB.prepareCommit(1L));
        } finally {
            closeQuietly(tableB);
            closeQuietly(tableA);
        }

        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
    }

    /**
     * A savepoint rollback (row-level error handling) keeps the batches flushed before the
     * savepoint pending. A successful commit of the shared connection, by any table, is what makes
     * them durable and clears the pending state for every writer.
     */
    @Test
    void sharedQueueSavepointRollbackKeepsEarlierBatchesUntilSharedCommit() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("shared-queue-savepoint.db");
        createTables(jdbcUrl);
        SharedQueueDialect dialect = new SharedQueueDialect();
        List<RowErrorEvent> rowErrors = new ArrayList<>();
        TestJdbcSinkWriter tableA =
                createWriter(
                        jdbcUrl,
                        "active_table",
                        dialect,
                        manualCommit(),
                        new TestSinkWriterContext(rowErrors::add));
        TestJdbcSinkWriter tableB = createWriter(jdbcUrl, "idle_table", dialect, manualCommit());
        try {
            // Rows 1 and 2 are flushed and a savepoint marks them as the last good batch.
            tableA.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));

            // Rows 3 and 4 fail with a row-level error and are rolled back to the savepoint.
            dialect.slot().failNextBatchWith(new SQLException("constraint violated", "23000"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 3, "third"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 4, "fourth"));
            assertEquals(2, rowErrors.size());

            // Rows 1 and 2 are still pending in the shared transaction.
            assertTrue(dialect.transactionState.hasPendingOrLostWork());
            assertFalse(dialect.transactionState.isPoisoned());

            // Table B commits the shared connection, which covers table A's rows 1 and 2.
            tableB.prepareCommit(1L);
            assertFalse(dialect.transactionState.hasPendingOrLostWork());
            tableA.prepareCommit(1L);
        } finally {
            closeQuietly(tableB);
            closeQuietly(tableA);
        }

        assertEquals(Arrays.asList("1:first", "2:second"), queryRows(jdbcUrl, "active_table"));
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
    }

    /**
     * A savepoint belongs to the whole shared transaction. If table B flushed after table A's
     * savepoint, table A's rollback to that savepoint discards table B's batch as well.
     */
    @Test
    void sharedQueueSavepointRollbackOverOtherTablesBatchFailsCommit() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("shared-queue-savepoint-other.db");
        createTables(jdbcUrl);
        SharedQueueDialect dialect = new SharedQueueDialect();
        List<RowErrorEvent> rowErrors = new ArrayList<>();
        TestJdbcSinkWriter tableA =
                createWriter(
                        jdbcUrl,
                        "active_table",
                        dialect,
                        manualCommit(),
                        new TestSinkWriterContext(rowErrors::add));
        TestJdbcSinkWriter tableB = createWriter(jdbcUrl, "idle_table", dialect, manualCommit());
        try {
            tableA.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));
            // Table B flushes after table A's savepoint.
            tableB.write(insertRow(IDLE_TABLE_ID, 10, "ten"));
            tableB.write(insertRow(IDLE_TABLE_ID, 11, "eleven"));

            dialect.slot().failNextBatchWith(new SQLException("constraint violated", "23000"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 3, "third"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 4, "fourth"));
            assertEquals(2, rowErrors.size());

            assertTrue(dialect.transactionState.isPoisoned());
            assertThrows(JdbcConnectorException.class, () -> tableB.prepareCommit(1L));
        } finally {
            closeQuietly(tableB);
            closeQuietly(tableA);
        }

        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
    }

    /**
     * Both tables flush a batch into the shared transaction. Table A's next batch then fails with
     * an error that is neither row-level nor a lost connection, so its checkpoint fails and rolls
     * the shared transaction back, discarding table B's batch.
     */
    private static void flushBothTablesThenRollBackThroughTableA(
            SharedQueueDialect dialect, TestJdbcSinkWriter tableA, TestJdbcSinkWriter tableB)
            throws Exception {
        tableB.write(insertRow(IDLE_TABLE_ID, 10, "ten"));
        tableB.write(insertRow(IDLE_TABLE_ID, 11, "eleven"));
        tableA.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
        tableA.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));

        dialect.slot().failNextBatchWith(new SQLException("out of disk space", "53100"));
        tableA.write(insertRow(ACTIVE_TABLE_ID, 3, "third"));
        assertThrows(Exception.class, () -> tableA.prepareCommit(1L));
    }

    /**
     * The shared-queue path through the production wiring: {@link
     * JdbcSinkWriter#initMultiTableResourceManager} builds the HikariCP-backed {@link
     * ConnectionPoolManager}, and both table writers get a pooled provider on queue index 0. The
     * cached connection is then closed, as a dropped connection would leave it; HikariCP rolls its
     * open transaction back and the pool replaces it on the next {@code getConnection(0)}. Neither
     * table may commit the replacement.
     */
    @Test
    void realPoolReplacedSharedConnectionFailsBothTablesCommit() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("real-pool-replaced.db");
        createTables(jdbcUrl);
        JdbcSinkWriter tableA = createPooledWriter(jdbcUrl, "active_table");
        JdbcSinkWriter tableB = createPooledWriter(jdbcUrl, "idle_table");
        MultiTableResourceManager<ConnectionPoolManager> resourceManager =
                tableA.initMultiTableResourceManager(2, 1);
        ConnectionPoolManager pool = resourceManager.getSharedResource().get();
        try {
            tableA.setMultiTableResourceManager(resourceManager, 0);
            tableB.setMultiTableResourceManager(resourceManager, 0);

            // batch_size = 2: both tables flush a batch into the shared transaction.
            tableA.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            tableA.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));
            tableB.write(insertRow(IDLE_TABLE_ID, 10, "ten"));
            tableB.write(insertRow(IDLE_TABLE_ID, 11, "eleven"));

            pool.getConnection(0).close();

            assertThrows(JdbcConnectorException.class, () -> tableA.prepareCommit(1L));
            assertThrows(JdbcConnectorException.class, () -> tableB.prepareCommit(1L));
            assertTrue(pool.getTransactionState(0).isPoisoned());
        } finally {
            closeQuietly(tableB);
            closeQuietly(tableA);
            resourceManager.close();
        }

        assertTrue(queryRows(jdbcUrl, "active_table").isEmpty());
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
    }

    /**
     * Healthy path through the same production wiring: two tables on one queue index, several
     * checkpoints, no failures. Every checkpoint must commit and leave nothing pending, so the
     * shared state never fails a healthy job.
     */
    @Test
    void realPoolSharedQueueCommitsHealthyCheckpoints() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("real-pool-healthy.db");
        createTables(jdbcUrl);
        JdbcSinkWriter tableA = createPooledWriter(jdbcUrl, "active_table");
        JdbcSinkWriter tableB = createPooledWriter(jdbcUrl, "idle_table");
        MultiTableResourceManager<ConnectionPoolManager> resourceManager =
                tableA.initMultiTableResourceManager(2, 1);
        ConnectionPoolManager pool = resourceManager.getSharedResource().get();
        try {
            tableA.setMultiTableResourceManager(resourceManager, 0);
            tableB.setMultiTableResourceManager(resourceManager, 0);

            for (int checkpoint = 1; checkpoint <= 3; checkpoint++) {
                // Table A flushes on batch_size; table B's single row is flushed by prepareCommit.
                tableA.write(insertRow(ACTIVE_TABLE_ID, checkpoint * 10 + 1, "a" + checkpoint));
                tableA.write(insertRow(ACTIVE_TABLE_ID, checkpoint * 10 + 2, "b" + checkpoint));
                tableB.write(insertRow(IDLE_TABLE_ID, checkpoint * 10, "c" + checkpoint));

                tableA.prepareCommit(checkpoint);
                tableB.prepareCommit(checkpoint);

                assertFalse(pool.getTransactionState(0).hasPendingOrLostWork());
            }
        } finally {
            closeQuietly(tableB);
            closeQuietly(tableA);
            resourceManager.close();
        }

        assertEquals(
                Arrays.asList("11:a1", "12:b1", "21:a2", "22:b2", "31:a3", "32:b3"),
                queryRows(jdbcUrl, "active_table"));
        assertEquals(Arrays.asList("10:c1", "20:c2", "30:c3"), queryRows(jdbcUrl, "idle_table"));
    }

    /** A plain {@link JdbcSinkWriter}, so the real pooled provider is used once it is wired. */
    private static JdbcSinkWriter createPooledWriter(String jdbcUrl, String table) {
        Map<String, Object> options = new HashMap<>(manualCommit());
        options.put("url", jdbcUrl);
        options.put("driver", "org.sqlite.JDBC");
        options.put("database", "main");
        options.put("table", table);
        options.put("generate_sink_sql", true);
        options.put("primary_keys", Arrays.asList("id"));
        options.put("max_retries", 1);
        return new JdbcSinkWriter(
                TablePath.of("main", table),
                new TestSinkWriterContext(),
                new SqliteDialect(),
                JdbcSinkConfig.of(ReadonlyConfig.fromMap(options)),
                tableSchema(),
                tableSchema(),
                0);
    }

    private static Map<String, Object> manualCommit() {
        Map<String, Object> options = new HashMap<>();
        options.put("auto_commit", false);
        options.put("batch_size", 2);
        return options;
    }

    private static void closeQuietly(JdbcSinkWriter writer) {
        try {
            writer.close();
        } catch (Exception ignored) {
            // Some tests leave the writer failed on purpose.
        }
    }

    private static TestJdbcSinkWriter createWriter(
            String jdbcUrl, String table, TrackingSqliteDialect dialect) {
        return createWriter(jdbcUrl, table, dialect, new HashMap<>());
    }

    private static TestJdbcSinkWriter createWriter(
            String jdbcUrl, String table, SqliteDialect dialect, Map<String, Object> extraOptions) {
        return createWriter(jdbcUrl, table, dialect, extraOptions, new TestSinkWriterContext());
    }

    private static TestJdbcSinkWriter createWriter(
            String jdbcUrl,
            String table,
            SqliteDialect dialect,
            Map<String, Object> extraOptions,
            TestSinkWriterContext context) {
        Map<String, Object> options = new HashMap<>(extraOptions);
        options.put("url", jdbcUrl);
        options.put("driver", "org.sqlite.JDBC");
        options.put("database", "main");
        options.put("table", table);
        options.put("generate_sink_sql", true);
        options.put("primary_keys", Arrays.asList("id"));
        options.put("max_retries", 1);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);

        assertTrue(config.get(JdbcSinkOptions.GENERATE_SINK_SQL));
        assertNull(sinkConfig.getSimpleSql());
        return new TestJdbcSinkWriter(
                TablePath.of("main", table),
                context,
                dialect,
                sinkConfig,
                tableSchema(),
                tableSchema(),
                0);
    }

    private static TableSchema tableSchema() {
        return TableSchema.builder()
                .columns(
                        Arrays.asList(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, 10L, false, null, "INTEGER"),
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, "TEXT")))
                .build();
    }

    private static SeaTunnelRow insertRow(String tableId, int id, String name) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setTableId(tableId);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static void createTables(String jdbcUrl) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE `active_table` (`id` INTEGER PRIMARY KEY, `name` TEXT)");
            statement.execute("CREATE TABLE `idle_table` (`id` INTEGER PRIMARY KEY, `name` TEXT)");
        }
    }

    private static List<String> queryRows(String jdbcUrl, String table) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT `id`, `name` FROM `%s` ORDER BY `id`", table))) {
            while (resultSet.next()) {
                rows.add(resultSet.getInt("id") + ":" + resultSet.getString("name"));
            }
        }
        return rows;
    }

    private static Map<SinkIdentifier, SinkWriter.Context> buildContextMap(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers) {
        Map<SinkIdentifier, SinkWriter.Context> contexts = new LinkedHashMap<>();
        for (SinkIdentifier identifier : writers.keySet()) {
            contexts.put(identifier, new TestSinkWriterContext());
        }
        return contexts;
    }

    private static class TrackingSqliteDialect extends SqliteDialect {
        private TrackingConnectionProvider connectionProvider;
        private int generatedUpsertSqlCalls;

        @Override
        public JdbcConnectionProvider getJdbcConnectionProvider(
                JdbcConnectionConfig jdbcConnectionConfig) {
            connectionProvider = new TrackingConnectionProvider(jdbcConnectionConfig);
            return connectionProvider;
        }

        @Override
        public java.util.Optional<String> getUpsertStatement(
                String database, String tableName, String[] fieldNames, String[] pkNames) {
            generatedUpsertSqlCalls++;
            return super.getUpsertStatement(database, tableName, fieldNames, pkNames);
        }

        private TrackingConnectionProvider getConnectionProvider() {
            return connectionProvider;
        }
    }

    private static class TrackingConnectionProvider implements JdbcConnectionProvider {
        private final JdbcConnectionConfig jdbcConfig;
        private Connection connection;
        private Connection delegate;
        private boolean failNextBatch;
        private SQLException nextBatchFailure;
        private boolean replaceDeadConnectionOnGet;
        private int reestablishConnectionCalls;

        private TrackingConnectionProvider(JdbcConnectionConfig jdbcConfig) {
            this.jdbcConfig = jdbcConfig;
        }

        private void failNextBatch() {
            failNextBatch = true;
        }

        /** Fails the next batch with {@code failure} and keeps the connection open. */
        private void failNextBatchWith(SQLException failure) {
            nextBatchFailure = failure;
        }

        /** Mimics ConnectionPoolManager, which swaps a dead cached connection on getConnection. */
        private void replaceDeadConnectionOnGet() {
            replaceDeadConnectionOnGet = true;
        }

        /** Simulates the server or network killing the current connection. */
        private void dropConnection() throws SQLException {
            delegate.close();
        }

        @Override
        public Connection getConnection() {
            if (replaceDeadConnectionOnGet && connection != null) {
                try {
                    if (connection.isClosed()) {
                        closeConnection();
                        return getOrEstablishConnection();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return connection;
        }

        @Override
        public boolean isConnectionValid() throws SQLException {
            return connection != null && !connection.isClosed();
        }

        @Override
        public Connection getOrEstablishConnection() throws SQLException {
            if (!isConnectionValid()) {
                delegate = DriverManager.getConnection(jdbcConfig.getUrl());
                delegate.setAutoCommit(jdbcConfig.isAutoCommit());
                connection = wrapConnection(delegate);
            }
            return connection;
        }

        @Override
        public void closeConnection() {
            if (connection == null) {
                return;
            }
            try {
                connection.close();
            } catch (SQLException ignored) {
                // The broken connection can already be closed by the simulated network failure.
            } finally {
                connection = null;
            }
        }

        @Override
        public Connection reestablishConnection() throws SQLException {
            reestablishConnectionCalls++;
            closeConnection();
            return getOrEstablishConnection();
        }

        private Connection wrapConnection(Connection delegate) {
            return (Connection)
                    Proxy.newProxyInstance(
                            Connection.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                try {
                                    Object result = method.invoke(delegate, args);
                                    if (result instanceof PreparedStatement
                                            && "prepareStatement".equals(method.getName())) {
                                        return wrapStatement(delegate, (PreparedStatement) result);
                                    }
                                    return result;
                                } catch (InvocationTargetException exception) {
                                    throw exception.getCause();
                                }
                            });
        }

        private PreparedStatement wrapStatement(
                Connection owner, PreparedStatement preparedStatement) {
            return (PreparedStatement)
                    Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            (proxy, method, args) -> {
                                if ("executeBatch".equals(method.getName()) && failNextBatch) {
                                    failNextBatch = false;
                                    owner.close();
                                    throw new SQLException("connection dropped", "08S01");
                                }
                                if ("executeBatch".equals(method.getName())
                                        && nextBatchFailure != null) {
                                    SQLException failure = nextBatchFailure;
                                    nextBatchFailure = null;
                                    throw failure;
                                }
                                try {
                                    return method.invoke(preparedStatement, args);
                                } catch (InvocationTargetException exception) {
                                    throw exception.getCause();
                                }
                            });
        }
    }

    /**
     * Gives every writer created with it a provider on one shared connection and one shared
     * transaction state, like the writers on one queue index of {@link ConnectionPoolManager}.
     */
    private static class SharedQueueDialect extends SqliteDialect {
        private final JdbcTransactionState transactionState = new JdbcTransactionState();
        private TrackingConnectionProvider slot;

        @Override
        public JdbcConnectionProvider getJdbcConnectionProvider(
                JdbcConnectionConfig jdbcConnectionConfig) {
            if (slot == null) {
                slot = new TrackingConnectionProvider(jdbcConnectionConfig);
            }
            return new SharedQueueProvider(slot, transactionState);
        }

        private TrackingConnectionProvider slot() {
            return slot;
        }
    }

    /** One writer's view of the shared connection, like SimpleJdbcConnectionPoolProviderProxy. */
    private static class SharedQueueProvider implements JdbcConnectionProvider {
        private final TrackingConnectionProvider slot;
        private final JdbcTransactionState transactionState;

        private SharedQueueProvider(
                TrackingConnectionProvider slot, JdbcTransactionState transactionState) {
            this.slot = slot;
            this.transactionState = transactionState;
        }

        @Override
        public Connection getConnection() {
            return slot.getConnection();
        }

        @Override
        public boolean isConnectionValid() throws SQLException {
            return slot.isConnectionValid();
        }

        @Override
        public Connection getOrEstablishConnection() throws SQLException {
            return slot.getOrEstablishConnection();
        }

        @Override
        public void closeConnection() {
            slot.closeConnection();
        }

        @Override
        public Connection reestablishConnection() throws SQLException {
            return slot.reestablishConnection();
        }

        @Override
        public JdbcTransactionState getTransactionState() {
            return transactionState;
        }
    }

    private static class TestJdbcSinkWriter extends JdbcSinkWriter {
        private TestJdbcSinkWriter(
                TablePath sinkTablePath,
                SinkWriter.Context context,
                SqliteDialect dialect,
                JdbcSinkConfig jdbcSinkConfig,
                TableSchema tableSchema,
                TableSchema databaseTableSchema,
                Integer primaryKeyIndex) {
            super(
                    sinkTablePath,
                    context,
                    dialect,
                    jdbcSinkConfig,
                    tableSchema,
                    databaseTableSchema,
                    primaryKeyIndex);
        }

        @Override
        public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
                int tableSize, int queueSize) {
            return new MultiTableResourceManager<ConnectionPoolManager>() {};
        }

        @Override
        public void setMultiTableResourceManager(
                MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
                int queueIndex) {
            // Keep the per-writer provider so the test can deterministically break one table only.
        }
    }

    private static class TestSinkWriterContext implements SinkWriter.Context {
        private final RowErrorCollector rowErrorCollector;

        private TestSinkWriterContext() {
            this(null);
        }

        private TestSinkWriterContext(RowErrorCollector rowErrorCollector) {
            this.rowErrorCollector = rowErrorCollector;
        }

        @Override
        public Optional<RowErrorCollector> getRowErrorCollector() {
            return Optional.ofNullable(rowErrorCollector);
        }

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
            return new DefaultEventProcessor();
        }
    }
}
