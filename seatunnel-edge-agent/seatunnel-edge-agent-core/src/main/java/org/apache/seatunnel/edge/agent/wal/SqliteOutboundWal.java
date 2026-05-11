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

package org.apache.seatunnel.edge.agent.wal;

import org.apache.seatunnel.edge.agent.batch.AccumulatedRecord;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SQLite outbound queue with WAL journal mode and explicit {@code PENDING -> SENDING -> ACKED}
 * transitions (failed sends roll back to {@code PENDING} and bump {@code attempts}).
 */
public final class SqliteOutboundWal implements Closeable {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENDING = "SENDING";
    public static final String STATUS_ACKED = "ACKED";

    private final Path dbPath;
    private Connection connection;

    public SqliteOutboundWal(Path sqlitePath) {
        this.dbPath = sqlitePath.toAbsolutePath().normalize();
    }

    public void open() throws SQLException {
        Path parent = dbPath.getParent();
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                throw new SQLException("Cannot create SQLite parent directory: " + parent, e);
            }
        }
        String url = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        connection = DriverManager.getConnection(url);
        initSchemaAndPragmas();
    }

    private void initSchemaAndPragmas() throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA synchronous=NORMAL");
            st.execute(
                    "CREATE TABLE IF NOT EXISTS outbound_records ("
                            + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                            + "payload TEXT NOT NULL,"
                            + "status TEXT NOT NULL,"
                            + "attempts INTEGER NOT NULL,"
                            + "source_input TEXT,"
                            + "created_at_ms INTEGER NOT NULL,"
                            + "updated_at_ms INTEGER NOT NULL,"
                            + "CHECK(status IN ('"
                            + STATUS_PENDING
                            + "','"
                            + STATUS_SENDING
                            + "','"
                            + STATUS_ACKED
                            + "')))");
            st.execute(
                    "CREATE INDEX IF NOT EXISTS idx_outbound_pending ON outbound_records "
                            + "(status, attempts, id)");
        }
    }

    /**
     * Recovers rows stuck in {@code SENDING} back to {@code PENDING} after a crash between claim
     * and ACK.
     */
    public void recoverStaleSending() throws SQLException {
        long now = System.currentTimeMillis();
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "UPDATE outbound_records SET status=?, updated_at_ms=? WHERE status=?")) {
            ps.setString(1, STATUS_PENDING);
            ps.setLong(2, now);
            ps.setString(3, STATUS_SENDING);
            ps.executeUpdate();
        }
    }

    /** Inserts batched payloads as {@code PENDING}. */
    public void enqueuePending(List<AccumulatedRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        connection.setAutoCommit(false);
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "INSERT INTO outbound_records (payload, status, attempts, source_input, "
                                + "created_at_ms, updated_at_ms) VALUES (?,?,?,?,?,?)")) {
            for (AccumulatedRecord record : records) {
                ps.setString(1, record.getPayload());
                ps.setString(2, STATUS_PENDING);
                ps.setInt(3, 0);
                ps.setString(4, record.getSourceInputId());
                ps.setLong(5, now);
                ps.setLong(6, now);
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    /**
     * Claims up to {@code limit} {@code PENDING} rows with {@code attempts < maxAttempts}, marking
     * them {@code SENDING}.
     */
    public List<WalRecord> claimSendingBatch(int limit, int maxAttempts) throws SQLException {
        if (limit < 1) {
            return Collections.emptyList();
        }
        connection.setAutoCommit(false);
        try {
            List<Long> ids = new ArrayList<>(limit);
            try (PreparedStatement sel =
                    connection.prepareStatement(
                            "SELECT id FROM outbound_records WHERE status=? AND attempts < ? "
                                    + "ORDER BY id ASC LIMIT ?")) {
                sel.setString(1, STATUS_PENDING);
                sel.setInt(2, maxAttempts);
                sel.setInt(3, limit);
                try (ResultSet rs = sel.executeQuery()) {
                    while (rs.next()) {
                        ids.add(rs.getLong(1));
                    }
                }
            }
            if (ids.isEmpty()) {
                connection.commit();
                return Collections.emptyList();
            }
            long now = System.currentTimeMillis();
            updateStatusForIds(ids, STATUS_SENDING, now);
            List<WalRecord> rows = loadSendingRecords(ids);
            connection.commit();
            return rows;
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private void updateStatusForIds(List<Long> ids, String status, long updatedAtMs)
            throws SQLException {
        String sql =
                "UPDATE outbound_records SET status=?, updated_at_ms=? WHERE id=? AND status=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Long id : ids) {
                ps.setString(1, status);
                ps.setLong(2, updatedAtMs);
                ps.setLong(3, id);
                ps.setString(4, STATUS_PENDING);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private List<WalRecord> loadSendingRecords(List<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        StringBuilder sb =
                new StringBuilder(
                        "SELECT id, payload, attempts, source_input FROM outbound_records WHERE status=? "
                                + "AND id IN (");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('?');
        }
        sb.append(") ORDER BY id ASC");
        List<WalRecord> out = new ArrayList<>(ids.size());
        try (PreparedStatement ps = connection.prepareStatement(sb.toString())) {
            ps.setString(1, STATUS_SENDING);
            int idx = 2;
            for (Long id : ids) {
                ps.setLong(idx++, id);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(
                            new WalRecord(
                                    rs.getLong(1), rs.getString(2), rs.getInt(3), rs.getString(4)));
                }
            }
        }
        return out;
    }
    /** Marks {@code SENDING} rows as {@code ACKED} after successful upstream delivery. */
    public void ackSending(List<Long> ids) throws SQLException {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        String sql =
                "UPDATE outbound_records SET status=?, updated_at_ms=? WHERE id=? AND status=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Long id : ids) {
                ps.setString(1, STATUS_ACKED);
                ps.setLong(2, now);
                ps.setLong(3, id);
                ps.setString(4, STATUS_SENDING);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * Rolls failed {@code SENDING} rows back to {@code PENDING}, increments {@code attempts}, and
     * refreshes {@code updated_at_ms}.
     */
    public void revertSendingWithAttemptIncrement(List<Long> ids) throws SQLException {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        String sql =
                "UPDATE outbound_records SET status=?, attempts=attempts+1, updated_at_ms=? "
                        + "WHERE id=? AND status=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Long id : ids) {
                ps.setString(1, STATUS_PENDING);
                ps.setLong(2, now);
                ps.setLong(3, id);
                ps.setString(4, STATUS_SENDING);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IOException("Failed closing SQLite WAL", e);
        } finally {
            connection = null;
        }
    }

    /**
     * Interprets {@code sqlite path string} relative to {@code workingDirectory} when not absolute.
     */
    public static Path resolveSqlitePath(String sqlitePath, Path workingDirectory) {
        Path p = Paths.get(sqlitePath);
        if (p.isAbsolute()) {
            return p.normalize();
        }
        return workingDirectory.resolve(p).normalize();
    }
}
