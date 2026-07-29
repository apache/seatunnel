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

package org.apache.seatunnel.edge.agent.starter.wal.sqlite;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecord;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecordStatus;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqliteWalStore implements WalStore {

    private final Connection connection;
    private final boolean closeConnection;
    private final SqliteSourcePositionStore sourcePositionStore;

    @VisibleForTesting
    public SqliteWalStore(Path sqlitePath) throws SQLException {
        Path prepared = SqliteSchemaBootstrap.prepareSqlitePath(sqlitePath);
        Connection connection =
                DriverManager.getConnection("jdbc:sqlite:" + prepared.toAbsolutePath());
        SqliteSchemaBootstrap.applyConnectionPragmas(connection);
        SqliteSchemaBootstrap.initRuntimeSchema(connection);
        this.connection = connection;
        this.closeConnection = true;
        this.sourcePositionStore = new SqliteSourcePositionStore(connection, false);
    }

    public SqliteWalStore(Connection connection, boolean closeConnection) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.closeConnection = closeConnection;
        this.sourcePositionStore = new SqliteSourcePositionStore(connection, false);
    }

    @Override
    public EdgeSourcePositionStore sourcePositionStore() {
        return sourcePositionStore;
    }

    @Override
    public long append(EdgeEvent event) throws Exception {
        byte[] payload = event.getPayload();
        if (payload == null) {
            throw new IllegalArgumentException("event payload must not be null");
        }
        long now = System.currentTimeMillis();
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            long batchId = SqliteBatchIdAllocator.allocateNext(connection);
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            WalSqlStatements.INSERT, Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, event.getSourceId());
                statement.setBytes(2, payload);
                statement.setLong(3, event.getEventTime());
                statement.setBytes(4, MetadataSerde.serialize(event.getMetadata()));
                statement.setString(5, WalRecordStatus.PENDING.name());
                statement.setInt(6, 0);
                statement.setLong(7, now);
                statement.setLong(8, now);
                statement.setLong(9, batchId);
                statement.executeUpdate();
                try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        long rowId = generatedKeys.getLong(1);
                        connection.commit();
                        return rowId;
                    }
                }
            }
            connection.rollback();
            throw new SQLException("Failed to append EdgeEvent to WAL: generated id is missing.");
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    @Override
    public List<WalRecord> claimPending(int maxRecords, int maxAttempts) throws Exception {
        if (maxRecords < 1 || maxAttempts < 1) {
            return new ArrayList<>();
        }
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            List<WalRecord> records = selectPendingOrderByIdAsc(maxRecords, maxAttempts);
            long now = System.currentTimeMillis();
            for (WalRecord record : records) {
                markSending(record.getId(), now);
                record.setStatus(WalRecordStatus.SENDING);
                record.setAttemptCount(record.getAttemptCount() + 1);
                record.setUpdatedAt(now);
            }
            connection.commit();
            return records;
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    @Override
    public void ack(long recordId) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.UPDATE_STATUS_BY_ID)) {
            statement.setString(1, WalRecordStatus.ACKED.name());
            statement.setLong(2, System.currentTimeMillis());
            statement.setLong(3, recordId);
            statement.executeUpdate();
        }
    }

    @Override
    public int markExceededAsDead(int maxAttempts, int maxRecords) throws Exception {
        if (maxRecords < 1 || maxAttempts < 1) {
            return 0;
        }
        List<Long> ids = selectExceededPendingIds(maxAttempts, maxRecords);
        if (ids.isEmpty()) {
            return 0;
        }
        long now = System.currentTimeMillis();
        int updated = 0;
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.MARK_EXCEEDED_AS_DEAD)) {
            for (Long id : ids) {
                statement.setString(1, WalRecordStatus.DEAD.name());
                statement.setLong(2, now);
                statement.setLong(3, id);
                statement.setString(4, WalRecordStatus.PENDING.name());
                updated += statement.executeUpdate();
            }
        }
        return updated;
    }

    @Override
    public int resurrectSending(int maxRecords) throws Exception {
        return resurrectSending(maxRecords, 0L);
    }

    @Override
    public int resurrectSending(int maxRecords, long staleThresholdMs) throws Exception {
        if (maxRecords < 1) {
            return 0;
        }
        long now = System.currentTimeMillis();
        List<WalRecord> records;
        if (staleThresholdMs > 0) {
            long cutoff = now - staleThresholdMs;
            records = selectStaleSendingOrderByUpdatedAtAsc(maxRecords, cutoff);
        } else {
            records = selectSendingOrderByUpdatedAtAsc(maxRecords);
        }
        int updated = 0;
        for (WalRecord record : records) {
            updated += updateStatus(record.getId(), WalRecordStatus.PENDING, now);
        }
        return updated;
    }

    @Override
    public int cleanupAcked(long retentionMs, int maxRecords) throws Exception {
        if (maxRecords < 1) {
            return 0;
        }
        long cutoff = System.currentTimeMillis() - Math.max(0L, retentionMs);
        List<Long> ids = selectAckedIdsForCleanup(cutoff, maxRecords);
        int deleted = 0;
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.DELETE_BY_ID)) {
            for (Long id : ids) {
                statement.setLong(1, id);
                deleted += statement.executeUpdate();
            }
        }
        return deleted;
    }

    @Override
    public void close() throws SQLException {
        if (closeConnection) {
            connection.close();
        }
    }

    private List<WalRecord> selectPendingOrderByIdAsc(int maxRecords, int maxAttempts)
            throws Exception {
        List<WalRecord> records = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.SELECT_PENDING_ORDER_BY_ID_ASC)) {
            statement.setString(1, WalRecordStatus.PENDING.name());
            statement.setInt(2, maxAttempts);
            statement.setInt(3, maxRecords);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    records.add(toRecord(resultSet));
                }
            }
        }
        return records;
    }

    private List<WalRecord> selectSendingOrderByUpdatedAtAsc(int maxRecords) throws Exception {
        List<WalRecord> records = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(
                        WalSqlStatements.SELECT_BY_STATUS_ORDER_BY_UPDATED_AT_ASC)) {
            statement.setString(1, WalRecordStatus.SENDING.name());
            statement.setInt(2, maxRecords);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    records.add(toRecord(resultSet));
                }
            }
        }
        return records;
    }

    private List<WalRecord> selectStaleSendingOrderByUpdatedAtAsc(int maxRecords, long cutoffMs)
            throws Exception {
        List<WalRecord> records = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(
                        WalSqlStatements.SELECT_STALE_SENDING_ORDER_BY_UPDATED_AT_ASC)) {
            statement.setString(1, WalRecordStatus.SENDING.name());
            statement.setLong(2, cutoffMs);
            statement.setInt(3, maxRecords);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    records.add(toRecord(resultSet));
                }
            }
        }
        return records;
    }

    private List<Long> selectExceededPendingIds(int maxAttempts, int maxRecords)
            throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.SELECT_EXCEEDED_PENDING_IDS)) {
            statement.setString(1, WalRecordStatus.PENDING.name());
            statement.setInt(2, maxAttempts);
            statement.setInt(3, maxRecords);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getLong("id"));
                }
            }
        }
        return ids;
    }

    private WalRecord toRecord(ResultSet resultSet) throws Exception {
        long rowId = resultSet.getLong("id");
        long batchId = resultSet.getLong("batch_id");
        if (batchId <= 0) {
            batchId = rowId;
        }
        return WalRecord.builder()
                .id(rowId)
                .batchId(batchId)
                .sourceId(resultSet.getString("source_id"))
                .payload(resultSet.getBytes("payload"))
                .eventTime(resultSet.getLong("event_time"))
                .metadata(MetadataSerde.deserialize(resultSet.getBytes("metadata")))
                .status(WalRecordStatus.valueOf(resultSet.getString("status")))
                .attemptCount(resultSet.getInt("attempt_count"))
                .createdAt(resultSet.getLong("created_at"))
                .updatedAt(resultSet.getLong("updated_at"))
                .build();
    }

    private void markSending(long id, long now) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.MARK_SENDING)) {
            statement.setString(1, WalRecordStatus.SENDING.name());
            statement.setLong(2, now);
            statement.setLong(3, id);
            statement.setString(4, WalRecordStatus.PENDING.name());
            statement.executeUpdate();
        }
    }

    private int updateStatus(long id, WalRecordStatus status, long now) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.UPDATE_STATUS_BY_ID)) {
            statement.setString(1, status.name());
            statement.setLong(2, now);
            statement.setLong(3, id);
            return statement.executeUpdate();
        }
    }

    private List<Long> selectAckedIdsForCleanup(long cutoff, int maxRecords) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(WalSqlStatements.SELECT_ACKED_IDS_FOR_CLEANUP)) {
            statement.setString(1, WalRecordStatus.ACKED.name());
            statement.setLong(2, cutoff);
            statement.setInt(3, maxRecords);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getLong("id"));
                }
            }
        }
        return ids;
    }
}
