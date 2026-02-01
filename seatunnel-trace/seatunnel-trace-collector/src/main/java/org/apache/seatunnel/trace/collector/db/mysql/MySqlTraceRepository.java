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

package org.apache.seatunnel.trace.collector.db.mysql;

import org.apache.seatunnel.trace.collector.config.TraceCollectorConfig;
import org.apache.seatunnel.trace.collector.db.IngestedEvent;
import org.apache.seatunnel.trace.collector.db.TraceQuery;
import org.apache.seatunnel.trace.collector.db.TraceRepository;
import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;
import org.apache.seatunnel.trace.collector.model.TraceDetail;
import org.apache.seatunnel.trace.collector.model.TraceEntry;
import org.apache.seatunnel.trace.collector.model.TraceSummary;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MySqlTraceRepository implements TraceRepository {

    private final TraceCollectorConfig config;
    private final TraceCollectorMetrics metrics;
    private final String schema;

    public MySqlTraceRepository(TraceCollectorConfig config, TraceCollectorMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
        this.schema = normalizeSchema(config.getDbSchema());
    }

    @Override
    public void init() {
        try (Connection conn = open();
                Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + qualify("st_trace_event_raw")
                            + " ("
                            + "id BIGINT NOT NULL AUTO_INCREMENT,"
                            + "received_at DATETIME(3) NOT NULL,"
                            + "job_id VARCHAR(255) NULL,"
                            + "event_type VARCHAR(128) NULL,"
                            + "body_json JSON NOT NULL,"
                            + "PRIMARY KEY(id),"
                            + "KEY idx_st_trace_event_job_received(job_id, received_at)"
                            + ") ENGINE=InnoDB");

            st.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + qualify("st_trace")
                            + " ("
                            + "trace_id BIGINT NOT NULL,"
                            + "sink_task_id BIGINT NOT NULL,"
                            + "job_id VARCHAR(255) NULL,"
                            + "table_id VARCHAR(255) NULL,"
                            + "created_time_ms BIGINT NULL,"
                            + "received_at DATETIME(3) NOT NULL,"
                            + "payload LONGBLOB NULL,"
                            + "start_ts_ms BIGINT NULL,"
                            + "entry_count INT NULL,"
                            + "PRIMARY KEY(trace_id, sink_task_id),"
                            + "KEY idx_st_trace_job_received(job_id, received_at)"
                            + ") ENGINE=InnoDB");

            st.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + qualify("st_trace_entry")
                            + " ("
                            + "trace_id BIGINT NOT NULL,"
                            + "sink_task_id BIGINT NOT NULL,"
                            + "entry_index INT NOT NULL,"
                            + "stage SMALLINT NOT NULL,"
                            + "task_id BIGINT NOT NULL,"
                            + "ts_ms BIGINT NOT NULL,"
                            + "worker_address VARCHAR(255) NULL,"
                            + "task_group_name VARCHAR(255) NULL,"
                            + "task_class VARCHAR(255) NULL,"
                            + "PRIMARY KEY(trace_id, sink_task_id, entry_index)"
                            + ") ENGINE=InnoDB");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to init mysql repository", e);
        }
    }

    @Override
    public void ingest(List<IngestedEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        try (Connection conn = open()) {
            insertRaw(conn, events);
            insertTraces(conn, events);
            insertEntries(conn, events);
        } catch (Exception e) {
            metrics.dbWriteFailuresTotal.labels("all").inc();
            throw new RuntimeException("Failed to ingest events", e);
        }
    }

    private void insertRaw(Connection conn, List<IngestedEvent> events) throws SQLException {
        String sql =
                "INSERT INTO "
                        + qualify("st_trace_event_raw")
                        + "(received_at, job_id, event_type, body_json) "
                        + "VALUES (FROM_UNIXTIME(? / 1000.0), ?, ?, CAST(? AS JSON))";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IngestedEvent e : events) {
                ps.setLong(1, e.getReceivedTimeMs());
                ps.setString(2, e.getJobId());
                ps.setString(3, e.getEventType());
                ps.setString(4, e.getRawJson());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertTraces(Connection conn, List<IngestedEvent> events) throws SQLException {
        String sql =
                "INSERT INTO "
                        + qualify("st_trace")
                        + "(trace_id, sink_task_id, job_id, table_id, created_time_ms, received_at, payload, start_ts_ms, entry_count) "
                        + "VALUES (?, ?, ?, ?, ?, FROM_UNIXTIME(? / 1000.0), ?, ?, ?) "
                        + "ON DUPLICATE KEY UPDATE "
                        + "received_at=VALUES(received_at), "
                        + "payload=VALUES(payload), "
                        + "start_ts_ms=VALUES(start_ts_ms), "
                        + "entry_count=VALUES(entry_count)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IngestedEvent e : events) {
                if (e.getTraceId() == null || e.getSinkTaskId() == null) {
                    continue;
                }
                ps.setLong(1, e.getTraceId());
                ps.setLong(2, e.getSinkTaskId());
                ps.setString(3, e.getJobId());
                ps.setString(4, e.getTableId());
                if (e.getCreatedTimeMs() == null) {
                    ps.setObject(5, null);
                } else {
                    ps.setLong(5, e.getCreatedTimeMs());
                }
                ps.setLong(6, e.getReceivedTimeMs());
                ps.setBytes(7, e.getPayloadBytes());
                if (e.getStartTsMs() == null) {
                    ps.setObject(8, null);
                } else {
                    ps.setLong(8, e.getStartTsMs());
                }
                if (e.getEntryCount() == null) {
                    ps.setObject(9, null);
                } else {
                    ps.setInt(9, e.getEntryCount());
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertEntries(Connection conn, List<IngestedEvent> events) throws SQLException {
        String sql =
                "INSERT IGNORE INTO "
                        + qualify("st_trace_entry")
                        + "(trace_id, sink_task_id, entry_index, stage, task_id, ts_ms, worker_address, task_group_name, task_class) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IngestedEvent e : events) {
                if (e.getTraceId() == null || e.getSinkTaskId() == null || e.getEntries() == null) {
                    continue;
                }
                for (TraceEntry entry : e.getEntries()) {
                    ps.setLong(1, e.getTraceId());
                    ps.setLong(2, e.getSinkTaskId());
                    ps.setInt(3, entry.getIndex());
                    ps.setInt(4, entry.getStage());
                    ps.setLong(5, entry.getTaskId());
                    ps.setLong(6, entry.getTsMs());
                    ps.setString(7, entry.getWorkerAddress());
                    ps.setString(8, entry.getTaskGroupName());
                    ps.setString(9, entry.getTaskClass());
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
    }

    @Override
    public List<TraceSummary> queryTraces(TraceQuery query) {
        int limit = Math.max(1, Math.min(query.getLimit(), 2000));
        int offset = Math.max(0, query.getOffset());

        StringBuilder sql =
                new StringBuilder(
                        "SELECT trace_id, sink_task_id, job_id, table_id, created_time_ms, "
                                + "(UNIX_TIMESTAMP(received_at) * 1000 + FLOOR(MICROSECOND(received_at)/1000)) AS received_ms, "
                                + "start_ts_ms, entry_count "
                                + "FROM "
                                + qualify("st_trace")
                                + " WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (query.getJobId() != null && !query.getJobId().isEmpty()) {
            sql.append(" AND job_id = ?");
            params.add(query.getJobId());
        }
        if (query.getTableId() != null && !query.getTableId().isEmpty()) {
            sql.append(" AND table_id = ?");
            params.add(query.getTableId());
        }
        if (query.getFromMs() != null) {
            sql.append(" AND received_at >= FROM_UNIXTIME(? / 1000.0)");
            params.add(query.getFromMs());
        }
        if (query.getToMs() != null) {
            sql.append(" AND received_at <= FROM_UNIXTIME(? / 1000.0)");
            params.add(query.getToMs());
        }
        sql.append(" ORDER BY received_at DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            List<TraceSummary> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(
                            new TraceSummary(
                                    rs.getLong(1),
                                    rs.getLong(2),
                                    rs.getString(3),
                                    rs.getString(4),
                                    rs.getObject(5) == null ? null : rs.getLong(5),
                                    rs.getLong(6),
                                    rs.getObject(7) == null ? null : rs.getLong(7),
                                    rs.getObject(8) == null ? null : rs.getInt(8)));
                }
            }
            return out;
        } catch (SQLException e) {
            metrics.dbReadFailuresTotal.labels("queryTraces").inc();
            throw new RuntimeException("Failed to query traces", e);
        }
    }

    @Override
    public TraceDetail getTrace(long traceId, Long sinkTaskId) {
        if (sinkTaskId == null) {
            return null;
        }
        try (Connection conn = open()) {
            TraceSummary summary = queryOneTrace(conn, traceId, sinkTaskId);
            if (summary == null) {
                return null;
            }
            List<TraceEntry> entries = queryEntries(conn, traceId, sinkTaskId);
            return new TraceDetail(summary, entries);
        } catch (SQLException e) {
            metrics.dbReadFailuresTotal.labels("getTrace").inc();
            throw new RuntimeException("Failed to get trace detail", e);
        }
    }

    private TraceSummary queryOneTrace(Connection conn, long traceId, long sinkTaskId)
            throws SQLException {
        String sql =
                "SELECT trace_id, sink_task_id, job_id, table_id, created_time_ms, "
                        + "(UNIX_TIMESTAMP(received_at) * 1000 + FLOOR(MICROSECOND(received_at)/1000)) AS received_ms, "
                        + "start_ts_ms, entry_count "
                        + "FROM "
                        + qualify("st_trace")
                        + " WHERE trace_id = ? AND sink_task_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, traceId);
            ps.setLong(2, sinkTaskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new TraceSummary(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getObject(5) == null ? null : rs.getLong(5),
                        rs.getLong(6),
                        rs.getObject(7) == null ? null : rs.getLong(7),
                        rs.getObject(8) == null ? null : rs.getInt(8));
            }
        }
    }

    private List<TraceEntry> queryEntries(Connection conn, long traceId, long sinkTaskId)
            throws SQLException {
        String sql =
                "SELECT entry_index, stage, task_id, ts_ms, worker_address, task_group_name, task_class "
                        + "FROM "
                        + qualify("st_trace_entry")
                        + " WHERE trace_id = ? AND sink_task_id = ? ORDER BY entry_index ASC";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, traceId);
            ps.setLong(2, sinkTaskId);
            List<TraceEntry> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TraceEntry e = new TraceEntry();
                    e.setIndex(rs.getInt(1));
                    e.setStage(rs.getInt(2));
                    e.setTaskId(rs.getLong(3));
                    e.setTsMs(rs.getLong(4));
                    e.setWorkerAddress(rs.getString(5));
                    e.setTaskGroupName(rs.getString(6));
                    e.setTaskClass(rs.getString(7));
                    out.add(e);
                }
            }
            return out;
        }
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(
                config.getJdbcUrl(), config.getJdbcUsername(), config.getJdbcPassword());
    }

    private String qualify(String table) {
        if (schema.isEmpty()) {
            return "`" + table + "`";
        }
        return "`" + schema + "`.`" + table + "`";
    }

    private static String normalizeSchema(String schema) {
        if (schema == null) {
            return "";
        }
        String t = schema.trim();
        return t.isEmpty() ? "" : t;
    }
}
