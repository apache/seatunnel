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

package org.apache.seatunnel.trace.collector.db.postgres;

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
public class PostgresTraceRepository implements TraceRepository {

    private final TraceCollectorConfig config;
    private final TraceCollectorMetrics metrics;
    private final String schema;

    public PostgresTraceRepository(TraceCollectorConfig config, TraceCollectorMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
        this.schema = config.getDbSchema();
    }

    @Override
    public void init() {
        try (Connection conn = open()) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
                st.execute(
                        "CREATE TABLE IF NOT EXISTS "
                                + schema
                                + ".st_trace_event_raw ("
                                + "id BIGSERIAL PRIMARY KEY,"
                                + "received_at TIMESTAMPTZ NOT NULL,"
                                + "job_id TEXT,"
                                + "event_type TEXT,"
                                + "body_json JSONB NOT NULL"
                                + ")");
                st.execute(
                        "CREATE TABLE IF NOT EXISTS "
                                + schema
                                + ".st_trace ("
                                + "trace_id BIGINT NOT NULL,"
                                + "sink_task_id BIGINT NOT NULL,"
                                + "job_id TEXT,"
                                + "table_id TEXT,"
                                + "created_time_ms BIGINT,"
                                + "received_at TIMESTAMPTZ NOT NULL,"
                                + "payload BYTEA,"
                                + "start_ts_ms BIGINT,"
                                + "entry_count INT,"
                                + "PRIMARY KEY(trace_id, sink_task_id)"
                                + ")");
                st.execute(
                        "CREATE INDEX IF NOT EXISTS idx_st_trace_job_received ON "
                                + schema
                                + ".st_trace(job_id, received_at DESC)");
                st.execute(
                        "CREATE TABLE IF NOT EXISTS "
                                + schema
                                + ".st_trace_entry ("
                                + "trace_id BIGINT NOT NULL,"
                                + "sink_task_id BIGINT NOT NULL,"
                                + "entry_index INT NOT NULL,"
                                + "stage SMALLINT NOT NULL,"
                                + "task_id BIGINT NOT NULL,"
                                + "ts_ms BIGINT NOT NULL,"
                                + "worker_address TEXT,"
                                + "task_group_name TEXT,"
                                + "task_class TEXT,"
                                + "PRIMARY KEY(trace_id, sink_task_id, entry_index)"
                                + ")");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to init postgres repository", e);
        }
    }

    @Override
    public void ingest(List<IngestedEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        try (Connection conn = open()) {
            conn.setAutoCommit(false);

            insertRaw(conn, events);
            insertTraces(conn, events);
            insertEntries(conn, events);

            conn.commit();
        } catch (Exception e) {
            metrics.dbWriteFailuresTotal.labels("all").inc();
            throw new RuntimeException("Failed to ingest events", e);
        }
    }

    private void insertRaw(Connection conn, List<IngestedEvent> events) throws SQLException {
        String sql =
                "INSERT INTO "
                        + schema
                        + ".st_trace_event_raw(received_at, job_id, event_type, body_json) "
                        + "VALUES (to_timestamp(? / 1000.0), ?, ?, ?::jsonb)";
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
                        + schema
                        + ".st_trace(trace_id, sink_task_id, job_id, table_id, created_time_ms, received_at, payload, start_ts_ms, entry_count) "
                        + "VALUES (?, ?, ?, ?, ?, to_timestamp(? / 1000.0), ?, ?, ?) "
                        + "ON CONFLICT(trace_id, sink_task_id) DO UPDATE SET "
                        + "received_at=EXCLUDED.received_at, payload=EXCLUDED.payload, start_ts_ms=EXCLUDED.start_ts_ms, entry_count=EXCLUDED.entry_count";
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
                "INSERT INTO "
                        + schema
                        + ".st_trace_entry(trace_id, sink_task_id, entry_index, stage, task_id, ts_ms, worker_address, task_group_name, task_class) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "ON CONFLICT(trace_id, sink_task_id, entry_index) DO NOTHING";
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
                        "SELECT trace_id, sink_task_id, job_id, table_id, created_time_ms, extract(epoch from received_at)*1000 as received_ms, start_ts_ms, entry_count "
                                + "FROM "
                                + schema
                                + ".st_trace WHERE 1=1");
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
            sql.append(" AND received_at >= to_timestamp(? / 1000.0)");
            params.add(query.getFromMs());
        }
        if (query.getToMs() != null) {
            sql.append(" AND received_at <= to_timestamp(? / 1000.0)");
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
                                    rs.getLong(5),
                                    rs.getLong(6),
                                    rs.getLong(7),
                                    rs.getInt(8)));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query traces", e);
        }
    }

    @Override
    public TraceDetail getTrace(long traceId, Long sinkTaskId) {
        TraceSummary summary = selectTraceSummary(traceId, sinkTaskId);
        if (summary == null) {
            return new TraceDetail(null, new ArrayList<>());
        }
        List<TraceEntry> entries = selectEntries(traceId, summary.getSinkTaskId());
        return new TraceDetail(summary, entries);
    }

    private TraceSummary selectTraceSummary(long traceId, Long sinkTaskId) {
        String sql =
                "SELECT trace_id, sink_task_id, job_id, table_id, created_time_ms, extract(epoch from received_at)*1000 as received_ms, start_ts_ms, entry_count "
                        + "FROM "
                        + schema
                        + ".st_trace WHERE trace_id = ?"
                        + (sinkTaskId == null
                                ? " ORDER BY received_at DESC LIMIT 1"
                                : " AND sink_task_id = ?");
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, traceId);
            if (sinkTaskId != null) {
                ps.setLong(2, sinkTaskId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new TraceSummary(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getLong(5),
                        rs.getLong(6),
                        rs.getLong(7),
                        rs.getInt(8));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to select trace", e);
        }
    }

    private List<TraceEntry> selectEntries(long traceId, long sinkTaskId) {
        String sql =
                "SELECT entry_index, stage, task_id, ts_ms, worker_address, task_group_name, task_class "
                        + "FROM "
                        + schema
                        + ".st_trace_entry WHERE trace_id = ? AND sink_task_id = ? ORDER BY entry_index ASC";
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, traceId);
            ps.setLong(2, sinkTaskId);
            List<TraceEntry> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TraceEntry e =
                            new TraceEntry(
                                    rs.getInt(1),
                                    rs.getInt(2),
                                    rs.getLong(3),
                                    rs.getLong(4),
                                    rs.getString(5),
                                    rs.getString(6),
                                    rs.getString(7));
                    out.add(e);
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to select entries", e);
        }
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(
                config.getJdbcUrl(), config.getJdbcUsername(), config.getJdbcPassword());
    }

    @Override
    public void close() throws IOException {
        // no-op (DriverManager based)
    }
}
