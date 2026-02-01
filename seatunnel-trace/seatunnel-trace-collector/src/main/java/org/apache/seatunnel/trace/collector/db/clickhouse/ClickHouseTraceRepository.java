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

package org.apache.seatunnel.trace.collector.db.clickhouse;

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
public class ClickHouseTraceRepository implements TraceRepository {

    private final TraceCollectorConfig config;
    private final TraceCollectorMetrics metrics;

    public ClickHouseTraceRepository(TraceCollectorConfig config, TraceCollectorMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public void init() {
        try (Connection conn = open();
                Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE IF NOT EXISTS st_trace_event_raw ("
                            + "received_at DateTime64(3),"
                            + "job_id String,"
                            + "event_type String,"
                            + "body_json String"
                            + ") ENGINE = MergeTree ORDER BY (job_id, received_at)");

            st.execute(
                    "CREATE TABLE IF NOT EXISTS st_trace ("
                            + "trace_id Int64,"
                            + "sink_task_id Int64,"
                            + "job_id String,"
                            + "table_id String,"
                            + "created_time_ms Int64,"
                            + "received_at DateTime64(3),"
                            + "payload String,"
                            + "start_ts_ms Int64,"
                            + "entry_count Int32"
                            + ") ENGINE = MergeTree ORDER BY (job_id, received_at, trace_id, sink_task_id)");

            st.execute(
                    "CREATE TABLE IF NOT EXISTS st_trace_entry ("
                            + "trace_id Int64,"
                            + "sink_task_id Int64,"
                            + "entry_index Int32,"
                            + "stage UInt16,"
                            + "task_id Int64,"
                            + "ts_ms Int64,"
                            + "worker_address String,"
                            + "task_group_name String,"
                            + "task_class String"
                            + ") ENGINE = MergeTree ORDER BY (trace_id, sink_task_id, entry_index)");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to init clickhouse repository", e);
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
                "INSERT INTO st_trace_event_raw(received_at, job_id, event_type, body_json) VALUES (fromUnixTimestamp64Milli(?), ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IngestedEvent e : events) {
                ps.setLong(1, e.getReceivedTimeMs());
                ps.setString(2, nullToEmpty(e.getJobId()));
                ps.setString(3, nullToEmpty(e.getEventType()));
                ps.setString(4, nullToEmpty(e.getRawJson()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertTraces(Connection conn, List<IngestedEvent> events) throws SQLException {
        String sql =
                "INSERT INTO st_trace(trace_id, sink_task_id, job_id, table_id, created_time_ms, received_at, payload, start_ts_ms, entry_count) "
                        + "VALUES (?, ?, ?, ?, ?, fromUnixTimestamp64Milli(?), ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IngestedEvent e : events) {
                if (e.getTraceId() == null || e.getSinkTaskId() == null) {
                    continue;
                }
                ps.setLong(1, e.getTraceId());
                ps.setLong(2, e.getSinkTaskId());
                ps.setString(3, nullToEmpty(e.getJobId()));
                ps.setString(4, nullToEmpty(e.getTableId()));
                ps.setLong(5, e.getCreatedTimeMs() == null ? 0L : e.getCreatedTimeMs());
                ps.setLong(6, e.getReceivedTimeMs());
                ps.setString(
                        7,
                        e.getPayloadBytes() == null
                                ? ""
                                : java.util.Base64.getEncoder()
                                        .encodeToString(e.getPayloadBytes()));
                ps.setLong(8, e.getStartTsMs() == null ? 0L : e.getStartTsMs());
                ps.setInt(9, e.getEntryCount() == null ? 0 : e.getEntryCount());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertEntries(Connection conn, List<IngestedEvent> events) throws SQLException {
        String sql =
                "INSERT INTO st_trace_entry(trace_id, sink_task_id, entry_index, stage, task_id, ts_ms, worker_address, task_group_name, task_class) "
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
                    ps.setString(7, nullToEmpty(entry.getWorkerAddress()));
                    ps.setString(8, nullToEmpty(entry.getTaskGroupName()));
                    ps.setString(9, nullToEmpty(entry.getTaskClass()));
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
                        "SELECT trace_id, sink_task_id, job_id, table_id, created_time_ms, toUnixTimestamp64Milli(received_at), start_ts_ms, entry_count FROM st_trace WHERE 1=1");
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
            sql.append(" AND received_at >= fromUnixTimestamp64Milli(?)");
            params.add(query.getFromMs());
        }
        if (query.getToMs() != null) {
            sql.append(" AND received_at <= fromUnixTimestamp64Milli(?)");
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
        // ClickHouse: keep it simple, require sinkTaskId
        if (sinkTaskId == null) {
            return new TraceDetail(null, new ArrayList<>());
        }
        TraceSummary summary = selectTraceSummary(traceId, sinkTaskId);
        if (summary == null) {
            return new TraceDetail(null, new ArrayList<>());
        }
        List<TraceEntry> entries = selectEntries(traceId, sinkTaskId);
        return new TraceDetail(summary, entries);
    }

    private TraceSummary selectTraceSummary(long traceId, long sinkTaskId) {
        String sql =
                "SELECT trace_id, sink_task_id, job_id, table_id, created_time_ms, toUnixTimestamp64Milli(received_at), start_ts_ms, entry_count "
                        + "FROM st_trace WHERE trace_id = ? AND sink_task_id = ? ORDER BY received_at DESC LIMIT 1";
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(sql)) {
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
                        + "FROM st_trace_entry WHERE trace_id = ? AND sink_task_id = ? ORDER BY entry_index ASC";
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, traceId);
            ps.setLong(2, sinkTaskId);
            List<TraceEntry> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(
                            new TraceEntry(
                                    rs.getInt(1),
                                    rs.getInt(2),
                                    rs.getLong(3),
                                    rs.getLong(4),
                                    rs.getString(5),
                                    rs.getString(6),
                                    rs.getString(7)));
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

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
