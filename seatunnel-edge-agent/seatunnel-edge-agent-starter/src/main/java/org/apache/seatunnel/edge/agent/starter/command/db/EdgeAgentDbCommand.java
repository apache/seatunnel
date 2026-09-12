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

package org.apache.seatunnel.edge.agent.starter.command.db;

import org.apache.seatunnel.edge.agent.starter.command.EdgeAgentCommand;
import org.apache.seatunnel.edge.agent.starter.command.EdgeAgentPaths;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecordStatus;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class EdgeAgentDbCommand implements EdgeAgentCommand<DbCommandArgs> {

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private static final int PAYLOAD_PREVIEW_BYTES = 64;

    private final DbCommandArgs cli;
    private final DbSubcommand subcommand;
    private final EdgeAgentPaths paths;

    public EdgeAgentDbCommand(DbCommandArgs cli, DbSubcommand subcommand) {
        this.cli = cli;
        this.subcommand = subcommand;
        this.paths = EdgeAgentPaths.forDb(cli.getSqlitePathOverride());
    }

    @Override
    public void execute() throws Exception {
        switch (subcommand) {
            case INFO:
                runInfo();
                return;
            case WAL_SUMMARY:
                runWalSummary();
                return;
            case WAL_LIST:
                runWalList(cli.getStatus(), cli.getLimit());
                return;
            case WAL_SHOW:
                runWalShow(cli.getWalId().longValue());
                return;
            case POSITIONS:
                runPositions(cli.getSourceId());
                return;
            case WAL_PURGE_DEAD:
                runWalPurgeDead();
                return;
            case WAL_RETRY_DEAD:
                runWalRetryDead();
                return;
            case WAL_UNSTICK_SENDING:
                runWalUnstickSending();
                return;
            case WAL_PURGE_ACKED:
                runWalPurgeAcked();
                return;
            default:
                throw new IllegalStateException("Unhandled subcommand: " + subcommand);
        }
    }

    private void runInfo() throws Exception {
        Path db = paths.getSqlitePath();
        boolean running = paths.agentRunning();
        System.out.println("install-root:  " + paths.getInstallRoot());
        System.out.println("sqlite-path:   " + db);
        System.out.println("pid-file:      " + paths.getPidFile());
        System.out.println("agent-running: " + running);
        System.out.println("wal-row-ids:   db wal-list  (id column -> wal-show --id)");
        printFileSize("db-main", db);
        printFileSize("db-wal", Paths.get(db.toString() + "-wal"));
        printFileSize("db-shm", Paths.get(db.toString() + "-shm"));
    }

    private void printFileSize(String label, Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            System.out.printf(Locale.ROOT, "%s: %s (%d bytes)%n", label, path, Files.size(path));
        } else {
            System.out.printf(Locale.ROOT, "%s: %s (missing)%n", label, path);
        }
    }

    private void runWalSummary() throws Exception {
        try (EdgeAgentDbConnection db = EdgeAgentDbConnection.openReadOnly(paths.getSqlitePath())) {
            Connection connection = db.getConnection();
            System.out.println("WAL status counts:");
            try (PreparedStatement statement =
                            connection.prepareStatement(EdgeAgentDbSql.WAL_COUNT_BY_STATUS);
                    ResultSet rs = statement.executeQuery()) {
                boolean any = false;
                while (rs.next()) {
                    any = true;
                    System.out.printf(
                            Locale.ROOT, "  %-8s %d%n", rs.getString("status"), rs.getLong("cnt"));
                }
                if (!any) {
                    System.out.println("  (empty)");
                }
            }
            System.out.println("Oldest updated_at per status:");
            try (PreparedStatement statement =
                            connection.prepareStatement(EdgeAgentDbSql.WAL_OLDEST_BY_STATUS);
                    ResultSet rs = statement.executeQuery()) {
                boolean any = false;
                while (rs.next()) {
                    any = true;
                    long updatedAt = rs.getLong("oldest_updated_at");
                    System.out.printf(
                            Locale.ROOT,
                            "  %-8s %s (%d ms ago)%n",
                            rs.getString("status"),
                            formatTime(updatedAt),
                            ageMs(updatedAt));
                }
                if (!any) {
                    System.out.println("  (empty)");
                }
            }
        }
    }

    private void runWalList(String status, int limit) throws Exception {
        if (limit < 1) {
            throw new IllegalArgumentException("--limit must be >= 1");
        }
        if (status != null) {
            validateWalStatus(status);
        }

        try (EdgeAgentDbConnection db = EdgeAgentDbConnection.openReadOnly(paths.getSqlitePath())) {
            try (PreparedStatement statement =
                    db.getConnection().prepareStatement(EdgeAgentDbSql.WAL_LIST)) {
                statement.setString(1, status);
                statement.setString(2, status);
                statement.setInt(3, limit);
                try (ResultSet rs = statement.executeQuery()) {
                    System.out.printf(
                            Locale.ROOT,
                            "%8s %10s %-8s %7s %24s %12s %8s%n",
                            "id(pk)",
                            "batch_id",
                            "status",
                            "attempt",
                            "updated_at",
                            "source_id",
                            "bytes");
                    boolean any = false;
                    while (rs.next()) {
                        any = true;
                        long updatedAt = rs.getLong("updated_at");
                        System.out.printf(
                                Locale.ROOT,
                                "%8d %10d %-8s %7d %s (%dms) %-12s %8d%n",
                                rs.getLong("id"),
                                rs.getLong("batch_id"),
                                rs.getString("status"),
                                rs.getInt("attempt_count"),
                                formatTime(updatedAt),
                                ageMs(updatedAt),
                                nullToDash(rs.getString("source_id")),
                                rs.getInt("payload_bytes"));
                    }
                    if (!any) {
                        System.out.println("(no rows)");
                    }
                    System.out.println(
                            "Hint: use id(pk) (first column) with: db wal-show --id <id>");
                }
            }
        }
    }

    private void runWalShow(long id) throws Exception {
        try (EdgeAgentDbConnection db = EdgeAgentDbConnection.openReadOnly(paths.getSqlitePath())) {
            try (PreparedStatement statement =
                    db.getConnection().prepareStatement(EdgeAgentDbSql.WAL_SHOW)) {
                statement.setLong(1, id);
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        System.out.println("No WAL row with id=" + id);
                        return;
                    }
                    System.out.println("id:            " + rs.getLong("id"));
                    System.out.println("batch_id:      " + rs.getLong("batch_id"));
                    System.out.println("status:        " + rs.getString("status"));
                    System.out.println("attempt_count: " + rs.getInt("attempt_count"));
                    System.out.println(
                            "created_at:    "
                                    + formatTime(rs.getLong("created_at"))
                                    + " ("
                                    + ageMs(rs.getLong("created_at"))
                                    + " ms ago)");
                    System.out.println(
                            "updated_at:    "
                                    + formatTime(rs.getLong("updated_at"))
                                    + " ("
                                    + ageMs(rs.getLong("updated_at"))
                                    + " ms ago)");
                    System.out.println("source_id:     " + nullToDash(rs.getString("source_id")));
                    System.out.println("event_time:    " + rs.getLong("event_time"));
                    byte[] payload = rs.getBytes("payload");
                    int len = payload == null ? 0 : payload.length;
                    System.out.println("payload_bytes: " + len);
                    if (len > 0) {
                        int previewLen = Math.min(len, PAYLOAD_PREVIEW_BYTES);
                        byte[] preview = new byte[previewLen];
                        System.arraycopy(payload, 0, preview, 0, previewLen);
                        System.out.println("payload_utf8:  " + previewUtf8(preview));
                        System.out.println("payload_hex:   " + toHex(preview));
                        if (len > previewLen) {
                            System.out.println(
                                    "  ... truncated preview (" + previewLen + " bytes)");
                        }
                    }
                }
            }
        }
    }

    private void runPositions(String sourceId) throws Exception {
        try (EdgeAgentDbConnection db = EdgeAgentDbConnection.openReadOnly(paths.getSqlitePath())) {
            try (PreparedStatement statement =
                    db.getConnection().prepareStatement(EdgeAgentDbSql.POSITION_LIST)) {
                statement.setString(1, sourceId);
                statement.setString(2, sourceId);
                try (ResultSet rs = statement.executeQuery()) {
                    System.out.printf(
                            Locale.ROOT,
                            "%-20s %-40s %12s %24s%n",
                            "source_id",
                            "partition",
                            "offset",
                            "updated_at");
                    boolean any = false;
                    while (rs.next()) {
                        any = true;
                        long updatedAt = rs.getLong("updated_at");
                        System.out.printf(
                                Locale.ROOT,
                                "%-20s %-40s %12d %s%n",
                                rs.getString("source_id"),
                                rs.getString("partition_key"),
                                rs.getLong("offset_value"),
                                formatTime(updatedAt));
                    }
                    if (!any) {
                        System.out.println("(no rows)");
                    }
                }
            }
        }
    }

    private void runWalPurgeDead() throws Exception {
        DbWriteGuard.requireWriteAllowed(paths, cli);
        executeStatusMutation(
                WalRecordStatus.DEAD.name(),
                EdgeAgentDbSql.WAL_COUNT_BY_STATUS_FILTER,
                EdgeAgentDbSql.WAL_PURGE_DEAD,
                null,
                "DELETE DEAD");
    }

    private void runWalRetryDead() throws Exception {
        DbWriteGuard.requireWriteAllowed(paths, cli);
        long now = System.currentTimeMillis();
        executeStatusMutation(
                WalRecordStatus.DEAD.name(),
                EdgeAgentDbSql.WAL_COUNT_BY_STATUS_FILTER,
                EdgeAgentDbSql.WAL_RETRY_DEAD,
                new MutationBind(now, WalRecordStatus.PENDING.name()),
                "RESET DEAD -> PENDING");
    }

    private void runWalUnstickSending() throws Exception {
        DbWriteGuard.requireWriteAllowed(paths, cli);
        long now = System.currentTimeMillis();
        executeStatusMutation(
                WalRecordStatus.SENDING.name(),
                EdgeAgentDbSql.WAL_COUNT_BY_STATUS_FILTER,
                EdgeAgentDbSql.WAL_UNSTICK_SENDING,
                new MutationBind(now, WalRecordStatus.PENDING.name()),
                "RESET SENDING -> PENDING");
    }

    private void runWalPurgeAcked() throws Exception {
        DbWriteGuard.requireWriteAllowed(paths, cli);
        if (cli.getOlderThanMs() <= 0L) {
            throw new IllegalArgumentException(
                    "wal-purge-acked requires --older-than-ms <positive-ms>");
        }
        long cutoff = System.currentTimeMillis() - cli.getOlderThanMs();
        try (EdgeAgentDbConnection db =
                EdgeAgentDbConnection.openReadWrite(paths.getSqlitePath())) {
            long count = countAckedBefore(db, cutoff);
            if (cli.isDryRun()) {
                System.out.println(
                        "DRY-RUN: would delete "
                                + count
                                + " ACKED rows with updated_at < "
                                + cutoff);
                return;
            }
            if (!cli.isYes()) {
                throw new IllegalArgumentException("Refusing write without --yes");
            }
            try (PreparedStatement statement =
                    db.getConnection().prepareStatement(EdgeAgentDbSql.WAL_PURGE_ACKED_BEFORE)) {
                statement.setString(1, WalRecordStatus.ACKED.name());
                statement.setLong(2, cutoff);
                int deleted = statement.executeUpdate();
                System.out.println(
                        "Deleted " + deleted + " ACKED rows (updated_at < " + cutoff + ")");
            }
        }
    }

    private long countAckedBefore(EdgeAgentDbConnection db, long cutoff) throws SQLException {
        try (PreparedStatement statement =
                db.getConnection().prepareStatement(EdgeAgentDbSql.WAL_COUNT_ACKED_BEFORE)) {
            statement.setString(1, WalRecordStatus.ACKED.name());
            statement.setLong(2, cutoff);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void executeStatusMutation(
            String matchStatus, String countSql, String mutateSql, MutationBind bind, String label)
            throws Exception {
        try (EdgeAgentDbConnection db =
                EdgeAgentDbConnection.openReadWrite(paths.getSqlitePath())) {
            long count = countByStatus(db, countSql, matchStatus);
            if (cli.isDryRun()) {
                System.out.println("DRY-RUN: would affect " + count + " row(s): " + label);
                return;
            }
            if (!cli.isYes()) {
                throw new IllegalArgumentException("Refusing write without --yes");
            }
            int updated;
            if (bind == null) {
                try (PreparedStatement statement = db.getConnection().prepareStatement(mutateSql)) {
                    statement.setString(1, matchStatus);
                    updated = statement.executeUpdate();
                }
            } else {
                try (PreparedStatement statement = db.getConnection().prepareStatement(mutateSql)) {
                    statement.setString(1, bind.getTargetStatus());
                    statement.setLong(2, bind.getUpdatedAt());
                    statement.setString(3, matchStatus);
                    updated = statement.executeUpdate();
                }
            }
            System.out.println(label + ": affected " + updated + " row(s)");
        }
    }

    private long countByStatus(EdgeAgentDbConnection db, String sql, String status)
            throws SQLException {
        try (PreparedStatement statement = db.getConnection().prepareStatement(sql)) {
            statement.setString(1, status);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void validateWalStatus(String status) {
        try {
            WalRecordStatus.valueOf(status);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    "Invalid --status " + status + "; expected PENDING, SENDING, ACKED, or DEAD",
                    ex);
        }
    }

    private static String formatTime(long epochMs) {
        return TIME_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    private static long ageMs(long epochMs) {
        return Math.max(0L, System.currentTimeMillis() - epochMs);
    }

    private static String nullToDash(String value) {
        return value == null ? "-" : value;
    }

    private static String previewUtf8(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        return text.replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(String.format(Locale.ROOT, "%02x", b));
        }
        return builder.toString();
    }

    @AllArgsConstructor
    @Getter
    private static class MutationBind {
        private final long updatedAt;
        private final String targetStatus;
    }
}
