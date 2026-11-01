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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Builds the Oracle LOB re-select statement and classifies flashback failures. */
public final class OracleLobReselectSql {

    /** Snapshot too old. The commit SCN is no longer available for AS OF SCN. */
    static final int ORA_SNAPSHOT_TOO_OLD = 1555;

    /** Table definition changed. Flashback cannot read the table at the commit SCN. */
    static final int ORA_TABLE_DEFINITION_CHANGED = 1466;

    /** The SCN is outside the flashback window. */
    static final int ORA_INVALID_SCN = 8181;

    /** The user cannot run the flashback query. */
    static final int ORA_INSUFFICIENT_PRIVILEGES = 1031;

    private OracleLobReselectSql() {}

    /**
     * Returns a positive decimal SCN that can be inlined into {@code AS OF SCN}, or null when the
     * event has no usable commit SCN.
     */
    public static String usableCommitScn(String commitScn) {
        if (commitScn == null) {
            return null;
        }
        String trimmed = commitScn.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        for (int i = 0; i < trimmed.length(); i++) {
            if (trimmed.charAt(i) < '0' || trimmed.charAt(i) > '9') {
                return null;
            }
        }
        for (int i = 0; i < trimmed.length(); i++) {
            if (trimmed.charAt(i) != '0') {
                return trimmed;
            }
        }
        return null;
    }

    /**
     * Builds {@code SELECT lob... FROM schema.table [AS OF SCN ?] WHERE pk = ?}.
     *
     * <p>A usable commit SCN adds a bind marker. The numeric value is not inlined, so Oracle can
     * reuse one cursor for every execution of the same column set. Identifiers are double-quoted.
     */
    public static String buildQuery(
            String schema,
            String table,
            List<String> columns,
            List<String> primaryKeyColumns,
            String commitScn) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("LOB re-select requires at least one column");
        }
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("LOB re-select requires a primary key");
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quote(columns.get(i)));
        }
        String scn = usableCommitScn(commitScn);
        if (scn == null) {
            sql.append(" FROM ").append(quoteTable(schema, table));
        } else {
            sql.append(" FROM (SELECT * FROM ")
                    .append(quoteTable(schema, table))
                    .append(" AS OF SCN ?)");
        }
        sql.append(" WHERE ");
        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(quote(primaryKeyColumns.get(i))).append("=?");
        }
        return sql.toString();
    }

    /**
     * Whether the failure is specific to {@code AS OF SCN} and a current-row query can be tried.
     * Covers ORA-01555, ORA-01466, ORA-08181, and ORA-01031, including wrapped causes.
     */
    public static boolean isFlashbackUnavailable(Throwable error) {
        List<Throwable> seen = new ArrayList<Throwable>();
        return isFlashbackUnavailable(error, seen);
    }

    public static String quote(String identifier) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Oracle identifier must not be empty");
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String quoteTable(String schema, String table) {
        if (schema == null || schema.trim().isEmpty()) {
            return quote(table);
        }
        return quote(schema) + "." + quote(table);
    }

    private static boolean isFlashbackUnavailable(Throwable error, List<Throwable> seen) {
        if (error == null || contains(seen, error)) {
            return false;
        }
        seen.add(error);
        if (error instanceof SQLException) {
            SQLException sqlException = (SQLException) error;
            if (isFlashbackCode(sqlException.getErrorCode())
                    || messageIndicatesFlashback(sqlException.getMessage())) {
                return true;
            }
            if (isFlashbackUnavailable(sqlException.getNextException(), seen)) {
                return true;
            }
        } else if (messageIndicatesFlashback(error.getMessage())) {
            return true;
        }
        return isFlashbackUnavailable(error.getCause(), seen);
    }

    private static boolean contains(List<Throwable> seen, Throwable error) {
        for (Throwable item : seen) {
            if (item == error) {
                return true;
            }
        }
        return false;
    }

    private static boolean isFlashbackCode(int errorCode) {
        return errorCode == ORA_SNAPSHOT_TOO_OLD
                || errorCode == ORA_TABLE_DEFINITION_CHANGED
                || errorCode == ORA_INVALID_SCN
                || errorCode == ORA_INSUFFICIENT_PRIVILEGES;
    }

    private static boolean messageIndicatesFlashback(String message) {
        if (message == null) {
            return false;
        }
        return message.contains("ORA-01555")
                || message.contains("ORA-01466")
                || message.contains("ORA-08181")
                || message.contains("ORA-01031");
    }
}
