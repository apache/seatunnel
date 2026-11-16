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
     * Whether this {@code AS OF SCN} failure can fall back to the current row for this event only.
     * Matches Debezium {@code OracleConnection}: ORA-01555 and ORA-01466, including wrapped causes.
     * Privilege and invalid-SCN errors are not treated as a current-row fallback.
     */
    public static boolean shouldFallbackToCurrentRow(Throwable error) {
        List<Throwable> seen = new ArrayList<Throwable>();
        return shouldFallbackToCurrentRow(error, seen);
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

    private static boolean shouldFallbackToCurrentRow(Throwable error, List<Throwable> seen) {
        if (error == null || contains(seen, error)) {
            return false;
        }
        seen.add(error);
        if (error instanceof SQLException) {
            SQLException sqlException = (SQLException) error;
            if (isAgedFlashbackCode(sqlException.getErrorCode())
                    || messageIndicatesAgedFlashback(sqlException.getMessage())) {
                return true;
            }
            if (shouldFallbackToCurrentRow(sqlException.getNextException(), seen)) {
                return true;
            }
        } else if (messageIndicatesAgedFlashback(error.getMessage())) {
            return true;
        }
        return shouldFallbackToCurrentRow(error.getCause(), seen);
    }

    private static boolean contains(List<Throwable> seen, Throwable error) {
        for (Throwable item : seen) {
            if (item == error) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAgedFlashbackCode(int errorCode) {
        return errorCode == ORA_SNAPSHOT_TOO_OLD || errorCode == ORA_TABLE_DEFINITION_CHANGED;
    }

    private static boolean messageIndicatesAgedFlashback(String message) {
        if (message == null) {
            return false;
        }
        return message.contains("ORA-01555") || message.contains("ORA-01466");
    }
}
