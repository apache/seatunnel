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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Collects one JDBC batch in a DuckDB temporary table, then inserts it into DuckLake with one SQL
 * statement. With DuckDB JDBC 1.3.1, a JDBC executeBatch into DuckLake produced a separate Parquet
 * file per row in a batch.
 */
public class DuckLakeBulkStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    private final TableSchema tableSchema;
    private final JdbcRowConverter converter;
    private final String targetTable;
    // A unique, connection-local stage avoids collisions with other writers on pooled connections.
    private final String stageTable =
            quote("__seatunnel_ducklake_" + UUID.randomUUID().toString().replace("-", ""));
    private final String columns;
    // Keep the batch until the single lake INSERT succeeds, rather than partially staging on write.
    private final List<SeaTunnelRow> rows = new ArrayList<>();

    private transient Connection connection;

    public DuckLakeBulkStatementExecutor(
            String database, String table, TableSchema tableSchema, JdbcRowConverter converter) {
        this.tableSchema = tableSchema;
        this.converter = converter;
        String[] path = (database + "." + table).split("\\.", -1);
        if ((path.length != 2 && path.length != 3)
                || Arrays.stream(path).anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException(
                    "ducklake_bulk_write requires database and table to form schema.table"
                            + " or catalog.schema.table");
        }
        this.targetTable =
                Arrays.stream(path)
                        .map(DuckLakeBulkStatementExecutor::quote)
                        .collect(Collectors.joining("."));
        this.columns =
                Arrays.stream(tableSchema.getFieldNames())
                        .map(DuckLakeBulkStatementExecutor::quote)
                        .collect(Collectors.joining(", "));
    }

    /** Checks the attached target and input columns without creating a staging table or writing. */
    public void validateTarget(Connection connection) throws SQLException {
        try (Statement check = connection.createStatement();
                ResultSet ignored =
                        check.executeQuery(
                                "SELECT " + columns + " FROM " + targetTable + " WHERE FALSE")) {
            // Successful preparation and execution prove that the target and columns are visible.
        }
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        validateTarget(connection);
        this.connection = connection;
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        if (record.getRowKind() != RowKind.INSERT) {
            throw new SQLException(
                    "ducklake_bulk_write supports INSERT rows only; received "
                            + record.getRowKind(),
                    "0A000");
        }
        rows.add(record.copy());
    }

    @Override
    public void executeBatch() throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        // DELETE/TRUNCATE does not guarantee reclaiming old stage storage. Recreate per flush,
        // including after a failed transfer, so old rows and storage cannot accumulate.
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + stageTable);
            statement.execute(
                    "CREATE TEMP TABLE "
                            + stageTable
                            + " AS SELECT "
                            + columns
                            + " FROM "
                            + targetTable
                            + " WHERE FALSE");
            String placeholders =
                    Arrays.stream(tableSchema.getFieldNames())
                            .map(field -> "?")
                            .collect(Collectors.joining(", "));
            try (PreparedStatement stageInsert =
                    connection.prepareStatement(
                            "INSERT INTO "
                                    + stageTable
                                    + " ("
                                    + columns
                                    + ") VALUES ("
                                    + placeholders
                                    + ")")) {
                for (SeaTunnelRow row : rows) {
                    converter.toExternal(tableSchema, null, row, stageInsert);
                    stageInsert.addBatch();
                }
                stageInsert.executeBatch();
                statement.executeUpdate(
                        "INSERT INTO "
                                + targetTable
                                + " ("
                                + columns
                                + ") SELECT "
                                + columns
                                + " FROM "
                                + stageTable);
            }
            rows.clear();
        }
    }

    @Override
    public void clearBatch() {
        rows.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            if (connection != null) {
                // Use a fresh statement: DuckDB may close the statement that failed a lake insert.
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS " + stageTable);
                }
            }
        } finally {
            connection = null;
            rows.clear();
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
