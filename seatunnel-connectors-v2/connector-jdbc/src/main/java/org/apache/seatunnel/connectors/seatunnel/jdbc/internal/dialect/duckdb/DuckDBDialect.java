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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/** DuckDB dialect implementation. */
@Slf4j
public class DuckDBDialect implements JdbcDialect {

    // DuckDB default fetch size - optimized for DuckDB
    private static final int DEFAULT_FETCH_SIZE = 5000;
    // DuckDB optimal batch size for bulk operations
    private static final int OPTIMAL_BATCH_SIZE = 10000;
    // DuckDB connection validation timeout in seconds
    private static final int CONNECTION_VALIDATION_TIMEOUT = 10;

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DuckDBJdbcRowConverter();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return String.format("MOD(ABS(HASH(%s)), %d)", quoteIdentifier(fieldName), mod);
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DuckDBTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        if (StringUtils.isNotBlank(database)) {
            return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
        }
        return quoteIdentifier(tableName);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String tableIdentifier = tableIdentifier(database, tableName);
        String updateColumns =
                Arrays.stream(fieldNames)
                        .filter(field -> !Arrays.asList(uniqueKeyFields).contains(field))
                        .map(
                                field ->
                                        String.format(
                                                "%s = EXCLUDED.%s",
                                                quoteIdentifier(field), quoteIdentifier(field)))
                        .collect(Collectors.joining(", "));

        String conflictColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String placeholders =
                Arrays.stream(fieldNames).map(field -> "?").collect(Collectors.joining(", "));

        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ")
                .append(tableIdentifier)
                .append("(")
                .append(columns)
                .append(") VALUES (")
                .append(placeholders)
                .append(")");

        if (uniqueKeyFields.length > 0) {
            builder.append(" ON CONFLICT (")
                    .append(conflictColumns)
                    .append(") DO UPDATE SET ")
                    .append(updateColumns);
        }

        return Optional.of(builder.toString());
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // DuckDB specific optimizations
        int optimizedFetchSize = fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE;

        // For DuckDB, larger fetch sizes generally perform better due to columnar storage
        if (optimizedFetchSize < 1000) {
            optimizedFetchSize = Math.max(optimizedFetchSize, 1000);
        }

        statement.setFetchSize(optimizedFetchSize);

        // Set query timeout for DuckDB (30 seconds default)
        statement.setQueryTimeout(30);

        return statement;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tableIdentifier(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        String sql;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sql = String.format("SELECT COUNT(*) FROM (%s) tmp", table.getQuery());
        } else {
            sql = String.format("SELECT COUNT(*) FROM %s", tableIdentifier(table.getTablePath()));
        }

        try (PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0L;
    }

    @Override
    public Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quoteIdentifier(columnName);
        String sqlQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM (%s) WHERE %s >= ? LIMIT %s"
                                    + ") tmp",
                            quotedColumn, quotedColumn, table.getQuery(), quotedColumn, chunkSize);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM %s WHERE %s >= ? LIMIT %s"
                                    + ") tmp",
                            quotedColumn,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            chunkSize);
        }

        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
                return rs.getObject(1);
            }
        }
    }

    /**
     * Get optimal batch size for DuckDB operations DuckDB performs better with larger batches due
     * to its columnar storage
     */
    public int getOptimalBatchSize() {
        return OPTIMAL_BATCH_SIZE;
    }

    /**
     * Check if DuckDB supports bulk copy operations DuckDB has excellent bulk loading capabilities
     */
    public boolean supportsBulkCopy() {
        return true;
    }

    /**
     * Generate DuckDB-specific bulk insert statement Uses VALUES clause optimization for better
     * performance
     */
    public Optional<String> getBulkInsertStatement(
            String database, String tableName, String[] fieldNames, int batchSize) {
        String tableIdentifier = tableIdentifier(database, tableName);
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        StringBuilder valuesBuilder = new StringBuilder();
        for (int i = 0; i < batchSize; i++) {
            if (i > 0) {
                valuesBuilder.append(", ");
            }
            valuesBuilder.append("(");
            valuesBuilder.append(
                    Arrays.stream(fieldNames).map(field -> "?").collect(Collectors.joining(", ")));
            valuesBuilder.append(")");
        }

        return Optional.of(
                String.format(
                        "INSERT INTO %s (%s) VALUES %s",
                        tableIdentifier, columns, valuesBuilder.toString()));
    }

    /** Get connection validation timeout specific to DuckDB */
    public int getConnectionValidationTimeout() {
        return CONNECTION_VALIDATION_TIMEOUT;
    }
}
