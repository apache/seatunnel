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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * DuckDB dialect implementation for JDBC operations.
 *
 * <p>This dialect provides DuckDB-specific SQL generation, type mapping, and query optimization.
 * DuckDB is an in-process analytical database with columnar storage architecture, which benefits
 * from larger fetch sizes and batch operations.
 */
@Slf4j
public class DuckDBDialect implements JdbcDialect {

    /** DuckDB default fetch size - optimized for columnar storage architecture */
    private static final int DEFAULT_FETCH_SIZE = 5000;

    /** DuckDB optimal batch size for bulk operations - larger batches improve performance */
    private static final int OPTIMAL_BATCH_SIZE = 10000;

    /** DuckDB connection validation timeout in seconds */
    private static final int CONNECTION_VALIDATION_TIMEOUT = 10;

    /**
     * Get the dialect name identifier.
     *
     * @return the database identifier for DuckDB
     */
    @Override
    public String dialectName() {
        return DatabaseIdentifier.DUCKDB;
    }

    /**
     * Get the row converter for DuckDB data type conversions.
     *
     * @return DuckDB-specific JDBC row converter
     */
    @Override
    public JdbcRowConverter getRowConverter() {
        return new DuckDBJdbcRowConverter();
    }

    /**
     * Generate hash modulo expression for field partitioning.
     *
     * <p>Uses DuckDB's HASH function with absolute value to ensure positive hash values.
     *
     * @param fieldName the field name to hash
     * @param mod the modulo divisor for partitioning
     * @return SQL expression for hash-based partitioning
     */
    @Override
    public String hashModForField(String fieldName, int mod) {
        return String.format("MOD(ABS(HASH(%s)), %d)", quoteIdentifier(fieldName), mod);
    }

    /**
     * Get the type mapper for DuckDB data types.
     *
     * @return DuckDB-specific type mapper
     */
    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DuckDBTypeMapper();
    }

    /**
     * Quote identifier to handle special characters and reserved keywords.
     *
     * <p>DuckDB uses double quotes for identifiers and escapes embedded quotes by doubling them.
     *
     * @param identifier the identifier to quote
     * @return quoted identifier safe for SQL statements
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Build fully qualified table identifier.
     *
     * <p>In DuckDB, database parameter represents schema name. If schema is not specified, only
     * table name is returned.
     *
     * @param database the schema name (DuckDB uses schemas instead of databases)
     * @param tableName the table name
     * @return fully qualified table identifier
     */
    @Override
    public String tableIdentifier(String database, String tableName) {
        if (StringUtils.isNotBlank(database)) {
            return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
        }
        return quoteIdentifier(tableName);
    }

    /**
     * Generate UPSERT statement using DuckDB's INSERT ... ON CONFLICT syntax.
     *
     * <p>DuckDB supports PostgreSQL-compatible UPSERT syntax with ON CONFLICT clause. If unique key
     * fields are specified, conflicting rows will be updated; otherwise, a simple INSERT is
     * performed.
     *
     * @param database the schema name
     * @param tableName the table name
     * @param fieldNames all field names to insert/update
     * @param uniqueKeyFields fields that define uniqueness constraint
     * @return optional UPSERT SQL statement
     */
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

    /**
     * Create optimized prepared statement for DuckDB queries.
     *
     * <p>DuckDB's columnar storage benefits from larger fetch sizes. This method applies DuckDB-
     * specific optimizations including minimum fetch size enforcement and query timeout settings.
     *
     * @param connection the database connection
     * @param queryTemplate the SQL query template
     * @param fetchSize the requested fetch size (will be optimized for DuckDB)
     * @return configured prepared statement
     * @throws SQLException if statement creation fails
     */
    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        /*
         * DuckDB specific optimizations:
         * - Use larger fetch sizes for better performance with columnar storage
         * - Minimum fetch size of 1000 rows recommended
         */
        int optimizedFetchSize = fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE;

        if (optimizedFetchSize < 1000) {
            optimizedFetchSize = Math.max(optimizedFetchSize, 1000);
        }

        statement.setFetchSize(optimizedFetchSize);

        // Set query timeout for DuckDB (30 seconds default)
        statement.setQueryTimeout(30);

        return statement;
    }

    /**
     * Parse table path string into TablePath object.
     *
     * @param tablePath the table path string
     * @return parsed TablePath object
     */
    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath);
    }

    /**
     * Build table identifier from TablePath object.
     *
     * <p>DuckDB uses schema names instead of database names. If no schema is specified, defaults to
     * "main" schema.
     *
     * @param tablePath the table path containing schema and table name
     * @return fully qualified table identifier
     */
    @Override
    public String tableIdentifier(TablePath tablePath) {
        /*
         * For DuckDB, use schema name instead of database name
         * Default to "main" schema if not specified
         */
        String schemaName = tablePath.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = "main";
        }
        return tableIdentifier(schemaName, tablePath.getTableName());
    }

    /**
     * Get approximate row count for a table or query.
     *
     * <p>Executes COUNT(*) query to determine the number of rows. Supports both direct table
     * queries and custom SQL queries.
     *
     * @param connection the database connection
     * @param table the source table configuration
     * @return approximate row count, or 0 if no rows found
     * @throws SQLException if query execution fails
     */
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

    /**
     * Query the maximum value in the next chunk for split-based reading.
     *
     * <p>Used for parallel reading by splitting data based on a column's value range. Returns the
     * maximum value within the specified chunk size starting from the lower bound.
     *
     * @param connection the database connection
     * @param table the source table configuration
     * @param columnName the column name to use for splitting
     * @param chunkSize the number of rows in each chunk
     * @param includedLowerBound the lower bound value (inclusive)
     * @return the maximum value in the chunk
     * @throws SQLException if query execution fails or returns no results
     */
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
     * Get optimal batch size for DuckDB operations. DuckDB performs better with larger batches due
     * to its columnar storage architecture.
     */
    public int getOptimalBatchSize() {
        return OPTIMAL_BATCH_SIZE;
    }

    /**
     * Check if DuckDB supports bulk copy operations. DuckDB has excellent bulk loading
     * capabilities.
     */
    public boolean supportsBulkCopy() {
        return true;
    }

    /**
     * Generate DuckDB-specific bulk insert statement. Uses VALUES clause optimization for better
     * performance.
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

    /** Get connection validation timeout specific to DuckDB. */
    public int getConnectionValidationTimeout() {
        return CONNECTION_VALIDATION_TIMEOUT;
    }
}
