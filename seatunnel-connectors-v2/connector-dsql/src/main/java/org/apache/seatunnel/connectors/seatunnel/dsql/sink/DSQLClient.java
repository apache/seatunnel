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

package org.apache.seatunnel.connectors.seatunnel.dsql.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.dsql.config.DSQLSinkConfig;

import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;
import software.amazon.awssdk.services.dsql.model.GenerateAuthTokenRequest;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** DSQL client for handling database operations */
public class DSQLClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DSQLClient.class);

    private final DSQLSinkConfig config;
    // private final CustomPGDataSource pgDataSource;
    private final DsqlUtilities dsqlUtilities;
    private final CatalogTable catalogTable;
    private final Map<String, Integer> columnIndexMap;
    private final List<SqlType> columnTypes;
    private final String targetTableName;

    public DSQLClient(DSQLSinkConfig config, String targetTableName, CatalogTable catalogTable)
            throws SQLException {
        this.config = config;
        this.catalogTable = catalogTable;
        this.columnIndexMap = buildColumnIndexMap();
        this.columnTypes = buildColumnTypes();
        this.targetTableName = targetTableName;
        boolean hasDirectCredentials =
                config.getAccessKeyId() != null
                        && !config.getAccessKeyId().isEmpty()
                        && config.getSecretAccessKey() != null
                        && !config.getSecretAccessKey().isEmpty();

        if (hasDirectCredentials) {
            AwsCredentialsProvider provider =
                    new AwsCredentialsProvider() {
                        @Override
                        public AwsCredentials resolveCredentials() {
                            return AwsBasicCredentials.create(
                                    config.getAccessKeyId(), config.getSecretAccessKey());
                        }
                    };
            this.dsqlUtilities =
                    DsqlUtilities.builder()
                            .region(Region.of(config.getAwsRegion()))
                            .credentialsProvider(provider)
                            .build();
        } else if (config.getProfileName() != null && !config.getProfileName().isEmpty()) {
            this.dsqlUtilities =
                    DsqlUtilities.builder()
                            .region(Region.of(config.getAwsRegion()))
                            .credentialsProvider(
                                    ProfileCredentialsProvider.create(config.getProfileName()))
                            .build();
        } else {
            this.dsqlUtilities =
                    DsqlUtilities.builder()
                            .region(Region.of(config.getAwsRegion()))
                            .credentialsProvider(DefaultCredentialsProvider.create())
                            .build();
        }
        // Initialize AWS DSQL utilities

        // this.pgDataSource = createDataSource();
    }

    private CustomPGDataSource createDataSource() throws SQLException {
        String hostname = extractHostname(config.getClusterEndpoint());

        CustomPGDataSource dataSource = new CustomPGDataSource();
        dataSource.setServerNames(new String[] {hostname});
        dataSource.setPortNumbers(new int[] {5432});
        dataSource.setDatabaseName(config.getDatabaseName());
        dataSource.setUser(config.getUserName());

        // PostgreSQL SSL configuration for Aurora DSQL
        dataSource.setSslMode("verify-full");
        dataSource.setSslfactory("org.postgresql.ssl.DefaultJavaSSLFactory");

        LOG.info("Created DSQL data source for endpoint: {}", hostname);
        return dataSource;
    }

    /** Extract hostname from DSQL cluster endpoint */
    private String extractHostname(String clusterEndpoint) {
        if (clusterEndpoint == null) {
            throw new IllegalArgumentException("Cluster endpoint cannot be null");
        }

        // Handle DSQL ARN format: arn:aws:dsql:region:account:cluster/cluster-id
        if (clusterEndpoint.startsWith("arn:aws:dsql:")) {
            String[] parts = clusterEndpoint.split(":");
            if (parts.length >= 6) {
                String clusterId = parts[5].replace("cluster/", "");
                return clusterId + ".dsql." + parts[3] + ".on.aws";
            }
        }

        // If it's already a hostname, return as is
        return clusterEndpoint;
    }

    /** Generate a fresh authentication token for Aurora DSQL */
    private String generateAuthToken(String hostname) {
        GenerateAuthTokenRequest tokenGenerator =
                GenerateAuthTokenRequest.builder()
                        .hostname(hostname)
                        .region(Region.of(config.getAwsRegion()))
                        .build();

        if (config.getUserName().equals("admin")) {
            return dsqlUtilities.generateDbConnectAdminAuthToken(tokenGenerator);
        } else {
            return dsqlUtilities.generateDbConnectAuthToken(tokenGenerator);
        }
    }

    /** Custom PostgreSQL DataSource that provides dynamic token generation */
    private class CustomPGDataSource extends PGSimpleDataSource {
        @Override
        public Connection getConnection() throws SQLException {
            String hostname = getServerNames()[0];
            String token = generateAuthToken(hostname);
            return super.getConnection(getUser(), token);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            if (password != null && !password.isEmpty()) {
                return super.getConnection(username, password);
            }
            String hostname = getServerNames()[0];
            String token = generateAuthToken(hostname);
            return super.getConnection(username, token);
        }
    }

    /** Get a connection with fresh authentication token */
    private Connection getConnection() throws SQLException {
        // return pgDataSource.getConnection();
        return DSQLConnectionPool.getInstance(this.config, this.dsqlUtilities).getConnection();
    }

    private Map<String, Integer> buildColumnIndexMap() {
        Map<String, Integer> indexMap = new HashMap<>();
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            indexMap.put(columns.get(i).getName(), i);
        }
        return indexMap;
    }

    private List<SqlType> buildColumnTypes() {
        return catalogTable.getTableSchema().getColumns().stream()
                .map(column -> column.getDataType().getSqlType())
                .collect(Collectors.toList());
    }

    public void createTableIfNotExists() throws Exception {
        if (!config.isCreateTableIfNotExists()) {
            LOG.debug("Table creation is disabled, skipping");
            return;
        }

        String createTableSql = generateCreateTableSql();
        LOG.info("Creating table if not exists: {}", createTableSql);
        executeStatement(createTableSql);
    }

    private String generateCreateTableSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(this.targetTableName).append(" (");

        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            Column column = columns.get(i);
            sql.append(column.getName()).append(" ");
            sql.append(mapSeaTunnelTypeToSql(column.getDataType().getSqlType()));
        }

        // Add primary key constraint if specified
        if (config.getPrimaryKeys() != null && !config.getPrimaryKeys().isEmpty()) {
            sql.append(", PRIMARY KEY (")
                    .append(String.join(", ", config.getPrimaryKeys()))
                    .append(")");
        }

        sql.append(")");

        return sql.toString();
    }

    private String mapSeaTunnelTypeToSql(SqlType sqlType) {
        switch (sqlType) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case STRING:
                return "VARCHAR(255)";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BYTES:
                return "BLOB";
            default:
                return "VARCHAR(255)";
        }
    }

    public void batchInsert(List<SeaTunnelRow> rows) throws Exception {
        if (rows.isEmpty()) {
            return;
        }

        // Group rows by operation type
        Map<RowKind, List<SeaTunnelRow>> rowsByKind =
                rows.stream().collect(Collectors.groupingBy(SeaTunnelRow::getRowKind));

        // Process each operation type
        for (Map.Entry<RowKind, List<SeaTunnelRow>> entry : rowsByKind.entrySet()) {
            RowKind rowKind = entry.getKey();
            List<SeaTunnelRow> rowsOfKind = entry.getValue();

            switch (rowKind) {
                case INSERT:
                    if (config.getPrimaryKeys() != null && !config.getPrimaryKeys().isEmpty()) {
                        batchUpsertRows(rowsOfKind);
                    } else {
                        batchInsertRows(rowsOfKind);
                    }
                    break;
                case UPDATE_AFTER:
                    batchUpsertRows(rowsOfKind);
                    break;
                    //                case UPDATE_BEFORE:
                    //                    batchUpsertRows(rowsOfKind);
                    //                    break;
                case DELETE:
                    batchDeleteRows(rowsOfKind);
                    break;
                default:
                    LOG.warn("Unsupported row kind: {}", rowKind);
            }
        }
    }

    private void batchUpsertRows(List<SeaTunnelRow> rows) throws Exception {
        if (config.getPrimaryKeys() == null || config.getPrimaryKeys().isEmpty()) {
            LOG.warn("Cannot perform upsert without primary keys defined, falling back to insert");
            batchInsertRows(rows);
            return;
        }

        String upsertSql = generateUpsertSql();
        executeBatchOperation(upsertSql, rows, "UPSERT");
    }

    private void batchInsertRows(List<SeaTunnelRow> rows) throws Exception {
        String insertSql = generateInsertSql();
        executeBatchOperation(insertSql, rows, "INSERT");
    }

    private void batchDeleteRows(List<SeaTunnelRow> rows) throws Exception {
        if (config.getPrimaryKeys() == null || config.getPrimaryKeys().isEmpty()) {
            LOG.warn("Cannot delete rows without primary keys defined");
            return;
        }

        String deleteSql = generateDeleteSql();
        executeBatchOperation(deleteSql, rows, "DELETE");
    }

    private void executeBatchOperation(String sql, List<SeaTunnelRow> rows, String operation)
            throws Exception {
        LOG.debug("Batch {} {} rows with SQL: {}", operation, rows.size(), sql);

        long startTime = System.currentTimeMillis();
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount <= config.getMaxRetries()) {
            try (Connection conn = getConnection();
                    PreparedStatement stmt = conn.prepareStatement(sql)) {

                for (SeaTunnelRow row : rows) {
                    if ("DELETE".equals(operation)) {
                        setDeleteStatementParameters(stmt, row);
                    } else {
                        // Both INSERT and UPSERT use the same parameter setting
                        setInsertStatementParameters(stmt, row);
                    }
                    stmt.addBatch();
                }

                stmt.executeBatch();
                long duration = System.currentTimeMillis() - startTime;
                LOG.debug("Batch {} completed successfully in {}ms", operation, duration);
                return;

            } catch (Exception e) {
                lastException = e;
                retryCount++;
                if (retryCount <= config.getMaxRetries()) {
                    LOG.warn(
                            "Batch {} failed (attempt {}/{}), retrying in {}ms",
                            operation,
                            retryCount,
                            config.getMaxRetries() + 1,
                            config.getRetryDelayMs(),
                            e);
                    try {
                        TimeUnit.MILLISECONDS.sleep(config.getRetryDelayMs());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new Exception("Interrupted during retry delay", ie);
                    }
                }
            }
        }

        throw new Exception(
                "Batch "
                        + operation
                        + " failed after "
                        + (config.getMaxRetries() + 1)
                        + " attempts",
                lastException);
    }

    private String generateDeleteSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(this.targetTableName).append(" WHERE ");

        List<String> primaryKeys = config.getPrimaryKeys();
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(primaryKeys.get(i)).append(" = ?");
        }

        return sql.toString();
    }

    private void setDeleteStatementParameters(PreparedStatement stmt, SeaTunnelRow row)
            throws SQLException {
        List<String> primaryKeys = config.getPrimaryKeys();
        for (int i = 0; i < primaryKeys.size(); i++) {
            String pkColumn = primaryKeys.get(i);
            Integer columnIndex = columnIndexMap.get(pkColumn);
            if (columnIndex != null) {
                Object value = row.getField(columnIndex);
                if (value == null) {
                    stmt.setNull(i + 1, java.sql.Types.NULL);
                } else {
                    stmt.setObject(i + 1, value);
                }
            }
        }
    }

    private void setInsertStatementParameters(PreparedStatement stmt, SeaTunnelRow row)
            throws SQLException {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Object value = row.getField(i);
            if (value == null) {
                stmt.setNull(i + 1, java.sql.Types.NULL);
            } else {
                stmt.setObject(i + 1, value);
            }
        }
    }

    private String generateUpsertSql() {
        StringBuilder sql = new StringBuilder();
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        List<String> primaryKeys = config.getPrimaryKeys();

        // INSERT part
        sql.append("INSERT INTO ").append(this.targetTableName).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(columns.get(i).getName());
        }
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
        sql.append(")");

        // ON CONFLICT part
        sql.append(" ON CONFLICT (");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(primaryKeys.get(i));
        }
        sql.append(") DO UPDATE SET ");

        // UPDATE SET part (exclude primary keys)
        boolean first = true;
        for (Column column : columns) {
            if (!primaryKeys.contains(column.getName())) {
                if (!first) {
                    sql.append(", ");
                }
                sql.append(column.getName()).append(" = EXCLUDED.").append(column.getName());
                first = false;
            }
        }

        return sql.toString();
    }

    private String generateInsertSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(this.targetTableName).append(" (");

        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(columns.get(i).getName());
        }

        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
        sql.append(")");

        return sql.toString();
    }

    private void executeStatement(String sql) throws Exception {
        long startTime = System.currentTimeMillis();
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(sql);
            long duration = System.currentTimeMillis() - startTime;

            if (duration > 1000) {
                LOG.warn("Slow SQL execution ({}ms): {}", duration, truncateSql(sql));
            } else {
                LOG.debug("Statement executed successfully in {}ms", duration);
            }

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("Failed to execute statement after {}ms: {}", duration, truncateSql(sql), e);
            throw e;
        }
    }

    private String truncateSql(String sql) {
        final int maxLength = 1000;
        if (sql.length() <= maxLength) {
            return sql;
        }
        return sql.substring(0, maxLength) + "... [truncated]";
    }

    @Override
    public void close() throws IOException {
        // Connection is managed per-operation, no persistent connection to close
        try {
            getConnection().close();
        } catch (SQLException e) {
            LOG.debug("DSQL client close failed");
            throw new RuntimeException("DSQL client close failed");
        }
        LOG.debug("DSQL client closed");
    }
}
