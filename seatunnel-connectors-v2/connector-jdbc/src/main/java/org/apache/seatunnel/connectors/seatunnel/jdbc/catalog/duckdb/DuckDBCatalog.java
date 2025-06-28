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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/** DuckDB catalog implementation. */
@Slf4j
public class DuckDBCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL =
            "SELECT column_name, data_type, is_nullable, column_default, numeric_precision, numeric_scale, "
                    + "character_maximum_length, description "
                    + "FROM information_schema.columns "
                    + "WHERE table_schema = ? AND table_name = ? "
                    + "ORDER BY ordinal_position";

    private static final String SELECT_PK_SQL =
            "SELECT column_name "
                    + "FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_name = kcu.constraint_name "
                    + "WHERE tc.table_schema = ? AND tc.table_name = ? "
                    + "AND tc.constraint_type = 'PRIMARY KEY' "
                    + "ORDER BY kcu.ordinal_position";

    private static final String SELECT_CONSTRAINTS_SQL =
            "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name "
                    + "FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_name = kcu.constraint_name "
                    + "WHERE tc.table_schema = ? AND tc.table_name = ? "
                    + "AND tc.constraint_type IN ('UNIQUE', 'FOREIGN KEY') "
                    + "ORDER BY kcu.ordinal_position";

    /** Constructor for DuckDB catalog */
    public DuckDBCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(
                catalogName,
                username,
                pwd,
                urlInfo,
                StringUtils.isNotBlank(defaultSchema) ? defaultSchema : "main",
                "org.duckdb.DuckDBDriver");
    }

    /** Constructor with driver class name */
    public DuckDBCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(
                catalogName,
                username,
                pwd,
                urlInfo,
                StringUtils.isNotBlank(defaultSchema) ? defaultSchema : "main",
                driverClass);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (Connection conn = getConnection(defaultUrl)) {
            try (PreparedStatement ps =
                    conn.prepareStatement(
                            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?")) {
                ps.setString(1, databaseName);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to check if database %s exists", databaseName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try (Connection conn = getConnection(defaultUrl)) {
            return tableExists(conn, tablePath);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to check if table %s exists", tablePath.getFullName()),
                    e);
        }
    }

    private boolean tableExists(Connection connection, TablePath tablePath) throws SQLException {
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "SELECT table_name FROM information_schema.tables "
                                + "WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, tablePath.getSchemaName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableNotExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new CatalogException(
                    String.format("Table %s already exists", tablePath.getFullName()));
        }

        String createTableSql = buildCreateTableSql(tablePath, table);
        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps = conn.prepareStatement(createTableSql)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to create table %s", tablePath.getFullName()), e);
        }
    }

    private String buildCreateTableSql(TablePath tablePath, CatalogTable table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tablePath.getSchemaAndTableName("\"")).append(" (\n");

        // Add columns
        List<String> columnSqls = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            StringBuilder columnSql = new StringBuilder();
            columnSql.append("\"").append(column.getName()).append("\" ");
            columnSql.append(column.getSourceType());
            if (!column.isNullable()) {
                columnSql.append(" NOT NULL");
            }
            if (column.getDefaultValue() != null) {
                columnSql.append(" DEFAULT ").append(column.getDefaultValue());
            }
            if (column.getComment() != null) {
                columnSql.append(" COMMENT '").append(column.getComment()).append("'");
            }
            columnSqls.add(columnSql.toString());
        }

        // Add primary key
        PrimaryKey primaryKey = table.getTableSchema().getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            String pkColumns =
                    String.join(
                            ", ",
                            primaryKey.getColumnNames().stream()
                                    .map(name -> "\"" + name + "\"")
                                    .toArray(String[]::new));
            columnSqls.add("PRIMARY KEY (" + pkColumns + ")");
        }

        sb.append(String.join(",\n", columnSqls));
        sb.append("\n)");

        return sb.toString();
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        // Do not use try-with-resources as the connection is managed by connection pool
        Connection conn = getConnection(defaultUrl);
        try {
            // Get table columns
            List<Column> columns = getColumns(conn, tablePath);

            // Get primary key
            Optional<PrimaryKey> primaryKey = getPrimaryKey(conn, tablePath);

            // Get other constraints
            List<ConstraintKey> constraintKeys = getConstraintKeys(conn, tablePath);

            // Build table schema
            TableSchema.Builder schemaBuilder = TableSchema.builder().columns(columns);
            primaryKey.ifPresent(schemaBuilder::primaryKey);
            constraintKeys.forEach(schemaBuilder::constraintKey);

            // Build catalog table
            return CatalogTable.of(
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                    schemaBuilder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    getTableComment(conn.getMetaData(), tablePath).orElse(null),
                    catalogName);

        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to get table %s", tablePath.getFullName()), e);
        }
    }

    private List<Column> getColumns(Connection conn, TablePath tablePath) throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_COLUMNS_SQL)) {
            ps.setString(1, tablePath.getSchemaName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                DuckDBTypeMapper typeMapper = new DuckDBTypeMapper();
                while (rs.next()) {
                    columns.add(typeMapper.mappingColumn(rs, rs.getRow()));
                }
            }
        }
        return columns;
    }

    private Optional<PrimaryKey> getPrimaryKey(Connection conn, TablePath tablePath)
            throws SQLException {
        List<String> pkColumns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_PK_SQL)) {
            ps.setString(1, tablePath.getSchemaName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pkColumns.add(rs.getString("column_name"));
                }
            }
        }

        if (pkColumns.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(PrimaryKey.of("pk_" + tablePath.getTableName(), pkColumns));
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        // Use synchronized block to ensure thread safety and create new connection to avoid
        // connection closure issues
        synchronized (this) {
            Connection conn = null;
            try {
                // Always create a new connection to avoid unexpected closure of pooled connections
                conn = DriverManager.getConnection(defaultUrl, username, pwd);

                // Validate connection is valid
                if (conn.isClosed() || !conn.isValid(5)) {
                    throw new SQLException("Failed to establish valid connection");
                }

                log.info("Successfully created new connection for SQL query: {}", sqlQuery);
                return CatalogUtils.getCatalogTable(conn, sqlQuery);

            } catch (SQLException e) {
                log.error("Failed to execute SQL query: {}", sqlQuery, e);
                throw new SQLException("Failed to get table from SQL query: " + sqlQuery, e);
            } finally {
                // Ensure connection is properly closed
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        log.warn("Failed to close connection", e);
                    }
                }
            }
        }
    }

    private List<ConstraintKey> getConstraintKeys(Connection conn, TablePath tablePath)
            throws SQLException {
        HashMap<String, ConstraintKey> constraintKeys = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_CONSTRAINTS_SQL)) {
            ps.setString(1, tablePath.getSchemaName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("constraint_name");
                    String constraintType = rs.getString("constraint_type");
                    String columnName = rs.getString("column_name");

                    ConstraintKey.ConstraintType type;
                    if ("UNIQUE".equals(constraintType)) {
                        type = ConstraintKey.ConstraintType.UNIQUE_KEY;
                    } else if ("FOREIGN KEY".equals(constraintType)) {
                        type = ConstraintKey.ConstraintType.FOREIGN_KEY;
                    } else {
                        continue;
                    }

                    constraintKeys
                            .computeIfAbsent(
                                    constraintName,
                                    k -> ConstraintKey.of(type, constraintName, new ArrayList<>()))
                            .getColumnNames()
                            .add(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            columnName, ConstraintKey.ColumnSortType.ASC));
                }
            }
        }
        return new ArrayList<>(constraintKeys.values());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema'";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return String.format(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'",
                databaseName);
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    protected Optional<String> getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        // DuckDB doesn't support PostgreSQL-style table comments
        // For now, return empty comment to avoid NullPointerException
        try {
            // Try to get table comment using standard JDBC metadata
            try (ResultSet rs =
                    metaData.getTables(
                            null,
                            tablePath.getSchemaName(),
                            tablePath.getTableName(),
                            new String[] {"TABLE"})) {
                if (rs.next()) {
                    String comment = rs.getString("REMARKS");
                    return Optional.ofNullable(comment);
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to get table comment for table: {}", tablePath.getFullName(), e);
        }
        return Optional.empty();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }

        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format(
                                        "DROP TABLE %s", tablePath.getSchemaAndTableName("\"")))) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to drop table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws CatalogException {
        String databaseName = tablePath.getDatabaseName();
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new CatalogException(String.format("Database %s already exists", databaseName));
        }

        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("CREATE SCHEMA \"%s\"", databaseName))) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to create database %s", databaseName), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String databaseName = tablePath.getDatabaseName();
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("DROP SCHEMA \"%s\" CASCADE", databaseName))) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to drop database %s", databaseName), e);
        }
    }
}
