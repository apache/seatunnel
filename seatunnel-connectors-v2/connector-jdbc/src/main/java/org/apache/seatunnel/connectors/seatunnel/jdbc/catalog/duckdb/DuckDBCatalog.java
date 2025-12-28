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

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/** DuckDB catalog implementation. */
@Slf4j
public class DuckDBCatalog extends AbstractJdbcCatalog {

    /** SQL query to retrieve column metadata from information_schema */
    private static final String SELECT_COLUMNS_SQL =
            "SELECT column_name, data_type, is_nullable, column_default, numeric_precision, numeric_scale, "
                    + "character_maximum_length "
                    + "FROM information_schema.columns "
                    + "WHERE table_schema = ? AND table_name = ? "
                    + "ORDER BY ordinal_position";

    /** SQL query to retrieve primary key columns from information_schema */
    private static final String SELECT_PK_SQL =
            "SELECT column_name "
                    + "FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_name = kcu.constraint_name "
                    + "WHERE tc.table_schema = ? AND tc.table_name = ? "
                    + "AND tc.constraint_type = 'PRIMARY KEY' "
                    + "ORDER BY kcu.ordinal_position";

    /** SQL query to retrieve constraint keys (UNIQUE and FOREIGN KEY) from information_schema */
    private static final String SELECT_CONSTRAINTS_SQL =
            "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name "
                    + "FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_name = kcu.constraint_name "
                    + "WHERE tc.table_schema = ? AND tc.table_name = ? "
                    + "AND tc.constraint_type IN ('UNIQUE', 'FOREIGN KEY') "
                    + "ORDER BY kcu.ordinal_position";

    /**
     * Constructor for DuckDB catalog with default driver.
     *
     * @param catalogName the name of the catalog
     * @param urlInfo the JDBC URL information
     */
    public DuckDBCatalog(String catalogName, JdbcUrlUtil.UrlInfo urlInfo, String defaultSchema) {
        super(catalogName, "", "", urlInfo, defaultSchema, "org.duckdb.DuckDBDriver");
        final Class<String> stringClass = String.class;
    }

    /**
     * Check if a database (schema) exists in DuckDB.
     *
     * @param databaseName the name of the database to check
     * @return true if the database exists, false otherwise
     * @throws CatalogException if failed to check database existence
     */
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

    /**
     * Check if a table exists in DuckDB.
     *
     * @param tablePath the path of the table to check
     * @return true if the table exists, false otherwise
     * @throws CatalogException if failed to check table existence
     */
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

    /**
     * Internal method to check if a table exists using an existing connection.
     *
     * @param connection the database connection
     * @param tablePath the path of the table to check
     * @return true if the table exists, false otherwise
     * @throws SQLException if a database access error occurs
     */
    private boolean tableExists(Connection connection, TablePath tablePath) throws SQLException {
        // Use the schema name from tablePath, fallback to 'main' if not specified
        String schemaName = tablePath.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = "main";
        }

        try (PreparedStatement ps =
                connection.prepareStatement(
                        "SELECT table_name FROM information_schema.tables "
                                + "WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schemaName);
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Create a new table in DuckDB.
     *
     * @param tablePath the path of the table to create
     * @param table the catalog table definition
     * @param ignoreIfExists if true, ignore the operation if table already exists
     * @throws TableNotExistException if the table does not exist
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException if failed to create the table
     */
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

    /**
     * Build CREATE TABLE SQL statement for DuckDB.
     *
     * @param tablePath the path of the table
     * @param table the catalog table definition
     * @return the CREATE TABLE SQL statement
     */
    private String buildCreateTableSql(TablePath tablePath, CatalogTable table) {
        StringBuilder sb = new StringBuilder();
        // Build full table name with schema if specified
        String schemaName = tablePath.getSchemaName();
        String tableName;
        if (schemaName != null && !schemaName.trim().isEmpty() && !"main".equals(schemaName)) {
            tableName = "\"" + schemaName + "\".\"" + tablePath.getTableName() + "\"";
        } else {
            tableName = "\"" + tablePath.getTableName() + "\"";
        }
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

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
            /*
             * DuckDB does not support COMMENT syntax in CREATE TABLE
             * if (column.getComment() != null) {
             *     columnSql.append(" COMMENT '").append(column.getComment()).append("'");
             * }
             */
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

    /**
     * Get the CREATE TABLE SQL statement.
     *
     * @param tablePath the path of the table
     * @param table the catalog table definition
     * @param createIndex whether to create indexes (not used in DuckDB implementation)
     * @return the CREATE TABLE SQL statement
     */
    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return buildCreateTableSql(tablePath, table);
    }

    /**
     * Get table metadata from DuckDB.
     *
     * @param tablePath the path of the table
     * @return the catalog table with complete metadata
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException if failed to get table metadata
     */
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

    /**
     * Retrieve column metadata for a table.
     *
     * @param conn the database connection
     * @param tablePath the path of the table
     * @return list of columns with metadata
     * @throws SQLException if a database access error occurs
     */
    private List<Column> getColumns(Connection conn, TablePath tablePath) throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_COLUMNS_SQL)) {
            String schemaName = tablePath.getSchemaName();
            if (schemaName == null || schemaName.trim().isEmpty()) {
                schemaName = "main";
            }
            ps.setString(1, schemaName);
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                DuckDBTypeMapper typeMapper = new DuckDBTypeMapper();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Only need to read the first row to get column metadata
                if (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        columns.add(typeMapper.mappingColumn(metaData, i));
                    }
                }
            }
        }
        return columns;
    }

    /**
     * Retrieve primary key information for a table.
     *
     * @param conn the database connection
     * @param tablePath the path of the table
     * @return optional primary key if exists
     * @throws SQLException if a database access error occurs
     */
    private Optional<PrimaryKey> getPrimaryKey(Connection conn, TablePath tablePath)
            throws SQLException {
        List<String> pkColumns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_PK_SQL)) {
            String schemaName = tablePath.getSchemaName();
            if (schemaName == null || schemaName.trim().isEmpty()) {
                schemaName = "main";
            }
            ps.setString(1, schemaName);
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

    /**
     * Get table metadata from a SQL query result. This method creates a new connection to avoid
     * connection closure issues with pooled connections.
     *
     * @param sqlQuery the SQL query to execute
     * @return the catalog table derived from query result
     * @throws SQLException if failed to execute the query or get table metadata
     */
    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        /*
         * Use synchronized block to ensure thread safety and create new connection to avoid
         * connection closure issues
         */
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

    /**
     * Retrieve constraint keys (UNIQUE and FOREIGN KEY) for a table.
     *
     * @param conn the database connection
     * @param tablePath the path of the table
     * @return list of constraint keys
     * @throws SQLException if a database access error occurs
     */
    private List<ConstraintKey> getConstraintKeys(Connection conn, TablePath tablePath)
            throws SQLException {
        HashMap<String, ConstraintKey> constraintKeys = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_CONSTRAINTS_SQL)) {
            String schemaName = tablePath.getSchemaName();
            if (schemaName == null || schemaName.trim().isEmpty()) {
                schemaName = "main";
            }
            ps.setString(1, schemaName);
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

    /**
     * Get SQL query to list all databases (schemas) in DuckDB.
     *
     * @return SQL query string
     */
    @Override
    protected String getListDatabaseSql() {
        return "SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema'";
    }

    /**
     * Get SQL query to list all tables in a specific database (schema).
     *
     * @param databaseName the name of the database
     * @return SQL query string
     */
    @Override
    protected String getListTableSql(String databaseName) {
        return String.format(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'",
                databaseName);
    }

    /**
     * Extract table name from result set.
     *
     * @param rs the result set
     * @return the table name
     * @throws SQLException if a database access error occurs
     */
    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    /**
     * Get JDBC URL for a specific database name. In DuckDB, all schemas share the same connection
     * URL.
     *
     * @param databaseName the name of the database
     * @return the JDBC URL
     */
    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    /**
     * Get the table name format for connector options.
     *
     * @param tablePath the path of the table
     * @return the formatted table name with schema
     */
    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    /**
     * Get table comment from database metadata. DuckDB has limited support for table comments.
     *
     * @param metaData the database metadata
     * @param tablePath the path of the table
     * @return optional table comment if available
     * @throws SQLException if a database access error occurs
     */
    @Override
    protected Optional<String> getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        /*
         * DuckDB doesn't support PostgreSQL-style table comments
         * For now, return empty comment to avoid NullPointerException
         */
        try {
            // Try to get table comment using standard JDBC metadata
            String schemaName = tablePath.getSchemaName();
            if (schemaName == null || schemaName.trim().isEmpty()) {
                schemaName = "main";
            }
            try (ResultSet rs =
                    metaData.getTables(
                            null, schemaName, tablePath.getTableName(), new String[] {"TABLE"})) {
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

    /**
     * Drop a table from DuckDB.
     *
     * @param tablePath the path of the table to drop
     * @param ignoreIfNotExists if true, ignore the operation if table does not exist
     * @throws TableNotExistException if the table does not exist and ignoreIfNotExists is false
     * @throws CatalogException if failed to drop the table
     */
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

    /**
     * Create a new database (schema) in DuckDB.
     *
     * @param tablePath the table path containing the database name
     * @param ignoreIfExists if true, ignore the operation if database already exists
     * @throws CatalogException if failed to create the database
     */
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

    /**
     * Drop a database (schema) from DuckDB. This operation will cascade and drop all tables in the
     * schema.
     *
     * @param tablePath the table path containing the database name
     * @param ignoreIfNotExists if true, ignore the operation if database does not exist
     * @throws DatabaseNotExistException if the database does not exist and ignoreIfNotExists is
     *     false
     * @throws CatalogException if failed to drop the database
     */
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
