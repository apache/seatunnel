/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import com.mysql.cj.MysqlType;
import com.mysql.cj.jdbc.result.ResultSetImpl;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresCatalog extends AbstractJdbcCatalog {

    protected static final Set<String> SYS_DATABASES = new HashSet<>(9);

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("pg_catalog");
        SYS_DATABASES.add("root");
        SYS_DATABASES.add("pg_toast");
        SYS_DATABASES.add("pg_temp_1");
        SYS_DATABASES.add("pg_toast_temp_1");
        SYS_DATABASES.add("postgres");
        SYS_DATABASES.add("template0");
        SYS_DATABASES.add("template1");
    }

    protected final Map<String, Connection> connectionMap;

    public PostgresCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (SQLException e) {
                throw new CatalogException(
                        String.format("Failed to close %s via JDBC.", entry.getKey()), e);
            }
        }
        super.close();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (PreparedStatement ps =
                defaultConnection.prepareStatement("select datname from pg_database;")) {

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (!SYS_DATABASES.contains(databaseName)) {
                    databases.add(rs.getString(1));
                }
            }

            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        try (PreparedStatement ps =
                getConnection(dbUrl)
                        .prepareStatement(
                                "SELECT table_schema, table_name FROM information_schema.tables;")) {

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                String schemaName = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                if (org.apache.commons.lang3.StringUtils.isNotBlank(schemaName)
                        && !SYS_DATABASES.contains(schemaName)) {
                    tables.add(schemaName + "." + tableName);
                }
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());

            try (PreparedStatement ps =
                    conn.prepareStatement(
                            String.format(
                                    "SELECT * FROM %s WHERE 1 = 0;",
                                    tablePath.getFullNameWithQuoted("\"")))) {
                ResultSetMetaData tableMetaData = ps.getMetaData();
                TableSchema.Builder builder = TableSchema.builder();
                // add column
                for (int i = 1; i <= tableMetaData.getColumnCount(); i++) {
                    String columnName = tableMetaData.getColumnName(i);
                    SeaTunnelDataType<?> type = fromJdbcType(tableMetaData, i);
                    int columnDisplaySize = tableMetaData.getColumnDisplaySize(i);
                    String comment = tableMetaData.getColumnLabel(i);
                    boolean isNullable =
                            tableMetaData.isNullable(i) == ResultSetMetaData.columnNullable;
                    Object defaultValue =
                            getColumnDefaultValue(
                                            metaData,
                                            tablePath.getDatabaseName(),
                                            tablePath.getSchemaName(),
                                            tablePath.getTableName(),
                                            columnName)
                                    .orElse(null);

                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    columnName,
                                    type,
                                    columnDisplaySize,
                                    isNullable,
                                    defaultValue,
                                    comment);
                    builder.column(physicalColumn);
                }
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    public static Map<String, Object> getColumnsDefaultValue(TablePath tablePath, Connection conn) {
        Map<String, Object> columnsDefaultValue = new HashMap<>();

        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        String sql =
                "SELECT column_name, column_default "
                        + "FROM information_schema.columns "
                        + "WHERE table_schema = ? AND table_name = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("column_name");
                    Object defaultValue = resultSet.getObject("column_default");
                    columnsDefaultValue.put(columnName, defaultValue);
                }
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "Failed getting table(%s) columns default value",
                            tablePath.getFullName()),
                    e);
        }

        return columnsDefaultValue;
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        throw new UnsupportedOperationException("Unsupported create table");
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());

        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        String sql = "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + tableName + "\"";

        try (PreparedStatement ps = getConnection(dbUrl).prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    protected boolean createDatabaseInternal(String databaseName, Connection conn) {
        String sql = "CREATE DATABASE \"" + databaseName + "\"";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
            return true;
        } catch (SQLException e) {
            // Handle the exception or rethrow it as needed
            e.printStackTrace();
            return false;
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) throws CatalogException {
        String sql = "CREATE DATABASE \"" + databaseName + "\"";
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName())
                            .contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        String sql = "DROP DATABASE IF EXISTS \"" + databaseName + "\"";
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    /**
     * @see MysqlType
     * @see ResultSetImpl#getObjectStoredProc(int, int)
     */
    @SuppressWarnings("unchecked")
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnTypeName = metadata.getColumnTypeName(colIndex);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(
                PostgresDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(PostgresDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return new PostgresDataTypeConvertor().toSeaTunnelType(columnTypeName, dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    private String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + databaseName + suffix;
    }
}
