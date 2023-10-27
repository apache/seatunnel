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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractJdbcCatalog implements Catalog {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcCatalog.class);

    protected static final Set<String> SYS_DATABASES = new HashSet<>();

    protected final String catalogName;
    protected final String defaultDatabase;
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected final String suffix;
    protected final String defaultUrl;

    protected final Optional<String> defaultSchema;

    protected final Map<String, Connection> connectionMap;

    public AbstractJdbcCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {

        checkArgument(StringUtils.isNotBlank(username));
        checkArgument(StringUtils.isNotBlank(urlInfo.getUrlWithoutDatabase()));
        this.catalogName = catalogName;
        this.defaultDatabase = urlInfo.getDefaultDatabase().orElse(null);
        this.username = username;
        this.pwd = pwd;
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        this.defaultUrl = urlInfo.getOrigin();
        this.suffix = urlInfo.getSuffix();
        this.defaultSchema = Optional.ofNullable(defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    protected Connection getConnection(String url) {
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
    public void open() throws CatalogException {
        getConnection(defaultUrl);
        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
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
        connectionMap.clear();
        LOG.info("Catalog {} closing", catalogName);
    }

    protected String getSelectColumnsSql(TablePath tablePath) {
        throw new UnsupportedOperationException();
    }

    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl;
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        } else {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        }
        Connection conn = getConnection(dbUrl);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<PrimaryKey> primaryKey = getPrimaryKey(metaData, tablePath);
            List<ConstraintKey> constraintKeys = getConstraintKeys(metaData, tablePath);
            try (PreparedStatement ps = conn.prepareStatement(getSelectColumnsSql(tablePath));
                    ResultSet resultSet = ps.executeQuery()) {

                TableSchema.Builder builder = TableSchema.builder();
                while (resultSet.next()) {
                    builder.column(buildColumn(resultSet));
                }
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "",
                        catalogName);
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    protected Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getPrimaryKey(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getPrimaryKey(metaData, TablePath.of(database, schema, table));
    }

    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getConstraintKeys(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getConstraintKeys(metaData, TablePath.of(database, schema, table));
    }

    protected String getListDatabaseSql() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return queryString(
                    defaultUrl,
                    getListDatabaseSql(),
                    rs -> {
                        String s = rs.getString(1);
                        return SYS_DATABASES.contains(s) ? null : s;
                    });
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));

        return listDatabases().contains(databaseName);
    }

    protected String getListTableSql(String databaseName) {
        throw new UnsupportedOperationException();
    }

    protected String getTableName(ResultSet rs) throws SQLException {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);
        if (StringUtils.isNotBlank(schemaName) && !SYS_DATABASES.contains(schemaName)) {
            return schemaName + "." + tableName;
        }
        return null;
    }

    protected String getTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        try {
            return queryString(dbUrl, getListTableSql(databaseName), this::getTableName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(getTableName(tablePath));
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (defaultSchema.isPresent()) {
            tablePath =
                    new TablePath(
                            tablePath.getDatabaseName(),
                            defaultSchema.get(),
                            tablePath.getTableName());
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        createTableInternal(tablePath, table);
    }

    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        throw new UnsupportedOperationException();
    }

    protected void createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try {
            executeInternal(dbUrl, getCreateTableSql(tablePath, table));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");

        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }

        dropTableInternal(tablePath);
    }

    protected String getDropTableSql(TablePath tablePath) {
        throw new UnsupportedOperationException();
    }

    protected void dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try {
            // Will there exist concurrent drop for one table?
            executeInternal(dbUrl, getDropTableSql(tablePath));
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");

        if (databaseExists(tablePath.getDatabaseName())) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }

        createDatabaseInternal(tablePath.getDatabaseName());
    }

    protected String getCreateDatabaseSql(String databaseName) {
        throw new UnsupportedOperationException();
    }

    protected void createDatabaseInternal(String databaseName) {
        try {
            executeInternal(defaultUrl, getCreateDatabaseSql(databaseName));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    protected void closeDatabaseConnection(String databaseName) {
        String dbUrl = getUrlFromDatabaseName(databaseName);
        try {
            Connection connection = connectionMap.remove(dbUrl);
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed to close %s via JDBC.", dbUrl), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");

        if (!databaseExists(tablePath.getDatabaseName())) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }

        dropDatabaseInternal(tablePath.getDatabaseName());
    }

    protected String getDropDatabaseSql(String databaseName) {
        throw new UnsupportedOperationException();
    }

    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        try {
            executeInternal(defaultUrl, getDropDatabaseSql(databaseName));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    protected String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + suffix;
    }

    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getFullName();
    }

    @SuppressWarnings("MagicNumber")
    protected Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", getUrlFromDatabaseName(tablePath.getDatabaseName()));
        options.put("table-name", getOptionTableName(tablePath));
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    @FunctionalInterface
    public interface ResultSetConsumer<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    protected List<String> queryString(String url, String sql, ResultSetConsumer<String> consumer)
            throws SQLException {
        try (PreparedStatement ps = getConnection(url).prepareStatement(sql)) {
            List<String> result = new ArrayList<>();
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String value = consumer.apply(rs);
                if (value != null) {
                    result.add(value);
                }
            }
            return result;
        }
    }

    // If sql is DDL, the execute() method always returns false, so the return value
    // should not be used to determine whether changes were made in database.
    protected boolean executeInternal(String url, String sql) throws SQLException {
        LOG.info("create table sql is: {}", sql);
        try (PreparedStatement ps = getConnection(url).prepareStatement(sql)) {
            return ps.execute();
        }
    }

    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery);
    }
}
