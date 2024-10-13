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
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SQLPreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_METHOD;

@Slf4j
public abstract class AbstractJdbcCatalog implements Catalog {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcCatalog.class);

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
    public String name() {
        return catalogName;
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
            TableSchema.Builder tableSchemaBuilder =
                    buildColumnsReturnTablaSchemaBuilder(tablePath, conn);
            // add primary key
            primaryKey.ifPresent(tableSchemaBuilder::primaryKey);
            // add constraint key
            constraintKeys.forEach(tableSchemaBuilder::constraintKey);
            TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
            return CatalogTable.of(
                    tableIdentifier,
                    tableSchemaBuilder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    "",
                    catalogName);

        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    protected TableSchema.Builder buildColumnsReturnTablaSchemaBuilder(
            TablePath tablePath, Connection conn) throws SQLException {
        TableSchema.Builder columnsBuilder = TableSchema.builder();
        try (PreparedStatement ps = conn.prepareStatement(getSelectColumnsSql(tablePath));
                ResultSet resultSet = ps.executeQuery()) {
            buildColumnsWithErrorCheck(tablePath, resultSet, columnsBuilder);
        }
        return columnsBuilder;
    }

    protected void buildColumnsWithErrorCheck(
            TablePath tablePath, ResultSet resultSet, TableSchema.Builder builder)
            throws SQLException {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (resultSet.next()) {
            try {
                builder.column(buildColumn(resultSet));
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    catalogName, tablePath.getFullName(), unsupported);
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

    protected String getListViewSql(String databaseName) {
        throw new UnsupportedOperationException();
    }

    protected String getListSynonymSql(String databaseName) {
        throw new UnsupportedOperationException();
    }

    protected String getDatabaseWithConditionSql(String databaseName) {
        throw CommonError.unsupportedMethod(this.catalogName, "getDatabaseWithConditionSql");
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return queryString(defaultUrl, getListDatabaseSql(), rs -> rs.getString(1));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (StringUtils.isBlank(databaseName)) {
            return false;
        }
        try {
            return querySQLResultExists(defaultUrl, getDatabaseWithConditionSql(databaseName));
        } catch (SeaTunnelRuntimeException e) {
            if (e.getSeaTunnelErrorCode().getCode().equals(UNSUPPORTED_METHOD.getCode())) {
                log.warn(
                        "The catalog: {} is not supported the getDatabaseWithConditionSql for databaseExists",
                        this.catalogName);
                return listDatabases().contains(databaseName);
            }
            throw e;
        } catch (SQLException e) {
            throw new SeaTunnelException("Failed to querySQLResult", e);
        }
    }

    protected String getListTableSql(String databaseName) {
        throw new UnsupportedOperationException();
    }

    protected String getTableWithConditionSql(TablePath tablePath) {
        throw CommonError.unsupportedMethod(this.catalogName, "getTableWithConditionSql");
    }

    protected String getTableName(ResultSet rs) throws SQLException {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);
        if (StringUtils.isNotBlank(schemaName)) {
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

    public List<String> listViews(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }
        String dbUrl = getUrlFromDatabaseName(databaseName);
        try {
            return queryString(dbUrl, getListViewSql(databaseName), this::getTableName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    public List<String> listSynonym(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }
        String dbUrl = getUrlFromDatabaseName(databaseName);
        try {
            return queryString(dbUrl, getListSynonymSql(databaseName), this::getTableName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        String databaseName = tablePath.getDatabaseName();
        try {
            return querySQLResultExists(
                    this.getUrlFromDatabaseName(databaseName), getTableWithConditionSql(tablePath));
        } catch (SeaTunnelRuntimeException e1) {
            if (e1.getSeaTunnelErrorCode().getCode().equals(UNSUPPORTED_METHOD.getCode())) {
                log.warn(
                        "The catalog: {} is not supported the getTableWithConditionSql for tableExists ",
                        this.catalogName);
                try {
                    return databaseExists(tablePath.getDatabaseName())
                            && listTables(tablePath.getDatabaseName())
                                    .contains(getTableName(tablePath));
                } catch (DatabaseNotExistException e2) {
                    return false;
                }
            }
            throw e1;
        } catch (SQLException e) {
            throw new SeaTunnelException("Failed to querySQLResult", e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        createTable(tablePath, table, ignoreIfExists, true);
    }

    @Override
    public void createTable(
            TablePath tablePath, CatalogTable table, boolean ignoreIfExists, boolean createIndex)
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

        createTableInternal(tablePath, table, createIndex);
    }

    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        throw new UnsupportedOperationException();
    }

    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return Collections.singletonList(getCreateTableSql(tablePath, table, createIndex));
    }

    protected void createTableInternal(TablePath tablePath, CatalogTable table, boolean createIndex)
            throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try {
            final List<String> createTableSqlList =
                    getCreateTableSqls(tablePath, table, createIndex);
            for (String sql : createTableSqlList) {
                executeInternal(dbUrl, sql);
            }
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

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }
        truncateTableInternal(tablePath);
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
        return options;
    }

    @FunctionalInterface
    public interface ResultSetConsumer<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    protected List<String> queryString(String url, String sql, ResultSetConsumer<String> consumer)
            throws SQLException {
        try (PreparedStatement ps = getConnection(url).prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            List<String> result = new ArrayList<>();
            while (rs.next()) {
                String value = consumer.apply(rs);
                if (value != null) {
                    result.add(value);
                }
            }
            return result;
        }
    }

    protected boolean querySQLResultExists(String dbUrl, String sql) throws SQLException {
        try (PreparedStatement stmt = getConnection(dbUrl).prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            return rs.next();
        }
    }

    // If sql is DDL, the execute() method always returns false, so the return value
    // should not be used to determine whether changes were made in database.
    protected boolean executeInternal(String url, String sql) throws SQLException {
        LOG.info("Execute sql : {}", sql);
        try (PreparedStatement ps = getConnection(url).prepareStatement(sql)) {
            return ps.execute();
        }
    }

    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery);
    }

    protected void truncateTableInternal(TablePath tablePath) throws CatalogException {
        try {
            executeInternal(defaultUrl, getTruncateTableSql(tablePath));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed truncate table %s in catalog %s",
                            tablePath.getFullName(), this.catalogName),
                    e);
        }
    }

    protected String getTruncateTableSql(TablePath tablePath) {
        throw new UnsupportedOperationException();
    }

    protected String getExistDataSql(TablePath tablePath) {
        throw new UnsupportedOperationException();
    }

    public void executeSql(TablePath tablePath, String sql) {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    public boolean isExistsData(TablePath tablePath) {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection connection = getConnection(dbUrl);
        String sql = getExistDataSql(tablePath);
        try (PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery()) {

            return resultSet.next();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new SQLPreviewResult(getCreateTableSql(tablePath, catalogTable.get(), true));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(getDropTableSql(tablePath));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new SQLPreviewResult(getTruncateTableSql(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new SQLPreviewResult(getCreateDatabaseSql(tablePath.getDatabaseName()));
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new SQLPreviewResult(getDropDatabaseSql(tablePath.getDatabaseName()));
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
