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
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractJdbcCatalog implements Catalog {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcCatalog.class);
    protected final String catalogName;
    protected final String defaultDatabase;
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected final String suffix;
    protected final String defaultUrl;
    protected final JdbcDialect jdbcDialect;
    protected static final Set<String> SYS_DATABASES = new HashSet<>();

    public AbstractJdbcCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {

        checkArgument(StringUtils.isNotBlank(username));
        urlInfo.getDefaultDatabase()
                .orElseThrow(
                        () -> new IllegalArgumentException("Can't find default database in url"));
        checkArgument(StringUtils.isNotBlank(urlInfo.getUrlWithoutDatabase()));
        this.catalogName = catalogName;
        this.defaultDatabase = urlInfo.getDefaultDatabase().get();
        this.username = username;
        this.pwd = pwd;
        String baseUrl = urlInfo.getUrlWithoutDatabase();
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.defaultUrl = urlInfo.getOrigin();
        this.suffix = urlInfo.getSuffix();
        this.jdbcDialect = JdbcDialectLoader.load(this.baseUrl);
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return pwd;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    @Override
    public void open() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            // test connection, fail early if we cannot connect to database
            conn.getCatalog();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Catalog {} closing", catalogName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

            PreparedStatement ps = conn.prepareStatement(jdbcDialect.listDatabases());

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (!getSysDatabases().contains(databaseName)) {
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

        String dbUrl = jdbcDialect.getUrlFromDatabaseName(baseUrl, databaseName, suffix);
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(jdbcDialect.listTableSql(databaseName))) {

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(jdbcDialect.getTableName(rs));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String table) throws SQLException {
        return getPrimaryKey(metaData, database, table, table);
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {

        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.
        ResultSet rs = metaData.getPrimaryKeys(database, schema, table);

        // seq -> column name
        List<Pair<Integer, String>> primaryKeyColumns = new ArrayList<>();
        String pkName = null;
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            // all the PK_NAME should be the same
            pkName = rs.getString("PK_NAME");
            int keySeq = rs.getInt("KEY_SEQ");
            // KEY_SEQ is 1-based index
            primaryKeyColumns.add(Pair.of(keySeq, columnName));
        }
        // initialize size
        List<String> pkFields =
                primaryKeyColumns.stream()
                        .sorted(Comparator.comparingInt(Pair::getKey))
                        .map(Pair::getValue)
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pkFields)) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(pkName, pkFields));
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String table) throws SQLException {
        return getConstraintKeys(metaData, database, table, table);
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        ResultSet resultSet = metaData.getIndexInfo(database, schema, table, false, false);
        // index name -> index
        Map<String, ConstraintKey> constraintKeyMap = new HashMap<>();
        while (resultSet.next()) {
            String indexName = resultSet.getString("INDEX_NAME");
            String columnName = resultSet.getString("COLUMN_NAME");
            String unique = resultSet.getString("NON_UNIQUE");

            ConstraintKey constraintKey =
                    constraintKeyMap.computeIfAbsent(
                            indexName,
                            s -> {
                                ConstraintKey.ConstraintType constraintType =
                                        ConstraintKey.ConstraintType.KEY;
                                // 0 is unique.
                                if ("0".equals(unique)) {
                                    constraintType = ConstraintKey.ConstraintType.UNIQUE_KEY;
                                }
                                return ConstraintKey.of(
                                        constraintType, indexName, new ArrayList<>());
                            });

            ConstraintKey.ColumnSortType sortType =
                    "A".equals(resultSet.getString("ASC_OR_DESC"))
                            ? ConstraintKey.ColumnSortType.ASC
                            : ConstraintKey.ColumnSortType.DESC;
            ConstraintKey.ConstraintKeyColumn constraintKeyColumn =
                    new ConstraintKey.ConstraintKeyColumn(columnName, sortType);
            constraintKey.getColumnNames().add(constraintKeyColumn);
        }
        return new ArrayList<>(constraintKeyMap.values());
    }

    protected Optional<String> getColumnDefaultValue(
            DatabaseMetaData metaData, String table, String column) throws SQLException {
        return getColumnDefaultValue(metaData, null, null, table, column);
    }

    protected Optional<String> getColumnDefaultValue(
            DatabaseMetaData metaData, String database, String schema, String table, String column)
            throws SQLException {
        try (ResultSet resultSet = metaData.getColumns(database, schema, table, column)) {
            while (resultSet.next()) {
                String defaultValue = resultSet.getString("COLUMN_DEF");
                return Optional.ofNullable(defaultValue);
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName())
                            .contains(jdbcDialect.getTableName(tablePath));
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
        if (!createTableInternal(tablePath, table) && !ignoreIfExists) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }
    }

    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl =
                jdbcDialect.getUrlFromDatabaseName(baseUrl, tablePath.getDatabaseName(), suffix);
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

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (!dropTableInternal(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(catalogName, tablePath);
        }
    }

    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl =
                jdbcDialect.getUrlFromDatabaseName(baseUrl, tablePath.getDatabaseName(), suffix);
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                jdbcDialect.getDropTableSql(tablePath.getFullName()))) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
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
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }
        if (!createDatabaseInternal(tablePath.getDatabaseName()) && !ignoreIfExists) {
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");

        if (!dropDatabaseInternal(tablePath.getDatabaseName()) && !ignoreIfNotExists) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    protected SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return null;
    }

    protected Set<String> getSysDatabases() {
        return SYS_DATABASES;
    }

    protected Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put(
                "url",
                jdbcDialect.getUrlFromDatabaseName(baseUrl, tablePath.getDatabaseName(), suffix));
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    protected boolean createDatabaseInternal(String databaseName) {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format(jdbcDialect.createDatabaseSql(databaseName)))) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(jdbcDialect.dropDatabaseSql(databaseName))) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    // todo: If the origin source is mysql, we can directly use create table like to create the
    // target table?
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String dbUrl =
                jdbcDialect.getUrlFromDatabaseName(baseUrl, tablePath.getDatabaseName(), suffix);
        String createTableSql = jdbcDialect.createTableSql(tablePath, table);
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd);
                PreparedStatement ps = conn.prepareStatement(createTableSql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
    }
}
