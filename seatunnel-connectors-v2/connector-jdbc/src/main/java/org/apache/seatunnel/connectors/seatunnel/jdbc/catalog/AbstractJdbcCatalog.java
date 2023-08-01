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
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    protected final Optional<String> defaultSchema;

    protected Connection defaultConnection;

    public AbstractJdbcCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {

        checkArgument(StringUtils.isNotBlank(username));
        urlInfo.getDefaultDatabase()
                .orElseThrow(
                        () -> new IllegalArgumentException("Can't find default database in url"));
        checkArgument(StringUtils.isNotBlank(urlInfo.getUrlWithoutDatabase()));
        this.catalogName = catalogName;
        this.defaultDatabase = urlInfo.getDefaultDatabase().get();
        this.username = username;
        this.pwd = pwd;
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        this.defaultUrl = urlInfo.getOrigin();
        this.suffix = urlInfo.getSuffix();
        this.defaultSchema = Optional.ofNullable(defaultSchema);
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
        try {
            defaultConnection = DriverManager.getConnection(defaultUrl, username, pwd);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
    }

    @Override
    public void close() throws CatalogException {
        if (defaultConnection == null) {
            return;
        }
        try {
            defaultConnection.close();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to close %s via JDBC.", defaultUrl), e);
        }
        LOG.info("Catalog {} closing", catalogName);
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
            String columnName = resultSet.getString("COLUMN_NAME");
            if (columnName == null) {
                continue;
            }
            String indexName = resultSet.getString("INDEX_NAME");
            boolean noUnique = resultSet.getBoolean("NON_UNIQUE");

            ConstraintKey constraintKey =
                    constraintKeyMap.computeIfAbsent(
                            indexName,
                            s -> {
                                ConstraintKey.ConstraintType constraintType =
                                        ConstraintKey.ConstraintType.KEY;
                                if (!noUnique) {
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
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
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
        if (!createTableInternal(tablePath, table) && !ignoreIfExists) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }
    }

    protected abstract boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException;

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (!dropTableInternal(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(catalogName, tablePath);
        }
    }

    protected abstract boolean dropTableInternal(TablePath tablePath) throws CatalogException;

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

    protected abstract boolean createDatabaseInternal(String databaseName);

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");

        if (!dropDatabaseInternal(tablePath.getDatabaseName()) && !ignoreIfNotExists) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    protected abstract boolean dropDatabaseInternal(String databaseName) throws CatalogException;
}
