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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris;

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
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.iris.IrisTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.iris.IrisTypeMapper;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class IrisCatalog extends AbstractJdbcCatalog {

    private static final String LIST_TABLES_SQL_TEMPLATE =
            "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.Tables WHERE TABLE_SCHEMA='%s' and TABLE_TYPE != 'SYSTEM TABLE' and TABLE_TYPE != 'SYSTEM VIEW'";

    public IrisCatalog(
            String catalogName, String username, String password, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, password, urlInfo, null);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new IrisCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    public String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE \"%s\"", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE \"%s\"", databaseName);
    }

    @Override
    protected String getListTableSql(String tableSchemaName) {
        return String.format(LIST_TABLES_SQL_TEMPLATE, tableSchemaName);
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);
        // It's the system schema when schema name start with %
        if (schemaName.startsWith("%")) {
            return null;
        }
        return schemaName + "." + tableName;
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        Long columnLength = resultSet.getLong("COLUMN_SIZE");
        Long columnPrecision = columnLength;
        Integer columnScale = resultSet.getObject("DECIMAL_DIGITS", Integer.class);
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean isNullable = (resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return IrisTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        throw new SeaTunnelException("Not supported for list databases for iris");
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return querySQLResultExists(
                    this.getUrlFromDatabaseName(tablePath.getDatabaseName()),
                    getTableWithConditionSql(tablePath));
        } catch (SQLException e) {
            throw new SeaTunnelException("Failed to querySQLResult", e);
        }
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getSchemaName()) + " and TABLE_NAME = '%s'",
                tablePath.getTableName());
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    public List<String> listTables(String schemaName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return queryString(defaultUrl, getListTableSql(schemaName), this::getTableName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new IrisTypeMapper());
    }

    @Override
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
        try {
            Connection conn = getConnection(dbUrl);
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet resultSet =
                    metaData.getColumns(
                            null, tablePath.getSchemaName(), tablePath.getTableName(), null)) {
                Optional<PrimaryKey> primaryKey = getPrimaryKey(metaData, tablePath);
                List<ConstraintKey> constraintKeys = getConstraintKeys(metaData, tablePath);
                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, resultSet, builder);
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

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");
        createDatabaseInternal(tablePath.getDatabaseName());
    }

    @Override
    public void createTable(
            TablePath tablePath, CatalogTable table, boolean ignoreIfExists, boolean createIndex)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
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

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        truncateTableInternal(tablePath);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");
        dropDatabaseInternal(tablePath.getDatabaseName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT TOP 1 * FROM \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @VisibleForTesting
    public void setConnection(String url, Connection connection) {
        this.connectionMap.put(url, connection);
    }
}
