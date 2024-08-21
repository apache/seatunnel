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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift.RedshiftTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift.RedshiftTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RedshiftCatalog extends AbstractJdbcCatalog {

    private final String SELECT_COLUMNS =
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME ='%s' ORDER BY ordinal_position ASC";

    protected final Map<String, Connection> connectionMap;

    public RedshiftCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String schema) {
        super(catalogName, username, pwd, urlInfo, schema);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where datname = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " where table_schema = '%s' and table_name = '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
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
    protected String getListDatabaseSql() {
        return "select datname from pg_database";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT table_schema, table_name FROM information_schema.tables";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        return stringBuilder
                .append(rs.getString(1))
                .append(".")
                .append(rs.getString(2))
                .toString()
                .toLowerCase();
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        String createTableSql =
                new RedshiftCreateTableSqlBuilder(table, createIndex)
                        .build(tablePath, table.getOptions().get("fieldIde"));
        return CatalogUtils.getFieldIde(createTableSql, table.getOptions().get("fieldIde"));
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE %s;", tablePath.getSchemaName() + "." + tablePath.getTableName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE %s;", tablePath.getSchemaName() + "." + tablePath.getTableName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE `%s`;", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE `%s`;", databaseName);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(SELECT_COLUMNS, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("DATA_TYPE").toUpperCase();
        long precision = resultSet.getLong("NUMERIC_PRECISION");
        int scale = resultSet.getInt("NUMERIC_SCALE");
        long columnLength = resultSet.getLong("CHARACTER_MAXIMUM_LENGTH");
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        String isNullableStr = resultSet.getString("IS_NULLABLE");
        boolean isNullable = isNullableStr.equals("YES");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(precision)
                        .scale(scale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .build();
        return RedshiftTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select * from %s LIMIT 1;", tablePath.getFullName());
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(getUrlFromDatabaseName(defaultDatabase)),
                sqlQuery,
                new RedshiftTypeMapper());
    }
}
