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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlserverTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class SqlServerCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT col.TABLE_NAME AS table_name,\n"
                    + "       col.COLUMN_NAME AS column_name,\n"
                    + "       prop.VALUE AS comment,\n"
                    + "       col.ORDINAL_POSITION AS column_id,\n"
                    + "       col.DATA_TYPE AS type,\n"
                    + "       CASE WHEN col.DATA_TYPE in ('nchar','nvarchar','ntext') THEN col.CHARACTER_MAXIMUM_LENGTH\n"
                    + "            WHEN col.CHARACTER_OCTET_LENGTH IS NOT NULL AND col.CHARACTER_OCTET_LENGTH > 0 THEN col.CHARACTER_OCTET_LENGTH\n"
                    + "            ELSE scol.max_length end  AS max_length,\n"
                    + "       CASE WHEN col.DATA_TYPE in ('datetime','datetime2','datetimeoffset','date','time','smalldatetime' ) THEN col.DATETIME_PRECISION \n"
                    + "         ELSE col.NUMERIC_PRECISION end AS precision,\n"
                    + "       col.NUMERIC_SCALE AS scale,\n"
                    + "       col.IS_NULLABLE AS is_nullable,\n"
                    + "       col.COLUMN_DEFAULT AS default_value\n"
                    + "FROM INFORMATION_SCHEMA.COLUMNS col\n"
                    + "     JOIN sys.columns scol  ON OBJECT_ID(col.TABLE_SCHEMA + '.' + col.TABLE_NAME) = scol.object_id \n"
                    + "         AND col.COLUMN_NAME = scol.name"
                    + "     LEFT JOIN sys.extended_properties prop\n"
                    + "         ON prop.major_id = OBJECT_ID(col.TABLE_SCHEMA + '.' + col.TABLE_NAME)\n"
                    + "         AND prop.minor_id = col.ORDINAL_POSITION\n"
                    + "         AND prop.name = 'MS_Description'\n"
                    + "WHERE   col.TABLE_SCHEMA='%s' %s \n"
                    + "ORDER BY col.TABLE_NAME, col.ORDINAL_POSITION";

    public SqlServerCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT NAME FROM sys.databases";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT TABLE_SCHEMA, TABLE_NAME FROM "
                + databaseName
                + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        String tableSql =
                StringUtils.isNotEmpty(tablePath.getTableName())
                        ? "AND col.TABLE_NAME = '" + tablePath.getTableName() + "'"
                        : "";

        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tableSql);
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String dataType = resultSet.getString("type");
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getBoolean("is_nullable");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(dataType)
                        .length(columnLength)
                        .precision((long) precision)
                        .scale(scale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return SqlServerTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return SqlServerCreateTableSqlBuilder.builder(tablePath, table).build(tablePath, table);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getFullName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE %s", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE %s;", databaseName);
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + ";databaseName=" + databaseName + ";" + suffix;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                                .contains(tablePath.getSchemaAndTableName());
            }
            return listTables(defaultDatabase).contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SqlserverTypeMapper());
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select TOP 1 * from %s ;", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format("TRUNCATE TABLE  %s", tablePath.getFullNameWithQuoted("[", "]"));
    }
}
