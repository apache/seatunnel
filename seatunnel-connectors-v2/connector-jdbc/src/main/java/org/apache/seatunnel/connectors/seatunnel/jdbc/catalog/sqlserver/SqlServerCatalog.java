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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlserverTypeMapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SqlServerCatalog extends AbstractJdbcCatalog {

    private static final SqlServerDataTypeConvertor DATA_TYPE_CONVERTOR =
            new SqlServerDataTypeConvertor();

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT tbl.name AS table_name,\n"
                    + "       col.name AS column_name,\n"
                    + "       ext.value AS comment,\n"
                    + "       col.column_id AS column_id,\n"
                    + "       types.name AS type,\n"
                    + "       col.max_length AS max_length,\n"
                    + "       col.precision AS precision,\n"
                    + "       col.scale AS scale,\n"
                    + "       col.is_nullable AS is_nullable,\n"
                    + "       def.definition AS default_value\n"
                    + "FROM sys.tables tbl\n"
                    + "    INNER JOIN sys.columns col ON tbl.object_id = col.object_id\n"
                    + "    LEFT JOIN sys.types types ON col.user_type_id = types.user_type_id\n"
                    + "    LEFT JOIN sys.extended_properties ext ON ext.major_id = col.object_id AND ext.minor_id = col.column_id\n"
                    + "    LEFT JOIN sys.default_constraints def ON col.default_object_id = def.object_id AND ext.minor_id = col.column_id AND ext.name = 'MS_Description'\n"
                    + "WHERE schema_name(tbl.schema_id) = '%s' %s\n"
                    + "ORDER BY tbl.name, col.column_id";

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
                        ? "AND tbl.name = '" + tablePath.getTableName() + "'"
                        : "";

        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tableSql);
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String sourceType = resultSet.getString("type");
        //        String typeName = resultSet.getString("DATA_TYPE").toUpperCase();
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        SeaTunnelDataType<?> type = fromJdbcType(sourceType, precision, scale);
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        if (defaultValue != null) {
            defaultValue =
                    defaultValue.toString().replace("(", "").replace("'", "").replace(")", "");
        }
        boolean isNullable = resultSet.getBoolean("is_nullable");
        long bitLen = 0;
        StringBuilder sb = new StringBuilder(sourceType);
        Pair<SqlServerType, Map<String, Object>> parse = SqlServerType.parse(sourceType);
        switch (parse.getLeft()) {
            case BINARY:
            case VARBINARY:
                // Uniform conversion to bits
                if (columnLength != -1) {
                    bitLen = columnLength * 4 * 8;
                    sourceType = sb.append("(").append(columnLength).append(")").toString();
                } else {
                    sourceType = sb.append("(").append("max").append(")").toString();
                    bitLen = columnLength;
                }
                break;
            case TIMESTAMP:
                bitLen = columnLength << 3;
                break;
            case VARCHAR:
            case NCHAR:
            case NVARCHAR:
            case CHAR:
                if (columnLength != -1) {
                    sourceType = sb.append("(").append(columnLength).append(")").toString();
                } else {
                    sourceType = sb.append("(").append("max").append(")").toString();
                }
                break;
            case DECIMAL:
            case NUMERIC:
                sourceType =
                        sb.append("(")
                                .append(precision)
                                .append(",")
                                .append(scale)
                                .append(")")
                                .toString();
                break;
            case TEXT:
                columnLength = Integer.MAX_VALUE;
                break;
            case NTEXT:
                columnLength = Integer.MAX_VALUE >> 1;
                break;
            case IMAGE:
                bitLen = Integer.MAX_VALUE * 8L;
                break;
            default:
                break;
        }
        return PhysicalColumn.of(
                columnName,
                type,
                0,
                isNullable,
                defaultValue,
                comment,
                sourceType,
                false,
                false,
                bitLen,
                null,
                columnLength);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, int precision, int scale) {
        Pair<SqlServerType, Map<String, Object>> pair = SqlServerType.parse(typeName);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(SqlServerDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(SqlServerDataTypeConvertor.SCALE, scale);
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(pair.getLeft(), dataTypeProperties);
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
}
