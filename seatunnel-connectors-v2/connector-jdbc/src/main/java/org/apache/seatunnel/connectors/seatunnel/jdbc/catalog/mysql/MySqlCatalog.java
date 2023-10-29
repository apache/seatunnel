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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeMapper;

import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class MySqlCatalog extends AbstractJdbcCatalog {

    private static final MysqlDataTypeConvertor DATA_TYPE_CONVERTOR = new MysqlDataTypeConvertor();

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME ='%s' ORDER BY ORDINAL_POSITION ASC";

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("mysql");
        SYS_DATABASES.add("performance_schema");
        SYS_DATABASES.add("sys");
    }

    public MySqlCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo, null);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SHOW DATABASES;";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SHOW TABLES;";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getPrimaryKey(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getTableName(),
                tablePath.getTableName());
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getConstraintKeys(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getTableName(),
                tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String sourceType = resultSet.getString("COLUMN_TYPE");
        String typeName = resultSet.getString("DATA_TYPE").toUpperCase();
        int precision = resultSet.getInt("NUMERIC_PRECISION");
        int scale = resultSet.getInt("NUMERIC_SCALE");
        long columnLength = resultSet.getLong("CHARACTER_MAXIMUM_LENGTH");
        long octetLength = resultSet.getLong("CHARACTER_OCTET_LENGTH");
        if (sourceType.toLowerCase(Locale.ROOT).contains("unsigned")) {
            typeName += "_UNSIGNED";
        }
        SeaTunnelDataType<?> type = fromJdbcType(typeName, precision, scale);
        String comment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        String isNullableStr = resultSet.getString("IS_NULLABLE");
        boolean isNullable = isNullableStr.equals("YES");
        long bitLen = 0;
        MysqlType mysqlType = MysqlType.valueOf(typeName);
        switch (mysqlType) {
            case BIT:
                bitLen = precision;
                break;
            case CHAR:
            case VARCHAR:
                columnLength = octetLength;
                break;
            case BINARY:
            case VARBINARY:
                // Uniform conversion to bits
                bitLen = octetLength * 4 * 8L;
                break;
            case BLOB:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                bitLen = columnLength << 3;
                break;
            case JSON:
                columnLength = 4 * 1024 * 1024 * 1024L;
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
                sourceType.contains("unsigned"),
                sourceType.contains("zerofill"),
                bitLen,
                null,
                columnLength);
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return MysqlCreateTableSqlBuilder.builder(tablePath, table).build(table.getCatalogName());
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s;", tablePath.getFullName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE `%s`;", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE `%s`;", databaseName);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, int precision, int scale) {
        MysqlType mysqlType = MysqlType.getByName(typeName);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(MysqlDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(MysqlDataTypeConvertor.SCALE, scale);
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(mysqlType, dataTypeProperties);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new MySqlTypeMapper());
    }
}
