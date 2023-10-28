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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DamengCatalog extends AbstractJdbcCatalog {
    private static final DamengDataTypeConvertor DATA_TYPE_CONVERTOR =
            new DamengDataTypeConvertor();
    private static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList("SYS", "SYSDBA", "SYSSSO", "SYSAUDITOR", "CTISYS"));

    private static final String SELECT_COLUMNS_SQL =
            "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COLUMNS.DATA_LENGTH, COLUMNS.DATA_PRECISION, COLUMNS.DATA_SCALE "
                    + ", COLUMNS.NULLABLE, COLUMNS.DATA_DEFAULT, COMMENTS.COMMENTS ,"
                    + "CASE \n"
                    + "        WHEN COLUMNS.DATA_TYPE IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'VARBINARY', 'BINARY') THEN COLUMNS.DATA_TYPE || '(' || COLUMNS.DATA_LENGTH || ')'\n"
                    + "        WHEN COLUMNS.DATA_TYPE IN ('NUMERIC', 'DECIMAL', 'NUMBER') AND COLUMNS.DATA_PRECISION IS NOT NULL AND COLUMNS.DATA_SCALE IS NOT NULL AND COLUMNS.DATA_PRECISION != 0 AND COLUMNS.DATA_SCALE != 0 THEN COLUMNS.DATA_TYPE || '(' || COLUMNS.DATA_PRECISION || ', ' || COLUMNS.DATA_SCALE || ')'\n"
                    + "        ELSE COLUMNS.DATA_TYPE\n"
                    + "    END AS SOURCE_TYPE \n"
                    + "FROM ALL_TAB_COLUMNS COLUMNS "
                    + "LEFT JOIN ALL_COL_COMMENTS COMMENTS "
                    + "ON COLUMNS.OWNER = COMMENTS.SCHEMA_NAME "
                    + "AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME "
                    + "AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME "
                    + "WHERE COLUMNS.OWNER = '%s' "
                    + "AND COLUMNS.TABLE_NAME = '%s' "
                    + "ORDER BY COLUMNS.COLUMN_ID ASC";

    public DamengCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT name FROM v$database";
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new DamengCreateTableSqlBuilder(table).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
            return null;
        }
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("DATA_TYPE");
        String sourceTypeName = resultSet.getString("SOURCE_TYPE");
        long columnLength = resultSet.getLong("DATA_LENGTH");
        long columnPrecision = resultSet.getLong("DATA_PRECISION");
        long columnScale = resultSet.getLong("DATA_SCALE");
        String columnComment = resultSet.getString("COMMENTS");
        Object defaultValue = resultSet.getObject("DATA_DEFAULT");
        boolean isNullable = resultSet.getString("NULLABLE").equals("Y");

        SeaTunnelDataType<?> type = fromJdbcType(typeName, columnPrecision, columnScale);
        long bitLen = 0;
        long longColumnLength = 0;
        switch (typeName) {
            case DamengDataTypeConvertor.DM_BIT:
                bitLen = columnLength;
                break;
            case DamengDataTypeConvertor.DM_DECIMAL:
            case DamengDataTypeConvertor.DM_TIMESTAMP:
            case DamengDataTypeConvertor.DM_DATETIME:
            case DamengDataTypeConvertor.DM_TIME:
                columnLength = columnScale;
                break;
            case DamengDataTypeConvertor.DM_CHAR:
            case DamengDataTypeConvertor.DM_CHARACTER:
            case DamengDataTypeConvertor.DM_VARCHAR:
            case DamengDataTypeConvertor.DM_VARCHAR2:
            case DamengDataTypeConvertor.DM_LONGVARCHAR:
            case DamengDataTypeConvertor.DM_CLOB:
            case DamengDataTypeConvertor.DM_TEXT:
            case DamengDataTypeConvertor.DM_LONG:
                longColumnLength = columnLength;
                break;
            case DamengDataTypeConvertor.DM_BINARY:
            case DamengDataTypeConvertor.DM_VARBINARY:
            case DamengDataTypeConvertor.DM_BLOB:
            case DamengDataTypeConvertor.DM_BFILE:
            case DamengDataTypeConvertor.DM_IMAGE:
            case DamengDataTypeConvertor.DM_LONGVARBINARY:
                bitLen = columnLength * 8;
                break;
            default:
                break;
        }
        return PhysicalColumn.of(
                columnName,
                type,
                ((int) columnLength),
                isNullable,
                defaultValue,
                columnComment,
                sourceTypeName,
                false,
                false,
                bitLen,
                null,
                longColumnLength);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(DamengDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(DamengDataTypeConvertor.SCALE, scale);
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(typeName, dataTypeProperties);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                                .contains(tablePath.getSchemaAndTableName());
            }
            return listTables().contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    private List<String> listTables() {
        List<String> databases = listDatabases();
        return listTables(databases.get(0));
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DmdbTypeMapper());
    }
}
