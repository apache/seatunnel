/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu.XuguTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu.XuguTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class XuguCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    dc.COLUMN_NAME,\n"
                    + "    CASE\n"
                    + "        WHEN dc.TYPE_NAME LIKE 'INTERVAL%%' THEN 'INTERVAL' ELSE REGEXP_SUBSTR(dc.TYPE_NAME, '^[^(]+')\n"
                    + "    END AS TYPE_NAME,\n"
                    + "    dc.TYPE_NAME ||\n"
                    + "    CASE\n"
                    + "        WHEN dc.TYPE_NAME IN ('VARCHAR', 'CHAR') THEN '(' || dc.COLUMN_LENGTH || ')'\n"
                    + "        WHEN dc.TYPE_NAME IN ('NUMERIC') AND dc.COLUMN_PRECISION IS NOT NULL AND dc.COLUMN_SCALE IS NOT NULL THEN '(' || dc.COLUMN_PRECISION || ', ' || dc.COLUMN_SCALE || ')'\n"
                    + "        WHEN dc.TYPE_NAME IN ('NUMERIC') AND dc.COLUMN_PRECISION IS NOT NULL AND dc.COLUMN_SCALE IS NULL THEN '(' || dc.COLUMN_PRECISION || ')'\n"
                    + "        WHEN dc.TYPE_NAME IN ('TIMESTAMP') THEN '(' || dc.COLUMN_SCALE || ')'\n"
                    + "    END AS FULL_TYPE_NAME,\n"
                    + "    dc.COLUMN_LENGTH,\n"
                    + "    dc.COLUMN_PRECISION,\n"
                    + "    dc.COLUMN_SCALE,\n"
                    + "    dc.COLUMN_COMMENT,\n"
                    + "    dc.DEFAULT_VALUE,\n"
                    + "    CASE\n"
                    + "        dc.IS_NULLABLE WHEN TRUE THEN 'NO' ELSE 'YES'\n"
                    + "    END AS IS_NULLABLE\n"
                    + "FROM\n"
                    + "    (\n"
                    + "    SELECT\n"
                    + "        c.col_name AS COLUMN_NAME,\n"
                    + "        CASE\n"
                    + "            WHEN c.type_name = 'CHAR' AND c.\"VARYING\" = TRUE THEN 'VARCHAR'\n"
                    + "            WHEN c.type_name = 'DATETIME' AND c.TIMESTAMP_T = 'i' THEN 'TIMESTAMP' ELSE c.type_name\n"
                    + "        END AS TYPE_NAME,\n"
                    + "        DECODE(c.type_name,\n"
                    + "        'TINYINT', 1, 'SMALLINT', 2,\n"
                    + "        'INTEGER', 4, 'BIGINT', 8,\n"
                    + "        'FLOAT', 4, 'DOUBLE', 8,\n"
                    + "        'NUMERIC', 17,\n"
                    + "        'CHAR', DECODE(c.scale, -1, 60000, c.scale),\n"
                    + "        'DATE', 4, 'DATETIME', 8,\n"
                    + "        'TIMESTAMP', 8, 'DATETIME WITH TIME ZONE', 8,\n"
                    + "        'TIME', 4, 'TIME WITH TIME ZONE', 4,\n"
                    + "        'INTERVAL YEAR', 4, 'INTERVAL MONTH', 4,\n"
                    + "        'INTERVAL DAY', 4, 'INTERVAL HOUR', 4,\n"
                    + "        'INTERVAL MINUTE', 4, 'INTERVAL SECOND', 8,\n"
                    + "        'INTERVAL YEAR TO MONTH', 4,\n"
                    + "        'INTERVAL DAY TO HOUR', 4,\n"
                    + "        'INTERVAL DAY TO MINUTE', 4,\n"
                    + "        'INTERVAL DAY TO SECOND', 8,\n"
                    + "        'INTERVAL HOUR TO MINUTE', 4,\n"
                    + "        'INTERVAL HOUR TO SECOND', 8,\n"
                    + "        'INTERVAL MINUTE TO SECOND', 8,\n"
                    + "        'CLOB', 2147483648,\n"
                    + "        'BLOB', 2147483648, 'BINARY', 2147483648,\n"
                    + "        'GUID', 2, 'BOOLEAN', 1,\n"
                    + "        'ROWVERSION', 8, 'ROWID', 10, NULL) AS COLUMN_LENGTH,\n"
                    + "        DECODE(TRUNC(c.scale / 65536), 0, NULL, TRUNC(c.scale / 65536)::INTEGER) AS COLUMN_PRECISION,\n"
                    + "        DECODE(DECODE(c.type_name, 'CHAR',-1, c.scale),-1, NULL, MOD(c.scale, 65536)) AS COLUMN_SCALE,\n"
                    + "        c.comments AS COLUMN_COMMENT,\n"
                    + "        c.DEF_VAL AS DEFAULT_VALUE,\n"
                    + "        c.NOT_NULl AS IS_NULLABLE\n"
                    + "    FROM\n"
                    + "        dba_columns c\n"
                    + "    LEFT JOIN dba_tables tab ON\n"
                    + "        c.db_id = tab.db_id\n"
                    + "        AND c.table_id = tab.table_id\n"
                    + "    LEFT JOIN dba_schemas sc ON\n"
                    + "        tab.schema_id = sc.schema_id\n"
                    + "        AND tab.db_id = sc.db_id\n"
                    + "    WHERE\n"
                    + "        sc.schema_name = '%s'\n"
                    + "        AND tab.table_name = '%s'\n"
                    + ") AS dc \n";

    public XuguCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + "  where DB_NAME = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + "  where user_name = '%s' and table_name = '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT DB_NAME FROM dba_databases";
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new XuguCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
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
    protected String getListTableSql(String databaseName) {
        return "SELECT user_name ,table_name FROM all_users au \n"
                + "INNER JOIN all_tables at ON au.user_id=at.user_id AND au.db_id=at.db_id";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        Long columnPrecision = resultSet.getObject("COLUMN_PRECISION", Long.class);
        Integer columnScale = resultSet.getObject("COLUMN_SCALE", Integer.class);
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("YES");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(fullTypeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return XuguTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    private List<String> listTables() {
        List<String> databases = listDatabases();
        return listTables(databases.get(0));
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new XuguTypeMapper());
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
                "SELECT * FROM \"%s\".\"%s\" WHERE ROWNUM = 1",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        try {
            return getConstraintKeys(
                    metaData,
                    tablePath.getDatabaseName(),
                    tablePath.getSchemaName(),
                    tablePath.getTableName());
        } catch (SQLException e) {
            log.info("Obtain constraint failure", e);
            return new ArrayList<>();
        }
    }
}
