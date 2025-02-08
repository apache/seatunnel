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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class OracleCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    cols.COLUMN_NAME,\n"
                    + "    CASE \n"
                    + "        WHEN cols.data_type LIKE 'INTERVAL%%' THEN 'INTERVAL'\n"
                    + "        ELSE REGEXP_SUBSTR(cols.data_type, '^[^(]+')\n"
                    + "    END as TYPE_NAME,\n"
                    + "    cols.data_type || \n"
                    + "        CASE \n"
                    + "            WHEN cols.data_type IN ('VARCHAR', 'VARCHAR2', 'CHAR') THEN '(' || cols.data_length || ')'\n"
                    + "            WHEN cols.data_type IN ('NVARCHAR2', 'NCHAR') THEN '(' || cols.char_length || ')'\n"
                    + "            WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NOT NULL THEN '(' || cols.data_precision || ', ' || cols.data_scale || ')'\n"
                    + "            WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NULL THEN '(' || cols.data_precision || ')'\n"
                    + "            WHEN cols.data_type IN ('RAW') THEN '(' || cols.data_length || ')'\n"
                    + "        END AS FULL_TYPE_NAME,\n"
                    + "    cols.data_length AS COLUMN_LENGTH,\n"
                    + "    cols.data_precision AS COLUMN_PRECISION,\n"
                    + "    cols.data_scale AS COLUMN_SCALE,\n"
                    + "    com.comments AS COLUMN_COMMENT,\n"
                    + "    cols.data_default AS DEFAULT_VALUE,\n"
                    + "    CASE cols.nullable WHEN 'N' THEN 'NO' ELSE 'YES' END AS IS_NULLABLE\n"
                    + "FROM\n"
                    + "    all_tab_columns cols\n"
                    + "LEFT JOIN \n"
                    + "    all_col_comments com ON cols.table_name = com.table_name AND cols.column_name = com.column_name AND cols.owner = com.owner\n"
                    + "WHERE \n"
                    + "    cols.owner = '%s'\n"
                    + "    AND cols.table_name = '%s'\n"
                    + "ORDER BY \n"
                    + "    cols.column_id \n";

    private boolean decimalTypeNarrowing;

    public OracleCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        this(
                catalogName,
                username,
                pwd,
                urlInfo,
                defaultSchema,
                JdbcOptions.DECIMAL_TYPE_NARROWING.defaultValue());
    }

    public OracleCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            boolean decimalTypeNarrowing) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.decimalTypeNarrowing = decimalTypeNarrowing;
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return getListTableSql(tablePath.getDatabaseName())
                + "  and  OWNER = '"
                + tablePath.getSchemaName()
                + "' and table_name = '"
                + tablePath.getTableName()
                + "'";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>(Collections.singletonList("default"));
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new OracleCreateTableSqlBuilder(table, createIndex).build(tablePath).get(0);
    }

    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new OracleCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                + "  WHERE TABLE_NAME NOT LIKE 'MDRT_%'"
                + "  AND TABLE_NAME NOT LIKE 'MDRS_%'"
                + "  AND TABLE_NAME NOT LIKE 'MDXT_%'"
                + "  AND (TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%' AND IOT_NAME IS NULL)";
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
        // e.g NUMBER
        String typeName = resultSet.getString("TYPE_NAME");
        // e.g NUMBER(10, 2)
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
        return new OracleTypeConverter(decimalTypeNarrowing).convert(typeDefine);
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
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(
                defaultConnection, sqlQuery, new OracleTypeMapper(decimalTypeNarrowing));
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
                "select * from \"%s\".\"%s\" WHERE rownum = 1",
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
