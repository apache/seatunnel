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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.yashandb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class YashanDbCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    cols.COLUMN_NAME,\n"
                    + "    cols.DATA_TYPE,\n"
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
                    + "    cols.nullable AS IS_NULLABLE\n"
                    + "FROM\n"
                    + "    all_tab_columns cols\n"
                    + "LEFT JOIN \n"
                    + "    all_col_comments com ON cols.table_name = com.table_name AND cols.column_name = com.column_name AND cols.owner = com.owner\n"
                    + "WHERE \n"
                    + "    cols.owner = '%s'\n"
                    + "    AND cols.table_name = '%s'\n"
                    + "ORDER BY \n"
                    + "    cols.column_id \n";

    public YashanDbCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
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
        return getCreateTableSqls(tablePath, table, createIndex).get(0);
    }

    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new YashanDbCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                + "  WHERE TABLE_NAME NOT LIKE 'OL$%'"
                + "  AND TABLE_NAME NOT LIKE 'WRM$%'"
                + "  AND TABLE_NAME NOT LIKE 'WRH$%'"
                + "  AND TABLE_NAME NOT LIKE 'WRI$%'"
                + "  AND TABLE_NAME NOT LIKE 'YLS$%'"
                + "  AND TABLE_NAME NOT LIKE 'WRM$%'";
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
        // e.g NUMBER, TIMESTAMP, TIMESTAMP WITH TIME ZONE
        // DATA_TYPE from all_tab_columns may include precision like TIMESTAMP(6),
        // strip parenthesized digits to get the pure type name
        String typeName =
                resultSet.getString("DATA_TYPE").toUpperCase().replaceAll("\\(\\d+\\)", "").trim();
        // e.g NUMBER(10, 2)
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        Long columnPrecision = resultSet.getObject("COLUMN_PRECISION", Long.class);
        Integer columnScale = resultSet.getObject("COLUMN_SCALE", Integer.class);
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("Y");

        BasicTypeDefine<?> typeDefine =
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
        return new YashanDbTypeConverter().convert(typeDefine);
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
        return resolveQueryTable(sqlQuery).getTable();
    }

    /**
     * The YashanDB JDBC driver does not implement {@code PreparedStatement.getMetaData()}, so
     * execute the query once via {@code Statement} with the returned row count capped and reuse the
     * single {@link ResultSetMetaData} for both the query-derived table and the single-table
     * verification. The connection returned by {@code getConnection(defaultUrl)} is cached and
     * shared, so it must not be closed here.
     */
    @Override
    public QueryTableResolution resolveQueryTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        try (Statement stmt = defaultConnection.createStatement()) {
            stmt.setMaxRows(1);
            try (ResultSet rs = stmt.executeQuery(sqlQuery)) {
                ResultSetMetaData resultSetMetaData = rs.getMetaData();
                Optional<TablePath> singlePhysicalTablePath =
                        CatalogUtils.getSinglePhysicalTablePath(resultSetMetaData);
                CatalogTable catalogTable =
                        CatalogUtils.getCatalogTable(
                                resultSetMetaData, new YashanDbTypeMapper(), sqlQuery);

                PrimaryKey primaryKey =
                        extractPrimaryKey(defaultConnection, resultSetMetaData, sqlQuery);
                if (primaryKey == null) {
                    return new QueryTableResolution(catalogTable, singlePhysicalTablePath);
                }

                Set<String> queryColumns =
                        catalogTable.getTableSchema().getColumns().stream()
                                .map(Column::getName)
                                .collect(Collectors.toSet());
                if (!queryColumns.containsAll(primaryKey.getColumnNames())) {
                    return new QueryTableResolution(catalogTable, singlePhysicalTablePath);
                }

                TableSchema newSchema =
                        TableSchema.builder()
                                .columns(catalogTable.getTableSchema().getColumns())
                                .primaryKey(primaryKey)
                                .constraintKey(catalogTable.getTableSchema().getConstraintKeys())
                                .build();

                return new QueryTableResolution(
                        CatalogTable.of(
                                catalogTable.getTableId(),
                                newSchema,
                                catalogTable.getOptions(),
                                catalogTable.getPartitionKeys(),
                                catalogTable.getComment(),
                                catalogTable.getCatalogName()),
                        singlePhysicalTablePath);
            }
        }
    }

    private PrimaryKey extractPrimaryKey(
            Connection connection, ResultSetMetaData resultSetMetaData, String sqlQuery) {
        try {
            String tableName = resultSetMetaData.getTableName(1);
            if (StringUtils.isBlank(tableName)) {
                return null;
            }

            String databaseName = resultSetMetaData.getCatalogName(1);
            String schemaName = resultSetMetaData.getSchemaName(1);
            DatabaseMetaData dbMetaData = connection.getMetaData();

            TablePath tablePath =
                    TablePath.of(
                            StringUtils.isBlank(databaseName) ? null : databaseName,
                            StringUtils.isBlank(schemaName) ? null : schemaName,
                            tableName);

            return CatalogUtils.getPrimaryKey(dbMetaData, tablePath).orElse(null);
        } catch (SQLException e) {
            log.debug(
                    "Failed to extract primary key from database metadata for sql: {}",
                    sqlQuery,
                    e);
            return null;
        }
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
