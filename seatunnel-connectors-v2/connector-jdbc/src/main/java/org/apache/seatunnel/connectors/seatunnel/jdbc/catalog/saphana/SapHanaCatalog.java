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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.saphana;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeConverter.appendColumnSizeIfNeed;

@Slf4j
public class SapHanaCatalog extends AbstractJdbcCatalog {

    static {
        SYS_DATABASES.add("SYS");
        SYS_DATABASES.add("SYSTEM");
        SYS_DATABASES.add("SYS_DATABASES");
        SYS_DATABASES.add("_SYS_ADVISOR");
        SYS_DATABASES.add("_SYS_AFL");
        SYS_DATABASES.add("_SYS_BI");
        SYS_DATABASES.add("_SYS_BIC");
        SYS_DATABASES.add("_SYS_DATA_ANONYMIZATION");
        SYS_DATABASES.add("_SYS_DI");
        SYS_DATABASES.add("_SYS_EPM");
        SYS_DATABASES.add("_SYS_LDB");
        SYS_DATABASES.add("_SYS_PLAN_STABILITY");
        SYS_DATABASES.add("_SYS_REPO");
        SYS_DATABASES.add("_SYS_RT");
        SYS_DATABASES.add("_SYS_SECURITY");
        SYS_DATABASES.add("_SYS_SQL_ANALYZER");
        SYS_DATABASES.add("_SYS_STATISTICS");
        SYS_DATABASES.add("_SYS_TABLE_REPLICAS");
        SYS_DATABASES.add("_SYS_TASK");
        SYS_DATABASES.add("_SYS_TELEMETRY");
        SYS_DATABASES.add("_SYS_XS");
        SYS_DATABASES.add("_SYS_DI_CATALOG");
        SYS_DATABASES.add("_SYS_EPM_DATA");
        SYS_DATABASES.add("_SYS_DI_SU");
        SYS_DATABASES.add("_SYS_WORKLOAD_REPLAY");
        SYS_DATABASES.add("_SYS_AUDIT");
        SYS_DATABASES.add("_SYS_DI_BI_CATALOG");
        SYS_DATABASES.add("_SYS_DI_CDS_CATALOG");
        SYS_DATABASES.add("_SYS_DI_SEARCH_CATALOG");
        SYS_DATABASES.add("_SYS_DI_TO");
    }

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    C.COLUMN_NAME,\n"
                    + "    C.DATA_TYPE_NAME,\n"
                    + "    C.LENGTH,\n"
                    + "    C.SCALE,\n"
                    + "    C.IS_NULLABLE,\n"
                    + "    C.DEFAULT_VALUE,\n"
                    + "    C.COMMENTS,\n"
                    + "    E.DATA_TYPE_NAME AS ELEMENT_TYPE_NAME\n"
                    + "FROM\n"
                    + "    (SELECT * FROM SYS.TABLE_COLUMNS  UNION ALL SELECT * FROM SYS.VIEW_COLUMNS) C\n"
                    + "        LEFT JOIN\n"
                    + "    SYS.ELEMENT_TYPES E\n"
                    + "    ON\n"
                    + "        C.SCHEMA_NAME = E.SCHEMA_NAME\n"
                    + "            AND C.TABLE_NAME = E.OBJECT_NAME\n"
                    + "            AND C.COLUMN_NAME = E.ELEMENT_NAME\n"
                    + "WHERE\n"
                    + "    C.SCHEMA_NAME = '%s'\n"
                    + "  AND C.TABLE_NAME = '%s'\n"
                    + "ORDER BY\n"
                    + "    C.POSITION ASC;";

    public SapHanaCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where SCHEMA_NAME = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName()) + " and TABLE_NAME = '%s'",
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT SCHEMA_NAME FROM SCHEMAS";
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE SCHEMA \"%s\"", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP SCHEMA \"%s\"", databaseName);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new SapHanaCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE %s.%s",
                CatalogUtils.quoteIdentifier(tablePath.getDatabaseName(), "", "\""),
                CatalogUtils.quoteIdentifier(tablePath.getTableName(), "", "\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return String.format(
                "SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '%s'", databaseName);
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("DATA_TYPE_NAME");
        Long columnLength = resultSet.getLong("LENGTH");
        Integer columnScale = resultSet.getObject("SCALE", Integer.class);
        String fullTypeName = appendColumnSizeIfNeed(typeName, columnLength, columnScale);
        String columnComment = resultSet.getString("COMMENTS");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("TRUE");

        if (typeName.equalsIgnoreCase("ARRAY")) {
            fullTypeName =
                    appendColumnSizeIfNeed(
                                    resultSet.getString("ELEMENT_TYPE_NAME"),
                                    columnLength,
                                    columnScale)
                            + " ARRAY";
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(fullTypeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return SapHanaTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    private List<String> listTables() {
        List<String> databases = listDatabases();
        return listTables(databases.get(0));
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SapHanaTypeMapper());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\"",
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT 1 FROM \"%s\".\"%s\"",
                tablePath.getDatabaseName(), tablePath.getTableName());
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
