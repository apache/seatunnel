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
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeConverter.appendColumnSizeIfNeed;

@Slf4j
public class SapHanaCatalog extends AbstractJdbcCatalog {

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
    public String getListViewSql(String databaseName) {
        return String.format(
                "SELECT VIEW_NAME FROM SYS.VIEWS WHERE SCHEMA_NAME = '%s'", databaseName);
    }

    @Override
    public String getListSynonymSql(String databaseName) {
        return String.format(
                "SELECT SYNONYM_NAME FROM SYNONYMS WHERE SCHEMA_NAME = '%s'", databaseName);
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
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return querySQLResultExists(
                                this.getUrlFromDatabaseName(tablePath.getDatabaseName()),
                                getTableWithConditionSql(tablePath))
                        || querySQLResultExists(
                                this.getUrlFromDatabaseName(tablePath.getDatabaseName()),
                                String.format(
                                        getListViewSql(tablePath.getDatabaseName())
                                                + " AND VIEW_NAME = '%s'",
                                        tablePath.getTableName()))
                        || querySQLResultExists(
                                this.getUrlFromDatabaseName(tablePath.getDatabaseName()),
                                String.format(
                                        getListSynonymSql(tablePath.getDatabaseName())
                                                + " AND SYNONYM_NAME = '%s'",
                                        tablePath.getSchemaAndTableName()));
            }
            return querySQLResultExists(
                    this.getUrlFromDatabaseName(tablePath.getDatabaseName()),
                    getTableWithConditionSql(tablePath));
        } catch (DatabaseNotExistException e) {
            return false;
        } catch (SQLException e) {
            throw new SeaTunnelException("Failed to querySQLResult", e);
        }
    }

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
        Connection conn = getConnection(dbUrl);
        TablePath originalTablePath = tablePath;
        if (listSynonym(tablePath.getDatabaseName()).contains(tablePath.getTableName())) {
            String sql =
                    String.format(
                            "SELECT SYNONYM_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_SCHEMA  FROM SYNONYMS  WHERE SCHEMA_NAME = '%s' AND SYNONYM_NAME = '%s' ",
                            tablePath.getDatabaseName(), tablePath.getTableName());
            try (PreparedStatement statement = conn.prepareStatement(sql);
                    final ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String refDatabaseName = resultSet.getString("OBJECT_SCHEMA");
                    final String refTableName = resultSet.getString("OBJECT_NAME");
                    tablePath = TablePath.of(refDatabaseName, refTableName);
                }
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed getting SYNONYM %s", tablePath.getFullName()), e);
            }
        }
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<PrimaryKey> primaryKey = getPrimaryKey(metaData, tablePath);
            List<ConstraintKey> constraintKeys = getConstraintKeys(metaData, tablePath);
            try (PreparedStatement ps = conn.prepareStatement(getSelectColumnsSql(tablePath));
                    ResultSet resultSet = ps.executeQuery()) {

                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, resultSet, builder);
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier = getTableIdentifier(originalTablePath);
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "",
                        catalogName);
            }
        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
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
