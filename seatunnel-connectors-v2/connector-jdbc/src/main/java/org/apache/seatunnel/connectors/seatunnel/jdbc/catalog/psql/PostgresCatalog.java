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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeMapper;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class PostgresCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT \n"
                    + "    a.attname AS column_name, \n"
                    + "\t\tt.typname as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypmod = -1 THEN t.typname\n"
                    + "        WHEN t.typname = 'varchar' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' THEN 'char' || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'numeric' OR t.typname = 'decimal' THEN t.typname || '(' || ((a.atttypmod - 4) >> 16) || ', ' || ((a.atttypmod - 4) & 65535) || ')'\n"
                    + "        WHEN t.typname = 'bit' OR t.typname = 'bit varying' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz') THEN t.typname || '(' || a.atttypmod || ')'\n"
                    + "        ELSE t.typname || '' \n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN a.atttypmod = -1 THEN NULL\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar', 'bit', 'bit varying') THEN a.atttypmod - 4\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) >> 16\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_length,\n"
                    + "\t\tCASE\n"
                    + "        WHEN a.atttypmod = -1 THEN NULL\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) & 65535\n"
                    + "        WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz') THEN a.atttypmod\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_scale,\n"
                    + "\t\td.description AS column_comment,\n"
                    + "\t\tpg_get_expr(ad.adbin, ad.adrelid) AS default_value,\n"
                    + "\t\tCASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable\n"
                    + "FROM \n"
                    + "    pg_class c\n"
                    + "    JOIN pg_namespace n ON c.relnamespace = n.oid\n"
                    + "    JOIN pg_attribute a ON a.attrelid = c.oid\n"
                    + "    JOIN pg_type t ON a.atttypid = t.oid\n"
                    + "    LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid\n"
                    + "    LEFT JOIN pg_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid\n"
                    + "WHERE \n"
                    + "    n.nspname = '%s'\n"
                    + "    AND c.relname = '%s'\n"
                    + "    AND a.attnum > 0\n"
                    + "ORDER BY \n"
                    + "    a.attnum;";

    public PostgresCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where datname = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " where table_schema = '%s' and table_name= '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
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
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name");
        String fullTypeName = resultSet.getString("full_type_name");
        long columnLength = resultSet.getLong("column_length");
        int columnScale = resultSet.getInt("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getString("is_nullable").equals("YES");

        // dealingSpecialNumeric
        if (typeName.equals(PostgresTypeConverter.PG_NUMERIC) && columnLength < 1) {
            fullTypeName = "numeric(38,10)";
            columnLength = 38;
            columnScale = 10;
        }
        if (defaultValue != null && defaultValue.toString().contains("regclass")) {
            defaultValue = null;
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
        return PostgresTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected void createTableInternal(TablePath tablePath, CatalogTable table, boolean createIndex)
            throws CatalogException {
        PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                new PostgresCreateTableSqlBuilder(table, createIndex);
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try {
            String createTableSql = postgresCreateTableSqlBuilder.build(tablePath);
            executeInternal(dbUrl, createTableSql);

            if (postgresCreateTableSqlBuilder.isHaveConstraintKey) {
                String alterTableSql =
                        "ALTER TABLE "
                                + tablePath.getSchemaAndTableName("\"")
                                + " REPLICA IDENTITY FULL;";
                executeInternal(dbUrl, alterTableSql);
            }

            if (CollectionUtils.isNotEmpty(postgresCreateTableSqlBuilder.getCreateIndexSqls())) {
                for (String createIndexSql : postgresCreateTableSqlBuilder.getCreateIndexSqls()) {
                    executeInternal(dbUrl, createIndexSql);
                }
            }

        } catch (Exception ex) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), ex);
        }
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                new PostgresCreateTableSqlBuilder(table, createIndex);
        return postgresCreateTableSqlBuilder.build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return "DROP TABLE \""
                + tablePath.getSchemaName()
                + "\".\""
                + tablePath.getTableName()
                + "\"";
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return "CREATE DATABASE \"" + databaseName + "\"";
    }

    public String getExistDataSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return String.format("select * from \"%s\".\"%s\" limit 1", schemaName, tableName);
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return "TRUNCATE TABLE  \"" + schemaName + "\".\"" + tableName + "\"";
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return "DROP DATABASE \"" + databaseName + "\"";
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new PostgresTypeMapper());
    }
}
