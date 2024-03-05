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
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class PostgresCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT \n"
                    + "    a.attname AS column_name, \n"
                    + "    isc.udt_name as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN isc.udt_name = 'varchar' OR isc.udt_name = 'bit' OR isc.udt_name = 'bit varying' THEN isc.udt_name || '(' || (isc.character_maximum_length) || ')'\n"
                    + "        WHEN isc.udt_name = 'bpchar' THEN 'char' || '(' || (isc.character_maximum_length) || ')'\n"
                    + "        WHEN isc.udt_name = 'numeric' OR isc.udt_name = 'decimal' THEN isc.udt_name || '(' || (isc.numeric_precision) || ', ' || (isc.numeric_scale) || ')'\n"
                    + "        ELSE isc.udt_name\n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN isc.udt_name IN ('varchar', 'bpchar', 'bit', 'bit varying','text','json','') THEN isc.character_maximum_length\n"
                    + "        WHEN isc.udt_name IN ('time','timetz','timestampt','timestamptz','date') THEN isc.datetime_precision\n"
                    + "        ELSE isc.numeric_precision\n"
                    + "    END AS column_length,\n"
                    + "    isc.numeric_scale AS column_scale,\n"
                    + "    d.description AS column_comment,\n"
                    + "    pg_get_expr(ad.adbin, ad.adrelid) AS default_value,\n"
                    + "    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable\n"
                    + "FROM \n"
                    + "    information_schema.columns isc \n"
                    + "    LEFT JOIN pg_namespace n ON n.nspname = isc.table_schema\n"
                    + "    LEFT JOIN pg_class c ON isc.table_name = c.relname AND n.oid= c.relnamespace\n"
                    + "    LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = isc.column_name\n"
                    + "    LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid\n"
                    + "    LEFT JOIN pg_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid\n"
                    + "WHERE \n"
                    + "    isc.table_schema = '%s'\n"
                    + "    AND c.relname = '%s'\n"
                    + "    AND a.attnum > 0\n"
                    + "ORDER BY \n"
                    + "    a.attnum;";

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("pg_catalog");
        SYS_DATABASES.add("root");
        SYS_DATABASES.add("pg_toast");
        SYS_DATABASES.add("pg_temp_1");
        SYS_DATABASES.add("pg_toast_temp_1");
        SYS_DATABASES.add("postgres");
        SYS_DATABASES.add("template0");
        SYS_DATABASES.add("template1");
    }

    public PostgresCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "select datname from pg_database;";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT table_schema, table_name FROM information_schema.tables;";
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
    protected void createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                new PostgresCreateTableSqlBuilder(table);
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
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new PostgresTypeMapper());
    }
}
