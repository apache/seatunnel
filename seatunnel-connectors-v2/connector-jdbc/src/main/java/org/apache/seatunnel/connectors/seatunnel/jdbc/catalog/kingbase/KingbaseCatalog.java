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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.KingbaseTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.KingbaseTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_METHOD;

@Slf4j
public class KingbaseCatalog extends AbstractJdbcCatalog {

    protected static List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "INFORMATION_SCHEMA",
                            "SYSAUDIT",
                            "SYSLOGICAL",
                            "SYS_CATALOG",
                            "SYS_HM",
                            "XLOG_RECORD_READ"));

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            " SELECT \n"
                    + "    a.attname AS column_name,\n"
                    + "    CASE \n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) IN ('varchar', 'character varying') THEN 'VARCHAR'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) IN ('char', 'character') THEN 'CHAR'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) IN ('boolean', 'bool') THEN 'BOOL'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'real' THEN 'FLOAT4'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'double precision' THEN 'FLOAT8'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'integer' THEN 'INT4'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'bigint' THEN 'INT8'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'smallint' THEN 'INT2'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'time without time zone' THEN 'TIME'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'timestamp without time zone' THEN 'TIMESTAMP'\n"
                    + "        WHEN lower(format_type(a.atttypid, NULL)) = 'timestamp with time zone' THEN 'TIMESTAMPTZ'\n"
                    + "        ELSE format_type(a.atttypid, NULL)\n"
                    + "    END AS type_name,\n"
                    + "    format_type(a.atttypid, a.atttypmod) AS full_type_name,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypid IN (SELECT oid FROM sys_type WHERE typname IN ( 'CHAR','CHARACTER','VARCHAR','CHARACTER VARYING','BPCHAR') )\n"
                    + "        THEN ABS(a.atttypmod)     \n"
                    + "        WHEN a.atttypid IN (SELECT oid FROM sys_type WHERE typname IN ('NUMERIC', 'DECIMAL'))\n"
                    + "        THEN (a.atttypmod - 4) >> 16\n"
                    + "        WHEN a.atttypid IN (SELECT oid FROM sys_type WHERE typname IN ('INT', 'INTEGER', 'SMALLINT', 'BIGINT'))\n"
                    + "        THEN NULL\n"
                    + "        WHEN a.atttypid IN (SELECT oid FROM sys_type WHERE typname IN ('TIME','TIMESTAMPTZ', 'TIMESTAMP'))\n"
                    + "        THEN NULL\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_length,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypid IN (SELECT oid FROM sys_type WHERE typname IN ('NUMERIC', 'DECIMAL'))\n"
                    + "        THEN (a.atttypmod - 4) >> 16\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_precision,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypid IN (SELECT oid FROM sys_type WHERE typname IN ('NUMERIC', 'DECIMAL'))\n"
                    + "        THEN (a.atttypmod - 4) & 65535\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_scale,\n"
                    + "    d.description AS column_comment,\n"
                    + "    pg_get_expr(ad.adbin, ad.adrelid) AS default_value,\n"
                    + "    CASE \n"
                    + "        WHEN a.attnotnull = false THEN 'YES'\n"
                    + "        ELSE 'NO'\n"
                    + "    END AS is_nullable\n"
                    + "FROM \n"
                    + "    sys_class c\n"
                    + "    JOIN sys_namespace n ON c.relnamespace = n.oid\n"
                    + "    JOIN sys_attribute a ON a.attrelid = c.oid\n"
                    + "    LEFT JOIN sys_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum\n"
                    + "    LEFT JOIN sys_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum\n"
                    + "WHERE \n"
                    + "    n.nspname = '%s' \n"
                    + "    AND c.relname = '%s' \n"
                    + "    AND a.attnum > 0 \n"
                    + "    AND NOT a.attisdropped;";

    public KingbaseCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT current_database();";
    }

    /**
     * Override the databaseExists method because SELECT current_database() does not support WHERE
     */
    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (StringUtils.isBlank(databaseName)) {
            return false;
        }
        try {
            return querySQLResultExists(getUrlFromDatabaseName(databaseName), getListDatabaseSql());
        } catch (SeaTunnelRuntimeException e) {
            if (e.getSeaTunnelErrorCode().getCode().equals(UNSUPPORTED_METHOD.getCode())) {
                log.warn(
                        "The catalog: {} is not supported the getListDatabaseSql for databaseExists",
                        this.catalogName);
                return listDatabases().contains(databaseName);
            }
            throw e;
        } catch (SQLException e) {
            throw new CatalogException("查询数据库是否存在失败: " + databaseName, e);
        }
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new KingbaseCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT SCHEMANAME ,TABLENAME FROM SYS_TABLES";
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + "  where SCHEMANAME = '%s' and TABLENAME = '%s';",
                tablePath.getSchemaName(),
                tablePath.getTableName());
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
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        long columnPrecision = resultSet.getLong("COLUMN_PRECISION");
        int columnScale = resultSet.getInt("COLUMN_SCALE");
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
        return KingbaseTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new KingbaseTypeMapper());
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
                "select * from \"%s\".\"%s\" LIMIT 1",
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
