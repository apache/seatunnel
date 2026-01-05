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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeConverter;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Catalog implementation for DuckDB.
 *
 * <p>Note: DuckDB is an embedded database with a single-connection-per-database constraint in the
 * JVM. This catalog manages and owns the JDBC connection, which may be exposed to subclasses or
 * tests for controlled reuse.
 */
@Slf4j
public class DuckDBCatalog extends AbstractJdbcCatalog {

    private final DuckDBTypeConverter typeConverter;
    private static final String DEFAULT_DATABASE_NAME = "default";
    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    c.column_name AS column_name,\n"
                    + "    c.data_type   AS type_name,\n"
                    + "    CASE\n"
                    + "        WHEN c.character_maximum_length IS NOT NULL THEN\n"
                    + "            c.data_type || '(' || c.character_maximum_length || ')'\n"
                    + "        WHEN c.data_type ILIKE 'DECIMAL%%' \n"
                    + "          OR c.data_type ILIKE 'NUMERIC%%' THEN\n"
                    + "            c.data_type\n"
                    + "        WHEN c.datetime_precision IS NOT NULL THEN\n"
                    + "            c.data_type || '(' || c.datetime_precision || ')'\n"
                    + "        ELSE\n"
                    + "            c.data_type\n"
                    + "    END AS full_type_name,\n"
                    + "    c.character_maximum_length AS column_length,\n"
                    + "    c.numeric_scale            AS column_scale,\n"
                    + "    dc.comment                 AS column_comment,\n"
                    + "    c.column_default           AS default_value,\n"
                    + "    c.is_nullable              AS is_nullable\n"
                    + "FROM information_schema.columns c\n"
                    + "LEFT JOIN duckdb_columns dc\n"
                    + "       ON dc.schema_name = c.table_schema\n"
                    + "      AND dc.table_name  = c.table_name\n"
                    + "      AND dc.column_name = c.column_name\n"
                    + "WHERE c.table_schema = '%s'\n"
                    + "  AND c.table_name   = '%s'\n"
                    + "ORDER BY c.ordinal_position;\n";

    public DuckDBCatalog(String catalogName, JdbcUrlUtil.UrlInfo urlInfo, String defaultSchema) {
        super(catalogName, "duckdb", "", urlInfo, defaultSchema, "org.duckdb.DuckDBDriver");
        this.typeConverter = new DuckDBTypeConverter();
    }

    @Override
    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        Properties info = getConnectionProperties();
        if (driverClass != null) {
            log.info("try to find driver {}", driverClass);
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            try {
                // Driver Manager may load the wrong driver, prioritize finding the driver by class
                // name
                while (drivers.hasMoreElements()) {
                    Driver driver = drivers.nextElement();
                    if (StringUtils.equals(driver.getClass().getName(), driverClass)) {
                        try {
                            Connection connection = driver.connect(url, info);
                            connectionMap.put(url, connection);
                            return connection;
                        } catch (Exception e) {
                            log.info("try connector failed", e);
                        }
                    }
                }
            } catch (Exception e) {
                log.info("find driver error, back to DriverManager.getConnection", e);
            }
        }
        try {
            Connection connection = DriverManager.getConnection(url, info);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public List<CatalogTable> getTables(ReadonlyConfig config) throws CatalogException {
        // Get the list of specified tables
        List<String> tableNames = config.get(ConnectorCommonOptions.TABLE_NAMES);
        if (tableNames != null && !tableNames.isEmpty()) {
            Iterator<TablePath> tablePaths =
                    tableNames.stream().map(TablePath::of).filter(this::tableExists).iterator();
            return buildCatalogTablesWithErrorCheck(tablePaths);
        }
        // Get the list of table pattern
        String tablePatternStr = config.get(ConnectorCommonOptions.TABLE_PATTERN);
        if (StringUtils.isBlank(tablePatternStr)) {
            return Collections.emptyList();
        }
        Pattern tablePattern = Pattern.compile(tablePatternStr);
        List<TablePath> tablePaths = new ArrayList<>();
        final List<String> strings = listTables(DEFAULT_DATABASE_NAME);
        for (String tableName : strings) {
            if (StringUtils.isBlank(tableName)) {
                continue;
            }
            TablePath tablePath = TablePath.of(DEFAULT_DATABASE_NAME + "." + tableName);
            if (tablePattern.matcher(tablePath.getSchemaAndTableName()).matches()) {
                tablePaths.add(tablePath);
            }
        }
        return buildCatalogTablesWithErrorCheck(tablePaths.iterator());
    }

    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        // 1. Read column metadata from DuckDB system views
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name");
        String fullTypeName = resultSet.getString("full_type_name");
        long columnLength = resultSet.getLong("column_length");
        int columnScale = resultSet.getInt("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = "YES".equalsIgnoreCase(resultSet.getString("is_nullable"));
        // 2. Normalize DECIMAL / NUMERIC definitions for DuckDB
        // DuckDB allows DECIMAL/NUMERIC types without explicit precision/scale.
        // For schema introspection, we must provide a deterministic definition.
        // DuckDB supports up to DECIMAL(38, scale).
        if (isDuckDBDecimal(typeName)) {
            typeName = DuckDBTypeConverter.DUCKDB_DECIMAL;
            if (columnLength <= 0) {
                // DuckDB maximum supported precision
                columnLength = 38;
            }
            if (columnScale < 0) {
                columnScale = 0;
            }
            // Rebuild full type name if precision/scale is not explicitly defined
            if (fullTypeName == null || !fullTypeName.contains("(")) {
                fullTypeName = String.format("%s(%d,%d)", typeName, columnLength, columnScale);
            }
        }
        // 3. Sanitize default values
        // Unlike PostgreSQL, DuckDB does not use regclass or system OIDs.
        // Default values may be expressions (e.g. CURRENT_TIMESTAMP).
        // Empty defaults are treated as null.
        if (defaultValue instanceof String) {
            String dv = ((String) defaultValue).trim();
            if (dv.isEmpty()) {
                defaultValue = null;
            }
        }
        // 4. Build a unified type definition used by the catalog abstraction
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
        // 5. Convert to internal Column representation using DuckDB semantics
        return DuckDBTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    public String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "SELECT table_schema, table_name FROM information_schema.tables "
                        + "WHERE table_schema = '%s' AND table_name = '%s'",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return DuckDBCreateTableSqlBuilder.builder(tablePath, table, typeConverter, createIndex)
                .build(tablePath);
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT table_schema, table_name FROM information_schema.tables";
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    private boolean isDuckDBDecimal(String typeName) {
        return typeName.toUpperCase().startsWith(DuckDBTypeConverter.DUCKDB_DECIMAL);
    }
}
