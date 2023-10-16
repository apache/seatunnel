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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogFactorySelector;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class JdbcCatalogUtils {
    private static final String DEFAULT_CATALOG = "default";
    private static final TablePath DEFAULT_TABLE_PATH =
            TablePath.of("default", "default", "default");
    private static final TableIdentifier DEFAULT_TABLE_IDENTIFIER =
            TableIdentifier.of(
                    DEFAULT_CATALOG,
                    DEFAULT_TABLE_PATH.getDatabaseName(),
                    DEFAULT_TABLE_PATH.getSchemaName(),
                    DEFAULT_TABLE_PATH.getTableName());

    public static Map<TablePath, JdbcSourceTable> getTables(
            JdbcConnectionConfig jdbcConnectionConfig, List<JdbcSourceTableConfig> tablesConfig)
            throws SQLException {
        Map<TablePath, JdbcSourceTable> tables = new LinkedHashMap<>();

        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcConnectionConfig.getUrl(), jdbcConnectionConfig.getCompatibleMode());
        Optional<Catalog> catalog = findCatalog(jdbcConnectionConfig);
        if (catalog.isPresent()) {
            try (AbstractJdbcCatalog jdbcCatalog = (AbstractJdbcCatalog) catalog.get()) {
                log.info("Loading catalog tables for catalog : {}", jdbcCatalog.getClass());

                jdbcCatalog.open();
                for (JdbcSourceTableConfig tableConfig : tablesConfig) {
                    CatalogTable catalogTable =
                            getCatalogTable(tableConfig, jdbcCatalog, jdbcDialect);
                    TablePath tablePath = catalogTable.getTableId().toTablePath();
                    JdbcSourceTable jdbcSourceTable =
                            JdbcSourceTable.builder()
                                    .tablePath(tablePath)
                                    .query(tableConfig.getQuery())
                                    .partitionColumn(tableConfig.getPartitionColumn())
                                    .partitionNumber(tableConfig.getPartitionNumber())
                                    .partitionStart(tableConfig.getPartitionStart())
                                    .partitionEnd(tableConfig.getPartitionEnd())
                                    .catalogTable(catalogTable)
                                    .build();
                    tables.put(tablePath, jdbcSourceTable);
                    log.info("Loaded catalog table : {}", tablePath);
                }
                log.info(
                        "Loaded {} catalog tables for catalog : {}",
                        tables.size(),
                        jdbcCatalog.getClass());
            }
            return tables;
        }

        log.warn(
                "Catalog not found, loading tables from jdbc directly. url : {}",
                jdbcConnectionConfig.getUrl());
        try (Connection connection = getConnection(jdbcConnectionConfig)) {
            log.info("Loading catalog tables for jdbc : {}", jdbcConnectionConfig.getUrl());
            for (JdbcSourceTableConfig tableConfig : tablesConfig) {
                CatalogTable catalogTable = getCatalogTable(tableConfig, connection, jdbcDialect);
                TablePath tablePath = catalogTable.getTableId().toTablePath();
                JdbcSourceTable jdbcSourceTable =
                        JdbcSourceTable.builder()
                                .tablePath(tablePath)
                                .query(tableConfig.getQuery())
                                .partitionColumn(tableConfig.getPartitionColumn())
                                .partitionNumber(tableConfig.getPartitionNumber())
                                .partitionStart(tableConfig.getPartitionStart())
                                .partitionEnd(tableConfig.getPartitionEnd())
                                .catalogTable(catalogTable)
                                .build();

                tables.put(tablePath, jdbcSourceTable);
                log.info("Loaded catalog table : {}", tablePath);
            }
            log.info(
                    "Loaded {} catalog tables for jdbc : {}",
                    tables.size(),
                    jdbcConnectionConfig.getUrl());
            return tables;
        }
    }

    private static CatalogTable getCatalogTable(
            JdbcSourceTableConfig tableConfig,
            AbstractJdbcCatalog jdbcCatalog,
            JdbcDialect jdbcDialect)
            throws SQLException {
        if (Strings.isNullOrEmpty(tableConfig.getTablePath())
                && Strings.isNullOrEmpty(tableConfig.getQuery())) {
            throw new IllegalArgumentException(
                    "Either table path or query must be specified in source configuration.");
        }

        if (StringUtils.isNotEmpty(tableConfig.getTablePath())
                && StringUtils.isNotEmpty(tableConfig.getQuery())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            CatalogTable tableOfPath = null;
            try {
                tableOfPath = jdbcCatalog.getTable(tablePath);
            } catch (Exception e) {
                // ignore
                log.debug("User-defined table path: {}", tablePath);
            }
            CatalogTable tableOfQuery = jdbcCatalog.getTable(tableConfig.getQuery());
            if (tableOfPath == null) {
                TableIdentifier tableIdentifier;
                if (tableOfQuery.getTableId() != null) {
                    tableIdentifier =
                            convert(tableOfQuery.getTableId().getCatalogName(), tablePath);
                } else {
                    tableIdentifier = convert(tablePath);
                }
                return CatalogTable.of(tableIdentifier, tableOfQuery);
            }
            return mergeCatalogTable(tableOfPath, tableOfQuery);
        }
        if (StringUtils.isNotEmpty(tableConfig.getTablePath())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            return jdbcCatalog.getTable(tablePath);
        }

        CatalogTable catalogTable = jdbcCatalog.getTable(tableConfig.getQuery());
        return CatalogTable.of(DEFAULT_TABLE_IDENTIFIER, catalogTable);
    }

    private static CatalogTable mergeCatalogTable(
            CatalogTable tableOfPath, CatalogTable tableOfQuery) {
        TableSchema tableSchemaOfPath = tableOfPath.getTableSchema();
        Map<String, Column> columnsOfPath =
                tableSchemaOfPath.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        Set<String> columnKeysOfPath = columnsOfPath.keySet();
        TableSchema tableSchemaOfQuery = tableOfQuery.getTableSchema();
        Map<String, Column> columnsOfQuery =
                tableSchemaOfQuery.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        Set<String> columnKeysOfQuery = columnsOfQuery.keySet();

        if (columnKeysOfPath.equals(columnKeysOfQuery)) {
            boolean schemaEquals =
                    columnKeysOfPath.stream()
                            .allMatch(
                                    key ->
                                            columnsOfPath
                                                    .get(key)
                                                    .getDataType()
                                                    .equals(columnsOfQuery.get(key).getDataType()));
            if (schemaEquals) {
                return CatalogTable.of(
                        tableOfPath.getTableId(),
                        TableSchema.builder()
                                .primaryKey(tableSchemaOfPath.getPrimaryKey())
                                .constraintKey(tableSchemaOfPath.getConstraintKeys())
                                .columns(tableSchemaOfQuery.getColumns())
                                .build(),
                        tableOfPath.getOptions(),
                        tableOfPath.getPartitionKeys(),
                        tableOfPath.getComment(),
                        tableOfPath.getCatalogName());
            }
        }

        PrimaryKey primaryKeyOfPath = tableSchemaOfPath.getPrimaryKey();
        List<ConstraintKey> constraintKeysOfPath = tableSchemaOfPath.getConstraintKeys();
        List<String> partitionKeysOfPath = tableOfPath.getPartitionKeys();
        PrimaryKey primaryKeyOfQuery = null;
        List<ConstraintKey> constraintKeysOfQuery = new ArrayList<>();
        List<String> partitionKeysOfQuery = new ArrayList<>();

        if (primaryKeyOfPath != null
                && columnKeysOfQuery.containsAll(primaryKeyOfPath.getColumnNames())) {
            primaryKeyOfQuery = primaryKeyOfPath;
        }
        if (constraintKeysOfPath != null) {
            for (ConstraintKey constraintKey : constraintKeysOfPath) {
                Set<String> constraintKeyFields =
                        constraintKey.getColumnNames().stream()
                                .map(e -> e.getColumnName())
                                .collect(Collectors.toSet());
                if (columnKeysOfQuery.containsAll(constraintKeyFields)) {
                    constraintKeysOfQuery.add(constraintKey);
                }
            }
        }
        if (partitionKeysOfPath != null && columnKeysOfQuery.containsAll(partitionKeysOfPath)) {
            partitionKeysOfQuery = partitionKeysOfPath;
        }

        CatalogTable mergedCatalogTable =
                CatalogTable.of(
                        tableOfPath.getTableId(),
                        TableSchema.builder()
                                .primaryKey(primaryKeyOfQuery)
                                .constraintKey(constraintKeysOfQuery)
                                .columns(tableSchemaOfQuery.getColumns())
                                .build(),
                        tableOfQuery.getOptions(),
                        partitionKeysOfQuery,
                        tableOfQuery.getComment(),
                        tableOfQuery.getCatalogName());

        log.info("Merged catalog table of path {}", tableOfPath.getTableId().toTablePath());
        return mergedCatalogTable;
    }

    private static CatalogTable getCatalogTable(
            JdbcSourceTableConfig tableConfig, Connection connection, JdbcDialect jdbcDialect)
            throws SQLException {
        if (Strings.isNullOrEmpty(tableConfig.getTablePath())
                && Strings.isNullOrEmpty(tableConfig.getQuery())) {
            throw new IllegalArgumentException(
                    "Either table path or query must be specified in source configuration.");
        }

        if (StringUtils.isNotEmpty(tableConfig.getTablePath())
                && StringUtils.isNotEmpty(tableConfig.getQuery())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            CatalogTable tableOfPath = null;
            try {
                tableOfPath = CatalogUtils.getCatalogTable(connection, tablePath);
            } catch (Exception e) {
                // ignore
                log.debug("User-defined table path: {}", tablePath);
            }
            CatalogTable tableOfQuery =
                    getCatalogTable(connection, tableConfig.getQuery(), jdbcDialect);
            if (tableOfPath == null) {
                TableIdentifier tableIdentifier;
                if (tableOfQuery.getTableId() != null) {
                    tableIdentifier =
                            convert(tableOfQuery.getTableId().getCatalogName(), tablePath);
                } else {
                    tableIdentifier = convert(tablePath);
                }
                return CatalogTable.of(tableIdentifier, tableOfQuery);
            }
            return mergeCatalogTable(tableOfPath, tableOfQuery);
        }
        if (StringUtils.isNotEmpty(tableConfig.getTablePath())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            return CatalogUtils.getCatalogTable(connection, tablePath);
        }

        CatalogTable catalogTable =
                getCatalogTable(connection, tableConfig.getQuery(), jdbcDialect);
        return CatalogTable.of(DEFAULT_TABLE_IDENTIFIER, catalogTable);
    }

    private static CatalogTable getCatalogTable(
            Connection connection, String sqlQuery, JdbcDialect jdbcDialect) throws SQLException {
        ResultSetMetaData resultSetMetaData =
                jdbcDialect.getResultSetMetaData(connection, sqlQuery);
        return CatalogUtils.getCatalogTable(resultSetMetaData);
    }

    private static Optional<Catalog> findCatalog(JdbcConnectionConfig config) {
        Optional<CatalogFactory> catalogFactory = CatalogFactorySelector.select(config.getUrl());
        if (catalogFactory.isPresent()) {
            ReadonlyConfig catalogConfig = extractCatalogConfig(config);
            Catalog catalog = catalogFactory.get().createCatalog(DEFAULT_CATALOG, catalogConfig);
            return Optional.of(catalog);
        }

        log.debug("No catalog  found for jdbc url: {}", config.getUrl());
        return Optional.empty();
    }

    private static ReadonlyConfig extractCatalogConfig(JdbcConnectionConfig config) {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put(JdbcCatalogOptions.BASE_URL.key(), config.getUrl());
        config.getUsername()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.USERNAME.key(), val));
        config.getPassword()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.PASSWORD.key(), val));
        return ReadonlyConfig.fromMap(catalogConfig);
    }

    private static TableIdentifier convert(TablePath tablePath) {
        return convert(DEFAULT_CATALOG, tablePath);
    }

    private static TableIdentifier convert(String catalogName, TablePath tablePath) {
        return TableIdentifier.of(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    private static Connection getConnection(JdbcConnectionConfig config) throws SQLException {
        if (config.getUsername().isPresent() && config.getPassword().isPresent()) {
            return DriverManager.getConnection(
                    config.getUrl(), config.getUsername().get(), config.getPassword().get());
        }
        return DriverManager.getConnection(config.getUrl());
    }
}
