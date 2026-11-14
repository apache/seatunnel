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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AutoService(Factory.class)
@Slf4j
public class SqlServerIncrementalSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return SqlServerIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return SqlServerIncrementalSourceOptions.getBaseRule()
                .required(
                        SqlServerIncrementalSourceOptions.USERNAME,
                        SqlServerIncrementalSourceOptions.PASSWORD)
                .required(
                        SqlServerIncrementalSourceOptions.URL,
                        Conditions.notBlank(SqlServerIncrementalSourceOptions.URL))
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        SqlServerIncrementalSourceOptions.DATABASE_NAMES,
                        SqlServerIncrementalSourceOptions.SERVER_TIME_ZONE,
                        SqlServerIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        SqlServerIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        SqlServerIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        SqlServerIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        SqlServerIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        SqlServerIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        SqlServerIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        SqlServerIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        SqlServerIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                        SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                        SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE)
                .optional(
                        SqlServerIncrementalSourceOptions.STARTUP_MODE,
                        SqlServerIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        SqlServerIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        SqlServerIncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        SqlServerIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .conditional(
                        SqlServerIncrementalSourceOptions.STOP_MODE,
                        StopMode.TIMESTAMP,
                        SourceOptions.STOP_TIMESTAMP)
                .conditional(
                        SqlServerIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SqlServerIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } catch (Exception e) {
                log.warn(
                        "Failed to load JDBC driver {}",
                        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                        e);
            }
            ReadonlyConfig options = context.getOptions();
            List<String> databaseNames =
                    options.get(SqlServerIncrementalSourceOptions.DATABASE_NAMES);
            List<CatalogTable> catalogTables =
                    getCatalogTables(options, databaseNames, context.getClassLoader());
            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    options.getOptional(SqlServerIncrementalSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> toTablePath(text, databaseNames, true));
            }
            return new SqlServerIncrementalSource(options, catalogTables);
        };
    }

    private static List<CatalogTable> getCatalogTables(
            ReadonlyConfig options, List<String> databaseNames, ClassLoader classLoader) {
        List<String> tableNames = options.get(ConnectorCommonOptions.TABLE_NAMES);
        if (tableNames == null
                || tableNames.stream().allMatch(name -> name.split("\\.").length <= 3)) {
            return CatalogTableUtil.getCatalogTables(options, classLoader);
        }
        // Catalog#getTables parses table-names with TablePath.of, which splits a dotted
        // database name, so resolve the paths here and look them up in the catalog directly.
        List<TablePath> tablePaths =
                tableNames.stream()
                        .map(name -> toTablePath(name, databaseNames, false))
                        .collect(Collectors.toList());
        String factoryId = SqlServerIncrementalSource.IDENTIFIER.replace("-CDC", "");
        try (Catalog catalog =
                FactoryUtil.createOptionalCatalog(factoryId, options, classLoader, factoryId)
                        .orElseThrow(
                                () ->
                                        new SeaTunnelException(
                                                "Can not find catalog with factoryId "
                                                        + factoryId))) {
            catalog.open();
            List<CatalogTable> catalogTables =
                    catalog.buildCatalogTablesWithErrorCheck(
                            tablePaths.stream().filter(catalog::tableExists).iterator(), options);
            if (catalogTables.isEmpty()) {
                throw new SeaTunnelException("Can not find catalog tables " + tablePaths);
            }
            return catalogTables;
        }
    }

    /**
     * Parses a SQL Server table name. A name with more than three parts is read as {@code
     * database.schema.table} whose database name contains '.', and that database must be listed in
     * {@code database-names}.
     */
    static TablePath toTablePath(
            String tableName, List<String> databaseNames, boolean schemaFirst) {
        if (tableName.split("\\.").length <= 3) {
            return TablePath.of(tableName, schemaFirst);
        }
        int tableDot = tableName.lastIndexOf('.');
        int schemaDot = tableName.lastIndexOf('.', tableDot - 1);
        String databaseName = tableName.substring(0, schemaDot);
        if (databaseNames == null || !databaseNames.contains(databaseName)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot split '%s' into database, schema and table: a database name"
                                    + " containing '.' must be listed in database-names, and"
                                    + " schema and table names must not contain '.'",
                            tableName));
        }
        return TablePath.of(
                databaseName,
                tableName.substring(schemaDot + 1, tableDot),
                tableName.substring(tableDot + 1));
    }
}
