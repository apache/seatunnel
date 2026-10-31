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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
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
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.TableDiscoveryUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;

@AutoService(Factory.class)
@Slf4j
public class Db2IncrementalSourceFactory implements TableSourceFactory {

    private static final String DRIVER_CLASS_NAME = "com.ibm.db2.jcc.DB2Driver";

    @Override
    public String factoryIdentifier() {
        return Db2IncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return Db2IncrementalSourceOptions.getBaseRule()
                .required(
                        Db2IncrementalSourceOptions.USERNAME, Db2IncrementalSourceOptions.PASSWORD)
                .required(
                        Db2IncrementalSourceOptions.URL, notBlank(Db2IncrementalSourceOptions.URL))
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        Db2IncrementalSourceOptions.DATABASE_NAMES,
                        Db2IncrementalSourceOptions.SERVER_TIME_ZONE,
                        Db2IncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        Db2IncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        Db2IncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        Db2IncrementalSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        Db2IncrementalSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        Db2IncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        Db2IncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG)
                .optional(
                        Db2IncrementalSourceOptions.STARTUP_MODE,
                        Db2IncrementalSourceOptions.STOP_MODE)
                .conditional(
                        Db2IncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        Db2IncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        Db2IncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .conditional(
                        Db2IncrementalSourceOptions.STOP_MODE,
                        StopMode.TIMESTAMP,
                        SourceOptions.STOP_TIMESTAMP)
                .conditional(
                        Db2IncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return Db2IncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName(DRIVER_CLASS_NAME);
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver {}", DRIVER_CLASS_NAME, e);
            }
            ReadonlyConfig options =
                    resolveDb2Options(context.getOptions(), context.getClassLoader());
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(options, context.getClassLoader());
            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    options.getOptional(Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> TablePath.of(text, true));
            }
            return new Db2IncrementalSource(options, catalogTables);
        };
    }

    /**
     * Resolves configured table names against the DB2 catalog before they are used anywhere.
     *
     * <p>The catalog is opened only when folding would change at least one unquoted name. That
     * keeps uppercase and quoted configurations on the previous single-connection path, while
     * lowercase unquoted names get one additional short-lived connection to decide between the
     * folded spelling and the configured spelling. Without a DB2 catalog factory the names are
     * folded unconditionally, which is the behavior before this fallback existed.
     */
    static ReadonlyConfig resolveDb2Options(ReadonlyConfig options, ClassLoader classLoader) {
        boolean foldingChangesAnyName =
                Stream.concat(
                                options.getOptional(ConnectorCommonOptions.TABLE_NAMES)
                                        .map(List::stream)
                                        .orElseGet(Stream::empty),
                                options.getOptional(Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG)
                                        .map(
                                                tables ->
                                                        tables.stream()
                                                                .map(
                                                                        JdbcSourceTableConfig
                                                                                ::getTable))
                                        .orElseGet(Stream::empty))
                        .anyMatch(
                                name ->
                                        !TableDiscoveryUtils.normalizeConfiguredTableName(name)
                                                .equals(name));
        if (!foldingChangesAnyName) {
            return normalizeDb2Options(options, tablePath -> true);
        }
        Optional<Catalog> optionalCatalog =
                FactoryUtil.createOptionalCatalog(
                        DatabaseIdentifier.DB_2, options, classLoader, DatabaseIdentifier.DB_2);
        if (!optionalCatalog.isPresent()) {
            return normalizeDb2Options(options, tablePath -> true);
        }
        try (Catalog catalog = optionalCatalog.get()) {
            catalog.open();
            return normalizeDb2Options(options, catalog::tableExists);
        }
    }

    /**
     * Keeps catalog lookup, per-table overrides and capture filters in the same identifier space.
     * Each distinct configured name is resolved once through {@code tableExists}; see {@link
     * TableDiscoveryUtils#resolveConfiguredTableName(String, Predicate)} for the precedence. Table
     * patterns are regular expressions and must not be case-folded here.
     */
    static ReadonlyConfig normalizeDb2Options(
            ReadonlyConfig options, Predicate<TablePath> tableExists) {
        Map<String, String> resolvedNames = new HashMap<>();
        Function<String, String> resolver =
                name ->
                        resolvedNames.computeIfAbsent(
                                name,
                                configuredTable ->
                                        TableDiscoveryUtils.resolveConfiguredTableName(
                                                configuredTable, tableExists));
        Map<String, Object> normalized = new LinkedHashMap<>(options.getSourceMap());
        options.getOptional(ConnectorCommonOptions.TABLE_NAMES)
                .ifPresent(
                        names ->
                                normalized.put(
                                        ConnectorCommonOptions.TABLE_NAMES.key(),
                                        names.stream().map(resolver).collect(Collectors.toList())));
        options.getOptional(Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG)
                .ifPresent(
                        tables ->
                                normalized.put(
                                        Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG.key(),
                                        tables.stream()
                                                .map(table -> normalizeTableConfig(table, resolver))
                                                .collect(Collectors.toList())));
        return ReadonlyConfig.fromMap(normalized);
    }

    /**
     * Preserves user-configured primary keys and split-column override while resolving the table
     * name with the same resolver as {@code table-names}.
     */
    private static Map<String, Object> normalizeTableConfig(
            JdbcSourceTableConfig config, Function<String, String> resolver) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        normalized.put("table", resolver.apply(config.getTable()));
        if (config.getPrimaryKeys() != null) {
            normalized.put("primaryKeys", new ArrayList<>(config.getPrimaryKeys()));
        }
        if (config.getSnapshotSplitColumn() != null) {
            normalized.put("snapshotSplitColumn", config.getSnapshotSplitColumn());
        }
        return normalized;
    }
}
