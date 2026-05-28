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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class OracleIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return OracleIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OracleIncrementalSourceOptions.getBaseRule()
                .required(
                        OracleIncrementalSourceOptions.USERNAME,
                        OracleIncrementalSourceOptions.PASSWORD)
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .bundled(
                        OracleIncrementalSourceOptions.HOSTNAME,
                        OracleIncrementalSourceOptions.PORT)
                .optional(
                        OracleIncrementalSourceOptions.URL,
                        OracleIncrementalSourceOptions.DATABASE_NAMES,
                        OracleIncrementalSourceOptions.SCHEMA_NAMES,
                        OracleIncrementalSourceOptions.USE_SELECT_COUNT,
                        OracleIncrementalSourceOptions.SKIP_ANALYZE,
                        OracleIncrementalSourceOptions.SERVER_TIME_ZONE,
                        OracleIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        OracleIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        OracleIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        OracleIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        OracleIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        OracleIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        OracleIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        OracleIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        OracleIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        OracleIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED)
                .optional(
                        OracleIncrementalSourceOptions.STARTUP_MODE,
                        OracleIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        OracleIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        OracleIncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        OracleIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .conditional(
                        OracleIncrementalSourceOptions.STOP_MODE,
                        StopMode.TIMESTAMP,
                        SourceOptions.STOP_TIMESTAMP)
                .conditional(
                        OracleIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OracleIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName("oracle.jdbc.OracleDriver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver {}", "oracle.jdbc.OracleDriver", e);
            }
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            boolean enableSchemaChange =
                    context.getOptions()
                            .getOptional(SourceOptions.SCHEMA_CHANGES_ENABLED)
                            .orElse(
                                    // TODO remove this after all users used the new schema change
                                    // option
                                    context.getOptions()
                                            .getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                                            .map(
                                                    e ->
                                                            e.getOrDefault(
                                                                    OracleSourceConfigFactory
                                                                            .SCHEMA_CHANGE_KEY,
                                                                    SourceOptions
                                                                            .SCHEMA_CHANGES_ENABLED
                                                                            .defaultValue()
                                                                            .toString()))
                                            .map(Boolean::parseBoolean)
                                            .orElse(
                                                    SourceOptions.SCHEMA_CHANGES_ENABLED
                                                            .defaultValue()));
            if (!restoreTables.isEmpty() && enableSchemaChange) {
                catalogTables = mergeTableStruct(catalogTables, restoreTables);
            }

            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    context.getOptions()
                            .getOptional(OracleIncrementalSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables, tableConfigs.get(), s -> TablePath.of(s, true));
            }
            return new OracleIncrementalSource(context.getOptions(), catalogTables);
        };
    }
}
