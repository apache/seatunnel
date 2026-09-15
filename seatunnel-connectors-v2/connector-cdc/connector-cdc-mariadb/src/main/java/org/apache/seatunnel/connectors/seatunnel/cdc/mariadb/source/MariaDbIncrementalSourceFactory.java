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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbSourceConfigFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class MariaDbIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return MariaDbIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        MariaDbIncrementalSourceOptions.USERNAME,
                        MariaDbIncrementalSourceOptions.PASSWORD,
                        MariaDbIncrementalSourceOptions.URL)
                .exclusive(
                        MariaDbIncrementalSourceOptions.TABLE_NAMES,
                        MariaDbIncrementalSourceOptions.TABLE_PATTERN)
                .optional(
                        MariaDbIncrementalSourceOptions.DATABASE_NAMES,
                        MariaDbIncrementalSourceOptions.SERVER_ID,
                        MariaDbIncrementalSourceOptions.SERVER_TIME_ZONE,
                        MariaDbIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        MariaDbIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        MariaDbIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        MariaDbIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        MariaDbIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        MariaDbIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        MariaDbIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        MariaDbIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        MariaDbIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        MariaDbIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                        MariaDbIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                        MariaDbIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE,
                        MariaDbIncrementalSourceOptions.INT_TYPE_NARROWING,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS,
                        MariaDbIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET,
                        MariaDbIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS,
                        MariaDbIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                .optional(
                        MariaDbIncrementalSourceOptions.STARTUP_MODE,
                        MariaDbIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        MariaDbIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .conditional(
                        MariaDbIncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        MariaDbIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MariaDbIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            try {
                Class.forName("org.mariadb.jdbc.Driver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver org.mariadb.jdbc.Driver ", e);
            }
            ReadonlyConfig config = context.getOptions();
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(config, context.getClassLoader());
            boolean enableSchemaChange =
                    context.getOptions()
                            .getOptional(SourceOptions.SCHEMA_CHANGES_ENABLED)
                            .orElse(
                                    context.getOptions()
                                            .getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                                            .map(
                                                    e ->
                                                            e.getOrDefault(
                                                                    MariaDbSourceConfigFactory
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
                    context.getOptions().getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> TablePath.of(text, false));
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new MariaDbIncrementalSource<>(config, catalogTables);
        };
    }
}
