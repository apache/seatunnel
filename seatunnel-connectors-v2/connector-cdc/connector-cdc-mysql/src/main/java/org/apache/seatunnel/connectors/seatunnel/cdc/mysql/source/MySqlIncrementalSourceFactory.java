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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

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
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class MySqlIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return MySqlIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        MySqlIncrementalSourceOptions.USERNAME,
                        MySqlIncrementalSourceOptions.PASSWORD,
                        MySqlIncrementalSourceOptions.URL)
                .exclusive(
                        MySqlIncrementalSourceOptions.TABLE_NAMES,
                        MySqlIncrementalSourceOptions.TABLE_PATTERN)
                .optional(
                        MySqlIncrementalSourceOptions.DATABASE_NAMES,
                        MySqlIncrementalSourceOptions.SERVER_ID,
                        MySqlIncrementalSourceOptions.SERVER_TIME_ZONE,
                        MySqlIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        MySqlIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        MySqlIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        MySqlIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        MySqlIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        MySqlIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        MySqlIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        MySqlIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                        MySqlIncrementalSourceOptions.INT_TYPE_NARROWING)
                .optional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        MySqlIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .conditional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        MySqlIncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MySqlIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
            }
            ReadonlyConfig config = context.getOptions();
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(config, context.getClassLoader());
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
                                                                    MySqlSourceConfigFactory
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
                    new MySqlIncrementalSource<>(config, catalogTables);
        };
    }
}
