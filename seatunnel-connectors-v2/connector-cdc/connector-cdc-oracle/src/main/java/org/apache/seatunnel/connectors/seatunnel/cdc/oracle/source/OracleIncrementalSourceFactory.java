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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
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
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class OracleIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {

    static class EndpointValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "either 'url' or 'hostname'+'port' must be provided as the connection endpoint";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String value) {
            boolean hasUrl = config.getOptional(OracleIncrementalSourceOptions.URL).isPresent();
            boolean hasHostname =
                    config.getOptional(OracleIncrementalSourceOptions.HOSTNAME).isPresent();
            return hasUrl || hasHostname;
        }
    }

    static class SchemaChangeLogMiningValidator implements ConditionExtension<Boolean> {
        @Override
        public String description() {
            return "when schema changes are enabled, debezium log mining strategy cannot be online_catalog";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Boolean value) {
            if (value == null || !value) {
                return true;
            }
            Map<String, String> dbzProps = config.get(SourceOptions.DEBEZIUM_PROPERTIES);
            if (dbzProps == null) {
                return true;
            }
            String strategy = dbzProps.get(OracleSourceConfigFactory.LOG_MINING_STRATEGY_KEY);
            return !OracleSourceConfigFactory.LOG_MINING_STRATEGY_DEFAULT.equals(strategy);
        }
    }

    @Override
    public String factoryIdentifier() {
        return OracleIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OracleIncrementalSourceOptions.getBaseRule()
                .required(
                        OracleIncrementalSourceOptions.USERNAME,
                        Conditions.extension(
                                OracleIncrementalSourceOptions.USERNAME, new EndpointValidator()))
                .required(OracleIncrementalSourceOptions.PASSWORD)
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        ConnectorCommonOptions.TABLE_NAMES,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_NAMES)
                                .and(
                                        Conditions.extension(
                                                ConnectorCommonOptions.TABLE_NAMES,
                                                new SourceOptions.QualifiedTableNameValidator())))
                .bundled(
                        OracleIncrementalSourceOptions.HOSTNAME,
                        OracleIncrementalSourceOptions.PORT)
                .required(
                        OracleIncrementalSourceOptions.DATABASE_NAMES,
                        Conditions.notEmpty(OracleIncrementalSourceOptions.DATABASE_NAMES))
                .optional(
                        OracleIncrementalSourceOptions.URL,
                        OracleIncrementalSourceOptions.SCHEMA_NAMES,
                        OracleIncrementalSourceOptions.USE_SELECT_COUNT,
                        OracleIncrementalSourceOptions.SKIP_ANALYZE,
                        OracleIncrementalSourceOptions.SERVER_TIME_ZONE,
                        OracleIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        OracleIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        OracleIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        OracleIncrementalSourceOptions.TABLE_NAMES_CONFIG)
                .optional(
                        OracleIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                        Conditions.extension(
                                OracleIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                                new SchemaChangeLogMiningValidator()))
                .optional(
                        OracleIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                        OracleIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE)
                .optional(
                        OracleIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        Conditions.greaterOrEqual(
                                OracleIncrementalSourceOptions.CONNECT_TIMEOUT_MS, 0L))
                .optional(
                        OracleIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        Conditions.greaterOrEqual(
                                OracleIncrementalSourceOptions.CONNECT_MAX_RETRIES, 0))
                .optional(
                        OracleIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        Conditions.greaterThan(
                                OracleIncrementalSourceOptions.CONNECTION_POOL_SIZE, 0))
                .optional(
                        OracleIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        Conditions.greaterOrEqual(
                                OracleIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD, 0))
                .optional(
                        OracleIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        Conditions.greaterThan(
                                OracleIncrementalSourceOptions.INVERSE_SAMPLING_RATE, 0))
                .optional(
                        OracleIncrementalSourceOptions.STARTUP_MODE,
                        OracleIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        OracleIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
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
