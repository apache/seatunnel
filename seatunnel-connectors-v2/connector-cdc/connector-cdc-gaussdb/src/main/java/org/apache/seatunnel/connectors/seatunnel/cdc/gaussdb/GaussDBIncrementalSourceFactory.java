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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
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
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/** Factory for the GaussDB CDC source connector. */
@AutoService(Factory.class)
@Slf4j
public class GaussDBIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {

    /**
     * Returns the connector identifier used by plugin discovery.
     *
     * @return GaussDB CDC connector identifier
     */
    @Override
    public String factoryIdentifier() {
        return GaussDBIncrementalSource.IDENTIFIER;
    }

    /**
     * Defines the GaussDB CDC option contract. The option set follows PostgreSQL CDC because the
     * runtime uses the same logical replication path.
     *
     * @return required, optional, and mutually exclusive source options
     */
    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        JdbcSourceOptions.USERNAME,
                        JdbcSourceOptions.PASSWORD,
                        JdbcCommonOptions.URL)
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        JdbcSourceOptions.DATABASE_NAMES,
                        JdbcSourceOptions.SERVER_TIME_ZONE,
                        JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                        JdbcSourceOptions.CONNECT_MAX_RETRIES,
                        JdbcSourceOptions.CONNECTION_POOL_SIZE,
                        PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME,
                        PostgresIncrementalSourceOptions.SLOT_NAME,
                        PostgresIncrementalSourceOptions.SCHEMA_NAME,
                        PostgresIncrementalSourceOptions.REQUIRE_REPLICA_IDENTITY_FULL,
                        JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        JdbcSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        JdbcSourceOptions.INVERSE_SAMPLING_RATE,
                        JdbcSourceOptions.SPLIT_ALLOW_SAMPLING,
                        JdbcSourceOptions.TABLE_NAMES_CONFIG,
                        SourceOptions.SCHEMA_CHANGES_ENABLED,
                        SourceOptions.SCHEMA_CHANGES_INCLUDE,
                        SourceOptions.SCHEMA_CHANGES_EXCLUDE)
                .optional(PostgresSourceOptions.STARTUP_MODE, PostgresSourceOptions.STOP_MODE)
                .conditional(
                        PostgresSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        JdbcSourceOptions.EXACTLY_ONCE)
                .conditional(
                        PostgresSourceOptions.STARTUP_MODE,
                        StartupMode.SNAPSHOT_ONLY,
                        JdbcSourceOptions.EXACTLY_ONCE)
                .build();
    }

    /**
     * Returns the concrete source class created by this factory.
     *
     * @return GaussDB incremental source class
     */
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return GaussDBIncrementalSource.class;
    }

    /**
     * Creates a GaussDB source using PostgreSQL catalog discovery and logical replication runtime.
     *
     * @param context factory context containing source options and class loader
     * @param restoreTables catalog tables restored from checkpoint state
     * @param <T> emitted record type
     * @param <SplitT> source split type
     * @param <StateT> split state type
     * @return table source supplier
     */
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver org.postgresql.Driver", e);
            }
            ReadonlyConfig config = context.getOptions();
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables("Postgres", config, context.getClassLoader());
            if (!restoreTables.isEmpty() && config.get(SourceOptions.SCHEMA_CHANGES_ENABLED)) {
                catalogTables = mergeTableStruct(catalogTables, restoreTables);
            }
            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    config.getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables, tableConfigs.get(), s -> TablePath.of(s, true));
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new GaussDBIncrementalSource<>(config, catalogTables);
        };
    }
}
