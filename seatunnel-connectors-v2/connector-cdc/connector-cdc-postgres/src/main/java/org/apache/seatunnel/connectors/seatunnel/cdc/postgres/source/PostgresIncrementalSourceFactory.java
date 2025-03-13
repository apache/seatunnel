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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.option.PostgresOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
public class PostgresIncrementalSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return PostgresIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        JdbcSourceOptions.USERNAME,
                        JdbcSourceOptions.PASSWORD,
                        JdbcCatalogOptions.BASE_URL)
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        JdbcSourceOptions.DATABASE_NAMES,
                        JdbcSourceOptions.SERVER_TIME_ZONE,
                        JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                        JdbcSourceOptions.CONNECT_MAX_RETRIES,
                        JdbcSourceOptions.CONNECTION_POOL_SIZE,
                        PostgresOptions.DECODING_PLUGIN_NAME,
                        PostgresOptions.SLOT_NAME,
                        JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        JdbcSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        JdbcSourceOptions.TABLE_NAMES_CONFIG)
                .optional(PostgresSourceOptions.STARTUP_MODE, PostgresSourceOptions.STOP_MODE)
                .conditional(
                        PostgresSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        JdbcSourceOptions.EXACTLY_ONCE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return PostgresIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    context.getOptions().getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables, tableConfigs.get(), s -> TablePath.of(s, true));
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new PostgresIncrementalSource<>(context.getOptions(), catalogTables);
        };
    }
}
