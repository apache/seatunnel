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
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

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
                        Db2IncrementalSourceOptions.USERNAME,
                        Db2IncrementalSourceOptions.PASSWORD,
                        Db2IncrementalSourceOptions.URL)
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
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    context.getOptions()
                            .getOptional(Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> TablePath.of(text, true));
            }
            return new Db2IncrementalSource(context.getOptions(), catalogTables);
        };
    }
}
