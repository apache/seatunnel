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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceFactory;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceState;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;

/** Factory for the Vitess CDC source connector. */
@AutoService(Factory.class)
public class VitessSourceFactory implements ChangeStreamTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return VitessSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(VitessSourceOptions.HOSTNAME, VitessSourceOptions.KEYSPACE)
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        VitessSourceOptions.PORT,
                        VitessSourceOptions.USERNAME,
                        VitessSourceOptions.PASSWORD,
                        VitessSourceOptions.SHARD,
                        VitessSourceOptions.STARTUP_MODE,
                        VitessSourceOptions.TABLET_TYPE,
                        VitessSourceOptions.STOP_ON_RESHARD,
                        VitessSourceOptions.KEEPALIVE_INTERVAL_MS,
                        VitessSourceOptions.GRPC_HEADERS,
                        VitessSourceOptions.GRPC_MAX_INBOUND_MESSAGE_SIZE,
                        VitessSourceOptions.SERVER_TIME_ZONE,
                        SourceOptions.FORMAT,
                        SourceOptions.DEBEZIUM_PROPERTIES)
                .conditional(
                        VitessSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        VitessSourceOptions.STARTUP_SPECIFIC_OFFSET_VGTID)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return VitessSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return createVitessTableSource(context);
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context,
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        // Vitess schema evolution is intentionally out of scope for the first delivery, so the
        // runtime only needs the original catalog tables and the checkpointed split offsets.
        return createVitessTableSource(context);
    }

    @SuppressWarnings("unchecked")
    private <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createVitessTableSource(
                    TableSourceFactoryContext context) {
        return () -> {
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            VitessSourceConfig sourceConfig =
                    VitessSourceConfig.of(context.getOptions(), catalogTables);
            return (SeaTunnelSource<T, SplitT, StateT>) new VitessSource(sourceConfig);
        };
    }
}
