/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalog;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CASE_SENSITIVE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_END_SNAPSHOT_ID;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_INCREMENT_SCAN_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_START_SNAPSHOT_ID;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_START_SNAPSHOT_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_STREAM_SCAN_STRATEGY;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_USE_SNAPSHOT_ID;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_USE_SNAPSHOT_TIMESTAMP;

@Slf4j
@AutoService(Factory.class)
public class IcebergSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        CommonConfig.KEY_CATALOG_NAME,
                        CommonConfig.KEY_NAMESPACE,
                        CommonConfig.CATALOG_PROPS)
                .exclusive(CommonConfig.KEY_TABLE, SourceConfig.KEY_TABLE_LIST)
                .optional(
                        ConnectorCommonOptions.SCHEMA,
                        KEY_CASE_SENSITIVE,
                        KEY_START_SNAPSHOT_TIMESTAMP,
                        KEY_START_SNAPSHOT_ID,
                        KEY_END_SNAPSHOT_ID,
                        KEY_USE_SNAPSHOT_ID,
                        KEY_USE_SNAPSHOT_TIMESTAMP,
                        KEY_STREAM_SCAN_STRATEGY,
                        KEY_INCREMENT_SCAN_INTERVAL)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        SourceConfig config = new SourceConfig(options);
        CatalogTable catalogTable;
        if (options.get(ConnectorCommonOptions.SCHEMA) != null) {
            TablePath tablePath = config.getTableList().get(0).getTablePath();
            catalogTable = CatalogTableUtil.buildWithConfig(factoryIdentifier(), options);
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(catalogTable.getCatalogName(), tablePath);
            CatalogTable table = CatalogTable.of(tableIdentifier, catalogTable);
            return () ->
                    (SeaTunnelSource<T, SplitT, StateT>)
                            new IcebergSource(config, Collections.singletonList(table));
        }

        try (IcebergCatalog catalog =
                (IcebergCatalog)
                        new IcebergCatalogFactory().createCatalog(factoryIdentifier(), options)) {
            catalog.open();

            if (config.getTable() != null) {
                TablePath tablePath = config.getTableList().get(0).getTablePath();
                catalogTable = catalog.getTable(tablePath);
                return () ->
                        (SeaTunnelSource<T, SplitT, StateT>)
                                new IcebergSource(config, Collections.singletonList(catalogTable));
            }

            List<CatalogTable> catalogTables =
                    config.getTableList().stream()
                            .map(tableConfig -> catalog.getTable(tableConfig.getTablePath()))
                            .collect(Collectors.toList());
            return () ->
                    (SeaTunnelSource<T, SplitT, StateT>) new IcebergSource(config, catalogTables);
        }
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return IcebergSource.class;
    }
}
