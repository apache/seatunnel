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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb;

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
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;

@AutoService(Factory.class)
public class MongodbIncrementalSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return MongodbIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return MongodbSourceOptions.getBaseRule()
                .required(
                        MongodbSourceOptions.HOSTS,
                        MongodbSourceOptions.DATABASE,
                        MongodbSourceOptions.COLLECTION)
                .exclusive(ConnectorCommonOptions.SCHEMA, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        MongodbSourceOptions.USERNAME,
                        MongodbSourceOptions.PASSWORD,
                        MongodbSourceOptions.CONNECTION_OPTIONS,
                        MongodbSourceOptions.BATCH_SIZE,
                        MongodbSourceOptions.POLL_MAX_BATCH_SIZE,
                        MongodbSourceOptions.POLL_AWAIT_TIME_MILLIS,
                        MongodbSourceOptions.HEARTBEAT_INTERVAL_MILLIS,
                        MongodbSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB,
                        MongodbSourceOptions.STARTUP_MODE,
                        MongodbSourceOptions.STOP_MODE)
                .conditional(
                        MongodbSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MongodbIncrementalSource.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            List<CatalogTable> catalogTables = buildWithConfig(context.getOptions());
            List<String> collections = context.getOptions().get(MongodbSourceOptions.COLLECTION);
            validateCatalogTablesAndCollections(catalogTables, collections);
            catalogTables = updateAndValidateCatalogTableId(catalogTables, collections);
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new MongodbIncrementalSource<>(context.getOptions(), catalogTables);
        };
    }

    private List<CatalogTable> updateAndValidateCatalogTableId(
            List<CatalogTable> catalogTables, List<String> collections) {
        for (int i = 0; i < catalogTables.size(); i++) {
            CatalogTable catalogTable = catalogTables.get(i);
            String collectionName = collections.get(i);
            String fullName = catalogTable.getTablePath().getFullName();
            if (fullName.equals(TablePath.DEFAULT.getFullName())) {
                if (catalogTables.size() == 1) {
                    TableIdentifier updatedIdentifier =
                            TableIdentifier.of(
                                    catalogTable.getCatalogName(), TablePath.of(collectionName));
                    return Collections.singletonList(
                            CatalogTable.of(updatedIdentifier, catalogTable));
                } else if (!fullName.equals(collectionName)) {
                    throw new MongodbConnectorException(
                            ILLEGAL_ARGUMENT,
                            String.format(
                                    "Inconsistent naming found at index %d: The collection name '%s' must match the schema table name '%s'.",
                                    i, collectionName, fullName));
                }
            }
        }
        return catalogTables;
    }

    private void validateCatalogTablesAndCollections(
            List<CatalogTable> catalogTables, List<String> collections) {
        if (catalogTables.size() != collections.size()) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "The number of collections must be equal to the number of schema tables");
        }
    }

    private List<CatalogTable> buildWithConfig(ReadonlyConfig config) {
        String factoryId = config.get(ConnectorCommonOptions.PLUGIN_NAME).replace("-CDC", "");
        Map<String, Object> schemaMap = config.get(ConnectorCommonOptions.SCHEMA);
        if (schemaMap != null) {
            if (schemaMap.isEmpty()) {
                throw new SeaTunnelException("Schema config can not be empty");
            }
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(factoryId, config);
            return Collections.singletonList(catalogTable);
        }
        List<Map<String, Object>> schemaMaps = config.get(ConnectorCommonOptions.TABLE_CONFIGS);
        if (schemaMaps != null) {
            if (schemaMaps.isEmpty()) {
                throw new SeaTunnelException("tables_configs can not be empty");
            }
            return schemaMaps.stream()
                    .map(
                            map ->
                                    CatalogTableUtil.buildWithConfig(
                                            factoryId, ReadonlyConfig.fromMap(map)))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}
