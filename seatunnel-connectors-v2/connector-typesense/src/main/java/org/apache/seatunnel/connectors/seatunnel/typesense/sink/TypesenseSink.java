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

package org.apache.seatunnel.connectors.seatunnel.typesense.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseSinkState;

import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig.MAX_RETRY_COUNT;

public class TypesenseSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        TypesenseSinkState,
                        TypesenseCommitInfo,
                        TypesenseAggregatedCommitInfo>,
                SupportMultiTableSink,
                SupportSaveMode {

    private ReadonlyConfig config;
    private CatalogTable catalogTable;
    private final int maxBatchSize;
    private final int maxRetryCount;

    public TypesenseSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        maxBatchSize = config.get(MAX_BATCH_SIZE);
        maxRetryCount = config.get(MAX_RETRY_COUNT);
    }

    @Override
    public String getPluginName() {
        return "Typesense";
    }

    @Override
    public TypesenseSinkWriter createWriter(SinkWriter.Context context) {
        return new TypesenseSinkWriter(context, catalogTable, config, maxBatchSize, maxRetryCount);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        getPluginName());
        if (catalogFactory == null) {
            return Optional.empty();
        }
        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        SchemaSaveMode schemaSaveMode = config.get(SinkConfig.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(SinkConfig.DATA_SAVE_MODE);

        TablePath tablePath = TablePath.of("", catalogTable.getTableId().getTableName());
        catalog.open();
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, tablePath, null, null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
