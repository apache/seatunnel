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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchSinkState;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.MAX_RETRY_COUNT;

public class ElasticsearchSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        ElasticsearchSinkState,
                        ElasticsearchCommitInfo,
                        ElasticsearchAggregatedCommitInfo>,
                SupportMultiTableSink,
                SupportSaveMode,
                SupportSchemaEvolutionSink {

    private ReadonlyConfig config;
    private CatalogTable catalogTable;

    private final int maxBatchSize;

    private final int maxRetryCount;

    public ElasticsearchSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        maxBatchSize = config.get(MAX_BATCH_SIZE);
        maxRetryCount = config.get(MAX_RETRY_COUNT);
    }

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public ElasticsearchSinkWriter createWriter(SinkWriter.Context context) {
        return new ElasticsearchSinkWriter(
                context, catalogTable, config, maxBatchSize, maxRetryCount);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        getPluginName());
        if (catalogFactory == null) {
            return Optional.empty();
        }
        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        SchemaSaveMode schemaSaveMode = config.get(ElasticsearchSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(ElasticsearchSinkOptions.DATA_SAVE_MODE);

        TablePath tablePath = TablePath.of("", catalogTable.getTableId().getTableName());
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, tablePath, null, null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(SchemaChangeType.ADD_COLUMN);
    }
}
