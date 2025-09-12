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

package org.apache.seatunnel.connectors.seatunnel.easysearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.easysearch.catalog.EasysearchCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.EasysearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.easysearch.state.EasysearchAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.state.EasysearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.state.EasysearchSinkState;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class EasysearchSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        EasysearchSinkState,
                        EasysearchCommitInfo,
                        EasysearchAggregatedCommitInfo>,
                SupportSchemaEvolution,
                SupportSaveMode {

    private final ReadonlyConfig pluginConfig;
    private final CatalogTable catalogTable;

    public EasysearchSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public String getPluginName() {
        return "Easysearch";
    }

    @Override
    public SinkWriter<SeaTunnelRow, EasysearchCommitInfo, EasysearchSinkState> createWriter(
            SinkWriter.Context context) {
        return new EasysearchSinkWriter(context, catalogTable.getSeaTunnelRowType(), pluginConfig);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return SeaTunnelSink.super.getWriteCatalogTable();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        getPluginName());

        Catalog catalog;
        if (catalogFactory == null) {
            // If no CatalogFactory is found, use our EasysearchCatalogFactory directly
            catalogFactory = new EasysearchCatalogFactory();
        }

        catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), pluginConfig);
        SchemaSaveMode schemaSaveMode = pluginConfig.get(EasysearchSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = pluginConfig.get(EasysearchSinkOptions.DATA_SAVE_MODE);

        // Use the index name directly as both database and table name for Easysearch
        String indexName = catalogTable.getTableId().getTableName();
        TablePath tablePath = TablePath.of(indexName, indexName);
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, tablePath, null, null));
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(SchemaChangeType.ADD_COLUMN);
    }
}
