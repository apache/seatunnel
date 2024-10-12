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

package org.apache.seatunnel.connectors.seatunnel.weaviate.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.weaviate.catalog.WeaviateCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.weaviate.state.WeaviateAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.weaviate.state.WeaviateCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.weaviate.state.WeaviateSinkState;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class WeaviateSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        WeaviateSinkState,
                        WeaviateCommitInfo,
                        WeaviateAggregatedCommitInfo>,
                SupportSaveMode {

    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;

    public WeaviateSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public SinkWriter<SeaTunnelRow, WeaviateCommitInfo, WeaviateSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new WeaviateSinkWriter(
                context,
                catalogTable,
                WeaviateParameters.buildWithConfig(config),
                Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, WeaviateCommitInfo, WeaviateSinkState> restoreWriter(
            SinkWriter.Context context, List<WeaviateSinkState> states) {
        return new WeaviateSinkWriter(
                context, catalogTable, WeaviateParameters.buildWithConfig(config), states);
    }

    @Override
    public Optional<Serializer<WeaviateSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<WeaviateCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new WeaviateSinkCommitter(config));
    }

    @Override
    public Optional<Serializer<WeaviateCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return WeaviateCommonConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (catalogTable == null) {
            return Optional.empty();
        }
        CatalogFactory catalogFactory = new WeaviateCatalogFactory();
        Catalog catalog = catalogFactory.createCatalog(catalogTable.getCatalogName(), config);

        SchemaSaveMode schemaSaveMode = config.get(WeaviateSinkConfig.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(WeaviateSinkConfig.DATA_SAVE_MODE);

        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        catalog,
                        catalogTable.getTablePath(),
                        catalogTable,
                        null));
    }
}
