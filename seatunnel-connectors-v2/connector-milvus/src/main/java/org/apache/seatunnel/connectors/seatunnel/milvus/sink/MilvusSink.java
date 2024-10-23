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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

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
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusSinkState;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class MilvusSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        MilvusSinkState,
                        MilvusCommitInfo,
                        MilvusAggregatedCommitInfo>,
                SupportSaveMode {

    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;

    public MilvusSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState> createWriter(
            SinkWriter.Context context) {
        return new MilvusSinkWriter(context, catalogTable, config, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState> restoreWriter(
            SinkWriter.Context context, List<MilvusSinkState> states) {
        return new MilvusSinkWriter(context, catalogTable, config, states);
    }

    @Override
    public Optional<Serializer<MilvusSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<MilvusCommitInfo>> createCommitter() {
        return Optional.of(new MilvusSinkCommitter(config));
    }

    @Override
    public Optional<Serializer<MilvusCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return MilvusSinkConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (catalogTable == null) {
            return Optional.empty();
        }

        CatalogFactory catalogFactory = new MilvusCatalogFactory();
        Catalog catalog = catalogFactory.createCatalog(catalogTable.getCatalogName(), config);

        SchemaSaveMode schemaSaveMode = config.get(MilvusSinkConfig.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(MilvusSinkConfig.DATA_SAVE_MODE);

        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        catalog,
                        catalogTable.getTablePath(),
                        catalogTable,
                        null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
