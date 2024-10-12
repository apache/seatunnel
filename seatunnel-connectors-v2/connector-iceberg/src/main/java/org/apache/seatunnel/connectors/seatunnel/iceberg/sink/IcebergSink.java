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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.state.IcebergSinkState;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class IcebergSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        IcebergSinkState,
                        IcebergCommitInfo,
                        IcebergAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {
    private static final String PLUGIN_NAME = "Iceberg";
    private final SinkConfig config;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public IcebergSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.readonlyConfig = pluginConfig;
        this.config = new SinkConfig(pluginConfig);
        this.catalogTable = catalogTable;
        // Reset primary keys if need
        if (config.getPrimaryKeys().isEmpty()
                && Objects.nonNull(this.catalogTable.getTableSchema().getPrimaryKey())) {
            this.config.setPrimaryKeys(
                    this.catalogTable.getTableSchema().getPrimaryKey().getColumnNames());
        }
        // reset partition keys if need
        if (config.getPartitionKeys().isEmpty()
                && Objects.nonNull(this.catalogTable.getPartitionKeys())) {
            this.config.setPartitionKeys(this.catalogTable.getPartitionKeys());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public IcebergSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return IcebergSinkWriter.of(config, catalogTable);
    }

    @Override
    public SinkWriter<SeaTunnelRow, IcebergCommitInfo, IcebergSinkState> restoreWriter(
            SinkWriter.Context context, List<IcebergSinkState> states) throws IOException {
        return IcebergSinkWriter.of(config, catalogTable, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<IcebergCommitInfo, IcebergAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new IcebergAggregatedCommitter(config, catalogTable));
    }

    @Override
    public Optional<Serializer<IcebergAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<IcebergCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        "Iceberg");
        if (catalogFactory == null) {
            throw new IcebergConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, "Cannot find Doris catalog factory"));
        }
        Catalog catalog =
                catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), readonlyConfig);
        return Optional.of(
                new DefaultSaveModeHandler(
                        config.getSchemaSaveMode(),
                        config.getDataSaveMode(),
                        catalog,
                        catalogTable,
                        null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
