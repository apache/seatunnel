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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarSinkState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PLUGIN_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CLIENT_SERVICE_URL;

/**
 * Pulsar Sink implementation by using SeaTunnel sink API. This class contains the method to create
 * {@link PulsarSinkWriter} and {@link PulsarSinkCommitter}.
 */
public class PulsarSink
        implements SeaTunnelSink<
                SeaTunnelRow, PulsarSinkState, PulsarCommitInfo, PulsarAggregatedCommitInfo> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final PulsarClientConfig clientConfig;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public PulsarSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.catalogTable = catalogTable;

        /** client config */
        PulsarClientConfig.Builder clientConfigBuilder =
                PulsarClientConfig.builder().serviceUrl(readonlyConfig.get(CLIENT_SERVICE_URL));
        clientConfigBuilder.authPluginClassName(readonlyConfig.get(AUTH_PLUGIN_CLASS));
        clientConfigBuilder.authParams(readonlyConfig.get(AUTH_PARAMS));
        this.clientConfig = clientConfigBuilder.build();
    }

    @Override
    public SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> createWriter(
            SinkWriter.Context context) {
        return new PulsarSinkWriter(
                context, clientConfig, seaTunnelRowType, readonlyConfig, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> restoreWriter(
            SinkWriter.Context context, List<PulsarSinkState> states) {
        return new PulsarSinkWriter(
                context, clientConfig, seaTunnelRowType, readonlyConfig, states);
    }

    @Override
    public Optional<Serializer<PulsarSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<PulsarCommitInfo>> createCommitter() {
        return Optional.of(new PulsarSinkCommitter(clientConfig));
    }

    @Override
    public Optional<Serializer<PulsarCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return PulsarConfigUtil.IDENTIFIER;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
