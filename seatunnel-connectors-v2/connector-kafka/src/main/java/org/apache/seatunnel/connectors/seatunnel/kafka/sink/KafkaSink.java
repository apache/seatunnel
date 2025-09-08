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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Kafka Sink implementation by using SeaTunnel sink API. This class contains the method to create
 * {@link KafkaSinkWriter} and {@link KafkaSinkCommitter}.
 */
public class KafkaSink
        implements SeaTunnelSink<
                SeaTunnelRow, KafkaSinkState, KafkaCommitInfo, KafkaAggregatedCommitInfo> {

    private final ReadonlyConfig pluginConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private final CatalogTable catalogTable;

    public KafkaSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.pluginConfig = pluginConfig;
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> createWriter(
            SinkWriter.Context context) {
        return new KafkaSinkWriter(
                context, seaTunnelRowType, pluginConfig, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> restoreWriter(
            SinkWriter.Context context, List<KafkaSinkState> states) {
        return new KafkaSinkWriter(context, seaTunnelRowType, pluginConfig, states);
    }

    @Override
    public Optional<Serializer<KafkaSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<KafkaCommitInfo>> createCommitter() {
        return Optional.of(new KafkaSinkCommitter(pluginConfig));
    }

    @Override
    public Optional<Serializer<KafkaCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return KafkaBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
