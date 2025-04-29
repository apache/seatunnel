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

package org.apache.seatunnel.connectors.seatunnel.inlong.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.inlong.state.InlongAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.inlong.state.InlongCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.inlong.state.InlongSinkState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.IDENTIFIER;

/**
 * Inlong Sink implementation by using SeaTunnel sink API. This class contains the method to create
 * {@link InlongSinkWriter} and {@link InlongSinkCommitter}.
 */
public class InlongSink
        implements SeaTunnelSink<
                SeaTunnelRow, InlongSinkState, InlongCommitInfo, InlongAggregatedCommitInfo> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public InlongSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.catalogTable = catalogTable;
    }

    @Override
    public SinkWriter<SeaTunnelRow, InlongCommitInfo, InlongSinkState> createWriter(
            SinkWriter.Context context) {
        return new InlongSinkWriter(
                context, readonlyConfig, seaTunnelRowType, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, InlongCommitInfo, InlongSinkState> restoreWriter(
            SinkWriter.Context context, List<InlongSinkState> states) {
        return new InlongSinkWriter(context, readonlyConfig, seaTunnelRowType, states);
    }

    @Override
    public Optional<Serializer<InlongSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<InlongCommitInfo>> createCommitter() {
        return Optional.of(new InlongSinkCommitter(readonlyConfig));
    }

    @Override
    public Optional<Serializer<InlongCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
