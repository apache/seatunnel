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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.commiter.HudiSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiSinkState;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.HudiSinkWriter;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode.TABLE_CONFIG_NOT_FOUND;

@AutoService(SeaTunnelSink.class)
public class HudiSink
        implements SeaTunnelSink<
                        SeaTunnelRow, HudiSinkState, HudiCommitInfo, HudiAggregatedCommitInfo>,
                SupportMultiTableSink {

    private final HudiSinkConfig hudiSinkConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private final CatalogTable catalogTable;

    public HudiSink(ReadonlyConfig config, CatalogTable table) {
        this.hudiSinkConfig = HudiSinkConfig.of(config);
        this.catalogTable = table;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public String getPluginName() {
        return "Hudi";
    }

    @Override
    public SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        HudiTableConfig hudiTableConfig =
                getHudiTableConfig(hudiSinkConfig, catalogTable.getTableId().getTableName());
        return new HudiSinkWriter(
                context, seaTunnelRowType, hudiSinkConfig, hudiTableConfig, new ArrayList<>());
    }

    @Override
    public SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> restoreWriter(
            SinkWriter.Context context, List<HudiSinkState> states) throws IOException {
        HudiTableConfig hudiTableConfig =
                getHudiTableConfig(hudiSinkConfig, catalogTable.getTableId().getTableName());
        return new HudiSinkWriter(
                context, seaTunnelRowType, hudiSinkConfig, hudiTableConfig, states);
    }

    @Override
    public Optional<Serializer<HudiSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<HudiCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<HudiCommitInfo, HudiAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(
                new HudiSinkAggregatedCommitter(
                        getHudiTableConfig(
                                hudiSinkConfig, catalogTable.getTableId().getTableName()),
                        hudiSinkConfig,
                        seaTunnelRowType));
    }

    @Override
    public Optional<Serializer<HudiAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private HudiTableConfig getHudiTableConfig(HudiSinkConfig hudiSinkConfig, String tableName) {
        List<HudiTableConfig> tableList = hudiSinkConfig.getTableList();
        if (tableList.size() == 1) {
            return tableList.get(0);
        } else if (tableList.size() > 1) {
            Optional<HudiTableConfig> optionalHudiTableConfig =
                    tableList.stream()
                            .filter(table -> table.getTableName().equals(tableName))
                            .findFirst();
            if (!optionalHudiTableConfig.isPresent()) {
                throw new HudiConnectorException(
                        TABLE_CONFIG_NOT_FOUND,
                        "The corresponding table configuration is not found");
            }
            return optionalHudiTableConfig.get();
        }
        throw new HudiConnectorException(
                TABLE_CONFIG_NOT_FOUND, "The corresponding table configuration is not found");
    }
}
