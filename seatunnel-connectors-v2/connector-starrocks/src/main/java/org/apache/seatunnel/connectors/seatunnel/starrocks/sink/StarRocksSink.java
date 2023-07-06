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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksSinkStateSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfoSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.state.StarRocksSinkState;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@NoArgsConstructor
@AutoService(SeaTunnelSink.class)
public class StarRocksSink
        implements SeaTunnelSink<
                        SeaTunnelRow, StarRocksSinkState, StarRocksCommitInfo, StarRocksCommitInfo>,
                SupportDataSaveMode {

    private SeaTunnelRowType seaTunnelRowType;
    private SinkConfig sinkConfig;
    private DataSaveMode dataSaveMode;
    private CatalogTable catalogTable;

    public StarRocksSink(
            DataSaveMode dataSaveMode, SinkConfig sinkConfig, CatalogTable catalogTable) {
        this.dataSaveMode = dataSaveMode;
        this.sinkConfig = sinkConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return StarRocksCatalogFactory.IDENTIFIER;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new StarRocksCatalogFactory().optionRule());
        sinkConfig = SinkConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
        if (StringUtils.isEmpty(sinkConfig.getTable()) && catalogTable != null) {
            sinkConfig.setTable(catalogTable.getTableId().getTableName());
        }
        dataSaveMode = DataSaveMode.KEEP_SCHEMA_AND_DATA;
    }

    private void autoCreateTable(String template) {
        StarRocksCatalog starRocksCatalog =
                new StarRocksCatalog(
                        "StarRocks",
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getJdbcUrl());
        if (!starRocksCatalog.databaseExists(sinkConfig.getDatabase())) {
            starRocksCatalog.createDatabase(TablePath.of(sinkConfig.getDatabase(), ""), true);
        }
        if (!starRocksCatalog.tableExists(
                TablePath.of(sinkConfig.getDatabase(), sinkConfig.getTable()))) {
            starRocksCatalog.createTable(
                    StarRocksSaveModeUtil.fillingCreateSql(
                            template,
                            sinkConfig.getDatabase(),
                            sinkConfig.getTable(),
                            catalogTable.getTableSchema()));
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, StarRocksCommitInfo, StarRocksSinkState> createWriter(
            SinkWriter.Context context) {
        return new StarRocksSinkWriter(
                context, sinkConfig, seaTunnelRowType, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, StarRocksCommitInfo, StarRocksSinkState> restoreWriter(
            SinkWriter.Context context, List<StarRocksSinkState> states) {
        return new StarRocksSinkWriter(context, sinkConfig, seaTunnelRowType, states);
    }

    @Override
    public Optional<SinkCommitter<StarRocksCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new StarRocksCommitter(sinkConfig));
    }

    @Override
    public Optional<Serializer<StarRocksCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new StarRocksCommitInfoSerializer());
    }

    @Override
    public Optional<Serializer<StarRocksSinkState>> getWriterStateSerializer() {
        return Optional.of(new StarRocksSinkStateSerializer());
    }

    @Override
    public Optional<SinkAggregatedCommitter<StarRocksCommitInfo, StarRocksCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<StarRocksCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return dataSaveMode;
    }

    @Override
    public List<DataSaveMode> supportedDataSaveModeValues() {
        return Collections.singletonList(DataSaveMode.KEEP_SCHEMA_AND_DATA);
    }

    @Override
    public void handleSaveMode(DataSaveMode saveMode) {
        if (catalogTable != null) {
            autoCreateTable(sinkConfig.getSaveModeCreateTemplate());
        }
    }
}
