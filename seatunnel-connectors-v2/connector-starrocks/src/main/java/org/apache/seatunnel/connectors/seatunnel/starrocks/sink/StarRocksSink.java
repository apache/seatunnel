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

import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.USERNAME;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

@AutoService(SeaTunnelSink.class)
public class StarRocksSink extends AbstractSimpleSink<SeaTunnelRow, Void> implements SupportDataSaveMode {

    private SeaTunnelRowType seaTunnelRowType;
    private SinkConfig sinkConfig;
    private DataSaveMode dataSaveMode;
    @Override
    public String getPluginName() {
        return "StarRocks";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, NODE_URLS.key(), DATABASE.key(), TABLE.key(), USERNAME.key(), PASSWORD.key());
        if (!result.isSuccess()) {
            throw new StarRocksConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    getPluginName(), PluginType.SINK, result.getMsg()));
        }
        // TODO get catalog Table
        CatalogTable catalogTable = null;
        sinkConfig = SinkConfig.loadConfig(pluginConfig);
        if (StringUtils.isEmpty(sinkConfig.getTable())) {
            sinkConfig.setTable(catalogTable.getTableId().getTableName());
        }
        sinkConfig.setTable(catalogTable.getTableId().getTableName());
        dataSaveMode = DataSaveMode.KEEP_SCHEMA_AND_DATA;
    }

    private void autoCreateTable(String template) {
        StarRocksCatalog starRocksCatalog = new StarRocksCatalog("StarRocks", sinkConfig.getDatabase(),
            sinkConfig.getUsername(), sinkConfig.getPassword(), sinkConfig.getJdbcUrl());
        if (!starRocksCatalog.databaseExists(sinkConfig.getDatabase())) {
            starRocksCatalog.createDatabase(TablePath.of(sinkConfig.getDatabase(), ""), true);
        }
        // TODO get catalog Table
        CatalogTable catalogTable = null;
        if (!starRocksCatalog.tableExists(TablePath.of(sinkConfig.getDatabase(), sinkConfig.getTable()))) {
            starRocksCatalog.createTable(StarRocksSaveModeUtil.fillingCreateSql(template, sinkConfig.getDatabase(), sinkConfig.getTable(), catalogTable.getTableSchema()));
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new StarRocksSinkWriter(sinkConfig, seaTunnelRowType);
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
        autoCreateTable(sinkConfig.getSaveModeCreateTemplate());
    }
}
