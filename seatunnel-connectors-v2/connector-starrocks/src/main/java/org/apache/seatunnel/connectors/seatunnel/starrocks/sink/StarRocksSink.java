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

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import java.util.Optional;

public class StarRocksSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportSaveMode {

    private SeaTunnelRowType seaTunnelRowType;
    private final SinkConfig sinkConfig;
    private final DataSaveMode dataSaveMode;

    private final CatalogTable catalogTable;

    public StarRocksSink(SinkConfig sinkConfig, CatalogTable catalogTable) {
        this.sinkConfig = sinkConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.catalogTable = catalogTable;
        this.dataSaveMode = sinkConfig.getDataSaveMode();
    }

    @Override
    public String getPluginName() {
        return StarRocksCatalogFactory.IDENTIFIER;
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
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new StarRocksSinkWriter(sinkConfig, seaTunnelRowType);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        return Optional.empty();
    }

    /*@Override
    public DataSaveMode getUserConfigSaveMode() {
        return dataSaveMode;
    }

    @Override
    public void handleSaveMode(DataSaveMode saveMode) {
        if (catalogTable != null) {
            autoCreateTable(sinkConfig.getSaveModeCreateTemplate());
        }
    }*/
}
