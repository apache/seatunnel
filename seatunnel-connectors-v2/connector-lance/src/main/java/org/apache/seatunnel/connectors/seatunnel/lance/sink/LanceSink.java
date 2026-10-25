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

package org.apache.seatunnel.connectors.seatunnel.lance.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.lance.catalog.LanceCatalog;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.sink.commit.LanceAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.lance.sink.commit.LanceCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.lance.state.LanceSinkState;

import java.io.IOException;
import java.util.Optional;

public class LanceSink
        implements SeaTunnelSink<
                        SeaTunnelRow, LanceSinkState, LanceCommitInfo, LanceAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {

    private static final String PLUGIN_NAME = "Lance";

    private final LanceSinkConfig config;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public LanceSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.readonlyConfig = pluginConfig;
        this.config = new LanceSinkConfig(pluginConfig);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public LanceSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        TableSchema tableSchema = catalogTable.getTableSchema();
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        LanceSinkConfig sinkConfig = new LanceSinkConfig(readonlyConfig);
        LanceCatalog catalog = new LanceCatalog(catalogTable.getCatalogName(), readonlyConfig);
        return new LanceSinkWriter(rowType, tableSchema, sinkConfig, catalog);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        return Optional.empty();
    }
}
