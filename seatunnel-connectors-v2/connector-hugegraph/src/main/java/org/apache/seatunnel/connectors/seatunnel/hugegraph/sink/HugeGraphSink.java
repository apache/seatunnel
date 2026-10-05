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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;

import java.io.IOException;
import java.util.Optional;

public class HugeGraphSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink, SupportSaveMode {

    private final HugeGraphSinkConfig config;
    private final CatalogTable catalogTable;
    private final SeaTunnelRowType rowType;
    private final String tablePath;

    public HugeGraphSink(HugeGraphSinkConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.tablePath = catalogTable.getTablePath().toString();

        this.config.applyLegacyFieldSelection(rowType);
    }

    /**
     * Schema management and the DROP_DATA data drop run once on the coordinator via the engine's
     * SaveMode contract — see {@link HugeGraphSaveModeHandler}. Running it here (rather than in the
     * constructor as before) is what makes it correct on checkpoint restart and for multi-table
     * sinks: restart re-runs only the schema step (never dropping data), and each table drops only
     * its own labels instead of wiping the whole graph.
     */
    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        return Optional.of(
                new HugeGraphSaveModeHandler(config, rowType, catalogTable.getTablePath()));
    }

    @Override
    public String getPluginName() {
        return HugeGraphOptions.PLUGIN_NAME;
    }

    @Override
    public HugeGraphSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new HugeGraphSinkWriter(config, rowType, tablePath, context);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
