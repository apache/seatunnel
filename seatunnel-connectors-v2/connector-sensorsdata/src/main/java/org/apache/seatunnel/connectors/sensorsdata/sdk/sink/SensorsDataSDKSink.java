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

package org.apache.seatunnel.connectors.sensorsdata.sdk.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.sensorsdata.sdk.config.SensorsDataSDKSinkConfig;
import org.apache.seatunnel.connectors.sensorsdata.sdk.state.SensorsDataAggregatedCommitInfo;
import org.apache.seatunnel.connectors.sensorsdata.sdk.state.SensorsDataCommitInfo;
import org.apache.seatunnel.connectors.sensorsdata.sdk.state.SensorsDataSinkState;

import java.util.Optional;

/**
 * Sensors Data Sink implementation by using SeaTunnel sink API. This class contains the method to
 * create {@link AbstractSimpleSink}.
 */
public class SensorsDataSDKSink
        implements SeaTunnelSink<
                SeaTunnelRow,
                SensorsDataSinkState,
                SensorsDataCommitInfo,
                SensorsDataAggregatedCommitInfo> {

    private final SensorsDataSDKSinkConfig sinkConfig;
    private final SeaTunnelRowType seaTunnelRowType;

    private final CatalogTable catalogTable;

    public SensorsDataSDKSink(SensorsDataSDKSinkConfig sinkConfig, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.sinkConfig = sinkConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public String getPluginName() {
        return "SensorsData";
    }

    @Override
    public SinkWriter<SeaTunnelRow, SensorsDataCommitInfo, SensorsDataSinkState> createWriter(
            SinkWriter.Context context) {
        return new SensorsDataSDKWriter(seaTunnelRowType, sinkConfig);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
