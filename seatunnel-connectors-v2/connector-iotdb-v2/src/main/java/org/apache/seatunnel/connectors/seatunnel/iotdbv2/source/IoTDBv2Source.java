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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.constant.SourceConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.source.relational.IoTDBv2RelationalSourceReader;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.state.IoTDBv2SourceState;

import java.util.Collections;
import java.util.List;

public class IoTDBv2Source
        implements SeaTunnelSource<SeaTunnelRow, IoTDBv2SourceSplit, IoTDBv2SourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private CatalogTable catalogTable;
    private ReadonlyConfig pluginConfig;
    private String sqlDialect;

    public IoTDBv2Source(
            CatalogTable catalogTable, ReadonlyConfig pluginConfig, String sqlDialect) {
        this.catalogTable = catalogTable;
        this.pluginConfig = pluginConfig;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public String getPluginName() {
        return "IoTDBv2";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, IoTDBv2SourceSplit> createReader(
            SourceReader.Context readerContext) {
        if (SourceConstants.TABLE.equalsIgnoreCase(sqlDialect)) {
            return new IoTDBv2RelationalSourceReader(
                    pluginConfig, readerContext, catalogTable.getSeaTunnelRowType());
        }
        return new IoTDBv2SourceReader(
                pluginConfig, readerContext, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<IoTDBv2SourceSplit, IoTDBv2SourceState> createEnumerator(
            SourceSplitEnumerator.Context<IoTDBv2SourceSplit> enumeratorContext) throws Exception {
        return new IoTDBv2SourceSplitEnumerator(enumeratorContext, pluginConfig);
    }

    @Override
    public SourceSplitEnumerator<IoTDBv2SourceSplit, IoTDBv2SourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<IoTDBv2SourceSplit> enumeratorContext,
            IoTDBv2SourceState checkpointState)
            throws Exception {
        return new IoTDBv2SourceSplitEnumerator(enumeratorContext, pluginConfig, checkpointState);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }
}
