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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceReader.Context;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import com.clickhouse.client.ClickHouseNode;

import java.util.Collections;
import java.util.List;

public class ClickhouseSource
        implements SeaTunnelSource<SeaTunnelRow, ClickHouseSourceSplit, ClickhouseSourceState>,
        SupportParallelism {

    private final ClickhouseSourceConfig sourceConfig;
    private final List<ClickHouseNode> servers;
    private final CatalogTable catalogTable;
    private final SeaTunnelRowType rowTypeInfo;

    public ClickhouseSource(
            ClickhouseSourceConfig sourceConfig,
            List<ClickHouseNode> servers,
            CatalogTable catalogTable,
            String sql) {
        this.sourceConfig = sourceConfig;
        this.servers = servers;
        this.catalogTable = catalogTable;
        this.rowTypeInfo = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, ClickHouseSourceSplit> createReader(Context readerContext)
            throws Exception {
        return new ClickhouseSourceReader(servers, readerContext, rowTypeInfo);
    }

    @Override
    public SourceSplitEnumerator<ClickHouseSourceSplit, ClickhouseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<ClickHouseSourceSplit> enumeratorContext)
            throws Exception {
        return new ClickhouseSourceSplitEnumerator(enumeratorContext, sourceConfig, catalogTable);
    }

    @Override
    public SourceSplitEnumerator<ClickHouseSourceSplit, ClickhouseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<ClickHouseSourceSplit> enumeratorContext,
            ClickhouseSourceState checkpointState)
            throws Exception {
        return new ClickhouseSourceSplitEnumerator(enumeratorContext, sourceConfig, catalogTable);
    }
}
