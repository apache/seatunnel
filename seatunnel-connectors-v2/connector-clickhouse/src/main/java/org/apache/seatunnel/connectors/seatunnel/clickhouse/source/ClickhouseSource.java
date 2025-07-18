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
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import com.clickhouse.client.ClickHouseNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClickhouseSource
        implements SeaTunnelSource<SeaTunnelRow, ClickhouseSourceSplit, ClickhouseSourceState> {

    private final List<ClickHouseNode> servers;
    private final CatalogTable catalogTable;
    private final SeaTunnelRowType rowTypeInfo;
    private final ClickhouseSourceConfig clickhouseSourceConfig;
    private final Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables;

    public ClickhouseSource(
            List<ClickHouseNode> servers,
            CatalogTable catalogTable,
            Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables,
            ClickhouseSourceConfig clickhouseSourceConfig) {
        this.servers = servers;
        this.catalogTable = catalogTable;
        this.rowTypeInfo = catalogTable.getSeaTunnelRowType();
        this.clickhouseSourceTables = clickhouseSourceTables;
        this.clickhouseSourceConfig = clickhouseSourceConfig;
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
    public SourceReader<SeaTunnelRow, ClickhouseSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new ClickhouseSourceReader(
                servers, readerContext, rowTypeInfo, clickhouseSourceTables);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext) {
        return new ClickhouseSourceSplitEnumerator(
                enumeratorContext, clickhouseSourceConfig, clickhouseSourceTables, servers);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext,
            ClickhouseSourceState checkpointState) {
        return new ClickhouseSourceSplitEnumerator(
                enumeratorContext,
                clickhouseSourceConfig,
                clickhouseSourceTables,
                servers,
                checkpointState);
    }
}
