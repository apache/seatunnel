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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSourceTableConfig;

import java.util.List;
import java.util.stream.Collectors;

public class StarRocksSource
        implements SeaTunnelSource<SeaTunnelRow, StarRocksSourceSplit, StarRocksSourceState> {

    private SourceConfig sourceConfig;

    @Override
    public String getPluginName() {
        return StarRocksBaseOptions.CONNECTOR_IDENTITY;
    }

    public StarRocksSource(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceConfig.getTableConfigList().stream()
                .map(StarRocksSourceTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) {
        return new StarRocksSourceReader(readerContext, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<StarRocksSourceSplit, StarRocksSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> enumeratorContext,
            StarRocksSourceState checkpointState)
            throws Exception {
        return new StartRocksSourceSplitEnumerator(
                enumeratorContext, sourceConfig, checkpointState);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) {
        return new StartRocksSourceSplitEnumerator(enumeratorContext, sourceConfig);
    }
}
