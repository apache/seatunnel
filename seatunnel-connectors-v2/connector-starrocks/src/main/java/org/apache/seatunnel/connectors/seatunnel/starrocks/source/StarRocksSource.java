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

import java.util.Collections;
import java.util.List;

public class StarRocksSource
        implements SeaTunnelSource<SeaTunnelRow, StarRocksSourceSplit, StarRocksSourceState> {

    private CatalogTable catalogTable;
    private SourceConfig sourceConfig;

    @Override
    public String getPluginName() {
        return StarRocksBaseOptions.CONNECTOR_IDENTITY;
    }

    public StarRocksSource(SourceConfig sourceConfig, CatalogTable catalogTable) {
        this.sourceConfig = sourceConfig;
        this.catalogTable = catalogTable;
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
    public SourceReader createReader(SourceReader.Context readerContext) {
        return new StarRocksSourceReader(
                readerContext, catalogTable.getSeaTunnelRowType(), sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<StarRocksSourceSplit, StarRocksSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> enumeratorContext,
            StarRocksSourceState checkpointState)
            throws Exception {
        return new StartRocksSourceSplitEnumerator(
                enumeratorContext,
                sourceConfig,
                catalogTable.getSeaTunnelRowType(),
                checkpointState);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) {
        return new StartRocksSourceSplitEnumerator(
                enumeratorContext, sourceConfig, catalogTable.getSeaTunnelRowType());
    }
}
