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

package org.apache.seatunnel.connectors.seatunnel.easysearch.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

public class EasysearchSource
        implements SeaTunnelSource<SeaTunnelRow, EasysearchSourceSplit, EasysearchSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig pluginConfig;
    private final List<String> source;
    private final CatalogTable catalogTable;

    public EasysearchSource(
            ReadonlyConfig pluginConfig, List<String> source, CatalogTable catalogTable) {
        this.pluginConfig = pluginConfig;
        this.source = source;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "Easysearch";
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
    public SourceReader<SeaTunnelRow, EasysearchSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new EasysearchSourceReader(
                readerContext, pluginConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<EasysearchSourceSplit, EasysearchSourceState> createEnumerator(
            SourceSplitEnumerator.Context<EasysearchSourceSplit> enumeratorContext) {
        return new EasysearchSourceSplitEnumerator(enumeratorContext, pluginConfig, source);
    }

    @Override
    public SourceSplitEnumerator<EasysearchSourceSplit, EasysearchSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<EasysearchSourceSplit> enumeratorContext,
            EasysearchSourceState sourceState) {
        return new EasysearchSourceSplitEnumerator(
                enumeratorContext, sourceState, pluginConfig, source);
    }
}
