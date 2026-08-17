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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;

import java.util.List;
import java.util.Map;

/**
 * HugeGraph source. Bounded read of one vertex/edge label, or of all labels of a type in one job.
 *
 * <p>Single-label mode (option {@code label} set): at parallelism 1 it pages the label via the
 * server-side list API (server-side label and property-equality filtering); at parallelism &gt; 1
 * it splits the keyspace into shards and scans them in parallel. Read-all mode ({@code label}
 * omitted): one {@code LABEL_LIST} split per discovered label, each producing its own table. See
 * {@link HugeGraphSourceSplitEnumerator}.
 */
public class HugeGraphSource
        implements SeaTunnelSource<SeaTunnelRow, HugeGraphSourceSplit, HugeGraphSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private static final long serialVersionUID = 1L;

    private final List<CatalogTable> catalogTables;
    private final Map<String, LabelTableContext> labelContexts;
    private final HugeGraphSourceConfig sourceConfig;

    public HugeGraphSource(
            List<CatalogTable> catalogTables,
            Map<String, LabelTableContext> labelContexts,
            HugeGraphSourceConfig sourceConfig) {
        this.catalogTables = catalogTables;
        this.labelContexts = labelContexts;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getPluginName() {
        return HugeGraphOptions.PLUGIN_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return catalogTables;
    }

    @Override
    public SourceReader<SeaTunnelRow, HugeGraphSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new HugeGraphSourceReader(readerContext, sourceConfig, labelContexts);
    }

    @Override
    public SourceSplitEnumerator<HugeGraphSourceSplit, HugeGraphSourceState> createEnumerator(
            SourceSplitEnumerator.Context<HugeGraphSourceSplit> enumeratorContext) {
        return new HugeGraphSourceSplitEnumerator(
                enumeratorContext, sourceConfig, sourceConfig.getSplitSize());
    }

    @Override
    public SourceSplitEnumerator<HugeGraphSourceSplit, HugeGraphSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<HugeGraphSourceSplit> enumeratorContext,
            HugeGraphSourceState checkpointState) {
        return new HugeGraphSourceSplitEnumerator(
                enumeratorContext, sourceConfig, sourceConfig.getSplitSize(), checkpointState);
    }
}
