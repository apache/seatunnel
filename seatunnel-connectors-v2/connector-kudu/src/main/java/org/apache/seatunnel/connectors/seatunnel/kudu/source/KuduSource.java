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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSourceState;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class KuduSource
        implements SeaTunnelSource<SeaTunnelRow, KuduSourceSplit, KuduSourceState>,
                SupportParallelism {
    private KuduSourceConfig kuduSourceConfig;

    public KuduSource(KuduSourceConfig kuduSourceConfig) {
        this.kuduSourceConfig = kuduSourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return kuduSourceConfig.getTableConfigList().stream()
                .map(KuduSourceTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, KuduSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new KuduSourceReader(readerContext, kuduSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext) {
        return new KuduSourceSplitEnumerator(enumeratorContext, kuduSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext,
            KuduSourceState checkpointState) {
        return new KuduSourceSplitEnumerator(enumeratorContext, kuduSourceConfig, checkpointState);
    }

    @Override
    public String getPluginName() {
        return "Kudu";
    }
}
