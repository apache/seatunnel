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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.convert.MilvusConvertUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MilvusSource
        implements SeaTunnelSource<SeaTunnelRow, MilvusSourceSplit, MilvusSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;
    private final Map<TablePath, CatalogTable> sourceTables;

    public MilvusSource(ReadonlyConfig sourceConfig) {
        this.config = sourceConfig;
        this.sourceTables = MilvusConvertUtils.getSourceTables(config);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(sourceTables.values());
    }

    @Override
    public SourceReader<SeaTunnelRow, MilvusSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new MilvusSourceReader(readerContext, config, sourceTables);
    }

    @Override
    public SourceSplitEnumerator<MilvusSourceSplit, MilvusSourceState> createEnumerator(
            SourceSplitEnumerator.Context<MilvusSourceSplit> context) throws Exception {
        return new MilvusSourceSplitEnumertor(context, config, sourceTables, null);
    }

    @Override
    public SourceSplitEnumerator<MilvusSourceSplit, MilvusSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<MilvusSourceSplit> context,
            MilvusSourceState checkpointState)
            throws Exception {
        return new MilvusSourceSplitEnumertor(context, config, sourceTables, checkpointState);
    }

    @Override
    public String getPluginName() {
        return MilvusSourceConfig.CONNECTOR_IDENTITY;
    }
}
