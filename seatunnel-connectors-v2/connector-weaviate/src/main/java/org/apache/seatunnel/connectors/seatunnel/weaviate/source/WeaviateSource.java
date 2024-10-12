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

package org.apache.seatunnel.connectors.seatunnel.weaviate.source;

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
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.seatunnel.weaviate.convert.WeaviateConverter;

import java.util.Map;

public class WeaviateSource
        implements SeaTunnelSource<SeaTunnelRow, WeaviateSourceSplit, WeaviateSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;
    private final Map<TablePath, CatalogTable> sourceTables;

    public WeaviateSource(ReadonlyConfig config) {
        this.config = config;
        this.sourceTables =
                WeaviateConverter.getSourceTables(WeaviateParameters.buildWithConfig(config));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, WeaviateSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new WeaviateSourceReader(
                WeaviateParameters.buildWithConfig(config), readerContext, sourceTables);
    }

    @Override
    public SourceSplitEnumerator<WeaviateSourceSplit, WeaviateSourceState> createEnumerator(
            SourceSplitEnumerator.Context<WeaviateSourceSplit> enumeratorContext) throws Exception {
        return new WeaviateSourceSplitEnumertor(enumeratorContext, sourceTables, null);
    }

    @Override
    public SourceSplitEnumerator<WeaviateSourceSplit, WeaviateSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<WeaviateSourceSplit> enumeratorContext,
            WeaviateSourceState checkpointState)
            throws Exception {
        return new WeaviateSourceSplitEnumertor(enumeratorContext, sourceTables, checkpointState);
    }

    @Override
    public String getPluginName() {
        return WeaviateCommonConfig.CONNECTOR_IDENTITY;
    }
}
