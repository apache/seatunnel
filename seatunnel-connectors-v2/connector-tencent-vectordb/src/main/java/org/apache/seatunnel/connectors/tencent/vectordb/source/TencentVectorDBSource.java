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

package org.apache.seatunnel.connectors.tencent.vectordb.source;

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
import org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig;
import org.apache.seatunnel.connectors.tencent.vectordb.utils.ConnectorUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TencentVectorDBSource
        implements SeaTunnelSource<
                        SeaTunnelRow, TencentVectorDBSourceSplit, TencentVectorDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    private final ReadonlyConfig config;
    private final Map<TablePath, CatalogTable> sourceTables;

    public TencentVectorDBSource(ReadonlyConfig config) {
        this.config = config;
        ConnectorUtils connectorUtils = new ConnectorUtils(config);
        this.sourceTables = connectorUtils.getSourceTables();
    }

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /**
     * Create source reader, used to produce data.
     *
     * @param readerContext reader context.
     * @return source reader.
     * @throws Exception when create reader failed.
     */
    @Override
    public SourceReader<SeaTunnelRow, TencentVectorDBSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new TencentVectorDBSourceReader(readerContext, config, sourceTables);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(sourceTables.values());
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called only once
     * when start a source.
     *
     * @param enumeratorContext enumerator context.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    @Override
    public SourceSplitEnumerator<TencentVectorDBSourceSplit, TencentVectorDBSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<TencentVectorDBSourceSplit> enumeratorContext)
                    throws Exception {
        return new TencentVectorDBSourceSplitEnumertor(
                enumeratorContext, config, sourceTables, null);
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called when
     * restore from checkpoint.
     *
     * @param enumeratorContext enumerator context.
     * @param checkpointState checkpoint state.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    @Override
    public SourceSplitEnumerator<TencentVectorDBSourceSplit, TencentVectorDBSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<TencentVectorDBSourceSplit> enumeratorContext,
                    TencentVectorDBSourceState checkpointState)
                    throws Exception {
        return new TencentVectorDBSourceSplitEnumertor(
                enumeratorContext, config, sourceTables, checkpointState);
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    @Override
    public String getPluginName() {
        return TencentVectorDBSourceConfig.CONNECTOR_IDENTITY;
    }
}
