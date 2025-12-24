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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import java.util.Collections;
import java.util.List;

public class IoTDBSource
        implements SeaTunnelSource<SeaTunnelRow, IoTDBSourceSplit, IoTDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private CatalogTable catalogTable;
    private ReadonlyConfig pluginConfig;

    public IoTDBSource(CatalogTable catalogTable, ReadonlyConfig pluginConfig) {
        this.catalogTable = catalogTable;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public String getPluginName() {
        return "IoTDB";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, IoTDBSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new IoTDBSourceReader(
                pluginConfig, readerContext, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> createEnumerator(
            SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext) throws Exception {
        return new IoTDBSourceSplitEnumerator(enumeratorContext, pluginConfig);
    }

    @Override
    public SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext,
            IoTDBSourceState checkpointState)
            throws Exception {
        return new IoTDBSourceSplitEnumerator(enumeratorContext, pluginConfig, checkpointState);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }
}
