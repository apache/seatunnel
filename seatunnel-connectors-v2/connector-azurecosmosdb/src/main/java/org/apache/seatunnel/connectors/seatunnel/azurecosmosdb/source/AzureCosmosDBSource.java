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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import java.util.Collections;
import java.util.List;

public class AzureCosmosDBSource
        implements SeaTunnelSource<
                SeaTunnelRow, AzureCosmosDBSourceSplit, AzureCosmosDBSourceState> {

    private final AzureCosmosDBConfig config;
    private final CatalogTable catalogTable;

    public AzureCosmosDBSource(AzureCosmosDBConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "AzureCosmosDB";
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
    public SourceSplitEnumerator<AzureCosmosDBSourceSplit, AzureCosmosDBSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<AzureCosmosDBSourceSplit> enumeratorContext)
                    throws Exception {
        return new AzureCosmosDBSourceSplitEnumerator(enumeratorContext, null);
    }

    @Override
    public SourceSplitEnumerator<AzureCosmosDBSourceSplit, AzureCosmosDBSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<AzureCosmosDBSourceSplit> enumeratorContext,
                    AzureCosmosDBSourceState checkpointState)
                    throws Exception {
        return new AzureCosmosDBSourceSplitEnumerator(enumeratorContext, checkpointState);
    }

    @Override
    public SourceReader<SeaTunnelRow, AzureCosmosDBSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new AzureCosmosDBSourceReader(
                readerContext, config, catalogTable.getSeaTunnelRowType());
    }
}
