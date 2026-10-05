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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.constant.BigtableIdentifier;

import java.util.List;

public class BigtableSource
        implements SeaTunnelSource<SeaTunnelRow, BigtableSourceSplit, BigtableSourceState>,
                SupportParallelism {

    private final CatalogTable catalogTable;
    private final BigtableParameters parameters;

    BigtableSource(BigtableParameters parameters, CatalogTable catalogTable) {
        this.parameters = parameters;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return BigtableIdentifier.IDENTIFIER_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, BigtableSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new BigtableSourceReader(
                parameters, readerContext, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<BigtableSourceSplit, BigtableSourceState> createEnumerator(
            SourceSplitEnumerator.Context<BigtableSourceSplit> enumeratorContext) throws Exception {
        return new BigtableSourceSplitEnumerator(enumeratorContext, parameters);
    }

    @Override
    public SourceSplitEnumerator<BigtableSourceSplit, BigtableSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<BigtableSourceSplit> enumeratorContext,
            BigtableSourceState checkpointState)
            throws Exception {
        return new BigtableSourceSplitEnumerator(enumeratorContext, parameters, checkpointState);
    }
}
