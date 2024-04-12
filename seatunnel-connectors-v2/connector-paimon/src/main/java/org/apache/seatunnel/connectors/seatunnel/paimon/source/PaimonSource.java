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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowTypeConverter;

import org.apache.paimon.table.Table;

import java.util.Collections;
import java.util.List;

/** Paimon connector source class. */
public class PaimonSource
        implements SeaTunnelSource<SeaTunnelRow, PaimonSourceSplit, PaimonSourceState> {

    private static final long serialVersionUID = 1L;

    private CatalogTable catalogTable;
    private PaimonConfig sourceConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private Table table;

    @Override
    public String getPluginName() {
        return PaimonConfig.CONNECTOR_IDENTITY;
    }

    public PaimonSource(PaimonConfig sourceConfig, CatalogTable catalogTable, Table paimonTable) {
        this.sourceConfig = sourceConfig;
        this.catalogTable = catalogTable;
        this.table = paimonTable;
        // TODO: Support column projection
        this.seaTunnelRowType = RowTypeConverter.convert(this.table.rowType());
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
    public SourceReader<SeaTunnelRow, PaimonSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new PaimonSourceReader(readerContext, table, seaTunnelRowType);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> createEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext) throws Exception {
        return new PaimonSourceSplitEnumerator(enumeratorContext, table);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext,
            PaimonSourceState checkpointState)
            throws Exception {
        return new PaimonSourceSplitEnumerator(enumeratorContext, table, checkpointState);
    }
}
