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

package org.apache.seatunnel.connectors.bigquery.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySourceOptions;

import java.util.Collections;
import java.util.List;

public class BigQuerySource
        implements SeaTunnelSource<SeaTunnelRow, BigQuerySourceSplit, BigQuerySourceState> {

    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private final String query;

    public BigQuerySource(ReadonlyConfig config, CatalogTable catalogTable, String query) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.query = query;
    }

    @Override
    public String getPluginName() {
        return BigQuerySourceOptions.IDENTIFIER;
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
    public SourceReader<SeaTunnelRow, BigQuerySourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new BigQuerySourceReader(readerContext, config);
    }

    @Override
    public SourceSplitEnumerator<BigQuerySourceSplit, BigQuerySourceState> createEnumerator(
            SourceSplitEnumerator.Context<BigQuerySourceSplit> enumeratorContext) throws Exception {
        return new BigQuerySourceSplitEnumerator(enumeratorContext, query, null);
    }

    @Override
    public SourceSplitEnumerator<BigQuerySourceSplit, BigQuerySourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<BigQuerySourceSplit> enumeratorContext,
            BigQuerySourceState checkpointState)
            throws Exception {
        return new BigQuerySourceSplitEnumerator(enumeratorContext, query, checkpointState);
    }
}
