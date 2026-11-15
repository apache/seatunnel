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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBConfig;

import java.util.Collections;
import java.util.List;

/**
 * Bounded Amazon DocumentDB source for a schema-driven collection scan.
 *
 * <p>V1 intentionally creates one split for the basic read path. Filter and projection are carried
 * by that split, while cursor progress is not checkpointed, so recovery restarts the scan.
 */
public class AmazonDocumentDBSource
        implements SeaTunnelSource<
                SeaTunnelRow, AmazonDocumentDBSourceSplit, AmazonDocumentDBSourceState> {

    private final AmazonDocumentDBConfig config;
    private final CatalogTable catalogTable;

    public AmazonDocumentDBSource(AmazonDocumentDBConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "AmazonDocumentDB";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    /** Starts a fresh single-split enumeration using the configured filter and projection. */
    @Override
    public SourceSplitEnumerator<AmazonDocumentDBSourceSplit, AmazonDocumentDBSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<AmazonDocumentDBSourceSplit> enumeratorContext) {
        return new AmazonDocumentDBSourceSplitEnumerator(
                enumeratorContext, null, config.getMatchQuery(), config.getProjection());
    }

    /** Restores only pending split descriptors; assigned cursor progress cannot be resumed. */
    @Override
    public SourceSplitEnumerator<AmazonDocumentDBSourceSplit, AmazonDocumentDBSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<AmazonDocumentDBSourceSplit> enumeratorContext,
                    AmazonDocumentDBSourceState checkpointState) {
        return new AmazonDocumentDBSourceSplitEnumerator(
                enumeratorContext, checkpointState, config.getMatchQuery(), config.getProjection());
    }

    /** Creates the blocking reader that converts BSON according to the declared catalog schema. */
    @Override
    public SourceReader<SeaTunnelRow, AmazonDocumentDBSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new AmazonDocumentDBSourceReader(
                readerContext, config, catalogTable.getSeaTunnelRowType());
    }
}
