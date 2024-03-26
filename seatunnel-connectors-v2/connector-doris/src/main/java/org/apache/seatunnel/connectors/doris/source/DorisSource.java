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

package org.apache.seatunnel.connectors.doris.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.source.reader.DorisSourceReader;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplit;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplitEnumerator;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class DorisSource
        implements SeaTunnelSource<SeaTunnelRow, DorisSourceSplit, DorisSourceState> {

    private static final long serialVersionUID = 6139826339248788618L;
    private final DorisConfig config;
    private final CatalogTable catalogTable;

    public DorisSource(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = DorisConfig.of(config);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "Doris";
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
    public SourceReader<SeaTunnelRow, DorisSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new DorisSourceReader(readerContext, config, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<DorisSourceSplit, DorisSourceState> createEnumerator(
            SourceSplitEnumerator.Context<DorisSourceSplit> enumeratorContext) {
        return new DorisSourceSplitEnumerator(
                enumeratorContext, config, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<DorisSourceSplit, DorisSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<DorisSourceSplit> enumeratorContext,
            DorisSourceState checkpointState) {
        return new DorisSourceSplitEnumerator(
                enumeratorContext, config, catalogTable.getSeaTunnelRowType(), checkpointState);
    }
}
