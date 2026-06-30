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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.enumerator.VitessSourceEnumerator;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.enumerator.VitessSourceEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.reader.VitessSourceReader;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;

import java.util.Collections;
import java.util.List;

/** SeaTunnel source implementation for Vitess CDC. */
public class VitessSource
        implements SeaTunnelSource<SeaTunnelRow, VitessSourceSplit, VitessSourceEnumeratorState> {

    public static final String IDENTIFIER = "Vitess-CDC";

    /** Immutable connector configuration used by reader and enumerator. */
    private final VitessSourceConfig sourceConfig;

    public VitessSource(VitessSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceConfig.getCatalogTables();
    }

    @Override
    public SourceReader<SeaTunnelRow, VitessSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new VitessSourceReader(readerContext, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<VitessSourceSplit, VitessSourceEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<VitessSourceSplit> enumeratorContext) {
        return new VitessSourceEnumerator(
                enumeratorContext,
                new VitessSourceEnumeratorState(
                        Collections.singletonList(sourceConfig.createInitialSplit())));
    }

    @Override
    public SourceSplitEnumerator<VitessSourceSplit, VitessSourceEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<VitessSourceSplit> enumeratorContext,
            VitessSourceEnumeratorState checkpointState) {
        return new VitessSourceEnumerator(enumeratorContext, checkpointState);
    }
}
