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

package org.apache.seatunnel.e2e.source.inmemory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

public class InMemorySource
        implements SeaTunnelSource<SeaTunnelRow, InMemorySourceSplit, InMemoryState> {

    private final ReadonlyConfig config;

    public InMemorySource(ReadonlyConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "InMemorySource";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("e2e", TablePath.DEFAULT),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "InMemorySource"));
    }

    @Override
    public SourceReader<SeaTunnelRow, InMemorySourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new InMemorySourceReader(Collections.emptyList(), readerContext);
    }

    @Override
    public SourceSplitEnumerator<InMemorySourceSplit, InMemoryState> createEnumerator(
            SourceSplitEnumerator.Context<InMemorySourceSplit> enumeratorContext) {
        return new InMemorySourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<InMemorySourceSplit, InMemoryState> restoreEnumerator(
            SourceSplitEnumerator.Context<InMemorySourceSplit> enumeratorContext,
            InMemoryState checkpointState) {
        return new InMemorySourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
}
