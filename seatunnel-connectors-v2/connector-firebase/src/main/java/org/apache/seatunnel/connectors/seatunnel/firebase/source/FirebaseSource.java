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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

public class FirebaseSource
        implements SeaTunnelSource<SeaTunnelRow, FirebaseSourceSplit, FirebaseSourceState> {
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;

    public FirebaseSource(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "Firebase";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, FirebaseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new FirebaseSourceReader(readerContext, config, catalogTable);
    }

    @Override
    public SourceSplitEnumerator<FirebaseSourceSplit, FirebaseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FirebaseSourceSplit> enumeratorContext) throws Exception {
        return new FirebaseSourceSplitEnumerator(enumeratorContext, config);
    }

    @Override
    public SourceSplitEnumerator<FirebaseSourceSplit, FirebaseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FirebaseSourceSplit> enumeratorContext,
            FirebaseSourceState checkpointState)
            throws Exception {
        return new FirebaseSourceSplitEnumerator(enumeratorContext, config, checkpointState);
    }

    @Override
    public Serializer<FirebaseSourceSplit> getSplitSerializer() {
        return new FirebaseSourceSplitSerializer();
    }

    @Override
    public Serializer<FirebaseSourceState> getEnumeratorStateSerializer() {
        return new FirebaseSourceStateSerializer();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }
}
