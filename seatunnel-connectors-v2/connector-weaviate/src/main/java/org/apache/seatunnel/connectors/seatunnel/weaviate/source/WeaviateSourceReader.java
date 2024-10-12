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

package org.apache.seatunnel.connectors.seatunnel.weaviate.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateParameters;

import io.weaviate.client.WeaviateClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public class WeaviateSourceReader implements SourceReader<SeaTunnelRow, WeaviateSourceSplit> {

    private final Deque<WeaviateSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final WeaviateParameters parameters;
    private final Context context;
    private Map<TablePath, CatalogTable> sourceTables;

    private WeaviateClient client;

    private volatile boolean noMoreSplit;

    public WeaviateSourceReader(
            WeaviateParameters parameters,
            Context context,
            Map<TablePath, CatalogTable> sourceTables) {
        this.parameters = parameters;
        this.context = context;
        this.sourceTables = sourceTables;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {}

    @Override
    public List<WeaviateSourceSplit> snapshotState(long checkpointId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public void addSplits(List<WeaviateSourceSplit> splits) {}

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
