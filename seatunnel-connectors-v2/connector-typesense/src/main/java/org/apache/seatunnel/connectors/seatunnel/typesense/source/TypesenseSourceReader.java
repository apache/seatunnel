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

package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.SourceCollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.source.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.source.SeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.source.TypesenseRecord;

import org.typesense.model.SearchResult;
import org.typesense.model.SearchResultHit;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class TypesenseSourceReader implements SourceReader<SeaTunnelRow, TypesenseSourceSplit> {

    SourceReader.Context context;

    private final ReadonlyConfig config;

    private final SeaTunnelRowDeserializer deserializer;

    private TypesenseClient typesenseClient;

    Deque<TypesenseSourceSplit> splits = new LinkedList<>();

    boolean noMoreSplit;

    private final long pollNextWaitTime = 1000L;

    public TypesenseSourceReader(
            SourceReader.Context context, ReadonlyConfig config, SeaTunnelRowType rowTypeInfo) {
        this.context = context;
        this.config = config;
        this.deserializer = new DefaultSeaTunnelRowDeserializer(rowTypeInfo);
    }

    @Override
    public void open() {
        typesenseClient = TypesenseClient.createInstance(this.config);
    }

    @Override
    public void close() {
        // Nothing , because typesense does not require
    }

    @Override
    public List<TypesenseSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<TypesenseSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            TypesenseSourceSplit split = splits.poll();
            if (split != null) {
                SourceCollectionInfo sourceCollectionInfo = split.getSourceCollectionInfo();
                int pageSize = sourceCollectionInfo.getQueryBatchSize();
                while (true) {
                    SearchResult searchResult =
                            typesenseClient.search(
                                    sourceCollectionInfo.getCollection(),
                                    sourceCollectionInfo.getQuery(),
                                    sourceCollectionInfo.getOffset(),
                                    sourceCollectionInfo.getQueryBatchSize());
                    Integer found = searchResult.getFound();
                    List<SearchResultHit> hits = searchResult.getHits();
                    for (SearchResultHit hit : hits) {
                        Map<String, Object> document = hit.getDocument();
                        SeaTunnelRow seaTunnelRow =
                                deserializer.deserialize(new TypesenseRecord(document));
                        output.collect(seaTunnelRow);
                    }
                    if ((double) found / pageSize - 1
                            > sourceCollectionInfo.getOffset() / pageSize) {
                        sourceCollectionInfo.setOffset(sourceCollectionInfo.getOffset() + pageSize);
                    } else {
                        break;
                    }
                }

            } else if (noMoreSplit) {
                log.info("Closed the bounded Typesense source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(pollNextWaitTime);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
