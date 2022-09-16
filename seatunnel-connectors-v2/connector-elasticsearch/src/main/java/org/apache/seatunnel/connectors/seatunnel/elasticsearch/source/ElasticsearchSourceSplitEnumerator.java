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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.source.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.source.SourceConfigDeaultConstant;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.SourceIndexInfo;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchSourceSplitEnumerator implements SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState> {

    private SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext;

    private Config pluginConfig;

    private EsRestClient esRestClient;

    public ElasticsearchSourceSplitEnumerator(SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext, Config pluginConfig) {
        this.enumeratorContext = enumeratorContext;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void open() {
        esRestClient = EsRestClient.createInstance(pluginConfig);
    }

    @Override
    public void run() throws Exception {

    }

    @Override
    public void close() throws IOException {
        esRestClient.close();
    }

    @Override
    public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {

    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {
        String scrolllTime = SourceConfigDeaultConstant.SCROLLL_TIME;
        if (pluginConfig.hasPath(SourceConfig.SCROLL_TIME)) {
            scrolllTime = pluginConfig.getString(SourceConfig.SCROLL_TIME);
        }
        int scrollSize = SourceConfigDeaultConstant.SCROLLL_SIZE;
        if (pluginConfig.hasPath(SourceConfig.SCROLL_SIZE)) {
            scrollSize = pluginConfig.getInt(SourceConfig.SCROLL_SIZE);
        }

        List<IndexDocsCount> indexDocsCounts = esRestClient.getIndexDocsCount(pluginConfig.getString(SourceConfig.INDEX));
        indexDocsCounts = indexDocsCounts.stream().filter(x -> x.getDocsCount() != null && x.getDocsCount() > 0)
                .sorted(Comparator.comparingLong(IndexDocsCount::getDocsCount)).collect(Collectors.toList());
        List<ElasticsearchSourceSplit> splits = new ArrayList<>();
        int parallelism = enumeratorContext.currentParallelism();
        List<String> sources = pluginConfig.getStringList(SourceConfig.SOURCE);

        for (int i = 0; i < indexDocsCounts.size(); i++) {
            IndexDocsCount indexDocsCount = indexDocsCounts.get(i);
            if (i % parallelism == subtaskId) {
                splits.add(new ElasticsearchSourceSplit(new SourceIndexInfo(indexDocsCount.getIndex(), sources, scrolllTime, scrollSize), subtaskId));
            }
        }

        enumeratorContext.assignSplit(subtaskId, splits);
        enumeratorContext.signalNoMoreSplits(subtaskId);
    }

    @Override
    public ElasticsearchSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
