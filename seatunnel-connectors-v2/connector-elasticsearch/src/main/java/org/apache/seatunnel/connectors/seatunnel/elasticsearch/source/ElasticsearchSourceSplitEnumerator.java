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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchSourceSplitEnumerator implements SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState> {

    private SourceSplitEnumerator.Context<ElasticsearchSourceSplit> context;

    private Config pluginConfig;

    private EsRestClient esRestClient;

    private final Object stateLock = new Object();

    private Map<Integer, List<ElasticsearchSourceSplit>> pendingSplit;

    private volatile boolean shouldEnumerate;

    public ElasticsearchSourceSplitEnumerator(SourceSplitEnumerator.Context<ElasticsearchSourceSplit> context, Config pluginConfig) {
        this(context, null, pluginConfig);
    }

    public ElasticsearchSourceSplitEnumerator(SourceSplitEnumerator.Context<ElasticsearchSourceSplit> context, ElasticsearchSourceState sourceState, Config pluginConfig) {
        this.context = context;
        this.pluginConfig = pluginConfig;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {
        esRestClient = EsRestClient.createInstance(pluginConfig);
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<ElasticsearchSourceSplit> newSplits = getElasticsearchSplit();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug("No more splits to assign." +
                " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private void addPendingSplit(Collection<ElasticsearchSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (ElasticsearchSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>())
                    .add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<ElasticsearchSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}",
                        assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error("Failed to assign splits {} to reader {}",
                            assignmentForReader, reader, e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private List<ElasticsearchSourceSplit> getElasticsearchSplit() {
        List<ElasticsearchSourceSplit> splits = new ArrayList<>();
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
        List<String> sources = pluginConfig.getStringList(SourceConfig.SOURCE);
        for (IndexDocsCount indexDocsCount : indexDocsCounts) {
            splits.add(new ElasticsearchSourceSplit(String.valueOf(indexDocsCount.getIndex().hashCode()), new SourceIndexInfo(indexDocsCount.getIndex(), sources, scrolllTime, scrollSize)));
        }
        return splits;
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
        throw new UnsupportedOperationException("Unsupported handleSplitRequest: " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to IoTDBSourceSplitEnumerator.",
                subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public ElasticsearchSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new ElasticsearchSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
