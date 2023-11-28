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

package org.apache.seatunnel.connectors.seatunnel.easysearch.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.easysearch.client.EasysearchClient;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.source.SourceIndexInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorException;

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

import static org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorErrorCode.UNSUPPORTED_OPERATION;

@Slf4j
public class EasysearchSourceSplitEnumerator
        implements SourceSplitEnumerator<EasysearchSourceSplit, EasysearchSourceState> {

    private final Object stateLock = new Object();
    private final SourceSplitEnumerator.Context<EasysearchSourceSplit> context;
    private final Config pluginConfig;
    private final Map<Integer, List<EasysearchSourceSplit>> pendingSplit;
    private final List<String> source;
    private EasysearchClient ezsClient;
    private volatile boolean shouldEnumerate;

    public EasysearchSourceSplitEnumerator(
            SourceSplitEnumerator.Context<EasysearchSourceSplit> context,
            Config pluginConfig,
            List<String> source) {
        this(context, null, pluginConfig, source);
    }

    public EasysearchSourceSplitEnumerator(
            SourceSplitEnumerator.Context<EasysearchSourceSplit> context,
            EasysearchSourceState sourceState,
            Config pluginConfig,
            List<String> source) {
        this.context = context;
        this.pluginConfig = pluginConfig;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
        this.source = source;
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void open() {
        ezsClient = EasysearchClient.createInstance(pluginConfig);
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<EasysearchSourceSplit> newSplits = getEasysearchSplit();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private void addPendingSplit(Collection<EasysearchSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (EasysearchSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<EasysearchSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private List<EasysearchSourceSplit> getEasysearchSplit() {
        List<EasysearchSourceSplit> splits = new ArrayList<>();
        String scrollTime = SourceConfig.SCROLL_TIME.defaultValue();
        if (pluginConfig.hasPath(SourceConfig.SCROLL_TIME.key())) {
            scrollTime = pluginConfig.getString(SourceConfig.SCROLL_TIME.key());
        }
        int scrollSize = SourceConfig.SCROLL_SIZE.defaultValue();
        if (pluginConfig.hasPath(SourceConfig.SCROLL_SIZE.key())) {
            scrollSize = pluginConfig.getInt(SourceConfig.SCROLL_SIZE.key());
        }
        Map query = SourceConfig.QUERY.defaultValue();
        if (pluginConfig.hasPath(SourceConfig.QUERY.key())) {
            query = (Map) pluginConfig.getAnyRef(SourceConfig.QUERY.key());
        }

        List<IndexDocsCount> indexDocsCounts =
                ezsClient.getIndexDocsCount(pluginConfig.getString(SourceConfig.INDEX.key()));
        indexDocsCounts =
                indexDocsCounts.stream()
                        .filter(x -> x.getDocsCount() != null && x.getDocsCount() > 0)
                        .sorted(Comparator.comparingLong(IndexDocsCount::getDocsCount))
                        .collect(Collectors.toList());
        for (IndexDocsCount indexDocsCount : indexDocsCounts) {
            splits.add(
                    new EasysearchSourceSplit(
                            String.valueOf(indexDocsCount.getIndex().hashCode()),
                            new SourceIndexInfo(
                                    indexDocsCount.getIndex(),
                                    source,
                                    query,
                                    scrollTime,
                                    scrollSize)));
        }
        return splits;
    }

    @Override
    public void close() throws IOException {
        ezsClient.close();
    }

    @Override
    public void addSplitsBack(List<EasysearchSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new EasysearchConnectorException(
                UNSUPPORTED_OPERATION, "Unsupported handleSplitRequest: " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to IoTDBSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public EasysearchSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new EasysearchSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
