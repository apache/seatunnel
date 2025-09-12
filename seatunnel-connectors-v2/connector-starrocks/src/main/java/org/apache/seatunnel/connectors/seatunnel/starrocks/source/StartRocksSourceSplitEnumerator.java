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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.StarRocksQueryPlanReadClient;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.model.QueryPartition;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSourceTableConfig;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Slf4j
public class StartRocksSourceSplitEnumerator
        implements SourceSplitEnumerator<StarRocksSourceSplit, StarRocksSourceState> {
    private SourceConfig sourceConfig;
    private StarRocksQueryPlanReadClient starRocksQueryPlanReadClient;
    private final Map<Integer, List<StarRocksSourceSplit>> pendingSplit;
    private final ConcurrentLinkedQueue<String> pendingTables;

    private final Object stateLock = new Object();
    private final Context<StarRocksSourceSplit> context;

    public StartRocksSourceSplitEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> context,
            SourceConfig sourceConfig) {
        this(context, sourceConfig, null);
    }

    public StartRocksSourceSplitEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> context,
            SourceConfig sourceConfig,
            StarRocksSourceState sourceState) {
        this.sourceConfig = sourceConfig;
        this.starRocksQueryPlanReadClient = new StarRocksQueryPlanReadClient(sourceConfig);

        List<String> tables =
                sourceConfig.getTableConfigList().stream()
                        .map(StarRocksSourceTableConfig::getTable)
                        .collect(Collectors.toList());

        this.context = context;
        this.pendingSplit = new HashMap<>();
        this.pendingTables = new ConcurrentLinkedQueue<>(tables);
        if (sourceState != null) {
            this.pendingSplit.putAll(sourceState.getPendingSplit());
            this.pendingTables.addAll(sourceState.getPendingTables());
        }
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                String table = pendingTables.poll();
                log.info("Splitting table {}.", table);
                List<StarRocksSourceSplit> newSplits = getStarRocksSourceSplit(table);
                log.info("Split table {} into {} splits.", table, newSplits.size());
                addPendingSplit(newSplits);
            }
        }
        synchronized (stateLock) {
            assignSplit(readers);
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void addSplitsBack(List<StarRocksSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to StartRocksSourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return this.pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to StartRocksSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public StarRocksSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new StarRocksSourceState(pendingSplit, pendingTables);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    @Override
    public void open() {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw CommonError.unsupportedOperation(
                String.format("SubTask: %d", subtaskId), "handleSplitRequest");
    }

    private void addPendingSplit(Collection<StarRocksSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (StarRocksSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split.getSplitId(), ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<StarRocksSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info(
                        "Assign splits {} to reader {}",
                        assignmentForReader.stream()
                                .map(StarRocksSourceSplit::getSplitId)
                                .collect(Collectors.joining(",")),
                        reader);
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

    List<StarRocksSourceSplit> getStarRocksSourceSplit(String table) {
        List<StarRocksSourceSplit> sourceSplits = new ArrayList<>();
        List<QueryPartition> partitions = starRocksQueryPlanReadClient.findPartitions(table);
        for (int i = 0; i < partitions.size(); i++) {
            sourceSplits.add(
                    new StarRocksSourceSplit(
                            partitions.get(i), String.valueOf(partitions.get(i).hashCode())));
        }
        return sourceSplits;
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
