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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.StarRocksQueryPlanReadClient;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.model.QueryPartition;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class StartRocksSourceSplitEnumerator
        implements SourceSplitEnumerator<StarRocksSourceSplit, StarRocksSourceState> {
    private SourceConfig sourceConfig;
    private StarRocksQueryPlanReadClient starRocksQueryPlanReadClient;
    private final Map<Integer, List<StarRocksSourceSplit>> pendingSplit;

    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;
    private final Context<StarRocksSourceSplit> context;

    public StartRocksSourceSplitEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> context,
            SourceConfig sourceConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this(context, sourceConfig, seaTunnelRowType, null);
    }

    public StartRocksSourceSplitEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> context,
            SourceConfig sourceConfig,
            SeaTunnelRowType seaTunnelRowType,
            StarRocksSourceState sourceState) {
        this.sourceConfig = sourceConfig;
        this.starRocksQueryPlanReadClient =
                new StarRocksQueryPlanReadClient(sourceConfig, seaTunnelRowType);

        this.context = context;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<StarRocksSourceSplit> newSplits = getStarRocksSourceSplit();

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
            return new StarRocksSourceState(shouldEnumerate, pendingSplit);
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
        throw new StarRocksConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
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
                        String.join(
                                ",",
                                assignmentForReader.stream()
                                        .map(p -> p.getSplitId())
                                        .collect(Collectors.toList())),
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

    List<StarRocksSourceSplit> getStarRocksSourceSplit() {
        List<StarRocksSourceSplit> sourceSplits = new ArrayList<>();
        List<QueryPartition> partitions = starRocksQueryPlanReadClient.findPartitions();
        for (QueryPartition partition : partitions) {
            sourceSplits.add(
                    new StarRocksSourceSplit(partition, String.valueOf(partition.hashCode())));
        }
        return sourceSplits;
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
