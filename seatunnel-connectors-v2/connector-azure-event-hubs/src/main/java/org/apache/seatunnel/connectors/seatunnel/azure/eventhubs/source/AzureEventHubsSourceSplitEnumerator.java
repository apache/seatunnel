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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.HashUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Discovers Event Hubs partitions once and assigns each partition to a stable reader owner. */
@Slf4j
public class AzureEventHubsSourceSplitEnumerator
        implements SourceSplitEnumerator<AzureEventHubsSourceSplit, AzureEventHubsSourceState> {

    private final AzureEventHubsSourceConfig config;
    private final Context<AzureEventHubsSourceSplit> context;
    private final EventHubsConsumerFactory consumerFactory;
    private final Set<AzureEventHubsSourceSplit> pendingSplits = new HashSet<>();
    private final boolean restored;

    private boolean initialized;

    public AzureEventHubsSourceSplitEnumerator(
            AzureEventHubsSourceConfig config,
            Context<AzureEventHubsSourceSplit> context,
            EventHubsConsumerFactory consumerFactory,
            AzureEventHubsSourceState restoredState) {
        this.config = config;
        this.context = context;
        this.consumerFactory = consumerFactory;
        this.restored = restoredState != null;
        if (restored) {
            pendingSplits.addAll(restoredState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        // Discovery uses a short-lived client in run(); restored enumerators skip discovery.
    }

    @Override
    public synchronized void run() {
        if (!initialized) {
            if (!restored) {
                pendingSplits.addAll(discoverSplits());
            }
            initialized = true;
        }
        assignSplits();
    }

    private List<AzureEventHubsSourceSplit> discoverSplits() {
        List<AzureEventHubsSourceSplit> splits = new ArrayList<>();
        try (EventHubsConsumer consumer = consumerFactory.create(config)) {
            List<String> partitionIds = consumer.partitionIds();
            partitionIds.sort(Comparator.naturalOrder());
            for (String partitionId : partitionIds) {
                long sequenceNumber =
                        consumer.initialSequenceNumber(partitionId, config.getStartMode());
                splits.add(
                        new AzureEventHubsSourceSplit(
                                config.getEventHubName(), partitionId, sequenceNumber));
            }
        }
        if (splits.isEmpty()) {
            throw new IllegalStateException(
                    "Azure Event Hub '" + config.getEventHubName() + "' has no partitions");
        }
        log.info(
                "Discovered {} Event Hubs partition(s) for {} with start_mode={}",
                splits.size(),
                config.getEventHubName(),
                config.getStartMode());
        return splits;
    }

    private void assignSplits() {
        if (!initialized) {
            return;
        }
        int parallelism = context.currentParallelism();
        Set<Integer> registeredReaders = context.registeredReaders();
        Map<Integer, List<AzureEventHubsSourceSplit>> assignments = new HashMap<>();
        for (AzureEventHubsSourceSplit split : pendingSplits) {
            int owner = splitOwner(split, parallelism);
            if (registeredReaders.contains(owner)) {
                assignments.computeIfAbsent(owner, ignored -> new ArrayList<>()).add(split);
            }
        }
        assignments.forEach(
                (subtaskId, splits) -> {
                    splits.sort(Comparator.comparing(AzureEventHubsSourceSplit::getPartitionId));
                    context.assignSplit(subtaskId, splits);
                    pendingSplits.removeAll(splits);
                });
    }

    static int splitOwner(AzureEventHubsSourceSplit split, int parallelism) {
        int hash = split.getEventHubName().hashCode() * 31 + split.getPartitionId().hashCode();
        return HashUtils.bucketIndex(hash, parallelism);
    }

    @Override
    public synchronized void addSplitsBack(List<AzureEventHubsSourceSplit> splits, int subtaskId) {
        if (splits == null || splits.isEmpty()) {
            return;
        }
        pendingSplits.removeAll(splits);
        pendingSplits.addAll(splits);
        assignSplits();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new UnsupportedOperationException(
                "Azure Event Hubs source does not support split requests: " + subtaskId);
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        assignSplits();
    }

    @Override
    public synchronized AzureEventHubsSourceState snapshotState(long checkpointId) {
        return new AzureEventHubsSourceState(pendingSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Reader split state, not an external checkpoint store, owns partition positions.
    }

    @Override
    public void close() throws IOException {
        // No long-lived enumerator resources.
    }
}
