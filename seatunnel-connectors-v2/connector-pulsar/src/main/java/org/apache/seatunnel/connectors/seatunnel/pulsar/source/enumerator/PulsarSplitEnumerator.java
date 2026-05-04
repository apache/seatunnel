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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarAdminConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.PulsarConsumerMetadata;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.SubscriptionStartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.LatestMessageStopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.MultiTablePartitionDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.PulsarDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicPatternDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.admin.PulsarAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PulsarSplitEnumerator
        implements SourceSplitEnumerator<PulsarPartitionSplit, PulsarSplitEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSplitEnumerator.class);

    private final SourceSplitEnumerator.Context<PulsarPartitionSplit> context;
    private final PulsarAdminConfig adminConfig;
    private final PulsarDiscoverer partitionDiscoverer;
    private final long partitionDiscoveryIntervalMs;
    private final Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap;
    private final Boundedness boundedness;
    private final Object stateLock = new Object();

    private final Set<TopicPartition> assignedPartitions;
    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be
     * ready.
     */
    private final Map<Integer, Set<PulsarPartitionSplit>> pendingPartitionSplits;

    private PulsarAdmin pulsarAdmin;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;
    private volatile boolean initialized = false;

    private ScheduledThreadPoolExecutor executor = null;

    public PulsarSplitEnumerator(
            SourceSplitEnumerator.Context<PulsarPartitionSplit> context,
            PulsarAdminConfig adminConfig,
            PulsarDiscoverer partitionDiscoverer,
            long partitionDiscoveryIntervalMs,
            Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap,
            Boundedness boundedness) {
        this(
                context,
                adminConfig,
                partitionDiscoverer,
                partitionDiscoveryIntervalMs,
                consumerMetadataMap,
                boundedness,
                Collections.emptySet());
    }

    public PulsarSplitEnumerator(
            SourceSplitEnumerator.Context<PulsarPartitionSplit> context,
            PulsarAdminConfig adminConfig,
            PulsarDiscoverer partitionDiscoverer,
            long partitionDiscoveryIntervalMs,
            Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap,
            Boundedness boundedness,
            Set<TopicPartition> assignedPartitions) {
        this.context = context;
        this.adminConfig = adminConfig;
        this.partitionDiscoverer = partitionDiscoverer;
        this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
        this.consumerMetadataMap = consumerMetadataMap;
        this.boundedness = boundedness;
        this.assignedPartitions = new HashSet<>(assignedPartitions);
        this.pendingPartitionSplits = new HashMap<>();
    }

    @Override
    public void open() {
        this.pulsarAdmin = PulsarConfigUtil.createAdmin(adminConfig);
        if (partitionDiscoveryIntervalMs > 0) {
            executor =
                    new ScheduledThreadPoolExecutor(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setDaemon(true);
                                thread.setName("pulsar-split-discovery-executor");
                                return thread;
                            });
            executor.scheduleAtFixedRate(
                    () -> {
                        if (!initialized) {
                            return;
                        }
                        try {
                            discoverySplits();
                        } catch (Exception e) {
                            LOG.error("Dynamic discovery failure:", e);
                        }
                    },
                    partitionDiscoveryIntervalMs,
                    partitionDiscoveryIntervalMs,
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() throws Exception {
        // Run one discovery synchronously so topic-pattern conflicts and initial splits are
        // resolved on the runtime side before periodic discovery begins.
        discoverySplits();
        initialized = true;
    }

    private void discoverySplits() {
        synchronized (stateLock) {
            Set<TopicPartition> subscribedTopicPartitions =
                    partitionDiscoverer.getSubscribedTopicPartitions(pulsarAdmin);
            checkPartitionChanges(subscribedTopicPartitions);
        }
    }

    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions) {
        Set<TopicPartition> newPartitions = getNewPartitions(fetchedPartitions);
        if (partitionDiscoveryIntervalMs <= 0 && !noMoreNewPartitionSplits) {
            LOG.debug("Partition discovery is disabled.");
            noMoreNewPartitionSplits = true;
        }
        if (newPartitions.isEmpty()) {
            return;
        }
        List<PulsarPartitionSplit> newSplits =
                newPartitions.stream()
                        .map(this::createPulsarPartitionSplit)
                        .collect(Collectors.toList());
        addPartitionSplitChangeToPendingAssignments(newSplits);
        assignPendingPartitionSplits(context.registeredReaders());
    }

    private PulsarPartitionSplit createPulsarPartitionSplit(TopicPartition partition) {
        TablePath tablePath = resolveTablePath(partition);
        PulsarConsumerMetadata consumerMetadata = consumerMetadataMap.get(tablePath);
        if (consumerMetadata == null) {
            throw new PulsarConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("No consumer metadata found for table path '%s'", tablePath));
        }
        StopCursor partitionStopCursor = consumerMetadata.getStopCursor().copy();
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(partition, partitionStopCursor, null, tablePath);
        if (partitionStopCursor instanceof LatestMessageStopCursor) {
            ((LatestMessageStopCursor) partitionStopCursor).prepare(pulsarAdmin, partition);
        }
        if (consumerMetadata.getStartCursor() instanceof SubscriptionStartCursor) {
            ((SubscriptionStartCursor) consumerMetadata.getStartCursor())
                    .ensureSubscription(
                            consumerMetadata.getConsumerConfig().getSubscriptionName(),
                            partition,
                            pulsarAdmin);
        }
        return split;
    }

    private Set<TopicPartition> getNewPartitions(Set<TopicPartition> fetchedPartitions) {
        Consumer<TopicPartition> duplicateOrMarkAsRemoved = fetchedPartitions::remove;
        assignedPartitions.forEach(duplicateOrMarkAsRemoved);
        pendingPartitionSplits.forEach(
                (reader, splits) ->
                        splits.forEach(
                                split -> duplicateOrMarkAsRemoved.accept(split.getPartition())));

        if (!fetchedPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", fetchedPartitions);
        }

        return fetchedPartitions;
    }

    private void addPartitionSplitChangeToPendingAssignments(
            Collection<PulsarPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (PulsarPartitionSplit split : newPartitionSplits) {
            int ownerReader = getSplitOwner(split.getPartition(), numReaders);
            pendingPartitionSplits.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
        }
        LOG.debug("Assigned {} to {} readers.", newPartitionSplits, numReaders);
    }

    static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.getTopic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of pulsar partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + tp.getPartition()) % numReaders;
    }

    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            Set<PulsarPartitionSplit> pendingAssignmentForReader =
                    pendingPartitionSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {

                // Mark pending partitions as already assigned
                pendingAssignmentForReader.forEach(
                        split -> assignedPartitions.add(split.getPartition()));

                // Assign pending splits to reader
                LOG.info("Assigning splits to readers {}", pendingAssignmentForReader);
                context.assignSplit(pendingReader, new ArrayList<>(pendingAssignmentForReader));
            }
        }

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
            LOG.debug(
                    "No more PulsarPartitionSplits to assign. Sending NoMoreSplitsEvent to reader {}.",
                    pendingReaders);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    @Override
    public void close() throws IOException {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        addPartitionSplitChangeToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().contains(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingPartitionSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Do nothing because Pulsar source push split.
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.debug("Adding reader {} to PulsarSourceEnumerator.", subtaskId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public PulsarSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new PulsarSplitEnumeratorState(assignedPartitions);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // nothing
    }

    private boolean hasTopicPattern(PulsarDiscoverer discoverer) {
        if (discoverer instanceof TopicPatternDiscoverer) {
            return true;
        }
        return discoverer instanceof MultiTablePartitionDiscoverer
                && ((MultiTablePartitionDiscoverer) discoverer).hasTopicPattern();
    }

    private TablePath resolveTablePath(TopicPartition partition) {
        if (partitionDiscoverer instanceof MultiTablePartitionDiscoverer) {
            return ((MultiTablePartitionDiscoverer) partitionDiscoverer).getTablePath(partition);
        }
        return consumerMetadataMap.keySet().iterator().next();
    }
}
