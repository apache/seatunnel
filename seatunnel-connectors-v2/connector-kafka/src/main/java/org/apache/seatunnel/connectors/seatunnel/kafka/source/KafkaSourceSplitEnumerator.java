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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class KafkaSourceSplitEnumerator
        implements SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> {

    private static final String CLIENT_ID_PREFIX = "seatunnel";

    private final Map<TablePath, ConsumerMetadata> tablePathMetadataMap;
    private final Context<KafkaSourceSplit> context;
    private final long discoveryIntervalMillis;
    private final AdminClient adminClient;
    private final KafkaSourceConfig kafkaSourceConfig;
    private final Map<TopicPartition, KafkaSourceSplit> pendingSplit;
    private final Map<TopicPartition, KafkaSourceSplit> assignedSplit;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledFuture;

    private final Map<String, TablePath> topicMappingTablePathMap = new HashMap<>();

    KafkaSourceSplitEnumerator(
            KafkaSourceConfig kafkaSourceConfig,
            Context<KafkaSourceSplit> context,
            KafkaSourceState sourceState) {
        this.kafkaSourceConfig = kafkaSourceConfig;
        this.tablePathMetadataMap = kafkaSourceConfig.getMapMetadata();
        this.context = context;
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
        this.adminClient = initAdminClient(this.kafkaSourceConfig.getProperties());
        this.discoveryIntervalMillis = kafkaSourceConfig.getDiscoveryIntervalMillis();
    }

    @VisibleForTesting
    protected KafkaSourceSplitEnumerator(
            AdminClient adminClient,
            Map<TopicPartition, KafkaSourceSplit> pendingSplit,
            Map<TopicPartition, KafkaSourceSplit> assignedSplit) {
        this.tablePathMetadataMap = new HashMap<>();
        this.context = null;
        this.discoveryIntervalMillis = -1;
        this.adminClient = adminClient;
        this.kafkaSourceConfig = null;
        this.pendingSplit = pendingSplit;
        this.assignedSplit = assignedSplit;
    }

    @Override
    public void open() {
        if (discoveryIntervalMillis > 0) {
            this.executor =
                    Executors.newScheduledThreadPool(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setDaemon(true);
                                thread.setName("kafka-partition-dynamic-discovery");
                                return thread;
                            });
            this.scheduledFuture =
                    executor.scheduleWithFixedDelay(
                            () -> {
                                try {
                                    discoverySplits();
                                } catch (Exception e) {
                                    log.error("Dynamic discovery failure:", e);
                                }
                            },
                            discoveryIntervalMillis,
                            discoveryIntervalMillis,
                            TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        fetchPendingPartitionSplit();
        setPartitionStartOffset();
        assignSplit();
    }

    private void setPartitionStartOffset() throws ExecutionException, InterruptedException {
        Set<TopicPartition> pendingTopicPartitions = pendingSplit.keySet();
        Map<TopicPartition, Long> topicPartitionOffsets = new HashMap<>();
        // Set kafka TopicPartition based on the topicPath granularity
        Map<TablePath, Set<TopicPartition>> tablePathPartitionMap =
                pendingTopicPartitions.stream()
                        .collect(
                                Collectors.groupingBy(
                                        tp -> topicMappingTablePathMap.get(tp.topic()),
                                        Collectors.toSet()));
        for (TablePath tablePath : tablePathPartitionMap.keySet()) {
            // Supports topic list fine-grained Settings for kafka consumer configurations
            ConsumerMetadata metadata = tablePathMetadataMap.get(tablePath);
            Set<TopicPartition> topicPartitions = tablePathPartitionMap.get(tablePath);
            switch (metadata.getStartMode()) {
                case EARLIEST:
                    topicPartitionOffsets.putAll(
                            listOffsets(topicPartitions, OffsetSpec.earliest()));
                    break;
                case GROUP_OFFSETS:
                    topicPartitionOffsets.putAll(listConsumerGroupOffsets(topicPartitions));
                    break;
                case LATEST:
                    topicPartitionOffsets.putAll(listOffsets(topicPartitions, OffsetSpec.latest()));
                    break;
                case TIMESTAMP:
                    topicPartitionOffsets.putAll(
                            listOffsets(
                                    topicPartitions,
                                    OffsetSpec.forTimestamp(metadata.getStartOffsetsTimestamp())));
                    break;
                case SPECIFIC_OFFSETS:
                    topicPartitionOffsets.putAll(metadata.getSpecificStartOffsets());
                    break;
                default:
                    break;
            }
        }

        topicPartitionOffsets.forEach(
                (key, value) -> {
                    if (pendingSplit.containsKey(key)) {
                        pendingSplit.get(key).setStartOffset(value);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        if (this.adminClient != null) {
            adminClient.close();
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    @Override
    public void addSplitsBack(List<KafkaSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            Map<TopicPartition, ? extends KafkaSourceSplit> nextSplit = convertToNextSplit(splits);
            // remove them from the assignedSplit, so we can reassign them
            nextSplit.keySet().forEach(assignedSplit::remove);
            pendingSplit.putAll(nextSplit);
        }
    }

    private Map<TopicPartition, ? extends KafkaSourceSplit> convertToNextSplit(
            List<KafkaSourceSplit> splits) {
        try {
            Map<TopicPartition, Long> listOffsets =
                    listOffsets(
                            splits.stream()
                                    .map(KafkaSourceSplit::getTopicPartition)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList()),
                            OffsetSpec.latest());
            splits.forEach(
                    split -> {
                        split.setStartOffset(split.getEndOffset() + 1);
                        split.setEndOffset(listOffsets.get(split.getTopicPartition()));
                    });
            return splits.stream()
                    .collect(Collectors.toMap(KafkaSourceSplit::getTopicPartition, split -> split));
        } catch (Exception e) {
            throw new KafkaConnectorException(
                    KafkaConnectorErrorCode.ADD_SPLIT_BACK_TO_ENUMERATOR_FAILED, e);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Do nothing because Kafka source push split.
    }

    @Override
    public void registerReader(int subtaskId) {
        /*        if (!pendingSplit.isEmpty()) {
            assignSplit();
        }*/
    }

    @Override
    public KafkaSourceState snapshotState(long checkpointId) throws Exception {
        return new KafkaSourceState(new HashSet<>(assignedSplit.values()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Do nothing
    }

    private AdminClient initAdminClient(Properties properties) {
        Properties props = new Properties();
        if (properties != null) {
            props.putAll(properties);
        }
        props.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceConfig.getBootstrap());
        if (properties.get("client.id") != null) {
            props.setProperty(
                    ConsumerConfig.CLIENT_ID_CONFIG, properties.get("client.id").toString());
        } else {
            props.setProperty(
                    ConsumerConfig.CLIENT_ID_CONFIG,
                    CLIENT_ID_PREFIX + "-enumerator-admin-client-" + this.hashCode());
        }

        return AdminClient.create(props);
    }

    private Set<KafkaSourceSplit> getTopicInfo() throws ExecutionException, InterruptedException {
        Collection<String> topics = new HashSet<>();
        for (TablePath tablePath : tablePathMetadataMap.keySet()) {
            ConsumerMetadata metadata = tablePathMetadataMap.get(tablePath);
            Set<String> currentPathTopics = new HashSet<>();
            if (metadata.isPattern()) {
                Pattern pattern = Pattern.compile(metadata.getTopic());
                currentPathTopics.addAll(
                        this.adminClient.listTopics().names().get().stream()
                                .filter(t -> pattern.matcher(t).matches())
                                .collect(Collectors.toSet()));
            } else {
                currentPathTopics.addAll(Arrays.asList(metadata.getTopic().split(",")));
            }
            currentPathTopics.forEach(topic -> topicMappingTablePathMap.put(topic, tablePath));
            topics.addAll(currentPathTopics);
        }
        log.info("Discovered topics: {}", topics);
        Collection<TopicPartition> partitions =
                adminClient.describeTopics(topics).all().get().values().stream()
                        .flatMap(
                                t ->
                                        t.partitions().stream()
                                                .map(
                                                        p ->
                                                                new TopicPartition(
                                                                        t.name(), p.partition())))
                        .collect(Collectors.toSet());
        Map<TopicPartition, Long> latestOffsets = listOffsets(partitions, OffsetSpec.latest());
        return partitions.stream()
                .map(
                        partition -> {
                            // Obtain the corresponding topic TablePath from kafka topic
                            TablePath tablePath = topicMappingTablePathMap.get(partition.topic());
                            KafkaSourceSplit split = new KafkaSourceSplit(tablePath, partition);
                            split.setEndOffset(latestOffsets.get(split.getTopicPartition()));
                            return split;
                        })
                .collect(Collectors.toSet());
    }

    private synchronized void assignSplit() {
        Map<Integer, List<KafkaSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID = 0; taskID < context.currentParallelism(); taskID++) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }

        pendingSplit.forEach(
                (key, value) -> {
                    if (!assignedSplit.containsKey(key)) {
                        readySplit.get(getSplitOwner(key, context.currentParallelism())).add(value);
                    }
                });

        readySplit.forEach(
                (id, split) -> {
                    context.assignSplit(id, split);
                    if (discoveryIntervalMillis <= 0) {
                        context.signalNoMoreSplits(id);
                    }
                });

        assignedSplit.putAll(pendingSplit);
        pendingSplit.clear();
    }

    private static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
        return (startIndex + tp.partition()) % numReaders;
    }

    private Map<TopicPartition, Long> listOffsets(
            Collection<TopicPartition> partitions, OffsetSpec offsetSpec)
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> topicPartitionOffsets =
                partitions.stream()
                        .collect(Collectors.toMap(partition -> partition, __ -> offsetSpec));

        return adminClient
                .listOffsets(topicPartitionOffsets)
                .all()
                .thenApply(
                        result -> {
                            Map<TopicPartition, Long> offsets = new HashMap<>();
                            result.forEach(
                                    (tp, offsetsResultInfo) -> {
                                        if (offsetsResultInfo != null) {
                                            offsets.put(tp, offsetsResultInfo.offset());
                                        }
                                    });
                            return offsets;
                        })
                .get();
    }

    public Map<TopicPartition, Long> listConsumerGroupOffsets(Collection<TopicPartition> partitions)
            throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsOptions options =
                new ListConsumerGroupOffsetsOptions().topicPartitions(new ArrayList<>(partitions));
        return adminClient
                .listConsumerGroupOffsets(kafkaSourceConfig.getConsumerGroup(), options)
                .partitionsToOffsetAndMetadata()
                .thenApply(
                        result -> {
                            Map<TopicPartition, Long> offsets = new HashMap<>();
                            result.forEach(
                                    (tp, oam) -> {
                                        if (oam != null) {
                                            offsets.put(tp, oam.offset());
                                        }
                                    });
                            return offsets;
                        })
                .get();
    }

    private void discoverySplits() throws ExecutionException, InterruptedException {
        fetchPendingPartitionSplit();
        assignSplit();
    }

    private void fetchPendingPartitionSplit() throws ExecutionException, InterruptedException {
        getTopicInfo()
                .forEach(
                        split -> {
                            if (!assignedSplit.containsKey(split.getTopicPartition())) {
                                if (!pendingSplit.containsKey(split.getTopicPartition())) {
                                    pendingSplit.put(split.getTopicPartition(), split);
                                }
                            }
                        });
    }
}
