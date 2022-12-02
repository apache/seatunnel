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
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class KafkaSourceSplitEnumerator implements SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> {

    private static final String CLIENT_ID_PREFIX = "seatunnel";

    private final ConsumerMetadata metadata;
    private final Context<KafkaSourceSplit> context;
    private long discoveryIntervalMillis;
    private AdminClient adminClient;

    private Map<TopicPartition, KafkaSourceSplit> pendingSplit;
    private final Map<TopicPartition, KafkaSourceSplit> assignedSplit;
    private ScheduledExecutorService executor;
    private ScheduledFuture scheduledFuture;

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context) {
        this.metadata = metadata;
        this.context = context;
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
    }

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context,
                               KafkaSourceState sourceState) {
        this(metadata, context);
    }

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context,
                               long discoveryIntervalMillis) {
        this(metadata, context);
        this.discoveryIntervalMillis = discoveryIntervalMillis;
    }

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context,
                               KafkaSourceState sourceState, long discoveryIntervalMillis) {
        this(metadata, context, sourceState);
        this.discoveryIntervalMillis = discoveryIntervalMillis;
    }

    @Override
    public void open() {
        this.adminClient = initAdminClient(this.metadata.getProperties());
        if (discoveryIntervalMillis > 0) {
            this.executor = Executors.newScheduledThreadPool(1, runnable -> {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                thread.setName("kafka-partition-dynamic-discovery");
                return thread;
            });
            this.scheduledFuture = executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        discoverySplits();
                    } catch (Exception e) {
                        log.error("Dynamic discovery failure:", e);
                    }
                }, discoveryIntervalMillis, discoveryIntervalMillis, TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        fetchPendingPartitionSplit();
        setPartitionStartOffset();
        assignSplit();
    }

    private void setPartitionStartOffset() throws ExecutionException, InterruptedException {
        Collection<TopicPartition> topicPartitions = pendingSplit.keySet();
        Map<TopicPartition, Long> topicPartitionOffsets = null;
        switch (metadata.getStartMode()) {
            case EARLIEST:
                topicPartitionOffsets = listOffsets(topicPartitions, OffsetSpec.earliest());
                break;
            case GROUP_OFFSETS:
                topicPartitionOffsets = listConsumerGroupOffsets(topicPartitions);
                break;
            case LATEST:
                topicPartitionOffsets = listOffsets(topicPartitions, OffsetSpec.latest());
                break;
            case TIMESTAMP:
                topicPartitionOffsets = listOffsets(topicPartitions, OffsetSpec.forTimestamp(metadata.getStartOffsetsTimestamp()));
                break;
            case SPECIFIC_OFFSETS:
                topicPartitionOffsets = metadata.getSpecificStartOffsets();
                break;
            default:
                break;
        }
        topicPartitionOffsets.entrySet().forEach(entry -> {
            if (pendingSplit.containsKey(entry.getKey())) {
                pendingSplit.get(entry.getKey()).setStartOffset(entry.getValue());
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
            pendingSplit.putAll(convertToNextSplit(splits));
            assignSplit();
        }
    }

    private Map<TopicPartition, ? extends KafkaSourceSplit> convertToNextSplit(List<KafkaSourceSplit> splits) {
        try {
            Map<TopicPartition, Long> listOffsets =
                listOffsets(splits.stream().map(KafkaSourceSplit::getTopicPartition).collect(Collectors.toList()), OffsetSpec.latest());
            splits.forEach(split -> {
                split.setStartOffset(split.getEndOffset() + 1);
                split.setEndOffset(listOffsets.get(split.getTopicPartition()));
            });
            return splits.stream().collect(Collectors.toMap(split -> split.getTopicPartition(), split -> split));
        } catch (Exception e) {
            throw new KafkaConnectorException(KafkaConnectorErrorCode.ADD_SPLIT_BACK_TO_ENUMERATOR_FAILED, e);
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
        if (!pendingSplit.isEmpty()) {
            assignSplit();
        }
    }

    @Override
    public KafkaSourceState snapshotState(long checkpointId) throws Exception {
        return new KafkaSourceState(assignedSplit.values().stream().collect(Collectors.toSet()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Do nothing
    }

    private AdminClient initAdminClient(Properties properties) {
        Properties props = new Properties(properties);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.metadata.getBootstrapServers());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-admin-client-" + this.hashCode());
        return AdminClient.create(props);
    }

    private Set<KafkaSourceSplit> getTopicInfo() throws ExecutionException, InterruptedException {
        Collection<String> topics;
        if (this.metadata.isPattern()) {
            Pattern pattern = Pattern.compile(this.metadata.getTopic());
            topics = this.adminClient.listTopics().names().get().stream()
                .filter(t -> pattern.matcher(t).matches()).collect(Collectors.toSet());
        } else {
            topics = Arrays.asList(this.metadata.getTopic().split(","));
        }
        log.info("Discovered topics: {}", topics);

        Collection<TopicPartition> partitions =
            adminClient.describeTopics(topics).all().get().values().stream().flatMap(t -> t.partitions().stream()
                .map(p -> new TopicPartition(t.name(), p.partition()))).collect(Collectors.toSet());
        Map<TopicPartition, Long> latestOffsets = listOffsets(partitions, OffsetSpec.latest());
        return partitions.stream().map(partition -> {
            KafkaSourceSplit split = new KafkaSourceSplit(partition);
            split.setEndOffset(latestOffsets.get(split.getTopicPartition()));
            return split;
        }).collect(Collectors.toSet());
    }

    private synchronized void assignSplit() {
        Map<Integer, List<KafkaSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID = 0; taskID < context.currentParallelism(); taskID++) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }

        pendingSplit.entrySet().forEach(s -> {
            if (!assignedSplit.containsKey(s.getKey())) {
                readySplit.get(getSplitOwner(s.getKey(), context.currentParallelism()))
                    .add(s.getValue());
            }
        });

        readySplit.forEach(context::assignSplit);

        assignedSplit.putAll(pendingSplit);
        pendingSplit.clear();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
        return (startIndex + tp.partition()) % numReaders;
    }

    private Map<TopicPartition, Long> listOffsets(Collection<TopicPartition> partitions, OffsetSpec offsetSpec) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = partitions.stream().collect(Collectors.toMap(partition -> partition, __ -> offsetSpec));

        return adminClient.listOffsets(topicPartitionOffsets).all()
            .thenApply(
                result -> {
                    Map<TopicPartition, Long>
                        offsets = new HashMap<>();
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

    public Map<TopicPartition, Long> listConsumerGroupOffsets(Collection<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions().topicPartitions(new ArrayList<>(partitions));
        return adminClient
            .listConsumerGroupOffsets(metadata.getConsumerGroup(), options)
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
        getTopicInfo().forEach(split -> {
            if (!assignedSplit.containsKey(split.getTopicPartition())) {
                if (!pendingSplit.containsKey(split.getTopicPartition())) {
                    pendingSplit.put(split.getTopicPartition(), split);
                }
            }
        });
    }
}
