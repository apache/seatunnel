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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;
import org.apache.seatunnel.shade.com.google.common.collect.Sets;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqAdminUtil;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class RocketMqSourceSplitEnumerator
        implements SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> {

    private static final long DEFAULT_DISCOVERY_INTERVAL_MILLIS = 60 * 1000;
    private final Map<MessageQueue, RocketMqSourceSplit> assignedSplit;
    private final ConsumerMetadata metadata;
    private final Context<RocketMqSourceSplit> context;
    private final Map<MessageQueue, RocketMqSourceSplit> pendingSplit;
    private ScheduledExecutorService executor;
    private ScheduledFuture scheduledFuture;
    // ms
    private long discoveryIntervalMillis;

    public RocketMqSourceSplitEnumerator(
            ConsumerMetadata metadata, SourceSplitEnumerator.Context<RocketMqSourceSplit> context) {
        this.metadata = metadata;
        this.context = context;
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
    }

    public RocketMqSourceSplitEnumerator(
            ConsumerMetadata metadata,
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context,
            long discoveryIntervalMillis) {
        this(metadata, context);
        this.discoveryIntervalMillis = discoveryIntervalMillis;
    }

    private static int getSplitOwner(MessageQueue messageQueue, int numReaders) {
        int startIndex = ((messageQueue.getQueueId() * 31) & 0x7FFFFFFF) % numReaders;
        return (startIndex + messageQueue.getQueueId()) % numReaders;
    }

    @Override
    public void open() {
        discoveryIntervalMillis =
                discoveryIntervalMillis > 0
                        ? discoveryIntervalMillis
                        : DEFAULT_DISCOVERY_INTERVAL_MILLIS;
        if (discoveryIntervalMillis > 0) {
            this.executor =
                    Executors.newScheduledThreadPool(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setDaemon(true);
                                thread.setName("RocketMq-messageQueue-dynamic-discovery");
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
    public void run() throws Exception {
        fetchPendingPartitionSplit();
        setPartitionStartOffset();
        assignSplit();
    }

    @Override
    public void close() throws IOException {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    @Override
    public void addSplitsBack(List<RocketMqSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.putAll(convertToNextSplit(splits));
            assignSplit();
        }
    }

    private Map<MessageQueue, ? extends RocketMqSourceSplit> convertToNextSplit(
            List<RocketMqSourceSplit> splits) {
        try {
            Map<MessageQueue, Long> listOffsets =
                    listOffsets(
                            splits.stream()
                                    .map(RocketMqSourceSplit::getMessageQueue)
                                    .collect(Collectors.toList()),
                            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            splits.forEach(
                    split -> {
                        split.setStartOffset(split.getEndOffset() + 1);
                        split.setEndOffset(listOffsets.get(split.getMessageQueue()));
                    });
            return splits.stream()
                    .collect(Collectors.toMap(split -> split.getMessageQueue(), split -> split));
        } catch (Exception e) {
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.ADD_SPLIT_BACK_TO_ENUMERATOR_FAILED, e);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // No-op
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit();
        }
    }

    @Override
    public RocketMqSourceState snapshotState(long checkpointId) throws Exception {
        return new RocketMqSourceState(assignedSplit.values().stream().collect(Collectors.toSet()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No-op
    }

    private void discoverySplits() {
        fetchPendingPartitionSplit();
        assignSplit();
    }

    private void fetchPendingPartitionSplit() {
        getTopicInfo()
                .forEach(
                        split -> {
                            if (!assignedSplit.containsKey(split.getMessageQueue())) {
                                if (!pendingSplit.containsKey(split.getMessageQueue())) {
                                    pendingSplit.put(split.getMessageQueue(), split);
                                }
                            }
                        });
    }

    private Set<RocketMqSourceSplit> getTopicInfo() {
        log.info("Configured topics: {}", metadata.getTopics());
        List<Map<MessageQueue, TopicOffset>> offsetTopics =
                RocketMqAdminUtil.offsetTopics(metadata.getBaseConfig(), metadata.getTopics());
        Set<RocketMqSourceSplit> sourceSplits = Sets.newConcurrentHashSet();
        offsetTopics.forEach(
                messageQueueOffsets -> {
                    messageQueueOffsets.forEach(
                            (messageQueue, topicOffset) -> {
                                sourceSplits.add(
                                        new RocketMqSourceSplit(
                                                messageQueue,
                                                topicOffset.getMinOffset(),
                                                topicOffset.getMaxOffset()));
                            });
                });
        return sourceSplits;
    }

    private void setPartitionStartOffset() throws MQClientException {
        Collection<MessageQueue> topicPartitions = pendingSplit.keySet();
        Map<MessageQueue, Long> topicPartitionOffsets = null;
        switch (metadata.getStartMode()) {
            case CONSUME_FROM_FIRST_OFFSET:
                topicPartitionOffsets =
                        listOffsets(topicPartitions, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                break;
            case CONSUME_FROM_LAST_OFFSET:
                topicPartitionOffsets =
                        listOffsets(topicPartitions, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                break;
            case CONSUME_FROM_TIMESTAMP:
                topicPartitionOffsets =
                        listOffsets(topicPartitions, ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
                break;
            case CONSUME_FROM_GROUP_OFFSETS:
                topicPartitionOffsets = listConsumerGroupOffsets(topicPartitions);
                if (topicPartitionOffsets.isEmpty()) {
                    topicPartitionOffsets =
                            listOffsets(
                                    topicPartitions, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                }
                break;
            case CONSUME_FROM_SPECIFIC_OFFSETS:
                topicPartitionOffsets = metadata.getSpecificStartOffsets();
                // Fill in broker name
                setMessageQueueBroker(topicPartitions, topicPartitionOffsets);
                break;
            default:
                throw new RocketMqConnectorException(
                        RocketMqConnectorErrorCode.UNSUPPORTED_START_MODE_ERROR,
                        metadata.getStartMode().name());
        }
        topicPartitionOffsets
                .entrySet()
                .forEach(
                        entry -> {
                            if (pendingSplit.containsKey(entry.getKey())) {
                                pendingSplit.get(entry.getKey()).setStartOffset(entry.getValue());
                            }
                        });
    }

    private void setMessageQueueBroker(
            Collection<MessageQueue> topicPartitions,
            Map<MessageQueue, Long> topicPartitionOffsets) {
        Map<String, String> flatTopicPartitions =
                topicPartitions.stream()
                        .collect(
                                Collectors.toMap(
                                        messageQueue ->
                                                messageQueue.getTopic()
                                                        + "-"
                                                        + messageQueue.getQueueId(),
                                        MessageQueue::getBrokerName));
        for (MessageQueue messageQueue : topicPartitionOffsets.keySet()) {
            String key = messageQueue.getTopic() + "-" + messageQueue.getQueueId();
            if (flatTopicPartitions.containsKey(key)) {
                messageQueue.setBrokerName(flatTopicPartitions.get(key));
            }
        }
    }

    private Map<MessageQueue, Long> listOffsets(
            Collection<MessageQueue> messageQueues, ConsumeFromWhere consumeFromWhere) {
        Map<MessageQueue, Long> results = Maps.newConcurrentMap();
        Map<MessageQueue, TopicOffset> messageQueueOffsets =
                RocketMqAdminUtil.flatOffsetTopics(metadata.getBaseConfig(), metadata.getTopics());
        switch (consumeFromWhere) {
            case CONSUME_FROM_FIRST_OFFSET:
                messageQueues.forEach(
                        messageQueue -> {
                            TopicOffset topicOffset = messageQueueOffsets.get(messageQueue);
                            results.put(messageQueue, topicOffset.getMinOffset());
                        });
                break;
            case CONSUME_FROM_LAST_OFFSET:
                messageQueues.forEach(
                        messageQueue -> {
                            TopicOffset topicOffset = messageQueueOffsets.get(messageQueue);
                            results.put(messageQueue, topicOffset.getMaxOffset());
                        });
                break;
            case CONSUME_FROM_TIMESTAMP:
                results.putAll(
                        RocketMqAdminUtil.searchOffsetsByTimestamp(
                                metadata.getBaseConfig(),
                                messageQueues,
                                metadata.getStartOffsetsTimestamp()));
                break;
            default:
                // No-op
                break;
        }
        return results;
    }

    /** list consumer group offsets */
    public Map<MessageQueue, Long> listConsumerGroupOffsets(
            Collection<MessageQueue> messageQueues) {
        return RocketMqAdminUtil.currentOffsets(
                metadata.getBaseConfig(), metadata.getTopics(), new HashSet<>(messageQueues));
    }

    private synchronized void assignSplit() {
        Map<Integer, List<RocketMqSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID = 0; taskID < context.currentParallelism(); taskID++) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }
        pendingSplit
                .entrySet()
                .forEach(
                        s -> {
                            if (!assignedSplit.containsKey(s.getKey())) {
                                readySplit
                                        .get(
                                                getSplitOwner(
                                                        s.getKey(), context.currentParallelism()))
                                        .add(s.getValue());
                            }
                        });
        readySplit.forEach(context::assignSplit);
        assignedSplit.putAll(pendingSplit);
        pendingSplit.clear();
    }
}
