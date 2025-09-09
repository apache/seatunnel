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
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

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
        implements SourceSplitEnumerator<RocketMQPartitionSplit, RocketMqSourceState> {

    private final ConsumerMetadata metadata;
    private final Context<RocketMQPartitionSplit> context;
    private ScheduledExecutorService executor;
    private ScheduledFuture scheduledFuture;
    private final Map<MessageQueue, RocketMQPartitionSplit> assignedSplit;
    private final Map<MessageQueue, RocketMQPartitionSplit> pendingSplit;
    // ms
    private long discoveryIntervalMillis;
    Map<MessageQueue, Long> specificStartupOffsets;
    private volatile boolean initialized;
    private final Object lock = new Object();
    /** The topic used for this RocketMQSource. */
    private final List<String> topics;

    private final long consumerOffsetTimestamp;
    private final boolean isStreamingMode;

    public RocketMqSourceSplitEnumerator(
            ConsumerMetadata metadata,
            Context<RocketMQPartitionSplit> context,
            long discoveryIntervalMillis,
            boolean isStreamingMode) {
        this.metadata = metadata;
        final RocketMqBaseConfiguration config = this.metadata.getBaseConfig();
        this.topics = this.metadata.getTopics();
        this.consumerOffsetTimestamp = this.metadata.getStartOffsetsTimestamp();
        this.context = context;
        this.discoveryIntervalMillis = discoveryIntervalMillis;
        this.isStreamingMode = isStreamingMode;
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
        specificStartupOffsets = this.metadata.getSpecificStartOffsets();
        // Set `rocketmq.client.logUseSlf4j` to `true` to avoid create many
        // `AsyncAppender-Dispatcher-Thread`
        System.setProperty("rocketmq.client.logUseSlf4j", "true");
    }

    public RocketMqSourceSplitEnumerator(
            ConsumerMetadata metadata,
            Set<RocketMQPartitionSplit> assignedSplit,
            Context<RocketMQPartitionSplit> context,
            long discoveryIntervalMillis,
            boolean isStreamingMode) {
        this(metadata, context, discoveryIntervalMillis, isStreamingMode);
        assignedSplit.forEach(split -> this.assignedSplit.put(split.getMessageQueue(), split));
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
                                    if (initialized) {
                                        discoverySplits();
                                    }
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
        synchronized (lock) {
            fetchPendingPartitionSplit();
            setPartitionStartOffset();
        }
        synchronized (lock) {
            assignSplit();
        }

        if (!initialized) {
            initialized = true;
        }
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
    public void addSplitsBack(List<RocketMQPartitionSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            Map<MessageQueue, ? extends RocketMQPartitionSplit> nextSplit =
                    convertToNextSplit(splits);
            // remove them from the assignedSplit, so we can reassign them
            nextSplit.keySet().forEach(assignedSplit::remove);
            pendingSplit.putAll(nextSplit);
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
        if (!pendingSplit.isEmpty() && initialized) {
            assignSplit();
        }
    }

    @Override
    public RocketMqSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (lock) {
            return new RocketMqSourceState(new HashSet<>(assignedSplit.values()));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No-op
    }

    private Map<MessageQueue, ? extends RocketMQPartitionSplit> convertToNextSplit(
            List<RocketMQPartitionSplit> splits) {
        Set<MessageQueue> messageQueues =
                splits.stream()
                        .map(RocketMQPartitionSplit::getMessageQueue)
                        .collect(Collectors.toSet());
        final Map<MessageQueue, Long> latestOffsets = new HashMap<>();

        Map<MessageQueue, Long> listOffsets =
                listOffsets(messageQueues, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        latestOffsets.putAll(listOffsets);

        splits.forEach(
                split -> {
                    split.setStartOffset(split.getEndOffset() + 1);
                    split.setEndOffset(
                            isStreamingMode
                                    ? Long.MAX_VALUE
                                    : latestOffsets.get(split.getMessageQueue()));
                });
        return splits.stream()
                .collect(Collectors.toMap(RocketMQPartitionSplit::getMessageQueue, split -> split));
    }

    private Set<RocketMQPartitionSplit> getTopicInfo() {
        log.info("Configured topics: {}", metadata.getTopics());
        List<Map<MessageQueue, TopicOffset>> offsetTopics =
                RocketMqAdminUtil.offsetTopics(metadata.getBaseConfig(), metadata.getTopics());
        Set<RocketMQPartitionSplit> sourceSplits = Sets.newConcurrentHashSet();
        offsetTopics.forEach(
                messageQueueOffsets -> {
                    messageQueueOffsets.forEach(
                            (messageQueue, topicOffset) -> {
                                sourceSplits.add(
                                        new RocketMQPartitionSplit(
                                                messageQueue,
                                                topicOffset.getMinOffset(),
                                                topicOffset.getMaxOffset()));
                            });
                });
        return sourceSplits;
    }

    public void fetchPendingPartitionSplit() {
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
                                                        + messageQueue.getBrokerName()
                                                        + "-"
                                                        + messageQueue.getQueueId(),
                                        MessageQueue::getBrokerName));
        for (MessageQueue messageQueue : topicPartitionOffsets.keySet()) {
            String key =
                    messageQueue.getTopic()
                            + "-"
                            + messageQueue.getBrokerName()
                            + "-"
                            + messageQueue.getQueueId();
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

    private void setPartitionStartOffset() {
        Set<MessageQueue> topicPartitions = pendingSplit.keySet();
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

        Map<MessageQueue, Long> latestOffsets =
                listOffsets(topicPartitions, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        topicPartitionOffsets.forEach(
                (key, value) -> {
                    if (pendingSplit.containsKey(key)) {
                        final RocketMQPartitionSplit rocketMQPartitionSplit = pendingSplit.get(key);
                        if (!isStreamingMode) {
                            if (value > latestOffsets.get(key)) return;
                            rocketMQPartitionSplit.setEndOffset(latestOffsets.get(key));
                        } else {
                            rocketMQPartitionSplit.setEndOffset(Long.MAX_VALUE);
                        }
                        rocketMQPartitionSplit.setStartOffset(value);
                    }
                });
    }

    /** list consumer group offsets */
    public Map<MessageQueue, Long> listConsumerGroupOffsets(
            Collection<MessageQueue> messageQueues) {
        return RocketMqAdminUtil.currentOffsets(
                metadata.getBaseConfig(), metadata.getTopics(), new HashSet<>(messageQueues));
    }

    private synchronized void assignSplit() {
        Map<Integer, List<RocketMQPartitionSplit>> readySplit =
                new HashMap<>(Common.COLLECTION_SIZE);
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
                    log.info("Assigning split {} to {}", split, id);
                    context.assignSplit(id, split);
                    if (discoveryIntervalMillis <= 0) {
                        context.signalNoMoreSplits(id);
                    }
                });

        assignedSplit.putAll(pendingSplit);
        pendingSplit.clear();
    }

    private static int getSplitOwner(MessageQueue messageQueue, int numReaders) {
        int startIndex =
                (((messageQueue.getTopic() + "-" + messageQueue.getBrokerName()).hashCode() * 31)
                                & 0x7FFFFFFF)
                        % numReaders;
        return (startIndex + messageQueue.getQueueId()) % numReaders;
    }

    private void discoverySplits() {
        fetchPendingPartitionSplit();
        assignSplit();
    }
}
