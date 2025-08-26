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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
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
import java.util.stream.Stream;

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
    private DefaultMQPullConsumer consumer;
    private final Object lock = new Object();
    /** The topic used for this RocketMQSource. */
    private final List<String> topics;

    private final long consumerOffsetTimestamp;

    public RocketMqSourceSplitEnumerator(
            ConsumerMetadata metadata,
            Context<RocketMQPartitionSplit> context,
            long discoveryIntervalMillis) {
        this.metadata = metadata;
        final RocketMqBaseConfiguration config = this.metadata.getBaseConfig();
        this.topics = this.metadata.getTopics();
        this.consumerOffsetTimestamp = this.metadata.getStartOffsetsTimestamp();
        this.context = context;
        this.discoveryIntervalMillis = discoveryIntervalMillis;
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
        specificStartupOffsets = this.metadata.getSpecificStartOffsets();
        // Set `rocketmq.client.logUseSlf4j` to `true` to avoid create many
        // `AsyncAppender-Dispatcher-Thread`
        System.setProperty("rocketmq.client.logUseSlf4j", "true");
        initialRocketMQConsumer(config);
    }

    public RocketMqSourceSplitEnumerator(
            ConsumerMetadata metadata,
            Set<RocketMQPartitionSplit> assignedSplit,
            Context<RocketMQPartitionSplit> context,
            long discoveryIntervalMillis) {
        this(metadata, context, discoveryIntervalMillis);
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
        if (consumer != null) {
            consumer.shutdown();
        }

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
        splits.forEach(
                split -> {
                    split.setStartOffset(split.getEndOffset() + 1);
                    split.setEndOffset(Long.MAX_VALUE);
                });
        return splits.stream()
                .collect(Collectors.toMap(RocketMQPartitionSplit::getMessageQueue, split -> split));
    }

    private void initialRocketMQConsumer(RocketMqBaseConfiguration config) {
        try {
            if (StringUtils.isNotBlank(config.getAccessKey())
                    && StringUtils.isNotBlank(config.getAccessKey())) {
                AclClientRPCHook aclClientRPCHook =
                        new AclClientRPCHook(
                                new SessionCredentials(
                                        config.getAccessKey(), config.getSecretKey()));
                consumer = new DefaultMQPullConsumer(config.getGroupId(), aclClientRPCHook);
            } else {
                consumer = new DefaultMQPullConsumer(config.getGroupId());
            }

            consumer.setNamesrvAddr(config.getNamesrvAddr());
            consumer.setInstanceName(
                    String.join(
                            "||",
                            ManagementFactory.getRuntimeMXBean().getName(),
                            String.join("||", topics),
                            config.getGroupId(),
                            "" + System.nanoTime()));
            consumer.start();
        } catch (MQClientException e) {
            log.error("Failed to initial RocketMQ consumer.", e);
            consumer.shutdown();
        }
    }

    private Set<RocketMQPartitionSplit> getTopicInfo() {
        log.info("Discovered topics: {}", topics);
        return topics.stream()
                .flatMap(
                        topic -> {
                            try {
                                return consumer.fetchSubscribeMessageQueues(topic).stream();
                            } catch (MQClientException e) {
                                log.error("Failed to subscribe topic:{}", topic, e);
                                return Stream.empty();
                            }
                        })
                .map(RocketMQPartitionSplit::new)
                .collect(Collectors.toSet());
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

    private void setPartitionStartOffset() throws MQClientException {
        Set<MessageQueue> pendingMessageQueues = pendingSplit.keySet();
        Map<MessageQueue, Long> topicPartitionOffsets = new HashMap<>();
        for (MessageQueue mq : pendingMessageQueues) {
            long offset;
            switch (metadata.getStartMode()) {
                case CONSUME_FROM_LAST_OFFSET:
                    offset = consumer.maxOffset(mq);
                    break;
                case CONSUME_FROM_FIRST_OFFSET:
                    offset = consumer.minOffset(mq);
                    break;
                case CONSUME_FROM_GROUP_OFFSETS:
                    offset = consumer.fetchConsumeOffset(mq, false);
                    // If broker throw exception,return -2.should be distinguished from the
                    // initialization scenario
                    if (offset <= -2) {
                        throw new RuntimeException(
                                "An error occurred while fetching offset,please check up server's log");
                    }
                    // the min offset return if consumer group first join,return a negative number
                    // if catch exception when fetch from broker.
                    // If you want consumer from earliest,please use OffsetResetStrategy.EARLIEST
                    if (offset <= 0) {
                        log.info(
                                "current consumer thread:{} has no committed offset,use Strategy:earliest instead",
                                mq);
                        offset = consumer.minOffset(mq);
                    }
                    break;
                case CONSUME_FROM_TIMESTAMP:
                    offset = consumer.searchOffset(mq, consumerOffsetTimestamp);
                    break;
                case CONSUME_FROM_SPECIFIC_OFFSETS:
                    if (specificStartupOffsets == null) {
                        throw new RuntimeException(
                                "StartMode is specific_offsets.But none offsets has been specified");
                    }
                    Long specificOffset = specificStartupOffsets.get(mq);
                    if (specificOffset != null) {
                        offset = specificOffset;
                    } else {
                        offset = consumer.fetchConsumeOffset(mq, false);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "current startMode is not supported" + metadata.getStartMode());
            }
            log.info(
                    "current consumer queue:{} start from offset of: {}",
                    mq.getBrokerName() + "-" + mq.getQueueId(),
                    offset);
            topicPartitionOffsets.put(mq, offset);
        }

        topicPartitionOffsets.forEach(
                (key, value) -> {
                    if (pendingSplit.containsKey(key)) {
                        pendingSplit.get(key).setStartOffset(value);
                    }
                });
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
