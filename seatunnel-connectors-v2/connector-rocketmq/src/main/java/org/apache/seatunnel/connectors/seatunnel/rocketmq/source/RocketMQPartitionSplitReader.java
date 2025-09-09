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

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RocketMQPartitionSplitReader
        implements SplitReader<MessageExt, RocketMQPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQPartitionSplitReader.class);

    private final long pollTimeOut = 10000L;

    private final List<String> topics;

    private final ConsumerMetadata metadata;

    private final Set<String> emptySplits = new HashSet<>();

    private final DefaultLitePullConsumer consumer;

    private final Map<MessageQueue, Long> stoppingOffsets;

    private final Set<MessageQueue> finishedPartition;

    private volatile boolean wakeup = false;

    private final Object lock = new Object();

    public RocketMQPartitionSplitReader(
            ConsumerMetadata metadata, SourceReader.Context readerContext) {
        this.metadata = metadata;
        RocketMqBaseConfiguration config = metadata.getBaseConfig();
        this.topics = metadata.getTopics();
        this.stoppingOffsets = new HashMap<>();
        this.finishedPartition = new HashSet<>();
        this.consumer =
                initDefaultLitePullConsumer(
                        config,
                        metadata,
                        !metadata.isEnabledCommitCheckpoint(),
                        readerContext.getIndexOfSubtask());
        try {
            this.consumer.start();
        } catch (MQClientException e) {
            // Start rocketmq failed
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.CONSUMER_START_ERROR, e);
        }
        System.setProperty("rocketmq.client.logUseSlf4j", "true");
    }

    @Override
    public RecordsWithSplitIds<MessageExt> fetch() throws IOException {
        List<MessageExt> messageExts;
        RocketMQPartitionSplitRecords recordsBySplits = new RocketMQPartitionSplitRecords();
        if (wakeup) {
            wakeup = false;
            recordsBySplits.prepareForRead();
            return recordsBySplits;
        }
        try {
            messageExts = consumer.poll(pollTimeOut);
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Fetch RocketMQ subscribe message queues of topic[%s] exception.",
                            String.join(",", topics)),
                    e);
            markEmptySplitsAsFinished(recordsBySplits);
            recordsBySplits.prepareForRead();
            return recordsBySplits;
        }
        Set<MessageQueue> finishedPartitions = new HashSet<>();
        for (MessageExt record : messageExts) {
            MessageQueue mq =
                    new MessageQueue(
                            record.getTopic(), record.getBrokerName(), record.getQueueId());
            final long stoppingOffset = getStoppingOffset(mq);

            if (finishedPartition.contains(mq)) {
                break;
            }

            if (metadata.getTags() == null
                    || metadata.getTags().isEmpty()
                    || metadata.getTags().contains("*")
                    || metadata.getTags().contains(record.getTags())) {
                Collection<MessageExt> recordsForSplit =
                        recordsBySplits.recordsForSplit(
                                toSplitId(
                                        record.getTopic(),
                                        record.getBrokerName(),
                                        record.getQueueId()));
                recordsForSplit.add(record);
            }

            // MessageQueue finished or offset > stopingOffset
            if (record.getQueueOffset() >= stoppingOffset - 1) {
                recordsBySplits.setPartitionStoppingOffset(
                        toSplitId(record.getTopic(), record.getBrokerName(), record.getQueueId()),
                        stoppingOffset);
                finishSplitAtRecord(
                        mq,
                        record.getQueueOffset(),
                        stoppingOffset,
                        finishedPartitions,
                        recordsBySplits);
                break;
            }
        }
        recordsBySplits.prepareForRead();

        markEmptySplitsAsFinished(recordsBySplits);

        if (!finishedPartitions.isEmpty()) {
            unassignPartitions(finishedPartitions);
        }

        LOG.debug(
                String.format(
                        "Fetch record splits for MetaQ subscribe message queues of topic[%s].",
                        String.join(",", topics)));
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RocketMQPartitionSplit> splitsChange) {
        // Get all the partition assignments and stopping offsets.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Starting offsets.
        Map<MessageQueue, Long> partitionsStartingOffsets = new HashMap<>();
        // Stopping offsets.
        List<MessageQueue> partitionsStoppingAtLatest = new ArrayList<>();

        // Assignment.
        List<MessageQueue> newPartitionAssignments =
                splitsChange.splits().stream()
                        .map(RocketMQPartitionSplit::getMessageQueue)
                        .collect(Collectors.toList());

        // Assign new partitions.
        try {
            newPartitionAssignments.addAll(consumer.assignment());
        } catch (Exception e) {
            LOG.error("Fetch RocketMQ assignment failed.", e);
        }
        consumer.assign(newPartitionAssignments);
        // Parse the starting and stopping offsets.
        splitsChange
                .splits()
                .forEach(
                        s -> {
                            parseStartingOffsets(s, partitionsStartingOffsets);
                            parseStoppingOffsets(s, partitionsStoppingAtLatest);
                        });

        // Seek on the newly assigned partitions to their stating offsets.
        seekToStartingOffsets(partitionsStartingOffsets);

        // Setup the stopping offsets.
        acquireAndSetStoppingOffsets(partitionsStoppingAtLatest);

        removeEmptySplits();
    }

    @Override
    public void wakeUp() {
        LOG.debug("Wake up the split reader in case the fetcher thread is blocking in fetch().");
        wakeup = true;
    }

    @Override
    public void close() {
        consumer.shutdown();
    }

    private DefaultLitePullConsumer initDefaultLitePullConsumer(
            RocketMqBaseConfiguration config,
            ConsumerMetadata metadata,
            boolean autoCommit,
            int indexOfSubtask) {
        DefaultLitePullConsumer consumer;
        if (StringUtils.isBlank(config.getAccessKey())
                && StringUtils.isBlank(config.getSecretKey())) {
            consumer = new DefaultLitePullConsumer(config.getGroupId());
        } else {
            consumer =
                    new DefaultLitePullConsumer(
                            config.getGroupId(),
                            new AclClientRPCHook(
                                    new SessionCredentials(
                                            config.getAccessKey(), config.getSecretKey())));
        }
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        String uniqueName =
                String.join(
                        "||",
                        ManagementFactory.getRuntimeMXBean().getName(),
                        String.join(",", metadata.getTopics()),
                        metadata.getBaseConfig().getGroupId(),
                        "" + System.nanoTime());
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName + indexOfSubtask);
        consumer.setAutoCommit(autoCommit);
        if (config.getBatchSize() != null) {
            consumer.setPullBatchSize(config.getBatchSize());
        }
        return consumer;
    }

    private void unassignPartitions(Set<MessageQueue> partitionsToUnassign) {
        Collection<MessageQueue> newAssignment;
        try {
            newAssignment = consumer.assignment();
        } catch (Exception e) {
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.GET_ASSIGNMENT_QUEUE_ERROR, e);
        }
        finishedPartition.addAll(partitionsToUnassign);
        newAssignment.removeAll(finishedPartition);
        LOG.info("assign partitions: {}", JsonUtils.toJsonString(newAssignment));
        if (!newAssignment.isEmpty()) {
            consumer.assign(newAssignment);
        }
    }

    private void removeEmptySplits() {
        Set<MessageQueue> emptyPartitions = new HashSet<>();
        // If none of the partitions have any records,
        try {
            for (MessageQueue mq : consumer.assignment()) {
                if (consumer.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE)
                        >= getStoppingOffset(mq)) {
                    emptyPartitions.add(mq);
                }
            }
        } catch (Exception e) {
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.GET_ASSIGNMENT_QUEUE_ERROR, e);
        }

        if (!emptyPartitions.isEmpty()) {
            LOG.debug(
                    "These assigning splits are empty and will be marked as finished in later fetch: {}",
                    emptyPartitions);
            // Add empty partitions to empty split set for later cleanup in fetch()
            emptySplits.addAll(
                    emptyPartitions.stream()
                            .map(
                                    mq ->
                                            toSplitId(
                                                    mq.getTopic(),
                                                    mq.getBrokerName(),
                                                    mq.getQueueId()))
                            .collect(Collectors.toSet()));
            // Un-assign partitions from RocketMq consumer
            unassignPartitions(emptyPartitions);
        }
    }

    private void seekToStartingOffsets(Map<MessageQueue, Long> partitionsStartingOffsets) {
        if (!partitionsStartingOffsets.isEmpty()) {
            LOG.info(
                    "Seeking starting offsets to : {}",
                    JsonUtils.toJsonString(partitionsStartingOffsets));
            for (Map.Entry<MessageQueue, Long> messageQueueEntry :
                    partitionsStartingOffsets.entrySet()) {
                try {
                    consumer.seek(messageQueueEntry.getKey(), messageQueueEntry.getValue());
                } catch (MQClientException e) {
                    throw new RocketMqConnectorException(
                            RocketMqConnectorErrorCode.SEEK_MESSAGE_QUEUE_OFFSET_ERROR, e);
                }
            }
        }
    }

    private void acquireAndSetStoppingOffsets(List<MessageQueue> partitionsStoppingAtLatest) {
        Map<MessageQueue, Long> endOffset = new HashMap<>();
        if (consumer.getOffsetStore() == null) {
            LOG.info(
                    "consumer offsetStore is null.partitionsStoppingAtLatest:{}",
                    partitionsStoppingAtLatest);
            return;
        }
        partitionsStoppingAtLatest.forEach(
                messageQueue -> {
                    endOffset.put(
                            messageQueue,
                            consumer.getOffsetStore()
                                    .readOffset(
                                            messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
                });
        stoppingOffsets.putAll(endOffset);
    }

    private void finishSplitAtRecord(
            MessageQueue mq,
            long currentOffset,
            long stoppingOffset,
            Set<MessageQueue> finishedPartitions,
            RocketMQPartitionSplitRecords recordsBySplits) {
        LOG.info(
                "{} has reached stopping offset {}, current offset is {}",
                mq,
                currentOffset,
                stoppingOffset);
        finishedPartitions.add(mq);
        recordsBySplits.addFinishedSplit(
                toSplitId(mq.getTopic(), mq.getBrokerName(), mq.getQueueId()));
    }

    private long getStoppingOffset(MessageQueue messageQueue) {
        return stoppingOffsets.getOrDefault(messageQueue, Long.MAX_VALUE);
    }

    private void parseStartingOffsets(
            RocketMQPartitionSplit split, Map<MessageQueue, Long> partitionsStartingOffsets) {
        MessageQueue messageQueue = split.getMessageQueue();
        if (split.getStartOffset() >= 0) {
            partitionsStartingOffsets.put(messageQueue, split.getStartOffset());
        }
    }

    private void parseStoppingOffsets(
            RocketMQPartitionSplit split, List<MessageQueue> partitionsStoppingAtLatest) {
        MessageQueue tp = split.getMessageQueue();
        if (split.getEndOffset() >= 0) {
            stoppingOffsets.put(tp, split.getEndOffset());
        } else {
            partitionsStoppingAtLatest.add(tp);
        }
    }

    private void markEmptySplitsAsFinished(RocketMQPartitionSplitRecords recordsBySplits) {
        // Some splits are discovered as empty when handling split additions. These splits should be
        // added to finished splits to clean up states in split fetcher and source reader.
        if (!emptySplits.isEmpty()) {
            recordsBySplits.finishedSplits.addAll(emptySplits);
            emptySplits.clear();
        }
    }

    public void notifyCheckpointComplete(
            Map<MessageQueue, Long> committedOffsets, OffsetCommitCallback callback) {
        committedOffsets.forEach(
                (mq, offset) -> {
                    consumer.getOffsetStore().updateOffset(mq, offset, false);
                    consumer.getOffsetStore().persist(mq);
                });
        LOG.info("Offset commit success.{},", JsonUtils.toJsonString(committedOffsets));
        callback.onComplete();
    }

    public String toSplitId(String topic, String brokerName, int queueId) {
        return topic + "-" + brokerName + "-" + queueId;
    }

    // ---------------- private helper class ------------------------

    private static class RocketMQPartitionSplitRecords implements RecordsWithSplitIds<MessageExt> {
        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<String, Long> stoppingOffsets = new HashMap<>();
        private final Map<String, Collection<MessageExt>> recordsBySplits;
        private Iterator<Map.Entry<String, Collection<MessageExt>>> splitIterator;
        private Iterator<MessageExt> recordIterator;
        private String currentSplitId;
        private Long currentSplitStoppingOffset;

        public RocketMQPartitionSplitRecords() {
            this.recordsBySplits = new HashMap<>();
        }

        private Collection<MessageExt> recordsForSplit(String splitId) {
            return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
        }

        private void setPartitionStoppingOffset(String splitId, long stoppingOffset) {
            stoppingOffsets.put(splitId, stoppingOffset);
        }

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        private void prepareForRead() {
            splitIterator = recordsBySplits.entrySet().iterator();
        }

        @Override
        @Nullable public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, Collection<MessageExt>> entry = splitIterator.next();
                recordIterator = entry.getValue().iterator();
                currentSplitId = entry.getKey();
                currentSplitStoppingOffset =
                        stoppingOffsets.getOrDefault(currentSplitId, Long.MAX_VALUE);
                return currentSplitId;
            } else {
                currentSplitId = null;
                recordIterator = null;
                currentSplitStoppingOffset = null;
                return null;
            }
        }

        @Override
        @Nullable public MessageExt nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentSplitId,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                MessageExt messageExt = recordIterator.next();
                // Only emit records before stopping offset
                if (messageExt.getQueueOffset() < currentSplitStoppingOffset) {
                    return messageExt;
                }
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
