package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
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

import lombok.SneakyThrows;

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

    private final Map<MessageQueue, Long> stoppingTimestamps;

    private final DefaultLitePullConsumer consumer;

    // todo batch mode need
    private final Map<MessageQueue, Long> stoppingOffsets;

    private volatile boolean wakeup = false;

    public RocketMQPartitionSplitReader(
            ConsumerMetadata metadata, SourceReader.Context readerContext) {
        this.metadata = metadata;
        RocketMqBaseConfiguration config = metadata.getBaseConfig();
        this.topics = metadata.getTopics();
        this.stoppingTimestamps = new HashMap<>();
        this.stoppingOffsets = new HashMap<>();
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
        messageExts =
                messageExts.stream()
                        .filter(
                                record ->
                                        metadata.getTags() == null
                                                || metadata.getTags().isEmpty()
                                                || metadata.getTags().contains(record.getTags()))
                        .collect(Collectors.toList());

        messageExts.forEach(
                record -> {
                    Collection<MessageExt> recordsForSplit =
                            recordsBySplits.recordsForSplit(toSplitId(record));
                    recordsForSplit.add(record);
                });

        recordsBySplits.prepareForRead();
        LOG.debug(
                String.format(
                        "Fetch record splits for MetaQ subscribe message queues of topic[%s].",
                        String.join(",", topics)));
        return recordsBySplits;
    }

    @SneakyThrows
    @Override
    public void handleSplitsChanges(SplitsChange<RocketMQPartitionSplit> splitsChange) {
        // Get all the partition assignments and stopping offsets.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Assignment.
        List<MessageQueue> newPartitionAssignments = new ArrayList<>();
        // Starting offsets.
        Map<MessageQueue, Long> partitionsStartingFromSpecifiedOffsets = new HashMap<>();
        List<MessageQueue> partitionsStartingFromEarliest = new ArrayList<>();
        List<MessageQueue> partitionsStartingFromLatest = new ArrayList<>();
        // Stopping offsets.
        List<MessageQueue> partitionsStoppingAtLatest = new ArrayList<>();

        // Parse the starting and stopping offsets.
        splitsChange
                .splits()
                .forEach(
                        s -> {
                            newPartitionAssignments.add(s.getMessageQueue());
                            parseStartingOffsets(
                                    s,
                                    partitionsStartingFromEarliest,
                                    partitionsStartingFromLatest,
                                    partitionsStartingFromSpecifiedOffsets);
                            parseStoppingOffsets(s, partitionsStoppingAtLatest);
                        });

        // Assign new partitions.
        newPartitionAssignments.addAll(consumer.assignment());
        consumer.assign(newPartitionAssignments);

        // Seek on the newly assigned partitions to their stating offsets.
        seekToStartingOffsets(
                partitionsStartingFromEarliest,
                partitionsStartingFromLatest,
                partitionsStartingFromSpecifiedOffsets);

        // Setup the stopping offsets.
        acquireAndSetStoppingOffsets(partitionsStoppingAtLatest);
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

    private void seekToStartingOffsets(
            List<MessageQueue> partitionsStartingFromEarliest,
            List<MessageQueue> partitionsStartingFromLatest,
            Map<MessageQueue, Long> partitionsStartingFromSpecifiedOffsets) {

        if (!partitionsStartingFromEarliest.isEmpty()) {
            for (MessageQueue messageQueue : partitionsStartingFromEarliest) {
                try {
                    consumer.seekToBegin(messageQueue);
                } catch (MQClientException e) {
                    LOG.error(
                            "Seeking starting offsets to beginning: {}",
                            partitionsStartingFromEarliest);
                }
            }
        }

        if (!partitionsStartingFromLatest.isEmpty()) {
            for (MessageQueue messageQueue : partitionsStartingFromEarliest) {
                try {
                    consumer.seekToEnd(messageQueue);
                } catch (MQClientException e) {
                    LOG.error("Seeking starting offsets to end: {}", partitionsStartingFromLatest);
                }
            }
        }

        if (!partitionsStartingFromSpecifiedOffsets.isEmpty()) {
            LOG.trace(
                    "Seeking starting offsets to specified offsets: {}",
                    partitionsStartingFromSpecifiedOffsets);
            for (Map.Entry<MessageQueue, Long> partitionsStartingFromSpecifiedOffsetsEntry :
                    partitionsStartingFromSpecifiedOffsets.entrySet()) {
                try {
                    consumer.seek(
                            partitionsStartingFromSpecifiedOffsetsEntry.getKey(),
                            partitionsStartingFromSpecifiedOffsetsEntry.getValue());
                } catch (MQClientException e) {
                    LOG.error(
                            "Seeking starting offsets to specified offsets: {}",
                            partitionsStartingFromSpecifiedOffsets);
                }
            }
        }
    }

    private void parseStartingOffsets(
            RocketMQPartitionSplit split,
            List<MessageQueue> partitionsStartingFromEarliest,
            List<MessageQueue> partitionsStartingFromLatest,
            Map<MessageQueue, Long> partitionsStartingFromSpecifiedOffsets) {
        MessageQueue tp = split.getMessageQueue();
        // Parse starting offsets.
        if (metadata.getStartMode() == StartMode.CONSUME_FROM_FIRST_OFFSET) {
            partitionsStartingFromEarliest.add(tp);
        } else if (metadata.getStartMode() == StartMode.CONSUME_FROM_LAST_OFFSET) {
            partitionsStartingFromLatest.add(tp);
        } else if (metadata.getStartMode() == StartMode.CONSUME_FROM_GROUP_OFFSETS) {
            // Do nothing here, the consumer will first try to get the committed offsets of
            // these partitions by default.
        } else {
            partitionsStartingFromSpecifiedOffsets.put(tp, split.getStartOffset());
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

    private void finishSplitAtRecord(
            MessageQueue messageQueue,
            long stoppingTimestamp,
            long currentOffset,
            RocketMQPartitionSplitRecords recordsBySplits) {
        LOG.debug(
                "{} has reached stopping timestamp {}, current offset is {}",
                messageQueue.getTopic() + "-" + messageQueue.getBrokerName(),
                stoppingTimestamp,
                currentOffset);
        recordsBySplits.addFinishedSplit(RocketMQPartitionSplit.toSplitId(messageQueue));
        stoppingTimestamps.remove(messageQueue);
    }

    private long getStoppingTimestamp(MessageQueue messageQueue) {
        return stoppingTimestamps.getOrDefault(messageQueue, Long.MAX_VALUE);
    }

    public void notifyCheckpointComplete(
            Map<MessageQueue, Long> committedOffsets, OffsetCommitCallback callback) {
        consumer.commit(committedOffsets, true);
        LOG.info("Offset commit success.{},", JsonUtils.toJsonString(committedOffsets));
        callback.onComplete();
    }

    public String toSplitId(MessageExt messageExt) {
        return messageExt.getTopic()
                + "-"
                + messageExt.getBrokerName()
                + "-"
                + messageExt.getQueueId();
    }

    // ---------------- private helper class ------------------------

    private static class RocketMQPartitionSplitRecords implements RecordsWithSplitIds<MessageExt> {
        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<MessageExt, Long> stoppingOffsets = new HashMap<>();
        private final Map<String, Collection<MessageExt>> recordsBySplits;
        private Iterator<Map.Entry<String, Collection<MessageExt>>> splitIterator;
        private Iterator<MessageExt> recordIterator;
        private String currentSplitId;

        public RocketMQPartitionSplitRecords() {
            this.recordsBySplits = new HashMap<>();
        }

        private Collection<MessageExt> recordsForSplit(String splitId) {
            return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
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
                return currentSplitId;
            } else {
                currentSplitId = null;
                recordIterator = null;
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
                return recordIterator.next();
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
