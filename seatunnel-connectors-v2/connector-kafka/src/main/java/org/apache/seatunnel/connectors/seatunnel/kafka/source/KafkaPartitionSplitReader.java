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

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaPartitionSplitReader
        implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionSplitReader.class);

    private static final String CLIENT_ID_PREFIX = "seatunnel";
    private final KafkaSourceConfig kafkaSourceConfig;

    private final KafkaConsumer<byte[], byte[]> consumer;

    private final Map<TopicPartition, Long> stoppingOffsets;

    private final String groupId;

    private final Set<String> emptySplits = new HashSet<>();

    private final long pollTimeout;

    public KafkaPartitionSplitReader(
            KafkaSourceConfig kafkaSourceConfig, SourceReader.Context context) {
        this.kafkaSourceConfig = kafkaSourceConfig;
        this.consumer = initConsumer(kafkaSourceConfig, context.getIndexOfSubtask());
        this.stoppingOffsets = new HashMap<>();
        this.groupId =
                kafkaSourceConfig.getProperties().getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        this.pollTimeout = kafkaSourceConfig.getPollTimeout();
    }

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        ConsumerRecords<byte[], byte[]> consumerRecords;
        try {
            consumerRecords = consumer.poll(Duration.ofMillis(pollTimeout));
        } catch (WakeupException | IllegalStateException e) {
            // IllegalStateException will be thrown if the consumer is not assigned any partitions.
            // This happens if all assigned partitions are invalid or empty (starting offset >=
            // stopping offset). We just mark empty partitions as finished and return an empty
            // record container, and this consumer will be closed by SplitFetcherManager.
            KafkaPartitionSplitRecords recordsBySplits =
                    new KafkaPartitionSplitRecords(ConsumerRecords.empty());
            markEmptySplitsAsFinished(recordsBySplits);
            return recordsBySplits;
        }
        KafkaPartitionSplitRecords recordsBySplits =
                new KafkaPartitionSplitRecords(consumerRecords);
        List<TopicPartition> finishedPartitions = new ArrayList<>();
        for (TopicPartition tp : consumerRecords.partitions()) {
            long stoppingOffset = getStoppingOffset(tp);
            final List<ConsumerRecord<byte[], byte[]>> recordsFromPartition =
                    consumerRecords.records(tp);

            if (recordsFromPartition.size() > 0) {
                final ConsumerRecord<byte[], byte[]> lastRecord =
                        recordsFromPartition.get(recordsFromPartition.size() - 1);

                // After processing a record with offset of "stoppingOffset - 1", the split reader
                // should not continue fetching because the record with stoppingOffset may not
                // exist. Keep polling will just block forever.
                if (lastRecord.offset() >= stoppingOffset - 1) {
                    recordsBySplits.setPartitionStoppingOffset(tp, stoppingOffset);
                    finishSplitAtRecord(
                            tp,
                            stoppingOffset,
                            lastRecord.offset(),
                            finishedPartitions,
                            recordsBySplits);
                }
            }
        }

        markEmptySplitsAsFinished(recordsBySplits);

        if (!finishedPartitions.isEmpty()) {
            unassignPartitions(finishedPartitions);
        }

        return recordsBySplits;
    }

    private void finishSplitAtRecord(
            TopicPartition tp,
            long stoppingOffset,
            long currentOffset,
            List<TopicPartition> finishedPartitions,
            KafkaPartitionSplitRecords recordsBySplits) {
        LOG.debug(
                "{} has reached stopping offset {}, current offset is {}",
                tp,
                stoppingOffset,
                currentOffset);
        finishedPartitions.add(tp);
        recordsBySplits.addFinishedSplit(tp.toString());
    }

    private void markEmptySplitsAsFinished(KafkaPartitionSplitRecords recordsBySplits) {
        // Some splits are discovered as empty when handling split additions. These splits should be
        // added to finished splits to clean up states in split fetcher and source reader.
        if (!emptySplits.isEmpty()) {
            recordsBySplits.finishedSplits.addAll(emptySplits);
            emptySplits.clear();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KafkaSourceSplit> splitsChange) {
        // Get all the partition assignments and stopping offsets.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Assignment.
        List<TopicPartition> newPartitionAssignments = new ArrayList<>();
        // Starting offsets.
        Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets = new HashMap<>();
        List<TopicPartition> partitionsStartingFromEarliest = new ArrayList<>();
        List<TopicPartition> partitionsStartingFromLatest = new ArrayList<>();
        // Stopping offsets.
        List<TopicPartition> partitionsStoppingAtLatest = new ArrayList<>();

        // Parse the starting and stopping offsets.
        splitsChange
                .splits()
                .forEach(
                        s -> {
                            newPartitionAssignments.add(s.getTopicPartition());
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

        // After acquiring the starting and stopping offsets, remove the empty splits if necessary.
        removeEmptySplits();

        maybeLogSplitChangesHandlingResult(splitsChange);
    }

    private void maybeLogSplitChangesHandlingResult(SplitsChange<KafkaSourceSplit> splitsChange) {
        if (LOG.isDebugEnabled()) {
            StringJoiner splitsInfo = new StringJoiner(",");
            Set<TopicPartition> assginment = consumer.assignment();
            for (KafkaSourceSplit split : splitsChange.splits()) {
                if (!assginment.contains(split.getTopicPartition())) {
                    continue;
                }

                long startingOffset =
                        retryOnWakeup(
                                () -> consumer.position(split.getTopicPartition()),
                                "logging starting position");
                long stoppingOffset = getStoppingOffset(split.getTopicPartition());
                splitsInfo.add(
                        String.format(
                                "[%s, start:%d, stop: %d]",
                                split.getTopicPartition(), startingOffset, stoppingOffset));
            }
            LOG.debug("SplitsChange handling result: {}", splitsInfo);
        }
    }

    private void removeEmptySplits() {
        List<TopicPartition> emptyPartitions = new ArrayList<>();
        // If none of the partitions have any records,
        for (TopicPartition tp : consumer.assignment()) {
            if (retryOnWakeup(
                            () -> consumer.position(tp),
                            "getting starting offset to check if split is empty")
                    >= getStoppingOffset(tp)) {
                emptyPartitions.add(tp);
            }
        }
        if (!emptyPartitions.isEmpty()) {
            LOG.debug(
                    "These assigning splits are empty and will be marked as finished in later fetch: {}",
                    emptyPartitions);
            // Add empty partitions to empty split set for later cleanup in fetch()
            emptySplits.addAll(
                    emptyPartitions.stream()
                            .map(TopicPartition::toString)
                            .collect(Collectors.toSet()));
            // Un-assign partitions from Kafka consumer
            unassignPartitions(emptyPartitions);
        }
    }

    private void unassignPartitions(Collection<TopicPartition> partitionsToUnassign) {
        Collection<TopicPartition> newAssignment = new HashSet<>(consumer.assignment());
        newAssignment.removeAll(partitionsToUnassign);
        consumer.assign(newAssignment);
    }

    private void acquireAndSetStoppingOffsets(List<TopicPartition> partitionsStoppingAtLatest) {
        Map<TopicPartition, Long> endOffset = consumer.endOffsets(partitionsStoppingAtLatest);
        stoppingOffsets.putAll(endOffset);
    }

    private void seekToStartingOffsets(
            List<TopicPartition> partitionsStartingFromEarliest,
            List<TopicPartition> partitionsStartingFromLatest,
            Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets) {

        if (!partitionsStartingFromEarliest.isEmpty()) {
            LOG.trace("Seeking starting offsets to beginning: {}", partitionsStartingFromEarliest);
            consumer.seekToBeginning(partitionsStartingFromEarliest);
        }

        if (!partitionsStartingFromLatest.isEmpty()) {
            LOG.trace("Seeking starting offsets to end: {}", partitionsStartingFromLatest);
            consumer.seekToEnd(partitionsStartingFromLatest);
        }

        if (!partitionsStartingFromSpecifiedOffsets.isEmpty()) {
            LOG.trace(
                    "Seeking starting offsets to specified offsets: {}",
                    partitionsStartingFromSpecifiedOffsets);
            partitionsStartingFromSpecifiedOffsets.forEach(consumer::seek);
        }
    }

    private void parseStoppingOffsets(
            KafkaSourceSplit split, List<TopicPartition> partitionsStoppingAtLatest) {
        TopicPartition tp = split.getTopicPartition();
        if (split.getEndOffset() >= 0) {
            stoppingOffsets.put(tp, split.getEndOffset());
        } else {
            partitionsStoppingAtLatest.add(tp);
        }
    }

    private long getStoppingOffset(TopicPartition tp) {
        return stoppingOffsets.getOrDefault(tp, Long.MAX_VALUE);
    }

    private void parseStartingOffsets(
            KafkaSourceSplit split,
            List<TopicPartition> partitionsStartingFromEarliest,
            List<TopicPartition> partitionsStartingFromLatest,
            Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets) {
        TopicPartition tp = split.getTopicPartition();
        // Parse starting offsets.
        ConsumerMetadata metadata = kafkaSourceConfig.getMapMetadata().get(split.getTablePath());
        if (metadata.getStartMode() == StartMode.EARLIEST) {
            partitionsStartingFromEarliest.add(tp);
        } else if (metadata.getStartMode() == StartMode.LATEST) {
            partitionsStartingFromLatest.add(tp);
        } else if (metadata.getStartMode() == StartMode.GROUP_OFFSETS) {
            // Do nothing here, the consumer will first try to get the committed offsets of
            // these partitions by default.
        } else {
            partitionsStartingFromSpecifiedOffsets.put(tp, split.getStartOffset());
        }
    }

    @Override
    public void wakeUp() {
        consumer.wakeup();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }

    public void notifyCheckpointComplete(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
            OffsetCommitCallback offsetCommitCallback) {
        consumer.commitAsync(offsetsToCommit, offsetCommitCallback);
    }

    private KafkaConsumer<byte[], byte[]> initConsumer(
            KafkaSourceConfig kafkaSourceConfig, int subtaskId) {

        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(kafkaSourceConfig.getClass().getClassLoader())) {
            Properties props = new Properties();
            kafkaSourceConfig
                    .getProperties()
                    .forEach(
                            (key, value) ->
                                    props.setProperty(String.valueOf(key), String.valueOf(value)));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaSourceConfig.getConsumerGroup());
            props.setProperty(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceConfig.getBootstrap());
            if (this.kafkaSourceConfig.getProperties().get("client.id") == null) {
                props.setProperty(
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        CLIENT_ID_PREFIX + "-consumer-" + subtaskId);
            } else {
                props.setProperty(
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        this.kafkaSourceConfig.getProperties().get("client.id").toString()
                                + "-"
                                + subtaskId);
            }
            props.setProperty(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());
            props.setProperty(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());
            props.setProperty(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    String.valueOf(kafkaSourceConfig.isCommitOnCheckpoint()));

            // Disable auto create topics feature
            props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
            return new KafkaConsumer<>(props);
        }
    }

    private <V> V retryOnWakeup(Supplier<V> consumerCall, String description) {
        try {
            return consumerCall.get();
        } catch (WakeupException we) {
            LOG.info(
                    "Caught WakeupException while executing Kafka consumer call for {}. Will retry the consumer call.",
                    description);
            return consumerCall.get();
        }
    }

    private static class KafkaPartitionSplitRecords
            implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {

        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<TopicPartition, Long> stoppingOffsets = new HashMap<>();
        private final ConsumerRecords<byte[], byte[]> consumerRecords;
        private final Iterator<TopicPartition> splitIterator;
        private Iterator<ConsumerRecord<byte[], byte[]>> recordIterator;
        private TopicPartition currentTopicPartition;
        private Long currentSplitStoppingOffset;

        private KafkaPartitionSplitRecords(ConsumerRecords<byte[], byte[]> consumerRecords) {
            this.consumerRecords = consumerRecords;
            this.splitIterator = consumerRecords.partitions().iterator();
        }

        private void setPartitionStoppingOffset(
                TopicPartition topicPartition, long stoppingOffset) {
            stoppingOffsets.put(topicPartition, stoppingOffset);
        }

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        @Nullable @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                currentTopicPartition = splitIterator.next();
                recordIterator = consumerRecords.records(currentTopicPartition).iterator();
                currentSplitStoppingOffset =
                        stoppingOffsets.getOrDefault(currentTopicPartition, Long.MAX_VALUE);
                return currentTopicPartition.toString();
            } else {
                currentTopicPartition = null;
                recordIterator = null;
                currentSplitStoppingOffset = null;
                return null;
            }
        }

        @Nullable @Override
        public ConsumerRecord<byte[], byte[]> nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentTopicPartition,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                final ConsumerRecord<byte[], byte[]> record = recordIterator.next();
                // Only emit records before stopping offset
                if (record.offset() < currentSplitStoppingOffset) {
                    return record;
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
