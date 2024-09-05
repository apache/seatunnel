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

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.fetch.KafkaSourceFetcherManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                ConsumerRecord<byte[], byte[]>,
                SeaTunnelRow,
                KafkaSourceSplit,
                KafkaSourceSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceReader.class);
    private final SourceReader.Context context;

    private final KafkaSourceConfig kafkaSourceConfig;
    private final SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> checkpointOffsetMap;

    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsetsOfFinishedSplits;

    KafkaSourceReader(
            BlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue,
            SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>
                    splitFetcherManager,
            RecordEmitter<ConsumerRecord<byte[], byte[]>, SeaTunnelRow, KafkaSourceSplitState>
                    recordEmitter,
            SourceReaderOptions options,
            KafkaSourceConfig kafkaSourceConfig,
            Context context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, options, context);
        this.kafkaSourceConfig = kafkaSourceConfig;
        this.context = context;
        this.checkpointOffsetMap = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
    }

    @Override
    protected void onSplitFinished(Map<String, KafkaSourceSplitState> finishedSplitIds) {
        finishedSplitIds.forEach(
                (ignored, splitState) -> {
                    if (splitState.getCurrentOffset() > 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getTopicPartition(),
                                new OffsetAndMetadata(splitState.getCurrentOffset()));
                    } else if (splitState.getEndOffset() > 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getTopicPartition(),
                                new OffsetAndMetadata(splitState.getEndOffset()));
                    }
                });
    }

    @Override
    protected KafkaSourceSplitState initializedState(KafkaSourceSplit split) {
        return new KafkaSourceSplitState(split);
    }

    @Override
    protected KafkaSourceSplit toSplitType(String splitId, KafkaSourceSplitState splitState) {
        return splitState.toKafkaSourceSplit();
    }

    @Override
    public List<KafkaSourceSplit> snapshotState(long checkpointId) {
        List<KafkaSourceSplit> sourceSplits = super.snapshotState(checkpointId);
        if (!kafkaSourceConfig.isCommitOnCheckpoint()) {
            return sourceSplits;
        }
        if (sourceSplits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
            logger.debug(
                    "checkpoint {} does not have an offset to submit for splits", checkpointId);
            checkpointOffsetMap.put(checkpointId, Collections.emptyMap());
        } else {
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
                    checkpointOffsetMap.computeIfAbsent(checkpointId, id -> new HashMap<>());
            for (KafkaSourceSplit kafkaSourceSplit : sourceSplits) {
                if (kafkaSourceSplit.getStartOffset() >= 0) {
                    offsetAndMetadataMap.put(
                            kafkaSourceSplit.getTopicPartition(),
                            new OffsetAndMetadata(kafkaSourceSplit.getStartOffset()));
                }
            }
            offsetAndMetadataMap.putAll(offsetsOfFinishedSplits);
        }
        return sourceSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        logger.debug("Committing offsets for checkpoint {}", checkpointId);
        if (!kafkaSourceConfig.isCommitOnCheckpoint()) {
            logger.debug("Submitting offsets after snapshot completion is prohibited");
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> committedPartitions =
                checkpointOffsetMap.get(checkpointId);

        if (committedPartitions == null) {
            logger.debug("Offsets for checkpoint {} have already been committed.", checkpointId);
            return;
        }

        if (committedPartitions.isEmpty()) {
            logger.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            return;
        }

        ((KafkaSourceFetcherManager) splitFetcherManager)
                .commitOffsets(
                        committedPartitions,
                        (ignored, e) -> {
                            if (e != null) {
                                logger.warn(
                                        "Failed to commit consumer offsets for checkpoint {}",
                                        checkpointId,
                                        e);
                                return;
                            }
                            offsetsOfFinishedSplits
                                    .keySet()
                                    .removeIf(committedPartitions::containsKey);
                            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                        });
    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!checkpointOffsetMap.isEmpty() && checkpointOffsetMap.firstKey() <= checkpointId) {
            checkpointOffsetMap.remove(checkpointOffsetMap.firstKey());
        }
    }
}
